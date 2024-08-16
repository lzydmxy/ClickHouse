#include <atomic>
#include <cassert>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <math.h>

#include <Poco/File.h>
#include <Poco/DateTimeFormatter.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>

#include <Storages/Consensus/Settings.h>
#include <Storages/Consensus/RaftLogStorage.h>
#include <Storages/Consensus/RaftStorage.h>
#include <Storages/Consensus/RaftStateMachine.h>
#include <Storages/Consensus/RaftServer.h>

using namespace nuraft;

namespace DB
{
namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}
}
namespace Consensus
{

RaftStateMachine::RaftStateMachine(
    const SettingsPtr & settings_,
    const RaftSettingsPtr & raft_settings_,
    RaftResponseQueue & responses_queue_,
    NuLogStorePtr log_store_)
    : raft_storage(settings_)
    , log_store(log_store_)
    , settings(settings_)
    , raft_settings(raft_settings_)
    , responses_queue(responses_queue_)
    , log(&(Poco::Logger::get("RaftStateMachine")))
{
    LOG_DEBUG(log, "Construct, log storage last index {}", log_store_->last_durable_index());
}

void RaftStateMachine::replayLog()
{
    if (!log_store)
    {
        LOG_WARNING(log, "Raft log storage is null");
        return;
    }

    if (!raft_server)
    {
        LOG_WARNING(log, "Raft server is null");
        return;
    }

    auto last_append_index = log_store->last_durable_index();
    auto last_committed_index = raft_server->getStateManager()->getCommittedIndex();

    if (last_append_index == last_committed_index)
    {
        LOG_INFO(log, "Last committed log is newest, no need replay, committed index {}", last_committed_index);
    }
    else if (last_committed_index > 0 && last_append_index > last_committed_index)
    {
        LOG_INFO(log, "Uncommitted raft log data, last append index {}, last committed index {}",
            last_append_index, last_committed_index);
    }
    else
    {
        LOG_WARNING(log, "Maybe lost raft log data or committed index not store to disk, last append index {}, last committed index {}",
            last_append_index, last_committed_index);
    }

    last_committed_idx.store(last_committed_index);
    auto config = raft_server->getStateManager()->load_config();
    config->set_log_idx(last_committed_index);
    raft_server->getStateManager()->save_config((*config.get()));

    auto saved_snapshot = raft_server->getStateManager()->loadSnapshotState();

    if (saved_snapshot != nullptr)
        last_snapshot_ptr = raft_server->getStateManager()->mergeSnapshotState(*saved_snapshot, last_committed_index);

    last_snapshot_time = Poco::DateTime();

    LOG_INFO(log, "Replay last committed index {} in log store", last_committed_idx);
}

void RaftStateMachine::setServer(DB::RaftServer * server)
{
    raft_server = server;
}

NuBufferPtr RaftStateMachine::pre_commit(const ulong log_idx, NuBuffer & data)
{
    // Nothing to do with pre-commit in this case.
    LOG_TRACE(log, "pre commit, log indx {}, data size {}", log_idx, data.size());
    return nullptr;
}

NuBufferPtr RaftStateMachine::commit(const ulong log_idx, NuBuffer & data)
{
    LOG_DEBUG(log, "Commit log index {}", log_idx);
    return commit(log_idx, data, true);
}

NuBufferPtr RaftStateMachine::commit(const ulong log_idx, NuBuffer & data, bool return_response)
{
    auto request = RaftRequest::read(data);

    LOG_DEBUG(log, "Commit log index {}, request {}", log_idx, request->toString());
    if (request->create_time > 0)
    {
        Int64 elapsed = Poco::Timestamp().epochMicroseconds() / 1000 - request->create_time;
        if (elapsed > 1000)
            LOG_WARNING(log, "Commit log {} request process time {} ms, req type {}",
                log_idx, elapsed, Consensus::toString(request->getOpNum()));
    }

    auto response = raft_storage.processRequest(request);
    if (response->error == Error::ZOK)
    {
        last_committed_idx.store(log_idx);
        raft_server->getStateManager()->saveCommittedIndex(log_idx);
    }

    if (!raft_server->isLeader())
        return nullptr;

    //Only leader commit need push response to queue!
    if (return_response)
    {
        LOG_DEBUG(log, "Return response, last commit index {}, id {}", last_committed_idx, toString(request->id));
        return response->write();
    }
    else
    {
        LOG_DEBUG(log, "Leader push response to queue, last commit index {}, id {}", last_committed_idx, toString(request->id));
        if (!responses_queue.push(response))
        {
            throw Exception(DB::ErrorCodes::SYSTEM_ERROR,
                "Could not push error response id {} to responses queue",
                response->id);
        }
        return nullptr;
    }
}

void RaftStateMachine::rollback(const ulong log_idx, NuBuffer & data)
{
    // Nothing to do with rollback,
    // as this example doesn't do anything on pre-commit.
    LOG_TRACE(log, "Rollback, log indx {}, data size {}", log_idx, data.size());
}

bool RaftStateMachine::chk_create_snapshot()
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    auto delta_seconds = (Poco::DateTime()-last_snapshot_time).seconds();
    auto is_create = delta_seconds >= settings->snapshot_interval_seconds;

    LOG_TRACE(log, "Can be created snapshot {}, delta seconds {}, last committed index {}",
        is_create, delta_seconds, last_committed_idx);

    return is_create;
}

void RaftStateMachine::create_snapshot(NuSnapshot & snapshot, async_result<bool>::handler_type & when_done)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    NuBufferPtr snp_buf = snapshot.serialize();
    last_snapshot_ptr = snapshot::deserialize(*snp_buf);

    last_snapshot_time = Poco::DateTime();
    //save snapshot state
    raft_server->getStateManager()->saveSnapshotState(snapshot);
    //remove old log segment file
    log_store->compact(snapshot.get_last_log_idx());

    ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);

    LOG_DEBUG(log, "Created snapshot, time {}", Poco::DateTimeFormatter::format(last_snapshot_time, std::string("%Y%m%d%H%M%S")));
}

NuSnapshotPtr RaftStateMachine::last_snapshot()
{
    //std::lock_guard<std::mutex> lock(snapshot_mutex);
    if (last_snapshot_ptr == nullptr)
        last_snapshot_ptr = raft_server->getStateManager()->loadSnapshotState();

    if (last_snapshot_ptr != nullptr)
        LOG_DEBUG(log, "Get last snapshot, last committed index {}, last snapshot index {}",
            last_committed_idx,
            last_snapshot_ptr->get_last_log_idx());
    else
        LOG_DEBUG(log, "Get last snapshot is null");

    return last_snapshot_ptr;
}

int RaftStateMachine::read_logical_snp_obj(NuSnapshot & snapshot, void *& user_snp_ctx, ulong obj_id, NuBufferPtr & data, bool & is_last_obj)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);

    // last snapshot doesn't exist.
    if (!last_snapshot_ptr)
    {
        data = nullptr;
        is_last_obj = true;
        LOG_INFO(log, "Cant find snapshot by last_log_idx {}, object id {}", snapshot.get_last_log_idx(), obj_id);
        return 0;
    }

    if (obj_id == 0)
    {
        // Object ID == 0: first object, put dummy data.
        data = buffer::alloc(sizeof(UInt32));
        buffer_serializer bs(data);
        bs.put_i32(0);
        is_last_obj = false;
        raft_storage.loadSnapshot(snapshot);
        LOG_INFO(log, "Read snapshot object, last_log_idx {}, object id {}, is_last {}", snapshot.get_last_log_idx(), obj_id, false);
        return 0;
    }

    auto segment = raft_storage.nextSegment(snapshot, obj_id);
    is_last_obj = segment->is_last;
    LOG_INFO(log, "Read snapshot object, last_log_idx {}, object id {}, is_last {}", snapshot.get_last_log_idx(), obj_id, is_last_obj);
    data = segment->serialize();
    user_snp_ctx = nullptr;
    return 0;
}

void RaftStateMachine::save_logical_snp_obj(NuSnapshot & snapshot, ulong & obj_id, NuBuffer & data, bool is_first_obj, bool is_last_obj)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    if (obj_id == 0)
    {
        // Object ID == 0: it contains dummy value, create snapshot context.
        raft_storage.beginSaveSnapshot(snapshot);
    }
    else
    {
        // Object ID > 0: actual snapshot value, save to local disk
        auto segment = std::make_shared<SnapshotSegment>();
        segment->deserialize(data);
        raft_storage.saveSnapshotSegment(snapshot, obj_id, segment);
    }
    LOG_INFO(log, "Save logical snapshot , object id {}, is_first_obj {}, is_last_obj {}", obj_id, is_first_obj, is_last_obj);
    // Request next object.
    obj_id++;
}

bool RaftStateMachine::apply_snapshot(NuSnapshot & snapshot)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    LOG_INFO(log, "Apply snapshot term {}, last log index {}, size {}",
        snapshot.get_last_log_term(), snapshot.get_last_log_idx(), snapshot.size());
    raft_storage.applySnapshot(snapshot);
    auto snp_buf = snapshot.serialize();
    last_snapshot_ptr = snapshot::deserialize(*snp_buf);
    last_committed_idx = last_snapshot_ptr->get_last_log_idx();
    return true;
}

void RaftStateMachine::free_user_snp_ctx(void *& user_snp_ctx)
{
    // In this example, `read_logical_snp_obj` doesn't create
    // `user_snp_ctx`. Nothing to do in this function.
    if (user_snp_ctx != nullptr)
    {
        free(user_snp_ctx);
        user_snp_ctx = nullptr;
    }
}

void RaftStateMachine::shutdown()
{
    LOG_INFO(log, "Shutting down raft state machine");
    if (shutdown_called)
        return;
    shutdown_called = true;
    raft_storage.finalize();
    LOG_INFO(log, "Shut down raft state machine");
}

}
