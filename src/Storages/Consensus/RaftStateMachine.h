#pragma once

#include <atomic>
#include <cassert>
#include <mutex>
#include <unordered_map>
#include <string.h>
#include <time.h>
#include <Poco/DateTime.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/Consensus/NuRaft.h>
#include <Storages/Consensus/Settings.h>
#include <Storages/Consensus/RaftStorage.h>
#include <Storages/Consensus/ThreadSafeQueue.h>

namespace DB
{
    class RaftServer;
}

namespace Consensus
{

class RequestProcessor;

class RaftStateMachine : public nuraft::state_machine
{
public:
    RaftStateMachine(
        const SettingsPtr & settings_,
        const RaftSettingsPtr & raft_settings_,
        RaftResponseQueue & responses_queue_,
        NuLogStorePtr log_store_ = nullptr);

    ~RaftStateMachine() override = default;

    void setServer(DB::RaftServer * server);

    void replayLog();

    NuBufferPtr pre_commit(const ulong log_idx, NuBuffer & data) override;
    NuBufferPtr commit(const ulong log_idx, NuBuffer & data) override;
    /// @ignore_response whether push response into queue
    /// Just for unit test
    NuBufferPtr commit(const ulong log_idx, NuBuffer & data, bool return_response);
    void rollback(const ulong log_idx, NuBuffer & data) override;

    /**
     * Decide to create snapshot or not.
     * Once the pre-defined condition is satisfied, Raft core will invoke
     * this function to ask if it needs to create a new snapshot.
     * If user-defined state machine does not want to create snapshot
     * at this time, this function will return `false`.
     *
     * @return `true` if wants to create snapshot.
     *         `false` if does not want to create snapshot.
     */
    bool chk_create_snapshot() override;

    /**
     * Create a snapshot corresponding to the given info.
     *
     * @param s Snapshot info to create.
     * @param when_done Callback function that will be called after
     *                  snapshot creation is done.
     */
    void create_snapshot(NuSnapshot & s, NuAsyncResult<bool>::handler_type & when_done) override;

    NuSnapshotPtr last_snapshot() override;

    /**
     * Read the given snapshot object.
     * This API is for snapshot sender (i.e., leader).
     *
     * Same as above, this is an optional API for users who want to
     * use logical snapshot.
     *
     * @param s Snapshot instance to read.
     * @param[in,out] user_snp_ctx
     *     User-defined instance that needs to be passed through
     *     the entire snapshot read. It can be a pointer to
     *     state machine specific iterators, or whatever.
     *     On the first `read_logical_snp_obj` call, it will be
     *     set to `null`, and this API may return a new pointer if necessary.
     *     Returned pointer will be passed to next `read_logical_snp_obj`
     *     call.
     * @param obj_id Object ID to read.
     * @param[out] data Buffer where the read object will be stored.
     * @param[out] is_last_obj Set `true` if this is the last object.
     * @return Negative number if failed.
     */
    int read_logical_snp_obj(NuSnapshot & s, void *& user_snp_ctx, ulong obj_id, NuBufferPtr & data, bool & is_last_obj) override;

    /**
     * Save the given snapshot object to local snapshot.
     * This API is for snapshot receiver (i.e., follower).
     *
     * This is an optional API for users who want to use logical
     * snapshot. Instead of splitting a snapshot into multiple
     * physical chunks, this API uses logical objects corresponding
     * to a unique object ID. Users are responsible for defining
     * what object is: it can be a key-value pair, a set of
     * key-value pairs, or whatever.
     *
     * Same as `commit()`, memory buffer is owned by caller.
     *
     * @param s Snapshot instance to save.
     * @param obj_id [in,out]
     *     Object ID.
     *     As a result of this API call, the next object ID
     *     that reciever wants to get should be set to
     *     this parameter.
     * @param data Payload of given object.
     * @param is_first_obj `true` if this is the first object.
     * @param is_last_obj `true` if this is the last object.
     */
    void save_logical_snp_obj(NuSnapshot & s, ulong & obj_id, NuBuffer & data, bool is_first_obj, bool is_last_obj) override;

    /**
     * Apply received snapshot to state machine.
     *
     * @param s Snapshot instance to apply.
     * @returm `true` on success.
     */
    bool apply_snapshot(NuSnapshot & s) override;

    void free_user_snp_ctx(void *& user_snp_ctx) override;

    ulong last_commit_index() override { return last_committed_idx; }

    RaftStorage & getStorage() { return raft_storage; }

    void shutdown();
private:
    //Storage to CDC with rocksdb/mergetree or others
    RaftStorage raft_storage;
    NuLogStorePtr log_store;
    SettingsPtr settings;
    RaftSettingsPtr raft_settings;
    DB::RaftServer * raft_server;
    RaftResponseQueue & responses_queue;
    Poco::Logger * log;
    // Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx;
    std::atomic<bool> shutdown_called{false};

    std::string snapshot_dir;
    std::mutex snapshot_mutex;
    Poco::DateTime last_snapshot_time;
    NuSnapshotPtr last_snapshot_ptr;
};

using RaftStateMachinePtr = std::shared_ptr<RaftStateMachine>;

}
