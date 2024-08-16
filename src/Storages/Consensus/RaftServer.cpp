#include <chrono>
#include <string>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <boost/algorithm/string.hpp>
#include <libnuraft/async.hxx>
#include <Common/ConcurrentBoundedQueue.h>
#include <Poco/NumberFormatter.h>
#include <Storages/Consensus/RaftServer.h>
#include <Storages/Consensus/RaftStateMachine.h>
#include <Storages/Consensus/RaftStateManager.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int RAFT_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
}

using Poco::NumberFormatter;

RaftServer::RaftServer(
    const SettingsPtr & settings_,
    const Poco::Util::AbstractConfiguration & config_,
    RaftResponseQueue & responses_queue_)
    : server_id(settings_->server_id)
    , settings(settings_)
    , config(config_)
    , responses_queue(responses_queue_)
    , log(&(Poco::Logger::get("RaftServer")))
{
    state_manager = RaftNew<RaftStateManager>(server_id, config, settings_);

    state_machine = RaftNew<RaftStateMachine>(
        settings,
        settings->raft_settings,
        responses_queue_,
        state_manager->load_log_store());
}

nuraft::cb_func::ReturnCode RaftServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * /* param */)
{
    /*
    * StateMachineExecution = 19,
    * InitialBatchCommited = 20,
    * ProcessReq = 1,
    * PreAppendLog = 21,
    * AppendLogs = 3,
    */
    switch(type)
    {
        case nuraft::cb_func::Type::BecomeFresh:
        case nuraft::cb_func::Type::BecomeLeader:
        {
            std::unique_lock lock(initialized_mutex);
            initialized_flag = true;
            initialized_cv.notify_all();
            break;
        }
        default:
            //LOG_INFO(log, "Nuraft callbackFunc type {}", static_cast<int>(type));
            break;
    }
    return nuraft::cb_func::ReturnCode::Ok;
}

void RaftServer::startup()
{
    auto raft_settings = settings->raft_settings;
    nuraft::raft_params params;
    params.heart_beat_interval_ = raft_settings->heart_beat_interval_ms;
    params.election_timeout_lower_bound_ = raft_settings->election_timeout_lower_bound_ms;
    params.election_timeout_upper_bound_ = raft_settings->election_timeout_upper_bound_ms;
    params.reserved_log_items_ = raft_settings->reserved_log_items;
    params.snapshot_distance_ = raft_settings->snapshot_distance;
    params.client_req_timeout_ = raft_settings->operation_timeout_ms;
    params.return_method_ = nuraft::raft_params::blocking;
    params.locking_method_type_ = nuraft::raft_params::dual_mutex;
    params.parallel_log_appending_ = raft_settings->log_fsync_mode == FsyncMode::FSYNC_PARALLEL;
    params.auto_forwarding_ = true;

    LOG_INFO(log, "Raft params, heart_beat_interval_ms={}, election_timeout_lower_bound_ms={}, election_timeout_upper_bound_ms={}, operation_timeout_ms={}, reserved_log_items={}, snapshot_distance={}, parallel_log_appending={}, auto_forwarding={}",
        params.heart_beat_interval_, params.election_timeout_lower_bound_, params.election_timeout_upper_bound_, params.client_req_timeout_,
        params.reserved_log_items_, params.snapshot_distance_,
        params.parallel_log_appending_, params.auto_forwarding_);

    nuraft::asio_service::options asio_opts{};
    asio_opts.thread_pool_size_ = raft_settings->nuraft_thread_size;
    nuraft::raft_server::init_options init_options;
    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.raft_callback_ = [this](nuraft::cb_func::Type type, nuraft::cb_func::Param * param) { return callbackFunc(type, param); };

    LOG_DEBUG(log, "Init state manager and state machine");
    dynamic_cast<RaftLogStorage &>(*state_manager->load_log_store()).setServer(this);
    state_machine->setServer(this);
    state_machine->replayLog();

    LOG_DEBUG(log, "Launcher init port {}", settings->internal_port);
    raft_instance = launcher.init(
        state_machine,
        state_manager,
        RaftNew<LoggerWrapper>("NuRaft", raft_settings->raft_logs_level),
        settings->internal_port,
        asio_opts,
        params,
        init_options);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");
}

RaftConnectionPtr RaftServer::getLeaderClient(RunnerID thread_idx)
{
    return state_manager->getClient(raft_instance->get_leader(), thread_idx);
}

int32 RaftServer::getLeader()
{
    return raft_instance->get_leader();
}

void RaftServer::shutdown()
{
    LOG_INFO(log, "Shutting down raft server");
    state_machine->shutdown();
    if (state_manager->load_log_store() && !state_manager->load_log_store()->flush())
        LOG_WARNING(log, "Log store flush error while server shutdown.");

    dynamic_cast<RaftLogStorage &>(*state_manager->load_log_store()).shutdown();

    if (!launcher.shutdown(settings->raft_settings->shutdown_timeout))
        LOG_WARNING(log, "Failed to shutdown RAFT server in {} seconds", 5);
    LOG_INFO(log, "Shut down raft server");
}

void RaftServer::putRequest(const RaftRequestPtr & request)
{
    LOG_DEBUG(log, "[putRequest]ID {}, Opnum {}", toString(request->id), Consensus::toString(request->getOpNum()));

    NuBuffers entries;
    entries.push_back(request->write());
    CMDResultPtr result = raft_instance->append_entries(entries);

    auto response = request->makeResponse();
    response->error = Consensus::convertToError(result->get_result_code());
    response->message = std::string(result->get_result_str());
    if (result->get_accepted() && result->get_result_code() == nuraft::cmd_result_code::OK)
    {
        if (result->has_result() && result->get() != nullptr)
            response->read(*(result->get()));
        else
            LOG_INFO(log, "Nuraft response buffer is null, accepted {}, code {}, message {}",
                result->get_accepted(),
                result->get_result_code(),
                result->get_result_str());
    }
    else
    {
        LOG_WARNING(log, "Nuraft error, accepted {}, error code {}, message {}",
            result->get_accepted(),
            result->get_result_code(),
            result->get_result_str());
    }
    if (!responses_queue.push(response))
    {
        throw Exception(ErrorCodes::SYSTEM_ERROR,
            "Could not push error response id {} to responses queue",
            response->id);
    }
}

CMDResultPtr RaftServer::putRequestBatch(const RaftRequests & request_batch)
{
    LOG_DEBUG(log, "Process the batch requests {}", request_batch.size());
    NuBuffers entries;
    for (const auto & request : request_batch)
    {
        LOG_TRACE(log, "Push request to entries opnum {}", request->getOpNum());
        entries.push_back(request->write());
    }
    /// append_entries write request
    CMDResultPtr result = raft_instance->append_entries(entries);
    return result;
}

bool RaftServer::isLeader() const
{
    return raft_instance->is_leader();
}


bool RaftServer::isObserver() const
{
    auto cluster_config = state_manager->get_cluster_config();
    return cluster_config->get_server(server_id)->is_learner();
}

bool RaftServer::isFollower() const
{
    return !isLeader() && !isObserver();
}

bool RaftServer::isLeaderAlive() const
{
    /// nuraft leader_ and role_ not sync
    return raft_instance->is_leader_alive() && raft_instance->get_leader() != -1;
}

uint64_t RaftServer::getFollowerCount() const
{
    return raft_instance->get_peer_info_all().size();
}

uint64_t RaftServer::getSyncedFollowerCount() const
{
    uint64_t last_log_idx = raft_instance->get_last_log_idx();
    const auto followers = raft_instance->get_peer_info_all();

    uint64_t stale_followers = 0;

    const uint64_t stale_follower_gap = raft_instance->get_current_params().stale_log_gap_;
    for (const auto & fl : followers)
    {
        if (last_log_idx > fl.last_log_idx_ + stale_follower_gap)
            stale_followers++;
    }
    return followers.size() - stale_followers;
}

void RaftServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);
    int64_t timeout = settings->raft_settings->startup_timeout;
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag.load(); }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}


ConfigUpdateActions RaftServer::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config_)
{
    return state_manager->getConfigurationDiff(config_);
}

bool RaftServer::applyConfigurationUpdate(const ConfigUpdateAction & task)
{
    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to add server with id {}", task.server->get_id());
        bool added = false;
        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) != nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully added", task.server->get_id());
                added = true;
                break;
            }

            if (!isLeader())
            {
                LOG_INFO(log, "We are not leader anymore, will not try to add server {}", task.server->get_id());
                break;
            }

            auto result = raft_instance->add_srv(*task.server);
            if (!result->get_accepted())
                LOG_INFO(
                    log,
                    "Command to add server {} was not accepted for the {} time, will sleep for {} ms and retry",
                    task.server->get_id(),
                    i + 1,
                    sleep_ms * (i + 1));

            LOG_DEBUG(log, "Wait for apply action AddServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!added)
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Configuration change to add server (id {}) was not accepted by RAFT after all {} retries",
                task.server->get_id(),
                settings->raft_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to remove server with id {}", task.server->get_id());

        bool removed = false;
        if (task.server->get_id() == state_manager->server_id())
        {
            LOG_INFO(
                log,
                "Trying to remove leader node (ourself), so will yield leadership and some other node (new leader) will try remove us. "
                "Probably you will have to run SYSTEM RELOAD CONFIG on the new leader node");

            raft_instance->yield_leadership();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * 5));
            return false;
        }

        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) == nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully removed", task.server->get_id());
                removed = true;
                break;
            }

            if (!isLeader())
            {
                LOG_INFO(log, "We are not leader anymore, will not try to remove server {}", task.server->get_id());
                break;
            }

            auto result = raft_instance->remove_srv(task.server->get_id());
            if (!result->get_accepted())
                LOG_INFO(
                    log,
                    "Command to remove server {} was not accepted for the {} time, will sleep for {} ms and retry",
                    task.server->get_id(),
                    i + 1,
                    sleep_ms * (i + 1));

            LOG_DEBUG(log, "Wait for apply action RemoveServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!removed)
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Configuration change to remove server (id {}) was not accepted by RAFT after all {} retries",
                task.server->get_id(),
                settings->raft_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        raft_instance->set_priority(task.server->get_id(), task.server->get_priority());
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));

    return true;
}

bool RaftServer::waitConfigurationUpdate(const ConfigUpdateAction & task)
{
    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to wait server with id {} to be added", task.server->get_id());
        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) != nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully added by leader", task.server->get_id());
                return true;
            }

            if (isLeader())
            {
                LOG_INFO(log, "We are leader now, probably we will have to add server {}", task.server->get_id());
                return false;
            }

            LOG_DEBUG(log, "Wait for action AddServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        return false;
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to wait remove of server with id {}", task.server->get_id());

        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) == nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully removed by leader", task.server->get_id());
                return true;
            }

            if (isLeader())
            {
                LOG_INFO(log, "We are leader now, probably we will have to remove server {}", task.server->get_id());
                return false;
            }

            LOG_DEBUG(log, "Wait for action RemoveServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        return false;
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        return true;
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));
    return true;
}

RaftLogInfo RaftServer::getLogInfo()
{
    RaftLogInfo log_info;
    auto log_store = state_manager->load_log_store();
    if (log_store)
    {
        log_info.first_log_idx = log_store->start_index();
        log_info.first_log_term = log_store->term_at(log_info.first_log_idx);
    }

    if (raft_instance)
    {
        log_info.last_log_idx = raft_instance->get_last_log_idx();
        log_info.last_log_term = raft_instance->get_last_log_term();
        log_info.last_committed_log_idx = raft_instance->get_committed_log_idx();
        log_info.leader_committed_log_idx = raft_instance->get_leader_committed_log_idx();
        log_info.target_committed_log_idx = raft_instance->get_target_committed_log_idx();
        log_info.last_snapshot_idx = raft_instance->get_last_snapshot_idx();
    }

    return log_info;
}

bool RaftServer::requestLeader()
{
    return isLeader() || raft_instance->request_leadership();
}

}
