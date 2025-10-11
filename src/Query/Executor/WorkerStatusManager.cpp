#include <atomic>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Interpreters/Context.h>
#include <Query/Executor/SegmentScheduler.h>
#include <Query/Executor/WorkerStatusManager.h>
#include <Query/Exchange/RpcChannelPool.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Logger.h>

namespace ProfileEvents
{
extern const Event AllWorkerSize;
extern const Event HealthWorkerSize;
extern const Event UnhealthWorkerSize;
extern const Event OpenWorkerSize;
extern const Event UnknownWorkerSize;
extern const Event HalfSelfCheckWorkerSize;
extern const Event HalfOtherCheckWorkerSize;
}

namespace DB
{
String ResourceStatus::toDebugString() const
{
    return fmt::format("status {} score {} update_time {}", scheduler_status, scheduler_score, last_status_create_time);
}

std::vector<std::vector<Cluster::Address>> WorkerStatusManager::selectHealthNode(const std::vector<std::vector<Cluster::Address>> & shards_addresses)
{
    std::vector<std::vector<Cluster::Address>> health_nodes;
    size_t shard_index = 0;
    for (const auto & shard : shards_addresses)
    {
        health_nodes.emplace_back(std::vector<Cluster::Address>());
        for (const auto & address : shard)
        {
            auto worker_id = WorkerID(address.host_name, address.rpc_port);
            bool exist = false;
            bool health = false;
            // auto now = std::chrono::system_clock::now();

            worker_status_map.update(worker_id, [&](WorkerStatus & val) {
                exist = true;
                if (likely(val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Close))
                {
                    health = true;
                }
                else if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
                {
                    if (val.circuit_break.is_checking)
                    {
                        LOG_DEBUG(log, "half open worker {} is checking", worker_id.toString());
                    }
                    else
                    {
                        LOG_DEBUG(log, "check half open worker {}", worker_id.toString());
                        val.circuit_break.is_checking = true;
                        health = true;
                    }
                }
                // else if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open)
                // {
                //     if (std::chrono::duration_cast<std::chrono::seconds>(now - val.circuit_break.open_time).count() > circuit_breaker_open_to_halfopen_wait_seconds)
                //     {
                //         LOG_TRACE(log, "worker: {} is timeout, set circuit breaker to half open.", worker_id.toString());
                //         val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::HalfOpen;
                //         val.circuit_break.fail_count = 0;
                //     }
                // }
            });

            if (health)
            {
                health_nodes.back().emplace_back(address);
            }

            if (!exist)
            {
                LOG_INFO(log, "Worker: {} first time to see, we were optimistic that it was healthy", worker_id.toString());
                health_nodes.back().emplace_back(address);
            }
        }
        if (health_nodes.back().empty())
        {
            LOG_WARNING(log, "Can't choose health nodes for shard {}", shard_index);
            health_nodes.back() = shard;
        }
    }

    return health_nodes;
}

void WorkerStatusManager::updateWorkerNode(const Protos::WorkerNodeResourceData & resource_info, UpdateSource source)
{
    auto id = getWorkerID(resource_info);
    auto now = std::chrono::system_clock::now();
    if (source == UpdateSource::ComeFromCoordinator)
    {
        worker_status_map.update(id, [&](WorkerStatus & val) {
            val.server_last_update_time = now;

            if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open
                && std::chrono::duration_cast<std::chrono::seconds>(now - val.circuit_break.open_time).count() > circuit_breaker_open_to_halfopen_wait_seconds)
            {
                LOG_DEBUG(log, "worker: {} is back, set circuit breaker to half open.", id.toString());
                val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::HalfOpen;
                val.circuit_break.fail_count = 0;
            }
            if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen && source == UpdateSource::ComeFromWorker)
            {
                LOG_DEBUG(log, "worker: {} is back, close circuit breaker.", id.toString());
                val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::Close;
                val.circuit_break.fail_count = 0;
                val.circuit_break.is_checking = false;
            }
        });

        return;
    }

    ResourceStatus resource_status(
        resource_info, recommended_concurrent_query_limit.load(std::memory_order_relaxed), health_worker_cpu_usage_threshold.load(std::memory_order_relaxed));

    LOG_TRACE(log, "update worker id {} : {}", id.toString(), resource_info.ShortDebugString());
    worker_status_map.updateEmplaceIfNotExist(
        id,
        [id, this, &now, &resource_status, source](WorkerStatus & val) {
            // Worker has restarted. We must put it ahead of status update.
            if (resource_status.register_time > val.resource_status.register_time)
            {
                auto context_ptr = context.lock();
                if (context_ptr)
                {
                    RpcChannelPool::getInstance().getClient(
                        resource_status.host_ports.getRPCAddress(), BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, /*refresh=*/true);
                    context_ptr->getOptimizerContext()->getSegmentScheduler()->workerRestarted(id, resource_status.host_ports, resource_status.register_time);
                }

            }
            val.server_last_update_time = now;
            if (val.resource_status.last_status_create_time < resource_status.last_status_create_time)
                val.resource_status = resource_status;

            if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open
                && std::chrono::duration_cast<std::chrono::seconds>(now - val.circuit_break.open_time).count() > circuit_breaker_open_to_halfopen_wait_seconds)
            {
                LOG_DEBUG(log, "worker: {} is back, set circuit breaker to half open.", id.toString());
                val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::HalfOpen;
                val.circuit_break.fail_count = 0;
            }
            if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen && source == UpdateSource::ComeFromWorker)
            {
                LOG_DEBUG(log, "worker: {} is back, close circuit breaker.", id.toString());
                val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::Close;
                val.circuit_break.fail_count = 0;
                val.circuit_break.is_checking = false;
            }
        },
        resource_status,
        now);
}

void WorkerStatusManager::setWorkerNodeDead(const WorkerID & key, int error_code)
{
    LOG_TRACE(log, "set worker: {} dead", key.toString());
    auto now = std::chrono::system_clock::now();
    worker_status_map.update(key, [&key, this, error_code, &now](WorkerStatus & val) {
        if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open)
        {
            LOG_TRACE(log, "worker: {}'s circuit break is open, wait RM to restart this worker.", key.toString());
            return;
        }
        size_t error_weight = 1;
        switch (error_code)
        {
            case EHOSTDOWN:
                error_weight = circuit_breaker_open_error_threshold.load(std::memory_order_relaxed) + 1;
                break;
            case ETIMEDOUT:
            case ECONNREFUSED:
                error_weight = 1;
                break;
            default:
                break;
        }
        val.circuit_break.fail_count += error_weight;
        if (val.circuit_break.fail_count > circuit_breaker_open_error_threshold.load(std::memory_order_relaxed)
            || val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
        {
            LOG_TRACE(log, "worker: {}'s fail_count {} open circuit break.", key.toString(), val.circuit_break.fail_count);
            val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::Open;
            val.circuit_break.fail_count = 0;
            val.circuit_break.is_checking = false;
            val.circuit_break.open_time = now;
            LOG_TRACE(log, "add unhealth worker {}", key.toString());
        }
    });
}

void WorkerStatusManager::restoreWorkerNode(const WorkerID & key)
{
    worker_status_map.update(key, [&](WorkerStatus & val) {
        if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
        {
            val.circuit_break.is_checking = false;
        }
    });
}


}
