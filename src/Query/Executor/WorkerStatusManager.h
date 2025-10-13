#pragma once

#include <Common/Logger.h>
#include <chrono>
#include <memory>
#include <mutex>
#include <Query/ProtosHelper/HostWithPorts.h>
#include <Interpreters/Cluster.h>

namespace DB
{

enum class ScheduleType : uint8_t
{
    Health = 1,
    Unknown = 2,
    Unhealth = 5,
};


enum class WorkerCircuitBreakerStatus : uint8_t
{
    Close = 1,
    HalfOpen = 2,
    Open = 3
};

struct WorkerCircuitBreaker
{
    int64_t fail_count{0};
    WorkerCircuitBreakerStatus breaker_status{WorkerCircuitBreakerStatus::Close};
    bool is_checking{false};
    std::chrono::system_clock::time_point open_time;
    String toDebugString() const
    {
        return fmt::format("breaker_status {} fail_count {} is_checking {}", breaker_status, fail_count, is_checking);
    }
};

struct ResourceStatus
{
    ResourceStatus() = default;
    explicit ResourceStatus(const Protos::WorkerNodeResourceData & info, size_t recommended_concurrent_query_limit, double health_worker_cpu_usage_threshold)
    {
        if (info.query_num() > recommended_concurrent_query_limit || info.cpu_usage_1min() > 100 * health_worker_cpu_usage_threshold)
            scheduler_status = ScheduleType::Unhealth;
        else
            scheduler_status = ScheduleType::Health;

        last_status_create_time = info.last_status_create_time();
        //scheduler score, less is better
        scheduler_score = 100.0 * info.query_num() / recommended_concurrent_query_limit + info.cpu_usage_1min();
        register_time = info.register_time();
        host_ports = HostWithPorts::createHostWithPorts(info.host_ports());
    }

    bool compare(const ResourceStatus & rhs) const
    {
        if (scheduler_status == rhs.scheduler_status)
            return scheduler_score < rhs.scheduler_score;
        else
            return scheduler_status < rhs.scheduler_status;
    }

    ScheduleType getStatus() const { return scheduler_status; }
    String toDebugString() const;

    ScheduleType scheduler_status{ScheduleType::Unknown};
    double scheduler_score{0};
    UInt64 last_status_create_time{0};
    UInt32 register_time{0};
    HostWithPorts host_ports;
};

template <typename KEY, typename VALUE, typename HASH = std::hash<KEY>, typename EQUAL = std::equal_to<KEY>, uint32_t MAP_COUNT = 23>
class ThreadSafeMap
{
    static_assert(MAP_COUNT > 0, "Invalid MAP_COUNT parameters.");

public:
    void set(const KEY & key, const VALUE & value)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(mutex[idx]);
        map[idx][key] = value;
    }

    std::optional<VALUE> get(const KEY & key)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(mutex[idx]);
        if (map[idx].count(key) == 0)
            return std::nullopt;
        return map[idx][key];
    }

    template <class... Args>
    void updateEmplaceIfNotExist(const KEY & key, const std::function<void(VALUE & value)> & call, Args &&... args)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(mutex[idx]);
        auto iter = map[idx].find(key);
        if (iter != map[idx].end())
            call(iter->second);
        else
            map[idx].emplace(std::make_pair(key, VALUE(args...)));
    }

    template <class Func>
    void update(const KEY & key, Func && call)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(mutex[idx]);
        auto iter = map[idx].find(key);
        if (iter != map[idx].end())
            call(iter->second);
    }

private:
    uint32_t mapIdx(const KEY & key) { return HASH{}(key) % MAP_COUNT; }

    std::unordered_map<KEY, VALUE, HASH, EQUAL> map[MAP_COUNT];
    bthread::Mutex mutex[MAP_COUNT];
};

struct WorkerStatus
{
    WorkerStatus() = default;
    WorkerStatus(ResourceStatus status, TimePoint time) : resource_status(status), server_last_update_time(time)
    {
    }
    ResourceStatus resource_status;
    TimePoint server_last_update_time;
    WorkerCircuitBreaker circuit_break;
    String toDebugString() const { return resource_status.toDebugString() + "\t" + circuit_break.toDebugString(); }
};

class WorkerStatusManager : public std::enable_shared_from_this<WorkerStatusManager>, WithContext
{
public:
    friend class VirtualWarehouseHandleImpl;
    enum class UpdateSource : uint8_t
    {
        ComeFromCoordinator = 1,
        ComeFromWorker = 2,
    };

    using WorkerVec = std::vector<WorkerID>;
    using WorkerVecPtr = std::shared_ptr<WorkerVec>;

    virtual ~WorkerStatusManager() = default;
    WorkerStatusManager() = default;
    explicit WorkerStatusManager(const ContextPtr global_context_) : WithContext(global_context_), log(getLogger("WorkerStatusManager")) { }

    void updateWorkerNode(const Protos::WorkerNodeResourceData & resource_info, UpdateSource source);

    std::vector<std::vector<Cluster::Address>> selectHealthNode(const std::vector<std::vector<Cluster::Address>> & shards_addresses);

    void setWorkerNodeDead(const WorkerID & key, int error_code);

    void restoreWorkerNode(const WorkerID & key);

    virtual std::optional<WorkerStatus> getWorkerStatus(const WorkerID & worker_id)
    {
        return worker_status_map.get(worker_id);
    }

    static WorkerID getWorkerID(const Protos::WorkerNodeResourceData & resource_info)
    {
        return WorkerID{resource_info.host_ports().host(), static_cast<uint16_t>(resource_info.host_ports().rpc_port())};
    }

private:
    ThreadSafeMap<WorkerID, WorkerStatus, WorkerIDHash> worker_status_map;

    std::atomic<size_t> recommended_concurrent_query_limit{480};
    std::atomic<double> health_worker_cpu_usage_threshold{0.95};
    std::atomic<int64_t> circuit_breaker_open_to_halfopen_wait_seconds{60};
    std::atomic<int64_t> unhealth_worker_recheck_wait_seconds{10};
    std::atomic<int64_t> circuit_breaker_open_error_threshold{10};
    mutable bthread::Mutex map_mutex;
    LoggerPtr log;
};


}
