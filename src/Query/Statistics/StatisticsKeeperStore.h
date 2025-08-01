#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Cluster.h>
#include <Query/Statistics/StatisticsMemoryStore.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Interpreters/Context.h>
#include <Common/ThreadPool.h>

namespace DB::QueryStatistics
{

struct StatisticsKeeperStore : public StatisticsMemoryStore, public WithMutableContext
{
    struct HostID
    {
        String host_name;
        UInt16 port;

        HostID() = default;

        explicit HostID(const Cluster::Address & address)
            : host_name(address.host_name), port(address.port) {}

        HostID(const String & host_name_, UInt16 port_)
            : host_name(host_name_), port(port_) {}

        static HostID fromString(const String & host_port_str);

        String toString() const
        {
            return Cluster::Address::toString(host_name, port);
        }

        String readableString() const
        {
            return host_name + ":" + DB::toString(port);
        }

        bool isLocalAddress(UInt16 clickhouse_port) const;

        static String applyToString(const HostID & host_id)
        {
            return host_id.toString();
        }
    };

    struct StatisticsLogEntry
    {
        String database;
        String table;
        HostID host;

        String toString() const;
        static StatisticsLogEntry fromString(const String & data);
    };

    StatisticsKeeperStore(ContextPtr context_);


    void updateTableStatisticsOnKeeper(String database, String table, String & node_data) const;
    void dropTableStatisticsOnKeeper(String database, String table) const;

    void startup();
    void shutdown();
    virtual ~StatisticsKeeperStore();

private:
    mutable std::mutex zookeeper_mutex;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};

    Coordination::Stat queue_node_stat;
    std::string statistics_path;
    std::string statistics_data_path;
    std::string statistics_update_queue_path;
    std::atomic<bool> stop_flag = true;
    std::atomic<bool> initialized = false;
    std::unique_ptr<ThreadFromGlobalPool> update_thread;
    std::unique_ptr<ThreadFromGlobalPool> cleanup_thread;

    /// saved the last executed task
    std::optional<String> last_executed_task_name;

    size_t max_tasks_in_queue = 1000;
    Int64 cleanup_delay_period = 10 * 60; // 10 minute (in seconds)

    Int64 task_max_lifetime = 60 * 60; // hour (in seconds)

    std::shared_ptr<Poco::Event> queue_updated_event = std::make_shared<Poco::Event>();
    std::shared_ptr<Poco::Event> cleanup_event = std::make_shared<Poco::Event>();

    LoggerPtr log;
    zkutil::ZooKeeperPtr getClient() const;

    bool initialize();
    void runMainThread();
    void runCleanupThread();
    void fetchStatistics(bool reinitialized);
    bool canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat);
    void cleanupQueue();

    void fetchTableStatisticsFromKeeper(String database, String table);
};

}
