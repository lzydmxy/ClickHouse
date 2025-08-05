#include <Query/Statistics/StatisticsKeeperStore.h>

#include <filesystem>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <base/sleep.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Net/NetException.h>
#include <Common/DNSResolver.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>

namespace fs = std::filesystem;

namespace DB::QueryStatistics
{

constexpr static auto queue_prefix = "update-";

static void filterAndSortQueueNodes(Strings & all_nodes)
{
    std::erase_if(all_nodes, [] (const String & s) { return !startsWith(s, queue_prefix); });
    ::sort(all_nodes.begin(), all_nodes.end());
}

bool StatisticsKeeperStore::HostID::isLocalAddress(UInt16 clickhouse_port) const
{
    try
    {
        return DB::isLocalAddress(DNSResolver::instance().resolveAddress(host_name, port), clickhouse_port);
    }
    catch (const DB::NetException &)
    {
        /// Avoid "Host not found" exceptions
        return false;
    }
    catch (const Poco::Net::NetException &)
    {
        /// Avoid "Host not found" exceptions
        return false;
    }
}

StatisticsKeeperStore::HostID StatisticsKeeperStore::HostID::fromString(const String & host_port_str)
{
    HostID res;
    std::tie(res.host_name, res.port) = Cluster::Address::fromString(host_port_str);
    return res;
}

String StatisticsKeeperStore::StatisticsLogEntry::toString() const
{
    WriteBufferFromOwnString wb;

    wb << "host: " << HostID::applyToString(host) << "\n";
    wb << "database: " << database << "\n";
    wb << "table: " << table << "\n";

    return wb.str();
}

StatisticsKeeperStore::StatisticsLogEntry StatisticsKeeperStore::StatisticsLogEntry::fromString(const String & data)
{
    String database, table, host_id_string;
    ReadBufferFromString rb(data);

    rb >> "host: " >> host_id_string >> "\n";
    rb >> "database: " >> database >> "\n";
    rb >> "table: " >> table >> "\n";
    assertEOF(rb);

    return StatisticsLogEntry{database, table, HostID::fromString(host_id_string)};
}

zkutil::ZooKeeperPtr StatisticsKeeperStore::getClient() const
{
    std::lock_guard lock{zookeeper_mutex};
    if (!zookeeper_client || zookeeper_client->expired())
    {
        zookeeper_client = getContext()->getZooKeeper();

        zookeeper_client->sync(statistics_path);
    }

    return zookeeper_client;
}

StatisticsKeeperStore::StatisticsKeeperStore(ContextPtr context_)
    : WithMutableContext(context_->getGlobalContext()),
    log(getLogger("StatisticsKeeperStore"))
{
    std::string path_prefix = context_->getConfigRef().getString("optimizer.statistics_path", "");
    if (path_prefix.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "KeeperMap is disabled because 'keeper_map_path_prefix' config is not defined");

    statistics_path = path_prefix;
    statistics_data_path = fs::path(statistics_path)/ "data";
    statistics_update_queue_path = fs::path(statistics_path)/ "queue";
}

StatisticsKeeperStore::~StatisticsKeeperStore()
{
    shutdown();
}

bool StatisticsKeeperStore::initialize()
{
    chassert(!initialized);
    LOG_DEBUG(log, "Initializing StatisticsKeeperStore thread");

    while (!stop_flag)
    {
        try
        {
            auto zookeeper = getClient();
            zookeeper->createAncestors(fs::path(statistics_path) / "");
            zookeeper->createAncestors(fs::path(statistics_data_path) / "");
            zookeeper->createAncestors(fs::path(statistics_update_queue_path) / "");
            initialized = true;
            return true;
        }
        catch (const Coordination::Exception & e)
        {
            if (!Coordination::isHardwareError(e.code))
            {
                /// A logical error.
                LOG_ERROR(log, "ZooKeeper error: {}. Failed to start statistics keeper worker.", getCurrentExceptionMessage(true));
                chassert(false);  /// Catch such failures in tests with debug build
            }

            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot initialize statistics path.");
        }

        /// Avoid busy loop when ZooKeeper is not available.
        sleepForSeconds(5);
    }

    return false;
}

void StatisticsKeeperStore::startup()
{
    [[maybe_unused]] bool prev_stop_flag = stop_flag.exchange(false);
    chassert(prev_stop_flag);
    update_thread = std::make_unique<ThreadFromGlobalPool>(&StatisticsKeeperStore::runMainThread, this);
    cleanup_thread = std::make_unique<ThreadFromGlobalPool>(&StatisticsKeeperStore::runCleanupThread, this);
}

void StatisticsKeeperStore::runMainThread()
{
    setThreadName("StatsKS");
    LOG_DEBUG(log, "Starting Statistics Keeper Store thread");

    while (!stop_flag)
    {
        try
        {
            bool reinitialized = !initialized;

            if (!initialized)
            {
                /// Stopped
                if (!initialize())
                    break;
                LOG_DEBUG(log, "Initialized DDLWorker thread");
            }

            cleanup_event->set();
            fetchStatistics(reinitialized);

            LOG_DEBUG(log, "Waiting for queue updates");
            queue_updated_event->wait();
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                initialized = false;
                LOG_INFO(log, "Lost ZooKeeper connection, will try to connect again: {}", getCurrentExceptionMessage(true));
            }
            else
            {
                LOG_ERROR(log, "Unexpected ZooKeeper error, will try to restart main thread: {}", getCurrentExceptionMessage(true));
                initialized = false;
            }
            sleepForSeconds(1);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Unexpected error, will try to restart main thread");
            initialized = false;
            sleepForSeconds(5);
        }
    }
}

void StatisticsKeeperStore::updateTableStatisticsOnKeeper(String database, String table, String & node_data) const
{
    String stats_data_path = fs::path(statistics_data_path) / database/ table ;
    String query_path_prefix = fs::path(statistics_update_queue_path) / queue_prefix;
    StatisticsLogEntry log_entry{database, table, {getFQDNOrHostName(), getContext()->getTCPPort()}};

    try
    {
        getClient()->createAncestors(stats_data_path);
        getClient()->createOrUpdate(stats_data_path, node_data, zkutil::CreateMode::Persistent);
        String node_path = getClient()->create(query_path_prefix, log_entry.toString(), zkutil::CreateMode::PersistentSequential);
        LOG_DEBUG(log, "Create node {} for update {}", node_path, stats_data_path);
    }
    catch (...)
    {
        LOG_DEBUG(log, "Can't update statistics on keeper for {}", stats_data_path);
    }
}

void StatisticsKeeperStore::dropTableStatisticsOnKeeper(String database, String table) const
{
    String node_data;
    String full_table_name = backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
    String stats_data_path = fs::path(statistics_data_path) / database/ table ;
    String query_path_prefix = fs::path(statistics_update_queue_path) / queue_prefix;
    StatisticsLogEntry log_entry{database, table, {getFQDNOrHostName(), getContext()->getTCPPort()}};

    try
    {
        if (getClient()->tryRemove(stats_data_path)!= Coordination::Error::ZOK)
            LOG_DEBUG(log, "Can't get node {} to drop, the Statistics maybe dropped before.", stats_data_path);

        String node_path = getClient()->create(query_path_prefix, log_entry.toString(), zkutil::CreateMode::PersistentSequential);
        LOG_DEBUG(log, "Create node {} for drop {}", node_path, stats_data_path);
    }
    catch (...)
    {
        LOG_DEBUG(log, "Can't update statistics on keeper for {}", stats_data_path);
    }
}

void StatisticsKeeperStore::fetchTableStatisticsFromKeeper(String database, String table)
{
    String node_data;
    String full_table_name = backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
    String stats_data_path = fs::path(statistics_data_path) / database/ table ;
    StatsTableIdentifier identifier{StorageID{database, table}};
    auto uniq_key = identifier.getUniqueKey(getContext());

    if (!getClient()->tryGet(stats_data_path, node_data))
    {
        /// It is Ok that node could be deleted just now.
        LOG_DEBUG(log, "Can't get node {}, the Statistics is dropped.", stats_data_path);
        {
            std::unique_lock<std::shared_mutex> lock(mtx);
            auto iter = entries.find(uniq_key);
            if (iter != entries.end())
            {
                entries.erase(uniq_key);
            }
            LOG_DEBUG(log, "Clear Statistics for table {} in memory", full_table_name);
        }
    }

    try
    {
        auto && [name, stats_data] = StatsData::deserialize(node_data);
        {
            std::unique_lock<std::shared_mutex> lock(mtx);
            entries[uniq_key] = std::make_shared<TableEntry>(identifier, stats_data);
        }

        LOG_DEBUG(log, "Fetch statistics for table {} and apply it", full_table_name);
    }
    catch (...)
    {
        LOG_WARNING(log, "Fetch statistics for table {} failed", full_table_name);
    }
}

void StatisticsKeeperStore::fetchStatistics(bool reinitialized)
{
    LOG_DEBUG(log, "Fetch statistics from keeper");
    auto zookeeper = getClient();

    /// Main thread of StatisticsKeeperStore was restarted, probably due to lost connection with ZooKeeper.
    /// We just load all data from Keeper
    if (reinitialized)
    {
        Strings databases = zookeeper->getChildren(statistics_data_path);
        for (const auto & database : databases)
        {
            String database_path = statistics_data_path + '/' + database;
            Strings tables = zookeeper->getChildren(database_path);
            for (const auto & table : tables)
            {
                fetchTableStatisticsFromKeeper(database, table);
            }
        }
    }

    Strings queue_nodes = zookeeper->getChildren(statistics_update_queue_path, &queue_node_stat, queue_updated_event);
    filterAndSortQueueNodes(queue_nodes);

    if (max_tasks_in_queue < queue_nodes.size())
        cleanup_event->set();

    /// Detect queue start, using:
    auto begin_node = queue_nodes.begin();
    String last_task_name;
    if (last_executed_task_name)
        last_task_name = *last_executed_task_name;
    begin_node = std::upper_bound(queue_nodes.begin(), queue_nodes.end(), last_task_name);

    if (begin_node == queue_nodes.end())
        LOG_DEBUG(log, "No tasks to run");
    else
        LOG_DEBUG(log, "Will run {} tasks starting from {}", std::distance(begin_node, queue_nodes.end()), *begin_node);


    for (auto it = begin_node; it != queue_nodes.end() && !stop_flag; ++it)
    {
        String entry_name = *it;
        LOG_TRACE(log, "run task {}", entry_name);

        String entry_path = fs::path(statistics_update_queue_path) / entry_name;
        String node_data;

        if (!zookeeper->tryGet(entry_path, node_data))
        {
            /// It is Ok that node could be deleted just now. It means that there are no current host in node's host list.
            continue;
        }

        auto log_entry = StatisticsLogEntry::fromString(node_data);

        if (log_entry.host.isLocalAddress(getContext()->getTCPPort()))
        {
            continue;
        }

        LOG_DEBUG(log, "Running update task {} from {}", entry_name, log_entry.host.toString());

        fetchTableStatisticsFromKeeper(log_entry.database, log_entry.table);
        last_executed_task_name.emplace(entry_path);
    }
}

void StatisticsKeeperStore::shutdown()
{
    bool prev_stop_flag = stop_flag.exchange(true);
    if (!prev_stop_flag)
    {
        queue_updated_event->set();
        cleanup_event->set();
        if (update_thread)
            update_thread->join();
        if (cleanup_thread)
            cleanup_thread->join();
    }
}

bool StatisticsKeeperStore::canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat)
{
    /// Delete node if its lifetime is expired (according to task_max_lifetime parameter)
    constexpr UInt64 zookeeper_time_resolution = 1000;
    Int64 zookeeper_time_seconds = stat.ctime / zookeeper_time_resolution;
    return zookeeper_time_seconds + task_max_lifetime < Poco::Timestamp().epochTime();
}

void StatisticsKeeperStore::cleanupQueue()
{
    LOG_DEBUG(log, "Cleaning queue");

    Strings queue_nodes = getClient()->getChildren(statistics_update_queue_path);
    filterAndSortQueueNodes(queue_nodes);

    size_t index = 0;

    if (queue_nodes.size() > max_tasks_in_queue)
    {
        LOG_DEBUG(log, "Try to clean {} - {} logs, because queue size is greater than {}", queue_nodes[0], queue_nodes[max_tasks_in_queue - 1], max_tasks_in_queue);
        for (; index < queue_nodes.size() - max_tasks_in_queue; ++index)
        {
            String node_name = queue_nodes[index];
            String node_path = fs::path(statistics_update_queue_path) / node_name;

            try
            {
                /// Already deleted
                if (!getClient()->exists(node_path))
                    continue;
                getClient()->tryRemove(node_path);
            }
            catch (...)
            {
                LOG_INFO(log, "An error occurred while checking and cleaning task {} from queue: {}", node_name, getCurrentExceptionMessage(false));
            }
        }
    }


    for (; index < queue_nodes.size(); ++index)
    {
        String node_name = queue_nodes[index];
        String node_path = fs::path(statistics_update_queue_path) / node_name;
        Coordination::Stat stat;

        try
        {
            /// Already deleted
            if (!getClient()->exists(node_path, &stat))
                continue;
            if (!canRemoveQueueEntry(node_name, stat))
                continue;
            getClient()->tryRemove(node_path);
        }
        catch (...)
        {
            LOG_INFO(log, "An error occurred while checking and cleaning task {} from queue: {}", node_name, getCurrentExceptionMessage(false));
        }
    }

    for (auto it = queue_nodes.cbegin(); it < queue_nodes.cend(); ++it)
    {
        if (stop_flag)
            return;

        String node_name = *it;
        String node_path = fs::path(statistics_update_queue_path) / node_name;

        Coordination::Stat stat;
        String dummy;

        try
        {
            /// Already deleted
            if (!getClient()->exists(node_path, &stat))
                continue;

            if (!canRemoveQueueEntry(node_name, stat))
                continue;

        }
        catch (...)
        {
            LOG_INFO(log, "An error occurred while checking and cleaning task {} from queue: {}", node_name, getCurrentExceptionMessage(false));
        }
    }
}

void StatisticsKeeperStore::runCleanupThread()
{
    setThreadName("StatsKSC");
    LOG_DEBUG(log, "Started StatisticsKeeperStoreLog cleanup thread");

    Int64 last_cleanup_time_seconds = 0;
    while (!stop_flag)
    {
        try
        {
            cleanup_event->wait();
            if (stop_flag)
                break;

            Int64 current_time_seconds = Poco::Timestamp().epochTime();
            if (last_cleanup_time_seconds && current_time_seconds < last_cleanup_time_seconds + cleanup_delay_period)
            {
                LOG_TRACE(log, "Too early to clean queue, will do it later.");
                continue;
            }

            cleanupQueue();
            last_cleanup_time_seconds = current_time_seconds;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

}
