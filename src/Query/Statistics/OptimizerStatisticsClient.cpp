#include <Query/Statistics/OptimizerStatisticsClient.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/getClusterName.h>
#include <Query/Exchange/RpcChannelPool.h>
#include <Query/ProtosHelper/RPCHelpers.h>
#include <Query/Protos/optimizer_statistics.pb.h>
//#include <Statistics/AutoStatisticsManager.h>
#include <Query/Statistics/StatisticsSettings.h>
//#include <Statistics/SubqueryHelper.h>
#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
//#include "DaemonManager/DaemonJobAutoStatistics.h"
//#include "Statistics/StatsTableIdentifier.h"
#include <Query/Exchange/bRPC/BrpcChannelPoolOptions.h>

namespace DB::QueryStatistics
{
void refreshClusterStatsCache(ContextPtr context, const StatsTableIdentifier & table_identifier, bool is_drop)
{
    (void)context;
    (void)table_identifier;
    (void)is_drop;
    // do nothing here for all nodes should subscribe the changes of statistics in keeper
}

StatisticsSettings fetchStatisticsSettings(ContextPtr context)
{
    (void)context;
    // TODO wujianchao implement this when we implement auto stats collecting
    return {};
}

}
