#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <boost/noncopyable.hpp>

#include <Interpreters/Context_fwd.h>
#include <Query/Executor/QueryMPPCoordinator.h>
#include <Query/Executor/QueryMPPManager.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
}

void QueryMPPManager::registerQuery(const String & query_id, std::weak_ptr<MPPQueryCoordinator> coordinator)
{
    auto res = coordinator_map.try_emplace(query_id, std::move(coordinator));
    if (!res.second)
    {
        throw Exception(
            "Mpp query with id = " + query_id + " is already running and can't be stopped",
            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
    }
}

void QueryMPPManager::clearQuery(const String & query_id)
{
    auto res = coordinator_map.erase(query_id);
    LOG_TRACE(log, "clear query: {} with res: {}", query_id, res);
}

QueryMPPCoordinatorPtr QueryMPPManager::getCoordinator(const String & query_id)
{
    QueryMPPCoordinatorPtr res;
    coordinator_map.if_contains(query_id, [&res](auto & pair) { res = pair.second.lock(); });
    return res;
}

}
