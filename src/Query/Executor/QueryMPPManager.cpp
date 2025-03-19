#include "QueryMPPManager.h"
#include <Common/Exception.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
}

void QueryMPPManager::registerQuery(const String & query_id, CoordinatorWeakPtr coordinator)
{
    std::unique_lock<std::shared_mutex> lock(coor_mutex);
    auto res = coordinator_map.try_emplace(query_id, std::move(coordinator));
    if (!res.second)
    {
        throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
            "Mpp query with id = {} is already running and can't be stopped", query_id);
    }
}

void QueryMPPManager::clearQuery(const String & query_id)
{
    std::unique_lock<std::shared_mutex> lock(coor_mutex);
    auto res = coordinator_map.erase(query_id);
    LOG_TRACE(log, "clear query: {} with res: {}", query_id, res);
}

QueryMPPCoordinatorPtr QueryMPPManager::getCoordinator(const String & query_id)
{
    std::shared_lock<std::shared_mutex> lock(coor_mutex);
    QueryMPPCoordinatorPtr res = nullptr;
    auto it = coordinator_map.find(query_id);
    if (it != coordinator_map.end())
        res = it->second.lock();
    return res;
}

}
