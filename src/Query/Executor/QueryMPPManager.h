#pragma once

#include <Common/Logger.h>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
#include <common/types.h>
#include <bthread/mutex.h>

namespace DB
{

class QueryMPPCoordinator;
using CoordinatorWeakPtr = std::weak_ptr<QueryMPPCoordinator>;
using CoordinatorMap = std::unordered_map<String, CoorinatorWeakPtr>;

class QueryMPPManager
{
public:
    static QueryMPPManager & instance()
    {
        static QueryMPPManager instance;
        return instance;
    }
    void registerQuery(const String & query_id, std::weak_ptr<QueryMPPCoordinator> coordinator);
    void clearQuery(const String & query_id);
    QueryMPPCoordinatorPtr getCoordinator(const String & query_id);

private:
    QueryMPPManager() = default;
    CoordinatorMap coordinator_map;
    LoggerPtr log {getLogger("QueryMPPManager")};
};

}
