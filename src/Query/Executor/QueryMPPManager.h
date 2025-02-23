#pragma once

#include <mutex>
#include <unordered_map>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Executor/QueryMPPCoordinator.h>

namespace DB
{

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
