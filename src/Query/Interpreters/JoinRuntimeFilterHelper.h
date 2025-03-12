#pragma once

#include <Interpreters/JoinUtils.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterBuilder.h>

namespace DB
{

class IJoin;
class RuntimeFilterConsumer;
using JoinPtr = std::shared_ptr<IJoin>;

class HashJoin;
using HashJoinPtr = std::shared_ptr<HashJoin>;

using RuntimeFilterConsumerPtr = std::shared_ptr<RuntimeFilterConsumer>;

class JoinRuntimeFiltersHelper
{
public:
    static void tryBuildRuntimeFilters(HashJoin & join);
    static void tryBuildRuntimeFilters(JoinPtr join);
    static void bypassRuntimeFilters(HashJoin & join, BypassType type, size_t total_size);
    static void buildAllRF(HashJoin & join, size_t total_size, const std::vector<const BlocksList *> & all_blocks, RuntimeFilterConsumerPtr rf_consumer);
    static void buildValueSetRF(const RuntimeFilter & rf_info, const String & name, const std::vector<const BlocksList *> & blocks,
                     RuntimeFilterConsumerPtr rf_consumer);

    static void buildBloomFilterRF(HashJoin & join, const RuntimeFilter & rf_info, const String & name, size_t ht_size, const std::vector<const BlocksList *> & blocks,
    RuntimeFilterConsumerPtr rf_consumer);

};

}
