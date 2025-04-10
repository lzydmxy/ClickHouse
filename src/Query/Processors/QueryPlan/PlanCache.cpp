#include <memory>
#include <Query/Analyzer/Analysis.h>
#include <Query/ProtosHelper/ASTSerDerHelper.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Processors/QueryPlan/PlanCache.h>
#include <Interpreters/StorageID.h>
#include <Query/Processors/QueryPlan/ReadStorageRowCountStepExt.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
//#include <Query/Processors/QueryPlan/QueryPlanner.h>

namespace DB
{

void PlanCacheManager::initialize(ContextMutablePtr context)
{
    if (!context->getOptimizerContext()->getPlanCacheManager())
    {
        auto manager_instance = std::make_unique<PlanCacheManager>();
        context->getOptimizerContext()->setPlanCacheManager(std::move(manager_instance));
    }

    auto * manager_instance = context->getOptimizerContext()->getPlanCacheManager();
    if (manager_instance->cache)
    {
        LOG_WARNING(getLogger("PlanCacheManager"), "PlanCacheManager already initialized");
        return;
    }

    auto max_size = context->getConfigRef().getUInt64("optimizer.plancache.max_cache_size", PlanCacheConfig::max_cache_size);
    auto expire_time = std::chrono::seconds(
        context->getConfigRef().getUInt64("optimizer.plancache.cache_expire_time", PlanCacheConfig::cache_expire_time));
    manager_instance->initialize(max_size, expire_time);
}

void PlanCacheManager::initialize(UInt64 max_size, std::chrono::seconds expire_time)
{
    Poco::Timestamp::TimeDiff the_time = expire_time.count() * 1000;
    cache = std::make_unique<CacheType>(max_size, the_time);
}

UInt128 PlanCacheManager::hash(const ASTPtr & query_ast, ContextMutablePtr & context)
{
    const auto & settings = context->getSettingsRef();
    String query;
    WriteBufferFromString query_buffer(query);
    serializeAST(query_ast, query_buffer);

    String settings_string;
    WriteBufferFromString buffer(settings_string);

    //todo: add whitelist in settings.write
    //const static std::unordered_set<String> whitelist{"enable_plan_cache", "force_plan_cache"};
    //settings.write(buffer, SettingsWriteFormat::DEFAULT, whitelist);
    settings.write(buffer, SettingsWriteFormat::DEFAULT);

    UInt128 key;
    SipHash hash;
    hash.update(query.data(), query.size());
    hash.update(settings_string.data(), settings_string.size());

    String current_database = context->getCurrentDatabase();
    hash.update(current_database.data(), current_database.size());

    key = hash.get128();

    return key;
}

PlanNodePtr PlanCacheManager::getNewPlanNode(PlanNodePtr node, ContextMutablePtr & context, bool cache_plan, PlanNodeId & max_id)
{
    if (max_id < node->getId())
        max_id = node->getId();

    if (node->getType() == QueryPlanStepType::TableScanStepExt)
    {
        auto step = QueryPlanStepHelper::copyQueryPlanStep(node->getStep(), context);
        auto * table_step = dynamic_cast<TableScanStepExt *>(step.get());
        if (cache_plan)
            table_step->cleanStorage();
        else
            table_step->setStorage(context);
        return PlanNodeBase::createPlanNode(node->getId(), step, {});
    }

    PlanNodes children;
    for (auto & child : node->getChildren())
    {
        auto result_node = getNewPlanNode(child, context, cache_plan, max_id);
        if (result_node)
            children.emplace_back(result_node);
    }

    auto step = QueryPlanStepHelper::copyQueryPlanStep(node->getStep(), context);
    return PlanNodeBase::createPlanNode(node->getId(), step, children);
}

void PlanCacheManager::invalidate(ContextMutablePtr)
{
    //todo: need to impl
}

QueryPlanExtPtr PlanCacheManager::getPlanFromCache(UInt128 query_hash, ContextMutablePtr & context)
{
    if (!context->getOptimizerContext()->getPlanCacheManager())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "plan cache has to be initialized");

    auto & cached = context->getOptimizerContext()->getPlanCacheManager()->instance();
    try
    {
        auto plan_object = cached.get(query_hash);
        if (!plan_object || !plan_object->plan_root)
            return nullptr;

        // check storage version
        for (auto & item : plan_object->query_info->tables_version)
        {
            auto storage = DatabaseCatalog::instance().tryGetTable(item.first, context);
            //tood: need latest_version in storage
            //if (!storage || storage->latest_version.toUInt64() != item.second)
            if (!storage)
            {
                cached.remove(query_hash);
                return nullptr;
            }
        }

        // check statistic version
        //todo: need Statistics

        /*
        for (auto & item : plan_object->query_info->stats_version)
        {
            Statistics::StatsTableIdentifier table_identifier{item.first};
            auto version_value = Statistics::getVersion(context, table_identifier);
            Int64 version = version_value.has_value() ? version_value.value().convertTo<Int64>() : 0;
            if (version != item.second)
            {
                cached.remove(query_hash);
                return nullptr;
            }
        }
        */

        PlanNodeId max_id;
        auto root  = PlanCacheManager::getNewPlanNode(plan_object->plan_root, context, false, max_id);
        CTEInfo cte_info;
        for (auto & cte : plan_object->cte_map)
            cte_info.add(cte.first, PlanCacheManager::getNewPlanNode(cte.second, context, false, max_id));

        if (plan_object->query_info && context->hasQueryContext())
        {
            for (auto & [database, table_info] : plan_object->query_info->query_access_info)
            {
                for (auto & [table, columns] : table_info)
                    context->addQueryAccessInfo(database, table, columns);
            }
        }

        context->getOptimizerContext()->createPlanNodeIdAllocator(max_id+1);
        return  std::make_unique<QueryPlanExt>(root, cte_info, context->getOptimizerContext()->getPlanNodeIdAllocator());
    }
    catch (...)
    {
        cached.remove(query_hash);
        return nullptr;
    }
}

bool PlanCacheManager::addPlanToCache(UInt128 query_hash, QueryPlanExtPtr & plan, AnalysisPtr analysis, ContextMutablePtr & context)
{
    if (!context->getOptimizerContext()->getPlanCacheManager())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "plan cache has to be initialized");

    auto & cache = context->getOptimizerContext()->getPlanCacheManager()->instance();
    auto root = plan->getPlanNode();
    UInt32 size = QueryPlanExt::getPlanNodeCount(root);

    if (size > context->getOptimizerContext()->getSettingsRef().max_plannode_count)
        return false;

    PlanNodeId max_id;
    PlanCacheManager::PlanObjectValue plan_object{};
    plan_object.plan_root = PlanCacheManager::getNewPlanNode(root, context, true, max_id);

    for (const auto & cte : plan->getCTEInfo().getCTEs())
        plan_object.cte_map.emplace(cte.first, PlanCacheManager::getNewPlanNode(cte.second, context, true, max_id));

    plan_object.query_info = std::make_shared<PlanCacheManager::PlanCacheInfo>();
    const auto & used_columns_map = analysis->getUsedColumns();
    for (const auto & [table_ast, storage_analysis] : analysis->getStorages())
    {
        if (!storage_analysis.storage)
            continue;
        auto storage_id = storage_analysis.storage->getStorageID();
        if (auto it = used_columns_map.find(storage_id); it != used_columns_map.end())
        {
            for (const auto & column : it->second)
                plan_object.query_info->query_access_info[backQuoteIfNeed(storage_id.getDatabaseName())][storage_id.getFullTableName()].emplace_back(column);
        }

        //todo: need Statistics
        /*
        Statistics::StatsTableIdentifier table_identifier{storage_id};
        auto version_value = Statistics::getVersion(context, table_identifier);
        Int64 version = version_value.has_value() ? version_value.value().convertTo<Int64>() : 0;
        plan_object.query_info->stats_version[storage_id] = version;
        plan_object.query_info->tables_version[storage_id] = storage_analysis.storage->latest_version.toUInt64();
        */
    }
    cache.add(query_hash, plan_object);
    return true;
}

bool PlanCacheManager::enableCachePlan(const ASTPtr & query_ast, ContextPtr context)
{
    if (!context->getOptimizerContext()->getSettingsRef().enable_plan_cache)
        return false;
    
    return query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>() || query_ast->as<ASTSelectIntersectExceptQuery>();
}

}
