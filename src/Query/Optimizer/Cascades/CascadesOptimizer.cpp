#include <Query/Optimizer/Cascades/CascadesOptimizer.h>

#include <Interpreters/Context.h>
#include <Query/Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Query/Optimizer/Cascades/GroupExpression.h>
#include <Query/Optimizer/Cascades/Task.h>
#include <Query/Optimizer/Property/PropertyEnforcer.h>
#include <Query/Optimizer/Rule/Implementation/SetJoinDistribution.h>
#include <Query/Optimizer/Rule/Transformation/CardinalityBasedJoinReorder.h>
#include <Query/Optimizer/Rule/Transformation/InlineCTE.h>
#include <Query/Optimizer/Rule/Transformation/InnerJoinAssociate.h>
#include <Query/Optimizer/Rule/Transformation/InnerJoinCommutation.h>
#include <Query/Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Query/Optimizer/Rule/Transformation/JoinToMultiJoin.h>
#include <Query/Optimizer/Rule/Transformation/LeftJoinToRightJoin.h>
#include <Query/Optimizer/Rule/Transformation/MagicSetForAggregation.h>
#include <Query/Optimizer/Rule/Transformation/PullOuterJoin.h>
#include <Query/Optimizer/Rule/Transformation/SelectivityBasedJoinReorder.h>
#include <Query/Optimizer/Rule/Transformation/SemiJoinPushDown.h>
#include <Query/Processors/QueryPlan/AnyStepExt.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Processors/QueryPlan/PlanPrinter.h>
#include <Query/Planner/GraphvizPrinter.h>
#include <Query/Processors/IQueryPlanStepExt.h>
#include <Query/Processors/QueryPlan/MultiJoinStepExt.h>
#include <Query/Processors/QueryPlan/PlanPattern.h>
#include <Storages/StorageDistributed.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int OPTIMIZER_TIMEOUT;
}


static bool hasCBOType(const std::set<QueryPlanStepType> & typs)
{
    static std::set<QueryPlanStepType> CBO_STEP_TYPE
        = {QueryPlanStepType::JoinStepExt, QueryPlanStepType::AggregatingStepExt, QueryPlanStepType::CTERefStepExt};
    for (const auto & type : CBO_STEP_TYPE)
    {
        if (typs.find(type) != typs.end())
            return true;
    }
    return false;
}

bool CascadesOptimizer::rewrite(QueryPlanExt & plan, ContextMutablePtr context) const
{
    LOG_TRACE(getLogger("CascadesOptimizer"), "Logical plan before CascadesOptimizer: \n{}", PlanPrinter::textLogicalPlan(plan, context));
    int id = context->getOptimizerContext()->getRuleId();
    CascadesContext cascades_context{
        context,
        plan.getCTEInfo(),
        context->getOptimizerContext()->getWorkerSize(),
        PlanPattern::maxJoinSize(plan, context),
        enable_cbo && hasCBOType(PlanPattern::extractStepTypes(plan))};

    auto start = std::chrono::high_resolution_clock::now();
    auto root = cascades_context.initMemo(plan.getPlanNode());

    auto root_id = root->getGroupId();
    auto single = Property{Partitioning{Partitioning::Handle::SINGLE}};
    single.getNodePartitioningRef().setComponent(Partitioning::Component::COORDINATOR);

    WinnerPtr winner;
    try
    {
        winner = optimize(root_id, cascades_context, single);
    }
    catch (...)
    {
        LOG_WARNING(cascades_context.getLog(), "Optimize failed: {}", cascades_context.getInfo());
        GraphvizPrinter::printMemo(cascades_context.getMemo(), root_id, context, toString(id) + "_CascadesOptimizer-Memo-Graph");
        throw;
    }
    LOG_DEBUG(cascades_context.getLog(), "{}", cascades_context.getInfo());
    GraphvizPrinter::printMemo(cascades_context.getMemo(), root_id, context, toString(id) + "_CascadesOptimizer-Memo-Graph");

    auto result = buildPlanNode(root_id, cascades_context, single);

    // enforce a gather with keep_order if offloading_with_query_plan enabled
    if (context->getOptimizerContext()->getSettingsRef().offloading_with_query_plan)
        result = PropertyEnforcer::enforceOffloadingGatherNode(result, *context);

    plan.getCTEInfo().clear();
    for (const auto & item : winner->getCTEActualProperties())
    {
        auto cte_id = item.first;
        auto cte_def_group = cascades_context.getMemo().getCTEDefGroupByCTEId(cte_id);
        auto cte = buildPlanNode(cte_def_group->getId(), cascades_context, item.second.first);
        plan.getCTEInfo().add(cte_id, cte);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG_DEBUG(cascades_context.getLog(), "Cascades use {} ms", ms_int.count());

    plan.update(result);
    return true;
}

WinnerPtr CascadesOptimizer::optimize(GroupId root_group_id, CascadesContext & context, const Property & required_prop)
{
    auto root_context = std::make_shared<OptimizationContext>(context, required_prop, std::numeric_limits<double>::max());
    auto root_group = context.getMemo().getGroupById(root_group_id);
    context.getTaskStack().push(std::make_shared<OptimizeGroup>(root_group, root_context));

    Stopwatch watch{CLOCK_THREAD_CPUTIME_ID};
    while (!context.getTaskStack().empty())
    {
        auto task = context.getTaskStack().top();
        context.getTaskStack().pop();
        task->execute();

        // Check to see if we have at least one plan, and if we have exceeded our
        // timeout limit
        double duration = watch.elapsedMilliseconds();
        if (duration >= context.getTaskExecutionTimeout())
        {
            throw Exception(ErrorCodes::OPTIMIZER_TIMEOUT, 
                "Cascades exhausted the time limit of {} ms", context.getTaskExecutionTimeout());
        }
        if (context.getTaskStack().size() > 100000)
        {
            throw Exception(ErrorCodes::OPTIMIZER_TIMEOUT, 
                "Cascades exhausted the task limit of 100000), there are {} tasks", context.getTaskStack().size());
        }
    }

    return root_group->getBestExpression(required_prop);
}


PlanNodePtr
CascadesOptimizer::buildPlanNode(GroupId root, CascadesContext & context, const Property & required_prop) // NOLINT(misc-no-recursion)
{
    auto group = context.getMemo().getGroupById(root);
    auto winner = group->getBestExpression(required_prop);

    auto input_properties = winner->getRequireChildren();

    PlanNodes children;
    for (size_t index = 0; index < input_properties.size(); ++index)
    {
        auto child = buildPlanNode(winner->getGroupExpr()->getChildrenGroups()[index], context, input_properties[index]);
        children.emplace_back(child);
    }

    return winner->buildPlanNode(context, children);
}

GroupExprPtr CascadesContext::initMemo(const PlanNodePtr & plan_node)
{
    PlanNodes nodes;
    std::queue<PlanNodePtr> queue;
    queue.push(plan_node);

    while (!queue.empty())
    {
        auto node = queue.front();
        for (const auto & child : node->getChildren())
        {
            queue.push(child);
        }
        if (const auto * read_step = dynamic_cast<const CTERefStepExt *>(node->getStep().get()))
        {
            if (!memo.containsCTEId(read_step->getId()))
            {
                auto cte_expr = initMemo(cte_info.getCTEDef(read_step->getId()));
                memo.recordCTEDefGroupId(read_step->getId(), cte_expr->getGroupId());
            }
        }
        queue.pop();
    }

    GroupExprPtr root_expr;
    recordPlanNodeIntoGroup(plan_node, root_expr, RuleType::INITIAL);
    return root_expr;
}

bool CascadesContext::recordPlanNodeIntoGroup(
    const PlanNodePtr & plan_node, GroupExprPtr & group_expr, RuleType produce_rule, GroupId target_group)
{
    auto new_group_expr = makeGroupExpression(plan_node, produce_rule);
    group_expr = memo.insertGroupExpr(new_group_expr, *this, target_group);
    // if memo exists the same expr, it will return the old expr
    // so it is not equal, and return false
    return group_expr == new_group_expr;
}

GroupExprPtr CascadesContext::makeGroupExpression(const PlanNodePtr & node, RuleType produce_rule)
{
    std::vector<GroupId> child_groups;
    for (auto & child : node->getChildren())
    {
        if (getQueryPlanStepType(child->getStep()) == QueryPlanStepType::AnyStepExt)
        {
            // Special case for LEAF
            const auto * const leaf = dynamic_cast<const AnyStepExt *>(child->getStep().get());
            auto child_group = leaf->getGroupId();
            child_groups.push_back(child_group);
        }
        else
        {
            // Create a GroupExpression for the child
            auto group_expr = makeGroupExpression(child, produce_rule);

            // Insert into the memo (this allows for duplicate detection)
            auto memo_expr = memo.insertGroupExpr(group_expr, *this);
            if (memo_expr == nullptr)
            {
                // Delete if need to (see InsertExpression spec)
                child_groups.push_back(group_expr->getGroupId());
            }
            else
            {
                child_groups.push_back(memo_expr->getGroupId());
            }
        }
    }
    return std::make_shared<GroupExpression>(node->getStep(), std::move(child_groups), produce_rule);
}

CascadesContext::CascadesContext(
    ContextMutablePtr context_, CTEInfo & cte_info_, size_t worker_size_, size_t max_join_size_, bool enable_cbo_)
    : context(context_)
    , cte_info(cte_info_)
    , worker_size(worker_size_)
    , support_filter(context->getOptimizerContext()->getSettingsRef().enable_join_graph_support_filter)
    , task_execution_timeout(context->getOptimizerContext()->getSettingsRef().cascades_optimizer_timeout)
    , enable_pruning((context->getOptimizerContext()->getSettingsRef().enable_cascades_pruning))
    , enable_auto_cte(context->getOptimizerContext()->getSettingsRef().cte_mode == CTEMode::AUTO)
    , enable_trace((context->getOptimizerContext()->getSettingsRef().log_optimizer_run_time))
    , enable_cbo(enable_cbo_ && context->getOptimizerContext()->getSettingsRef().enable_cbo)
    , max_join_size(max_join_size_)
    , cost_model(CostModel(*context_))
    , log(getLogger("CascadesOptimizer"))
{
    LOG_DEBUG(log, "max join size: {}", max_join_size_);
    LOG_DEBUG(log, "worker size: {}", worker_size_);
    implementation_rules.emplace_back(std::make_shared<SetJoinDistribution>());

    if (enable_cbo)
    {
        if (context->getOptimizerContext()->getSettingsRef().enable_join_reorder)
        {
            if (context->getOptimizerContext()->getSettingsRef().enable_non_equijoin_reorder && max_join_size_ <= context->getOptimizerContext()->getSettingsRef().max_graph_reorder_size)
            {
                transformation_rules.emplace_back(std::make_shared<InnerJoinAssociate>());
            }
            transformation_rules.emplace_back(std::make_shared<SemiJoinPushDown>());
            transformation_rules.emplace_back(std::make_shared<JoinEnumOnGraph>(support_filter));
            transformation_rules.emplace_back(std::make_shared<InnerJoinCommutation>());
            if (context->getOptimizerContext()->getSettingsRef().heuristic_join_reorder_enumeration_times > 0)
                transformation_rules.emplace_back(
                    std::make_shared<CardinalityBasedJoinReorder>(context->getOptimizerContext()->getSettingsRef().max_graph_reorder_size));
            transformation_rules.emplace_back(
                std::make_shared<SelectivityBasedJoinReorder>(context->getOptimizerContext()->getSettingsRef().max_graph_reorder_size));
            transformation_rules.emplace_back(std::make_shared<JoinToMultiJoin>());
        }

        // left join inner join reorder q78, 80
        transformation_rules.emplace_back(std::make_shared<PullLeftJoinThroughInnerJoin>());
        transformation_rules.emplace_back(std::make_shared<PullLeftJoinProjectionThroughInnerJoin>());
        transformation_rules.emplace_back(std::make_shared<PullLeftJoinFilterThroughInnerJoin>());

        transformation_rules.emplace_back(std::make_shared<LeftJoinToRightJoin>());
        transformation_rules.emplace_back(std::make_shared<MagicSetForAggregation>());
        transformation_rules.emplace_back(std::make_shared<MagicSetForProjectionAggregation>());
        transformation_rules.emplace_back(std::make_shared<SemiJoinPushDownProjection>());
        transformation_rules.emplace_back(std::make_shared<SemiJoinPushDownAggregate>());

        if (!cte_info.empty() && enable_auto_cte)
        {
            transformation_rules.emplace_back(std::make_shared<InlineCTE>());
            transformation_rules.emplace_back(std::make_shared<InlineCTEWithFilter>());
        }
    }

        // transformation_rules.emplace_back(std::make_shared<PushAggThroughInnerJoin>());
    }

size_t WorkerSizeFinder::find(QueryPlanExt & query_plan, const Context & context)
{
    if (context.getOptimizerContext()->getSettingsRef().enable_memory_catalog)
        return context.getOptimizerContext()->getSettingsRef().memory_catalog_worker_size;

    WorkerSizeFinder visitor{query_plan.getCTEInfo()};
    // default schedule to worker cluster
    std::optional<size_t> result = VisitorUtil::accept(query_plan.getPlanNode(), visitor, context);
    if (result.has_value())
        return result.value();
    return 1;
}

std::optional<size_t> WorkerSizeFinder::visitPlanNode(PlanNodeBase & node, const Context & context)
{
    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, context);
        if (result.has_value())
            return result;
    }
    return std::nullopt;
}

std::optional<size_t> WorkerSizeFinder::visitTableScanStepExtNode(TableScanStepExtNode & node, const Context & context)
{
    const auto storage = node.getStep()->getStorage();
    const auto * distributed_table = dynamic_cast<StorageDistributed *>(storage.get());

    /// diff: byconity uses work group
    if (distributed_table)
    {
        if (auto cluster = distributed_table->getCluster())
            return cluster->getShardCount();
    }
    return std::nullopt;
}

std::optional<size_t> WorkerSizeFinder::visitCTERefStepExtNode(CTERefStepExtNode & node, const Context & context)
{
    const auto * step = dynamic_cast<const CTERefStepExt *>(node.getStep().get());
    return VisitorUtil::accept(cte_info.getCTEDef(step->getId()), *this, context);
}

void CascadesContext::trace(const String & task_name, GroupId /*group_id*/, RuleType rule_type, UInt64 elapsed_ns)
{
    if (!enable_trace)
    {
        return;
    }
    auto & counter = rule_trace[rule_type][task_name];
    counter.elapsed_ns += elapsed_ns;
    counter.counts += 1;
}

String CascadesContext::getInfo() const
{
    std::stringstream ss;

    ss << "Group: " << memo.getGroups().size() << ' ';

    size_t total_logical_expr = 0;
    size_t total_physical_expr = 0;
    for (const auto & group : memo.getGroups())
    {
        total_logical_expr += group->getLogicalExpressions().size();
        total_physical_expr += group->getPhysicalExpressions().size();
    }
    ss << "Logical Expr: " << total_logical_expr << ' ';
    ss << "Physical Expr: " << total_physical_expr << ' ';
    ss << "Total Expr: " << memo.getExprs().size() << ' ';

    if (enable_trace)
    {
        ss << '\n';
        for (const auto & item : implementation_rules)
            if (rule_trace.contains(item->getType()))
                for (const auto & task_to_counter : rule_trace.at(item->getType()))
                    ss << "[" << task_to_counter.first << "] " << item->getName() << ": "
                       << static_cast<double>(task_to_counter.second.elapsed_ns) / 1000000 << " ms / " << task_to_counter.second.counts
                       << " counts" << '\n';

        for (const auto & item : transformation_rules)
            if (rule_trace.contains(item->getType()))
                for (const auto & task_to_counter : rule_trace.at(item->getType()))
                    ss << "[" << task_to_counter.first << "] " << item->getName() << ": "
                       << static_cast<double>(task_to_counter.second.elapsed_ns) / 1000000 << " ms / " << task_to_counter.second.counts
                       << " counts" << '\n';

        std::unordered_map<RuleType, UInt64> rule_to_logical_expression_counts;
        for (const auto & group : memo.getGroups())
        {
            for (const auto & item : group->getLogicalExpressions())
                rule_to_logical_expression_counts[item->getProduceRule()]++;
            for (const auto & item : group->getPhysicalExpressions())
                rule_to_logical_expression_counts[item->getProduceRule()]++;
        }

        for (const auto & item : implementation_rules)
            if (rule_to_logical_expression_counts.contains(item->getType()))
                ss << item->getName() << " produced: " << rule_to_logical_expression_counts[item->getType()] << " logical exprs.\n";

        for (const auto & item : transformation_rules)
            if (rule_to_logical_expression_counts.contains(item->getType()))
                ss << item->getName() << " produced: " << rule_to_logical_expression_counts[item->getType()] << " logical exprs.\n";
    }
    return ss.str();
}

}
