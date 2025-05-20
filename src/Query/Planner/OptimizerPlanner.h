#pragma once

#include <Interpreters/SelectQueryOptions.h>
#include <Analyzer/IQueryTreeNode.h>

#include <QueryPipeline/StreamLocalLimits.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class QueryNode;
struct SelectQueryInfo;

class QueryPlan;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;

class GlobalPlannerContext;
using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContextExt;
using PlannerContextExtPtr = std::shared_ptr<PlannerContextExt>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;


// Planner for optimizer
class OptimizerPlanner
{
public:
    /// Initialize planner with query tree after analysis phase
    OptimizerPlanner(const QueryTreeNodePtr & query_tree_,
        SelectQueryOptions & select_query_options_);

    void buildQueryPlanIfNeeded();

    const QueryPlan & getQueryPlan() const
    {
        return query_plan;
    }

    SelectQueryInfo buildSelectQueryInfo() const;

    QueryPlan & getQueryPlan()
    {
        return query_plan;
    }

    /// We support mapping QueryNode -> QueryPlanStep (the last step added to plan from this query)
    /// It is useful for parallel replicas analysis.
    using QueryNodeToPlanStepMapping = std::unordered_map<const QueryNode *, const QueryPlan::Node *>;
    const QueryNodeToPlanStepMapping & getQueryNodeToPlanStepMapping() const { return query_node_to_plan_step_mapping; }

private:
    void buildPlanForUnionNode();

    void buildPlanForQueryNode();

    QueryTreeNodePtr query_tree;
    SelectQueryOptions & select_query_options;
    PlannerContextExtPtr planner_context;
    QueryPlan query_plan;
    StorageLimitsList storage_limits;
    std::set<std::string> used_row_policies;
    QueryNodeToPlanStepMapping query_node_to_plan_step_mapping;

    ContextPtr context;
};

}
