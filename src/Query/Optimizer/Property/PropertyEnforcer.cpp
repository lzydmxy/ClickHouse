#include <Query/Optimizer/Property/PropertyEnforcer.h>

#include <Query/Optimizer/Cascades/GroupExpression.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_ENFORCE;
}

PlanNodePtr
PropertyEnforcer::enforceNodePartitioning(const PlanNodePtr & node, const Property & required, const Property & property, Context & context)
{
    QueryPlanStepPtr step_ptr = enforceNodePartitioning(node->getStep(), required, property, context);
    if (!step_ptr)
    {
        return node;
    }
    auto exchange = PlanNodeBase::createPlanNode(context.getOptimizerContext()->nextNodeId(), std::move(step_ptr), std::vector{node});
    return exchange;
}

PlanNodePtr PropertyEnforcer::enforceStreamPartitioning(
    const PlanNodePtr & node, const Property & required, const Property & property, Context & context)
{
    QueryPlanStepPtr step_ptr = enforceStreamPartitioning(node->getStep(), required, property, context);
    auto exchange = PlanNodeBase::createPlanNode(context.getOptimizerContext()->nextNodeId(), std::move(step_ptr), std::vector{node});
    return exchange;
}

GroupExprPtr PropertyEnforcer::enforceNodePartitioning(
    const GroupExprPtr & group_expr, const Property & required, const Property & property, const Context & context)
{
    QueryPlanStepPtr step_ptr = enforceNodePartitioning(group_expr->getStep(), required, property, context);
    if (!step_ptr)
    {
        return nullptr;
    }
    std::vector<GroupId> children = {group_expr->getGroupId()};
    auto result = std::make_shared<GroupExpression>(std::move(step_ptr), children);
    return result;
}

GroupExprPtr PropertyEnforcer::enforceStreamPartitioning(
    const GroupExprPtr & group_expr, const Property & required, const Property & property, const Context & context)
{
    QueryPlanStepPtr step_ptr = enforceStreamPartitioning(group_expr->getStep(), required, property, context);
    std::vector<GroupId> children = {group_expr->getGroupId()};
    auto result = std::make_shared<GroupExpression>(std::move(step_ptr), children);
    return result;
}

QueryPlanStepPtr PropertyEnforcer::enforceNodePartitioning(
    QueryPlanStepPtr step, const Property & required, const Property & actual, const Context & context)
{
    const auto & output_stream = step->getOutputStream();
    DataStreams streams{output_stream};
    Partitioning partitioning = required.getNodePartitioning();

    // if the stream is ordered, we need keep order when exchange data.
    bool keep_order = context.getOptimizerContext()->getSettingsRef().enable_shuffle_with_order;

    switch (partitioning.getHandle())
    {
        case PartitioningHandle::SINGLE:
            return std::make_unique<ExchangeStepExt>(streams, RExchangeMode::Enum::ExchangeMode_Enum_GATHER, partitioning, keep_order);
        case PartitioningHandle::FIXED_BROADCAST:
            return std::make_unique<ExchangeStepExt>(streams, RExchangeMode::Enum::ExchangeMode_Enum_BROADCAST, partitioning, keep_order);
        case PartitioningHandle::FIXED_ARBITRARY:
            if (partitioning.getComponent() == Component::WORKER
                && actual.getNodePartitioning().getComponent() == Component::COORDINATOR)
            {
                return std::make_unique<ExchangeStepExt>(streams, RExchangeMode::Enum::ExchangeMode_Enum_GATHER, partitioning, keep_order);
            }
            return std::make_unique<ExchangeStepExt>(streams, RExchangeMode::Enum::ExchangeMode_Enum_LOCAL_NO_NEED_REPARTITION, partitioning, keep_order);
        case PartitioningHandle::ARBITRARY:
            return nullptr;
        case PartitioningHandle::FIXED_HASH:
        case PartitioningHandle::BUCKET_TABLE:
            return std::make_unique<ExchangeStepExt>(streams, RExchangeMode::Enum::ExchangeMode_Enum_REPARTITION, partitioning, keep_order);
        default:
            throw Exception(ErrorCodes::ILLEGAL_ENFORCE, "Property Enforce error");
    }
}

QueryPlanStepPtr
PropertyEnforcer::enforceStreamPartitioning(QueryPlanStepPtr step, const Property & required, const Property &, const Context &)
{
    DataStreams streams;
    const DataStream & input_stream = step->getOutputStream();
    streams.emplace_back(input_stream);

    Partitioning partitioning = required.getStreamPartitioning();
    switch (partitioning.getHandle())
    {
        case PartitioningHandle::SINGLE:
            return std::make_unique<UnionStepExt>(streams, DataStream{}, OutputToInputs{}, 0, true);
        case PartitioningHandle::FIXED_HASH:
            return std::make_unique<LocalExchangeStepExt>(streams[0], RExchangeMode::Enum::ExchangeMode_Enum_REPARTITION, partitioning);
        case PartitioningHandle::FIXED_ARBITRARY:
            return std::make_unique<LocalExchangeStepExt>(streams[0], RExchangeMode::Enum::ExchangeMode_Enum_LOCAL_NO_NEED_REPARTITION, partitioning);
        case PartitioningHandle::ARBITRARY:
            return nullptr;
        default:
            throw Exception(ErrorCodes::ILLEGAL_ENFORCE, "Property Enforce error");
}
}

PlanNodePtr PropertyEnforcer::enforceOffloadingGatherNode(const PlanNodePtr & node, Context & context)
{
    // already Exchange
    if (node->getType() == QueryPlanStepType::ExchangeStepExt
        || (node->getChildren().size() == 1 && node->getChildren()[0]->getType() == QueryPlanStepType::ExchangeStepExt))
            return node;

    auto * projection = dynamic_cast<ProjectionStepExt *>(node->getStep().get());
    // add gather before final projection
    if (projection && projection->isFinalProject())
    {
        // offloading_with_query_plan need keep_order
        Partitioning partitioning;
        auto gather_step = std::make_unique<ExchangeStepExt>(
        DataStreams{node->getChildren()[0]->getStep()->getOutputStream()}, RExchangeMode::Enum::ExchangeMode_Enum_GATHER, partitioning, true);
        auto gather_node = PlanNodeBase::createPlanNode(context.getOptimizerContext()->nextNodeId(), std::move(gather_step), node->getChildren());
        node->replaceChildren(PlanNodes{gather_node});
        return node;
    }

    Partitioning partitioning;
    
    // offloading_with_query_plan need keep_order
    auto gather_step
        = std::make_unique<ExchangeStepExt>(DataStreams{node->getStep()->getOutputStream()}, RExchangeMode::Enum::ExchangeMode_Enum_GATHER, partitioning, true);
    return PlanNodeBase::createPlanNode(context.getOptimizerContext()->nextNodeId(), std::move(gather_step), PlanNodes{node});
}
}
