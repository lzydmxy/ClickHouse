#include <string>
#include <Query/Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>
#include <Query/Processors/QueryPlan/PlanSegmentSourceStepExt.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Query/Processors/QueryPlan/ValuesStepExt.h>
#include <Query/Processors/QueryPlan/ReadStorageRowCountStepExt.h>
#include <Query/Optimizer/Property/Property.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Storages/StorageMemory.h>

namespace DB
{
void PlanSegmentSplitter::split(QueryPlanExt & query_plan, PlanSegmentContext & plan_segment_context)
{
    PlanSegmentVisitor visitor{plan_segment_context, query_plan.getCTENodes()};

    if (plan_segment_context.context->getOptimizerContext()->getSettingsRef().distributed_max_parallel_size != 0)
        SetScalable::setScalable(query_plan.getRootNode(), query_plan.getCTENodes(), *plan_segment_context.context);
    size_t exchange_id = 0;
    PlanSegmentVisitorContext split_context{ {}, {}, exchange_id, plan_segment_context.context->getOptimizerContext()->getSettingsRef().exchange_shuffle_method_name};
    visitor.createPlanSegment(query_plan.getRootNode(), split_context);

    std::unordered_map<size_t, PlanSegmentTree::Node *> plan_mapping;

    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        plan_mapping[node.plan_segment->getPlanSegmentId()] = &node;
        if (node.plan_segment->getPlanSegmentOutputs()[0]->getPlanSegmentType() == RIPlanSegment::Enum::IPlanSegment_Enum_OUTPUT)
            plan_segment_context.plan_segment_tree->setRoot(&node);
    }

    // 1. set segment parallel
    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        auto inputs = node.plan_segment->getPlanSegmentInputs();
        for (auto & input : inputs)
        {
            /***
             * If a output is a gather node, its parallel size is always 1 since we should gather all data.
             */
            if (input->getExchangeMode() == RExchangeMode::GATHER)
            {
                node.plan_segment->setParallelSize(1);
            }
        }
    }

    // 2. set output parallel the same to segment parallel
    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        auto inputs = node.plan_segment->getPlanSegmentInputs();

        for (auto & input : inputs)
        {
            /***
             * SOURCE input has no plan semgnet id and it shouldn't include any child.
             */
            if (input->getPlanSegmentType() != RIPlanSegment::Enum::IPlanSegment_Enum_SOURCE && input->getPlanSegmentType() != RIPlanSegment::Enum::IPlanSegment_Enum_UNKNOWN)
            {
                auto * child_node = plan_mapping[input->getPlanSegmentId()];
                node.children.push_back(child_node);

                for (auto & output : child_node->plan_segment->getPlanSegmentOutputs())
                {
                    if (output->getExchangeId() == input->getExchangeId())
                    {
                        output->setShufflekeys(input->getShufflekeys());
                        output->setPlanSegmentId(node.plan_segment->getPlanSegmentId());
                        output->setExchangeMode(input->getExchangeMode());
                        output->setParallelSize(node.plan_segment->getParallelSize());
                        if (isLocalExchange(output->getExchangeMode()))
                        {
                            child_node->plan_segment->setHasLocalOutput(true);
                            node.plan_segment->setHasLocalInput(true);
                        }
                    }
                }
                /**
                 * If a output is a gather node, its parallel size is always 1 since we should gather all data.
                 */
                if (child_node->plan_segment->getPlanSegmentOutput()->getExchangeMode() == RExchangeMode::GATHER)
                {
                    node.plan_segment->setParallelSize(1);
                }
            }
        }
    }

    ParallelSizeChecker checker;
    checker.shard_number = plan_segment_context.shard_number;
    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        checker.segment = node.plan_segment.get();
        checker.children_segments.clear();
        for (auto & child : node.children)
        {
            checker.children_segments.emplace_back(child->plan_segment.get());
        }
        const Context & c = *plan_segment_context.context;
        auto sizes = VisitorUtil::accept(node.plan_segment->getQueryPlan().getRootNode(), checker, c);
        if (!sizes.empty())
        {
            auto first = sizes[0];
            for (auto size : sizes)
            {
                if (size != first && !plan_segment_context.context->getOptimizerContext()->getSettingsRef().bsp_mode)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Segment parallel size not match {} and {}", size, first);
                }
            }
        }
    }
}

PlanSegmentResult PlanSegmentVisitor::visitNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context)
{
    for (size_t i = 0; i < node->children.size(); ++i)
    {
        auto result_node = visitChild(node->children[i], split_context);
        if (result_node)
            node->children[i] = result_node;
    }

    return nullptr;
}

PlanSegmentResult PlanSegmentVisitor::visitChild(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context)
{
    return VisitorUtil::accept(node, *this, split_context);
}

PlanSegmentResult PlanSegmentVisitor::visitExchangeStepExtNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context)
{
    ExchangeStepExt * step = dynamic_cast<ExchangeStepExt *>(node->step.get());

    PlanSegmentInputs inputs;
    bool is_add_totals = false;
    bool is_add_extremes = false;
    for (auto & child : node->children)
    {
        String hash_func = plan_segment_context.context->getOptimizerContext()->getSettingsRef().exchange_shuffle_method_name;
        PlanSegmentVisitorContext child_context{
            {},
            {},
            split_context.exchange_id,
            step->getSchema().getHashFunc(hash_func),
            step->getSchema().getParams()};
        PlanSegment * plan_segment = createPlanSegment(child, child_context);

        is_add_totals |= child_context.is_add_totals;
        is_add_extremes |= child_context.is_add_extremes;
        auto input = std::make_shared<PlanSegmentInput>(step->getHeader(), RIPlanSegment::Enum::IPlanSegment_Enum_EXCHANGE);
        input->setShufflekeys(step->getSchema().getColumns());
        input->setPlanSegmentId(plan_segment->getPlanSegmentId());
        input->setExchangeMode(step->getExchangeMode());
        // TODO: Not support one ExchangeStep with multi children yet(multi children can't share one exchange id), we may need to support it later.
        input->setExchangeId(plan_segment->getPlanSegmentOutputs().back()->getExchangeId());
        input->setKeepOrder(step->needKeepOrder());
        input->setStable(step->getSchema().getBucketExpr() != nullptr);
        inputs.push_back(input);

        if (auto * output = dynamic_cast<PlanSegmentOutput *>(plan_segment->getPlanSegmentOutput().get()))
        {
            output->setKeepOrder(step->needKeepOrder());
        }

        split_context.children.emplace_back(plan_segment);
    }
    QueryPlanStepPtr remote_step = std::make_unique<RemoteExchangeSourceStepExt>(inputs, step->getOutputStream(), is_add_totals, is_add_extremes);
    remote_step->setStepDescription(step->getStepDescription());
    QueryPlanExt::Node remote_node{.step = std::move(remote_step), .children = {}, .id = node->id};
    plan_segment_context.query_plan.addNode(std::move(remote_node));
    split_context.scalable &= step->isScalable();
    return plan_segment_context.query_plan.getLastNode();
}

PlanSegmentResult PlanSegmentVisitor::visitCTERefStepExtNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context)
{
    auto * step = dynamic_cast<CTERefStepExt *>(node->step.get());
    auto * cte_node = cte_nodes.at(step->getId());
    Block header = cte_node->step->getOutputStream().header;

    PlanSegment * plan_segment;
    ExchangeStepExt * exchange_step = nullptr;
    if (!cte_plan_segments.contains(step->getId()))
    {
        if ( getQueryPlanStepType(cte_node->step) == QueryPlanStepType::ExchangeStepExt )
        {
            exchange_step = dynamic_cast<ExchangeStepExt *>(cte_node->step.get());
            PlanSegmentVisitorContext child_context{{}, {}, split_context.exchange_id, split_context.hash_func, split_context.params, split_context.is_add_extremes, split_context.is_add_totals, exchange_step->isScalable()};
            plan_segment = createPlanSegment(cte_node->children[0], child_context);
        }
        else
            plan_segment = createPlanSegment(cte_node, split_context);
        cte_plan_segments.emplace(step->getId(), std::make_pair(plan_segment, exchange_step));
    }
    else
    {
        std::tie(plan_segment, exchange_step) = cte_plan_segments.at(step->getId());
        auto output = std::make_shared<PlanSegmentOutput>(*plan_segment->getPlanSegmentOutput());
        output->setExchangeId(split_context.exchange_id++);
        plan_segment->appendPlanSegmentOutput(output);
    }

    std::shared_ptr<PlanSegmentInput> input = std::make_shared<PlanSegmentInput>(header, RIPlanSegment::Enum::IPlanSegment_Enum_EXCHANGE);
    input->setPlanSegmentId(plan_segment->getPlanSegmentId());
    input->setExchangeId(plan_segment->getPlanSegmentOutputs().back()->getExchangeId());
    if (exchange_step)
    {
        input->setShufflekeys(exchange_step->getSchema().getColumns());
        input->setExchangeMode(exchange_step->getExchangeMode());
    }
    else
        input->setExchangeMode(RExchangeMode::LOCAL_NO_NEED_REPARTITION);

    // split_context.inputs.emplace_back(input);
    split_context.children.emplace_back(plan_segment);

    QueryPlanStepPtr remote_step = std::make_unique<RemoteExchangeSourceStepExt>(PlanSegmentInputs{input}, step->getOutputStream(), false, false); // with totals is not expected used in queries with multiple table
    remote_step->setStepDescription(step->getStepDescription());
    QueryPlanExt::Node remote_node{.step = std::move(remote_step), .children = {}, .id = node->id};
    plan_segment_context.query_plan.addNode(std::move(remote_node));
    
    if (!plan_segment_context.context->getOptimizerContext()->getPlanNodeIdAllocator())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't get PlanNodeIdAllocator");

    // add projection to rename symbol
    QueryPlanExt::Node projection_node{
        .step = step->toProjectionStep(),
        .children = {plan_segment_context.query_plan.getLastNode()},
        .id = plan_segment_context.context->getOptimizerContext()->getPlanNodeIdAllocator()->nextId()};
    plan_segment_context.query_plan.addNode(std::move(projection_node));

    return plan_segment_context.query_plan.getLastNode();
}

PlanSegmentResult PlanSegmentVisitor::visitTotalsHavingStepExtNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & context)
{
    context.is_add_totals = true;
    return visitNode(node, context);
}

// PlanSegmentResult PlanSegmentVisitor::visitExtremesStepExtNode(QueryPlanExt::Node * node, PlanSegmentVisitorContext & context)
// {
//     context.is_add_extremes = true;
//     return visitNode(node, context);
// }

PlanSegment * PlanSegmentVisitor::createPlanSegment(QueryPlanExt::Node * node, size_t segment_id, PlanSegmentVisitorContext & split_context)
{
    /**
     * Be careful, after we create a sub_plan, some nodes in the original plan have been deleted and deconstructed.
     * More precisely, nodes that moved to sub_plan are deleted.
     */
    auto all_inputs = findInputs(node);
    split_context.inputs = all_inputs;

    QueryPlanExt sub_plan = plan_segment_context.query_plan.getSubPlan(node);
    auto [cluster_name, parallel] = findClusterAndParallelSize(sub_plan.getRootNode(), split_context);

    auto plan_segment = std::make_unique<PlanSegment>(segment_id, plan_segment_context.query_id, cluster_name);
    plan_segment->setQueryPlan(std::move(sub_plan));
    auto exchange_parallel_size = plan_segment_context.context->getOptimizerContext()->getSettingsRef().exchange_parallel_size;
    plan_segment->setExchangeParallelSize(exchange_parallel_size);

    RIPlanSegment::Enum output_type = segment_id == 0 ? RIPlanSegment::Enum::IPlanSegment_Enum_OUTPUT : RIPlanSegment::Enum::IPlanSegment_Enum_EXCHANGE;

    auto output = std::make_shared<PlanSegmentOutput>(plan_segment->getQueryPlan().getRootNode()->step->getOutputStream().header, output_type);

    output->setShuffleFunctionName(split_context.hash_func);
    output->setShuffleFunctionParams(split_context.params);
    if (output_type == RIPlanSegment::Enum::IPlanSegment_Enum_OUTPUT)
    {
        plan_segment->setParallelSize(1);
        output->setParallelSize(1);
    }
    else
    {
        plan_segment->setParallelSize(parallel);
        if (output->getExchangeMode() == RExchangeMode::GATHER)
            output->setParallelSize(1);
        else
            output->setParallelSize(parallel);
    }
    output->setExchangeParallelSize(exchange_parallel_size);
    output->setExchangeId(split_context.exchange_id++);
    plan_segment->appendPlanSegmentOutput(output);



    auto inputs = findInputs(plan_segment->getQueryPlan().getRootNode());
    if (inputs.empty())
        inputs.push_back(std::make_shared<PlanSegmentInput>(Block(), RIPlanSegment::Enum::IPlanSegment_Enum_UNKNOWN));
    if (unlikely(exchange_parallel_size > 1))
    {
        for (auto & input : inputs)
        {
            if (input->isStable())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "exchange_parallel_size can't be {} when input is stable for segment {} ",
                    exchange_parallel_size,
                    plan_segment->getPlanSegmentId());
            }
            input->setExchangeParallelSize(exchange_parallel_size);
        }
    }
    else
    {
        for (auto & input : inputs)
        {
            input->setExchangeParallelSize(exchange_parallel_size);
        }
    }

    if (inputs[0]->getExchangeMode() == RExchangeMode::GATHER)
        plan_segment->setParallelSize(1);

    plan_segment->appendPlanSegmentInputs(inputs);

    PlanSegmentTree::Node plan_segment_node{.plan_segment = std::move(plan_segment)};
    plan_segment_context.plan_segment_tree->addNode(std::move(plan_segment_node));
    return plan_segment_context.plan_segment_tree->getLastNode()->getPlanSegment();
}

PlanSegment * PlanSegmentVisitor::createPlanSegment(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context)
{
    size_t segment_id = plan_segment_context.getSegmentId();
    auto * result_node = VisitorUtil::accept(node, *this, split_context);
    if (!result_node)
        result_node = node;

    return createPlanSegment(result_node, segment_id, split_context);
}

PlanSegmentInputs PlanSegmentVisitor::findInputs(QueryPlanExt::Node * node)
{
    if (!node)
        return {};

    if (auto * join_step = dynamic_cast<JoinStep *>(node->step.get()))
    {
        PlanSegmentInputs inputs;
        for (auto & child : node->children)
        {
            auto child_input = findInputs(child);
            inputs.insert(inputs.end(), child_input.begin(), child_input.end());
        }
        return inputs;
    }
    else if (auto * remote_step = dynamic_cast<RemoteExchangeSourceStepExt *>(node->step.get()))
    {
        return remote_step->getInput();
    }
    else if (auto * source_step = dynamic_cast<PlanSegmentSourceStepExt *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(source_step->getOutputStream().header, RIPlanSegment::Enum::IPlanSegment_Enum_SOURCE);
        input->setStorageID(source_step->getStorageID());
        return {input};
    }
    else if (auto * table_scan_step = dynamic_cast<TableScanStepExt *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(table_scan_step->getOutputStream().header, RIPlanSegment::Enum::IPlanSegment_Enum_SOURCE);
        input->setStorageID(table_scan_step->getStorageID());
        return {input};
    }
    else if (auto * read_nothing = dynamic_cast<ReadNothingStep *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(read_nothing->getOutputStream().header, RIPlanSegment::Enum::IPlanSegment_Enum_SOURCE);
        return {input};
    }
    else if (auto * values = dynamic_cast<ValuesStepExt *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(values->getOutputStream().header, RIPlanSegment::Enum::IPlanSegment_Enum_SOURCE);
        return {input};
    }
    else if (auto * read_row_count_step = dynamic_cast<ReadStorageRowCountStepExt *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(read_row_count_step->getOutputStream().header, RIPlanSegment::Enum::IPlanSegment_Enum_SOURCE);
        return {input};
    }
    else
    {
        PlanSegmentInputs inputs;
        for (auto & child : node->children)
        {
            auto sub_input = findInputs(child);
            inputs.insert(inputs.end(), sub_input.begin(), sub_input.end());
        }
        return inputs;
    }
}

std::pair<String, size_t> PlanSegmentVisitor::findClusterAndParallelSize(QueryPlanExt::Node * node, PlanSegmentVisitorContext & split_context)
{
    bool input_has_table = false;
    for (auto & input : split_context.inputs)
    {
        if (input->getPlanSegmentType() == RIPlanSegment::Enum::IPlanSegment_Enum_SOURCE)
        {
            input_has_table = true;
            break;
        }
    }

    auto partitionings = SourceNodeFinder::find(node, cte_nodes, *plan_segment_context.context);


    switch (partitionings[0])
    {
        case Partitioning::Handle::COORDINATOR:
            return {"", 1}; // dispatch to coordinator if server is empty
        case Partitioning::Handle::SINGLE:
            return {plan_segment_context.cluster_name, 1};
        case Partitioning::Handle::FIXED_PASSTHROUGH:
            for (auto & input : split_context.inputs)
            {
                if (input->getExchangeMode() == RExchangeMode::LOCAL_NO_NEED_REPARTITION)
                {
                    size_t input_segment_id = input->getPlanSegmentId();
                    for (auto & child_segment : split_context.children)
                        if (input_segment_id == child_segment->getPlanSegmentId())
                            return {plan_segment_context.cluster_name, child_segment->getParallelSize()};
                }
            }
            break;
        case Partitioning::Handle::BUCKET_TABLE:
        case Partitioning::Handle::FIXED_HASH: {
            /// TODO wujianchao now we limit the non-leaf fragments parallel size, we should not limit
            /// if all input are not table type, parallel size should respect distributed_max_parallel_size setting
            size_t max_parallel_size = plan_segment_context.context->getOptimizerContext()->getSettingsRef().distributed_max_parallel_size;
            if (!input_has_table && !split_context.inputs.empty() && split_context.scalable)
            {
                size_t ret = plan_segment_context.shard_number;
                if (max_parallel_size > 0)
                {
                    if (max_parallel_size > 0 && max_parallel_size < ret)
                        ret = max_parallel_size;
                    return {plan_segment_context.cluster_name, ret};
                }
            }
            return {plan_segment_context.cluster_name, plan_segment_context.shard_number};
        }
        default:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown partition for PlanSegmentSplitter");
}

std::vector<Partitioning::Handle> SourceNodeFinder::find(QueryPlanExt::Node * node, CTENodes & cte_nodes, const Context & context)
{
    SourceNodeFinder visitor{cte_nodes};
    std::vector<Partitioning::Handle> result;
    for (auto item : VisitorUtil::accept(node, visitor, context))
    {
        if (item)
            result.emplace_back(item.value());
    }
    return result;
}

std::vector<std::optional<Partitioning::Handle>> SourceNodeFinder::visitNode(QueryPlanExt::Node * node, const Context & context)
{
    std::vector<std::optional<Partitioning::Handle>> result;
    for (const auto & child : node->children)
    {
        auto item = VisitorUtil::accept(child, *this, context);
        result.insert(result.end(), item.begin(), item.end());
    }
    return result;
}

std::vector<std::optional<Partitioning::Handle>> SourceNodeFinder::visitValuesStepExtNode(QueryPlanExt::Node *, const Context &)
{
    return {{Partitioning::Handle::SINGLE}};
}

std::vector<std::optional<Partitioning::Handle>> SourceNodeFinder::visitReadNothingStepNode(QueryPlanExt::Node *, const Context &)
{
    return {{Partitioning::Handle::SINGLE}};
}

std::vector<std::optional<Partitioning::Handle>> SourceNodeFinder::visitReadStorageRowCountStepExtNode(QueryPlanExt::Node *, const Context &)
{
    return {{Partitioning::Handle::FIXED_HASH}};
}

std::vector<std::optional<Partitioning::Handle>> SourceNodeFinder::visitTableScanStepExtNode(QueryPlanExt::Node * node, const Context &)
{
    // TODO wujianchao any table who does not support distributed reading?
    return {{Partitioning::Handle::FIXED_HASH}};
}

std::vector<std::optional<Partitioning::Handle>> SourceNodeFinder::visitRemoteExchangeSourceStepExtNode(QueryPlanExt::Node * node, const Context &)
{
    const auto * source_step = dynamic_cast<RemoteExchangeSourceStepExt *>(node->step.get());
    switch (source_step->getInput()[0]->getExchangeMode())
    {
        case RExchangeMode::GATHER:
            return {{Partitioning::Handle::SINGLE}};
        case RExchangeMode::BROADCAST:
        case RExchangeMode::REPARTITION:
            return {{Partitioning::Handle::FIXED_HASH}};
        case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
            return {{Partitioning::Handle::FIXED_PASSTHROUGH}};
        case RExchangeMode::BUCKET_REPARTITION:
            return {{Partitioning::Handle::FIXED_HASH}};
        case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            return {{Partitioning::Handle::FIXED_PASSTHROUGH}};
        case RExchangeMode::UNKNOWN:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange mode");
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange mode");
    }
}

std::vector<std::optional<Partitioning::Handle>> SourceNodeFinder::visitExchangeStepExtNode(QueryPlanExt::Node * node, const Context & context)
{
    const auto * source_step = dynamic_cast<ExchangeStepExt *>(node->step.get());
    switch (source_step->getExchangeMode())
    {
        case RExchangeMode::GATHER:
            return {{Partitioning::Handle::SINGLE}};
        case RExchangeMode::BROADCAST:
        case RExchangeMode::REPARTITION:
            return {{Partitioning::Handle::FIXED_HASH}};
        case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
        case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            return VisitorUtil::accept(node->children[0], *this, context);
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange mode");
    }
}


std::vector<std::optional<Partitioning::Handle>> SourceNodeFinder::visitCTERefStepExtNode(QueryPlanExt::Node * node, const Context & context)
{
    auto * step = dynamic_cast<CTERefStepExt *>(node->step.get());
    auto * cte_node = cte_nodes.at(step->getId());
    return VisitorUtil::accept(cte_node, *this, context);
}

void SetScalable::setScalable(QueryPlanExt::Node * node, CTENodes & cte_nodes, const Context & context)
{
    auto partitionings = SourceNodeFinder::find(node, cte_nodes, context);

    bool scalable = true;
    for (auto partition : partitionings)
    {
        if (partition == Partitioning::Handle::BUCKET_TABLE || partition == Partitioning::Handle::SINGLE)
        {
            scalable = false;
        }
    }

    SetScalable visitor(scalable, cte_nodes);
    std::vector<Partitioning::Handle> result;
    VisitorUtil::accept(node, visitor, context);
}

Void SetScalable::visitNode(QueryPlanExt::Node * node, const Context & context)
{
    for (const auto & child : node->children)
    {
        VisitorUtil::accept(child, *this, context);
    }
    return {};
}


Void SetScalable::visitExchangeStepExtNode(QueryPlanExt::Node * node, const Context & context)
{
    auto * source_step = dynamic_cast<ExchangeStepExt *>(node->step.get());
    source_step->setScalable(scalable);
    setScalable(node->children[0], cte_nodes, context);
    return {};
}

Void SetScalable::visitCTERefStepExtNode(QueryPlanExt::Node * node, const Context & context)
{
    auto * step = dynamic_cast<CTERefStepExt *>(node->step.get());
    auto * cte_node = cte_nodes.at(step->getId());
    return VisitorUtil::accept(cte_node, *this, context);
}

std::vector<size_t> ParallelSizeChecker::visitNode(QueryPlanExt::Node * node, const Context & context)
{
    std::vector<size_t> result;
    for (const auto & child : node->children)
    {
        auto item = VisitorUtil::accept(child, *this, context);
        result.insert(result.end(), item.begin(), item.end());
    }
    return result;
}

std::vector<size_t> ParallelSizeChecker::visitValuesStepExtNode(QueryPlanExt::Node *, const Context &)
{
    return {1};
}

std::vector<size_t> ParallelSizeChecker::visitReadNothingStepNode(QueryPlanExt::Node *, const Context &)
{
    return {1};
}

std::vector<size_t> ParallelSizeChecker::visitTableScanStepExtNode(QueryPlanExt::Node * node, const Context & context)
{
    auto * source_step = dynamic_cast<TableScanStepExt *>(node->step.get());
    if (source_step->getStorage()->isMergeTree())
        return {shard_number};
    if (context.getOptimizerContext()->getSettingsRef().enable_memory_catalog)
        if (auto memory_tree = dynamic_pointer_cast<StorageMemory>(source_step->getStorage()))
            return {shard_number};
    // if source node is not cnch table, schedule to coordinator. eg, system tables.
    return {1};
}

std::vector<size_t> ParallelSizeChecker::visitReadStorageRowCountStepExtNode(QueryPlanExt::Node *, const Context &)
{
    return {shard_number};
}

std::vector<size_t> ParallelSizeChecker::visitRemoteExchangeSourceStepExtNode(QueryPlanExt::Node * node, const Context &)
{
    const auto * source_step = dynamic_cast<RemoteExchangeSourceStepExt *>(node->step.get());
    for (const auto & input : source_step->getInput())
    {
        switch (input->getExchangeMode())
        {
            case RExchangeMode::GATHER:
                return {segment->getParallelSize()};
            case RExchangeMode::BROADCAST:
                continue;
            case RExchangeMode::REPARTITION:
                return {segment->getParallelSize()};
            case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
            case RExchangeMode::LOCAL_MAY_NEED_REPARTITION: {
                for (auto & child : children_segments)
                {
                    if (child->getPlanSegmentId() == source_step->getInput()[0]->getPlanSegmentId())
                    {
                        return {child->getParallelSize()};
                    }
                }
                break;
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange mode");
        }
    }
    return {};
}


}
