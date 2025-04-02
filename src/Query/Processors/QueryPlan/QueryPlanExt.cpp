

#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <cstddef>
#include <stack>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

// ENUM_WITH_PROTO_CONVERTER(
//     QueryPlanMode, // enum name
//     Protos::QueryPlan::PlanMode, // protobuf enum message
//     (TreeLike, 1),
//     (Flatten, 2));

QueryPlanExt::QueryPlanExt() = default;
QueryPlanExt::~QueryPlanExt() = default;
QueryPlanExt::QueryPlanExt(QueryPlanExt &&) noexcept = default;
QueryPlanExt & QueryPlanExt::operator=(QueryPlanExt &&) noexcept = default;

QueryPlanExt::QueryPlanExt(PlanNodePtr root_, PlanNodeIdAllocatorPtr id_allocator_)
    : plan_node(std::move(root_)), id_allocator(std::move(id_allocator_))
{
}

QueryPlanExt::QueryPlanExt(PlanNodePtr root_, CTEInfo cte_info_, PlanNodeIdAllocatorPtr id_allocator_)
    : plan_node(std::move(root_)), cte_info(std::move(cte_info_)), id_allocator(std::move(id_allocator_))
{
}

void QueryPlanExt::unitePlans(QueryPlanStepPtr step, std::vector<QueryPlanExtPtr> plans)
{
    if (isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite plans because current QueryPlanExt is already initialized");

    const auto & inputs = step->getInputStreams();
    size_t num_inputs = step->getInputStreams().size();
    if (num_inputs != plans.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unite QueryPlanExts using {} because step has different number of inputs. Has {} plans and {} inputs",
            step->getName(),
            plans.size(),
            num_inputs);

    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i].header;
        const auto & plan_header = plans[i]->getCurrentDataStream().header;
        if (!blocksHaveEqualStructure(step_header, plan_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot unite QueryPlanExts using {} because it has incompatible header with plan {} plan header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                plan_header.dumpStructure(),
                step_header.dumpStructure());
    }

    for (auto & plan : plans)
        nodes.splice(nodes.end(), std::move(plan->nodes));

    nodes.emplace_back(Node{.step = std::move(step)});
    root = &nodes.back();

    for (auto & plan : plans)
        root->children.emplace_back(plan->root);

    for (auto & plan : plans)
    {
        max_threads = std::max(max_threads, plan->max_threads);
        interpreter_context.insert(interpreter_context.end(), plan->interpreter_context.begin(), plan->interpreter_context.end());
    }
}

void QueryPlanExt::addStep(QueryPlanStepPtr step, PlanNodes children)
{
    (void)children;
    checkNotCompleted();

    size_t num_input_streams = step->getInputStreams().size();

    if (num_input_streams == 0)
    {
        if (isInitialized())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlanExt because step has no inputs, but QueryPlanExt is already initialized",
                step->getName());

        nodes.emplace_back(Node{.step = std::move(step)});
        root = &nodes.back();
        return;
    }
    else
    {
        if (!isInitialized())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlanExt because step has input, but QueryPlanExt is not initialized",
                step->getName());

        const auto & root_header = root->step->getOutputStream().header;
        const auto & step_header = step->getInputStreams().front().header;
        if (!blocksHaveEqualStructure(root_header, step_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlanExt because it has incompatible header with root step {} root header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                root_header.dumpStructure(),
                step_header.dumpStructure());

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}});
        root = &nodes.back();
        return;
    }
}

void QueryPlanExt::addNode(Node && node_, size_t id)
{
    nodes.emplace_back(std::move(node_));
    node_id_map[&node_] = id;
}

void QueryPlanExt::addRoot(Node && node_, size_t id)
{
    nodes.emplace_back(std::move(node_));
    root = &nodes.back();
    node_id_map[root] = id;
}

size_t QueryPlanExt::getNodeId(const Node * node)
{
    auto it = node_id_map.find(node);
    if (it != node_id_map.end())
        return it->second;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Node not found in map");
}

/**
 * Remove nodes that have no step and children.
 * Refresh children of each node since this childen maybe removed.
 */
void QueryPlanExt::freshPlan()
{
    for (auto it = nodes.begin(); it != nodes.end();)
        if (!it->step && it->children.empty())
            it = nodes.erase(it);
        else
            ++it;

    std::unordered_set<Node *> exists_nodes;

    for (auto & node : nodes)
        exists_nodes.insert(&node);

    for (auto & node : nodes)
    {
        std::vector<Node *> freshed_children;
        for (auto & child : node.children)
            if (exists_nodes.contains(child))
                freshed_children.push_back(child);
        node.children.swap(freshed_children);
    }
}

/**
 * Be careful, after we create a sub_plan, some nodes in the original plan have been deleted and deconstructed.
 * More precisely， nodes that moved to sub_plan are deleted.
 */
QueryPlanExt QueryPlanExt::getSubPlan(QueryPlan::Node * node_)
{
    QueryPlanExt sub_plan;

    std::stack<QueryPlan::Node *> plan_nodes;
    sub_plan.addRoot(Node{.step = std::move(node_->step), .children = node_->children}, getNodeId(node_));
    plan_nodes.push(sub_plan.getRoot());
    sub_plan.setResetStepId(reset_step_id);

    while (!plan_nodes.empty())
    {
        auto * current = plan_nodes.top();
        plan_nodes.pop();

        std::vector<Node *> result_children;
        for (auto & child : current->children)
        {
            sub_plan.addNode(Node{.step = std::move(node_->step), .children = child->children}, getNodeId(child));
            result_children.push_back(sub_plan.getLastNode());
            plan_nodes.push(sub_plan.getLastNode());
        }
        current->children.swap(result_children);
    }

    freshPlan();

    return sub_plan;
}

QueryPipelineBuilderPtr QueryPlanExt::buildQueryPipeline(
    const QueryPlanOptimizationSettings & optimization_settings, const BuildQueryPipelineSettings & build_pipeline_settings)
{
    checkInitialized();
    optimize(optimization_settings);

    struct Frame
    {
        Node * node = {};
        QueryPipelineBuilders pipelines;
    };

    QueryPipelineBuilderPtr last_pipeline;

    std::stack<Frame> stack;
    stack.push(Frame{.node = root, .pipelines = {}});
    Stopwatch watch;
    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (last_pipeline)
        {
            frame.pipelines.emplace_back(std::move(last_pipeline));
            last_pipeline = nullptr;
        }

        size_t next_child = frame.pipelines.size();
        if (next_child == frame.node->children.size())
        {
            bool limit_max_threads = frame.pipelines.empty();
            try
            {
                last_pipeline = frame.node->step->updatePipeline(std::move(frame.pipelines), build_pipeline_settings);
                // updatePipelineStepInfo(last_pipeline, frame.node->step, getNodeId(frame.node));
            }
            catch (const Exception & e) /// Typical for an incorrect username, password, or address.
            {
                LOG_ERROR(log, "Build pipeline error {}", e.what());
                throw;
            }

            if (limit_max_threads && max_threads)
                last_pipeline->limitMaxThreads(max_threads);

            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child], .pipelines = {}});
    }

    // TODO: add context in QueryPipelineBuilder (use globalContext instead now)
    // for (auto & context : interpreter_context)
    //     last_pipeline->addInterpreterContext(std::move(context));

    LOG_DEBUG(log, "Build pipeline takes: {}ms", watch.elapsedMilliseconds());
    return last_pipeline;
}

void QueryPlanExt::updatePipelineStepInfo(QueryPipelineBuilderPtr & pipeline_ptr, QueryPlanStepPtr & step, size_t step_id)
{
    // auto * source_step = dynamic_cast<ISourceStep *>(step.get());
    // TODO: add step_id in IProcessor
    // for (const auto & processor : pipeline_ptr->getProcessors())
    //     if (processor->getStepId() == -1 || source_step)
    //         processor->setStepId(step_id);
}

Pipe QueryPlanExt::convertToPipe(
    const QueryPlanOptimizationSettings & optimization_settings, const BuildQueryPipelineSettings & build_pipeline_settings)
{
    if (!isInitialized())
        return {};

    if (isCompleted())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert completed QueryPlan to Pipe");

    QueryPlanResourceHolder resources;
    return QueryPipelineBuilder::getPipe(std::move(*buildQueryPipeline(optimization_settings, build_pipeline_settings)), resources);
}

void QueryPlanExt::addInterpreterContext(std::shared_ptr<Context> context)
{
    interpreter_context.emplace_back(std::move(context));
}

static void explainPipelineStep(IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings)
{
    settings.out << String(settings.offset, settings.indent_char) << "(" << step.getName() << ")";
    if (dynamic_cast<TableScanStepExt *>(&step))
        settings.out << " # " << step.getStepDescription();
    settings.out << "\n";
    size_t current_offset = settings.offset;
    step.describePipeline(settings);
    if (current_offset == settings.offset)
        settings.offset += settings.indent;
}

void QueryPlanExt::explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options) const
{
    checkInitialized();

    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        size_t offset = 0;
        bool is_description_printed = false;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (!frame.is_description_printed)
        {
            settings.offset = frame.offset;
            explainPipelineStep(*frame.node->step, settings);
            frame.offset = settings.offset;
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child], frame.offset});
            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

// TODO: implement
// handle when plan is tree-like, i.e., plan_node + cte_info
// void QueryPlanExt::toProto(Protos::QueryPlan & proto) const
// {
//     if (plan_node)
//     {
//         proto.set_mode(QueryPlanModeConverter::toProto(QueryPlanMode::TreeLike));
//         toProtoTreeLike(proto);
//     }
//     else if (root)
//     {
//         proto.set_mode(QueryPlanModeConverter::toProto(QueryPlanMode::Flatten));
//         toProtoFlatten(proto);
//     }
//     else
//     {
//         throw Exception("Invalid QueryPlan", ErrorCodes::LOGICAL_ERROR);
//     }
// }

// TODO: implement
// void QueryPlanExt::fromProto(const Protos::QueryPlan & proto)
// {
//     auto mode = QueryPlanModeConverter::fromProto(proto.mode());
//     switch (mode)
//     {
//         case QueryPlanMode::TreeLike:
//             this->fromProtoTreeLike(proto);
//             break;
//         case QueryPlanMode::Flatten:
//             this->fromProtoFlatten(proto);
//             break;
//         default: {
//             throw Exception("Invalid QueryPlan Proto", ErrorCodes::LOGICAL_ERROR);
//         }
//     }
// }

// TODO: implement
// handle when plan is flatten, i.e., root + nodes + cte_nodes
// void QueryPlanExt::toProtoFlatten(Protos::QueryPlan & proto) const
// {
//     if (!root)
//         throw Exception("QueryPlan::toProtoFlatten() failed", ErrorCodes::LOGICAL_ERROR);

//     if (reset_step_id)
//     {
//         size_t id = 0;
//         for (const auto & node : nodes)
//             node.id = id++; // this is mutable field
//     }

//     for (const auto & node : nodes)
//     {
//         auto id = node.id;
//         auto & node_proto = (*proto.mutable_plan_nodes())[id];
//         node_proto.set_plan_id(id);
//         serializeQueryPlanStepToProto(node.step, *node_proto.mutable_step());
//         for (const auto & child : node.children)
//             node_proto.add_children(child->id);
//     }

//     proto.set_root_id(root->id);
// }

// TODO: implement
// void QueryPlanExt::fromProtoFlatten(const Protos::QueryPlan & proto)
// {
//     std::unordered_map<size_t, Node *> id_to_node;
//     const auto & id_to_node_proto = proto.plan_nodes();

//     auto context = !interpreter_context.empty() ? interpreter_context.back() : nullptr;

//     for (const auto & [id, node_proto] : id_to_node_proto)
//     {
//         if (node_proto.plan_id() != id)
//             throw Exception("Invalid Proto", ErrorCodes::LOGICAL_ERROR);
//         auto step = deserializeQueryPlanStepFromProto(node_proto.step(), context);
//         nodes.emplace_back(Node{step, {}, id});
//         id_to_node[id] = &nodes.back();
//     }

//     for (auto & node : nodes)
//     {
//         auto id = node.id;
//         for (auto child_id : id_to_node_proto.at(id).children())
//         {
//             auto * child = id_to_node[child_id];
//             node.children.emplace_back(child);
//         }
//     }

//     auto root_id = proto.root_id();
//     root = id_to_node[root_id];
// }

// TODO: implement
// support optimizer mode
// void QueryPlanExt::toProtoTreeLike(Protos::QueryPlan & proto) const
// {
//     if (!plan_node)
//         throw Exception("QueryPlan::toProtoTreeLike() failed", ErrorCodes::LOGICAL_ERROR);

//     std::queue<PlanNodePtr> queue;
//     queue.push(plan_node);

//     proto.set_root_id(plan_node->getId());
//     for (const auto & [cte_id, ptr] : this->cte_info.getCTEs())
//     {
//         queue.push(ptr);
//         (*proto.mutable_cte_id_mapping())[cte_id] = ptr->getId();
//     }

//     while (!queue.empty())
//     {
//         auto cur = queue.front();

//         auto plan_id = cur->getId();
//         auto * cur_pb = &(*proto.mutable_plan_nodes())[plan_id];

//         cur_pb->set_plan_id(plan_id);
//         serializeQueryPlanStepToProto(cur->getStep(), *cur_pb->mutable_step());
//         for (const auto & child : cur->getChildren())
//         {
//             queue.push(child);
//             cur_pb->add_children(child->getId());
//         }

//         queue.pop();
//     }
// }

// TODO: implement
// void QueryPlanExt::fromProtoTreeLike(const Protos::QueryPlan & proto)
// {
//     std::unordered_map<Int64, PlanNodePtr> id_to_plan;
//     ContextPtr context = interpreter_context.empty() ? nullptr : interpreter_context.back();
//     for (const auto & [plan_id, plan_pb] : proto.plan_nodes())
//     {
//         if (plan_pb.plan_id() != plan_id)
//             throw Exception("Invalid Proto", ErrorCodes::LOGICAL_ERROR);
//         auto step = deserializeQueryPlanStepFromProto(plan_pb.step(), context);
//         auto plan = PlanNodeBase::createPlanNode(plan_id, step);
//         id_to_plan[plan_id] = std::move(plan);
//     }

//     // set children
//     for (const auto & [plan_id, plan_pb] : proto.plan_nodes())
//     {
//         PlanNodes children;
//         for (auto child_id : plan_pb.children())
//             children.emplace_back(id_to_plan.at(child_id));
//         id_to_plan.at(plan_id)->replaceChildren(children);
//     }

//     for (auto [cte_id, plan_id] : proto.cte_id_mapping())
//         this->cte_info.add(cte_id, id_to_plan.at(plan_id));
//     auto root_id = proto.root_id();
//     this->setPlanNode(id_to_plan.at(root_id));
// }

std::set<StorageID> QueryPlanExt::allocateLocalTable(ContextPtr context)
{
    std::set<StorageID> res;
    for (const auto & node : nodes)
    {
        if (getQueryPlanStepType(node.step) == QueryPlanStepType::TableScanStepExt)
        {
            auto * table_scan = dynamic_cast<TableScanStepExt *>(node.step.get());
            /// have to get storage_id before allocate to get original storage_id
            /// instead of storage_id in cloud
            res.insert(table_scan->getStorageID());
            table_scan->allocate(context);
        }
        // else if (node.step->getType() == IQueryPlanStep::Type::TableWrite)
        // {
        //     auto write_step = dynamic_cast<TableWriteStep *>(node.step.get());
        //     write_step->allocate(context);
        // }
    }
    return res;
}

UInt32 QueryPlanExt::getPlanNodeCount(PlanNodePtr node)
{
    UInt32 size = 1;
    for (auto & child : node->getChildren())
        size += getPlanNodeCount(child);
    return size;
}

static PlanNodePtr copyPlanNode(const PlanNodePtr & plan, ContextMutablePtr & context)
{
    PlanNodes children;
    for (auto & child : plan->getChildren())
        children.emplace_back(copyPlanNode(child, context));
    // TODO: implement PlanNode
    // return PlanNodeBase::createPlanNode(plan->getId(), plan->getStep()->copy(context), children, plan->getStatistics());
    return {};
}

QueryPlanExtPtr QueryPlanExt::copy(ContextMutablePtr context)
{
    auto copy_plan_node = copyPlanNode(plan_node, context);
    CTEInfo copy_cte_info;
    for (const auto & [cte_id, cte_def] : cte_info.getCTEs())
        copy_cte_info.add(cte_id, copyPlanNode(cte_def, context));
    return std::make_unique<QueryPlanExt>(copy_plan_node, copy_cte_info, context->getOptimizerContext()->getPlanNodeIdAllocator());
}

}
