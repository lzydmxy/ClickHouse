#include <Query/Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Query/Executor/QueryMPPCoordinator.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <google/protobuf/util/json_util.h>
#include <Query/Optimizer/PlanOptimizer.h>
#include <Query/Optimizer/JoinOrderUtils.h>
#include <Query/Analyzer/QueryRewriter.h>
#include <Query/Planner/PlannerExt.h>
#include <Query/Analyzer/QueryAnalyzer.h>
#include <Query/Processors/QueryPlan/FinalSampleStepExt.h>
#include <Query/Planner/GraphvizPrinter.h>
#include <Interpreters/InterpreterFactory.h>
#include <Storages/StorageDistributed.h>


namespace ProfileEvents
{
    extern const Event QueryRewriterTime;
    extern const Event QueryAnalyzerTime;
    extern const Event QueryPlannerTime;
    extern const Event QueryOptimizerTime;
    extern const Event PlanSegmentSplitterTime;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_MANY_PLAN_SEGMENTS;
    extern const int OPTIMIZER_NONSUPPORT;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_PLAN_SEGMENT;
    extern const int PLAN_CACHE_NOT_USED;
    extern const int BAD_PREPARED_PARAMETER;
}

Block InterpreterSelectQueryUseOptimizer::getSampleBlock(const ASTPtr & query,
    const ContextPtr & context,
    const SelectQueryOptions & select_query_options)
{
    auto select_query_options_copy = select_query_options;
    select_query_options_copy.only_analyze = true;
    InterpreterSelectQueryUseOptimizer interpreter(query, context->getQueryContext(), select_query_options_copy);

    return interpreter.getSampleBlock();
}

Block InterpreterSelectQueryUseOptimizer::getSampleBlock()
{
    if (!block)
    {
        auto query_plan = getQueryPlan(true);
    }

    return block;
}

namespace
{
    struct RemoveSettings
    {
        using TypeToVisit = ASTSelectQuery;

        void visit(ASTSelectQuery & select_query, ASTPtr &) const
        {
            select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, nullptr);
        }
    };

    using RemoveSettingsVisitor = InDepthNodeVisitor<OneTypeMatcher<RemoveSettings>, true>;
}

InterpreterSelectQueryUseOptimizer::InterpreterSelectQueryUseOptimizer(
    const ASTPtr & query_ptr_,
    PlanNodePtr sub_plan_ptr_,
    CTEInfo cte_info_,
    ContextMutablePtr & context_,
    const SelectQueryOptions & options_)
    : query_ptr(query_ptr_ ? query_ptr_ : nullptr)
    , sub_plan_ptr(sub_plan_ptr_)
    , cte_info(std::move(cte_info_))
    , context(context_)
    , options(options_)
    , log(getLogger("InterpreterSelectQueryUseOptimizer"))
{
    interpret_sub_query = !!sub_plan_ptr;
}

QueryPlanExtPtr InterpreterSelectQueryUseOptimizer::getQueryPlan(bool skip_optimize)
{
    // When interpret sub query, reuse context info, e.g. PlanNodeIdAllocator, SymbolAllocator.
    if (interpret_sub_query)
    {
        QueryPlanExtPtr sub_query_plan = std::make_unique<QueryPlanExt>(sub_plan_ptr, cte_info, context->getOptimizerContext()->getPlanNodeIdAllocator());
        PlanOptimizer::optimize(*sub_query_plan, context);
        return sub_query_plan;
    }

    AnalysisPtr analysis;
    QueryPlanExtPtr query_plan;
    UInt128 query_hash;
    // not cache internal query
    bool enable_plan_cache = !options.is_internal && PlanCacheManager::enableCachePlan(query_ptr, context);
    // remove settings to avoid plan cache miss
    RemoveSettings remove_settings_data;
    RemoveSettingsVisitor(remove_settings_data).visit(query_ptr);

    if (!query_plan || context->getOptimizerContext()->getSettingsRef().iterative_optimizer_timeout == 999999)
    {
        buildQueryPlan(query_plan, analysis, skip_optimize);
        GraphvizPrinter::printLogicalPlan(*query_plan, context, "3997_build_plan_from_query");
        fillContextQueryAccessInfo(context, analysis);
        if (enable_plan_cache && query_hash && query_plan)
        {
            if (PlanCacheManager::addPlanToCache(query_hash, query_plan, analysis, context))
               LOG_INFO(log, "plan cache added");
        }
    }

    if (query_plan->getPlanNode())
        block = query_plan->getPlanNode()->getCurrentDataStream().header;
    GraphvizPrinter::printLogicalPlan(*query_plan, context, "3999_final_plan");
    query_plan->addInterpreterContext(context);
    LOG_DEBUG(log, "join order {}", JoinOrderUtils::getJoinOrder(*query_plan));
    return query_plan;
}

std::pair<PlanSegmentTreeUniqPtr, std::set<StorageID>> InterpreterSelectQueryUseOptimizer::getPlanSegment()
{
    Stopwatch stage_watch, total_watch;
    total_watch.start();
    setUnsupportedSettings(context);
    QueryPlanExtPtr query_plan = getQueryPlan();

    query_plan->setResetStepId(false);
    stage_watch.start();
    QueryPlanExt plan = PlanNodeToNodeVisitor::convert(*query_plan);

    LOG_DEBUG(log, "optimizer stage run time: plan normalize, {} ms", stage_watch.elapsedMilliseconds());
    stage_watch.restart();

    PlanSegmentTreeUniqPtr plan_segment_tree = std::make_unique<PlanSegmentTree>();
    ClusterInfoContext cluster_info_context{.query_plan = *query_plan, .context = context, .plan_segment_tree = plan_segment_tree};
    PlanSegmentContext plan_segment_context = ClusterInfoFinder::find(*query_plan, cluster_info_context);

    stage_watch.restart();
    std::set<StorageID> used_storage_ids = plan.allocateLocalTable(context);

    PlanSegmentSplitter::split(plan, plan_segment_context);
    context->getOptimizerContext()->logOptimizerProfile(log, "Optimizer total run time: ", "PlanSegment build {} ms", stage_watch.elapsedMilliseconds());
    ProfileEvents::increment(ProfileEvents::PlanSegmentSplitterTime, stage_watch.elapsedMilliseconds());

    resetFinalSampleSize(plan_segment_tree);
    setPlanSegmentInfoForExplainAnalyze(plan_segment_tree, context);
    GraphvizPrinter::printPlanSegment(plan_segment_tree, context);
    context->getOptimizerContext()->logOptimizerProfile(log, "Optimizer total run time: ", "Optimizer Total {} ms", total_watch.elapsedMilliseconds());

    if (context->getOptimizerContext()->getSettingsRef().log_segment_profiles)
    {
        segment_profiles = std::make_shared<std::vector<String>>();
        for (auto & node : plan_segment_tree->getNodes())
            segment_profiles->emplace_back(PlanSegmentDescription::getPlanSegmentDescription(node.plan_segment, true)->jsonPlanSegmentDescriptionAsString({}));
    }

    return std::make_pair(std::move(plan_segment_tree), std::move(used_storage_ids));
}

BlockIO InterpreterSelectQueryUseOptimizer::execute()
{
    if (!plan_segment_tree_ptr)
    {
        std::pair<PlanSegmentTreeUniqPtr, std::set<StorageID>> plan_segment_tree_and_used_storage_ids = getPlanSegment();
        plan_segment_tree_ptr = std::move(plan_segment_tree_and_used_storage_ids.first);
    }
    size_t plan_segment_num = plan_segment_tree_ptr->getNodes().size();


    UInt64 max_plan_segment_num = context->getOptimizerContext()->getSettingsRef().max_plan_segment_num;
    if (max_plan_segment_num != 0 && plan_segment_num > max_plan_segment_num)
        throw Exception(ErrorCodes::TOO_MANY_PLAN_SEGMENTS,
                "query_id:{} plan_segments size {} exceed max_plan_segment_num {}",
                context->getCurrentQueryId(),
                plan_segment_num,
                max_plan_segment_num);
    const auto coordinator = std::make_shared<QueryMPPCoordinator>(
        context->getOptimizerContext()->getClusterName(), std::move(plan_segment_tree_ptr), context, QueryMPPOptions());
    BlockIO res = coordinator->execute();
    return res;
}

void InterpreterSelectQueryUseOptimizer::setPlanSegmentInfoForExplainAnalyze(PlanSegmentTreeUniqPtr & plan_segment_tree, ContextMutablePtr context)
{
    if (context->getOptimizerContext()->getSettingsRef().log_explain_analyze_type == LogExplainAnalyzeType::QUERY_PIPELINE
        || context->getOptimizerContext()->getSettingsRef().log_explain_analyze_type == LogExplainAnalyzeType::AGGREGATED_QUERY_PIPELINE)
    {
        context->setSetting("report_segment_profiles", true);
        for (auto & segment_node : plan_segment_tree->getNodes())
            segment_node.plan_segment->setProfileType(RReportProfileType::Enum::ReportProfileType_Enum_QueryPipeline);
    }
    auto * final_segment = plan_segment_tree->getRoot()->getPlanSegment();
    if (final_segment->getQueryPlan().getRootNode())
    {
        ExplainAnalyzeVisitor explain_visitor;
        VisitorUtil::accept(final_segment->getQueryPlan().getRootNode(), explain_visitor, plan_segment_tree->getNodes());
    }
}

void InterpreterSelectQueryUseOptimizer::resetFinalSampleSize(PlanSegmentTreeUniqPtr & plan_segment_tree)
{
    for (auto & plan_segment : plan_segment_tree->getNodes())
    {
        if (plan_segment.getPlanSegment())
        {
            for (auto & node : plan_segment.getPlanSegment()->getQueryPlan().getNodes())
            {
                if (auto * sample = dynamic_cast<FinalSampleStepExt *>(node.step.get()))
                {
                    size_t sample_size = (sample->getSampleSize() + 1) / plan_segment.getPlanSegment()->getParallelSize();
                    sample->setSampleSize(sample_size);
                }
            }
        }
    }
}

void InterpreterSelectQueryUseOptimizer::fillContextQueryAccessInfo(ContextPtr context, AnalysisPtr & analysis)
{
    if (context->hasQueryContext())
    {
        const auto & used_columns_map = analysis->getUsedColumns();
        for (const auto & [table_ast, storage_analysis] : analysis->getStorages())
        {
            Names required_columns;
            auto storage_id = storage_analysis.storage->getStorageID();
            if (auto it = used_columns_map.find(storage_analysis.storage->getStorageID()); it != used_columns_map.end())
            {
                for (const auto & column : it->second)
                    required_columns.emplace_back(column);
            }
            context->getQueryContext()->addQueryAccessInfo(
                backQuoteIfNeed(storage_id.getDatabaseName()), storage_id.getFullTableName(), required_columns);
        }
    }
}

std::optional<std::set<StorageID>> InterpreterSelectQueryUseOptimizer::getUsedStorageIds()
{
    if (plan_segment_tree_ptr)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot call this getUsedStorageIds twice");
    }

    std::pair<PlanSegmentTreeUniqPtr, std::set<StorageID>> plan_segment_tree_and_used_storage_ids = getPlanSegment();

    plan_segment_tree_ptr = std::move(plan_segment_tree_and_used_storage_ids.first);
    return std::optional<std::set<StorageID>>(std::move(plan_segment_tree_and_used_storage_ids.second));
}

void InterpreterSelectQueryUseOptimizer::setUnsupportedSettings(ContextMutablePtr & context)
{
    if (!context->getSettingsRef().enable_optimizer)
        return;

    SettingsChanges setting_changes;
    context->applySettingsChanges(setting_changes);
}

void InterpreterSelectQueryUseOptimizer::fillQueryPlan(ContextPtr context, QueryPlanExt & query_plan)
{
    WriteBufferFromOwnString buffer;
    Protos::QueryPlanExt plan_pb;
    query_plan.toProto(plan_pb);
    String json_msg;
    google::protobuf::util::JsonPrintOptions pb_options;
    pb_options.preserve_proto_field_names = true;
    pb_options.always_print_primitive_fields = true;
    pb_options.add_whitespace = false;

    auto status = google::protobuf::util::MessageToJsonString(plan_pb, &json_msg, pb_options);
    if (!status.ok())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to convert QueryPlan to json: {}", status.message());
    buffer << json_msg;

    context->getOptimizerContext()->addQueryPlanInfo(buffer.str());
}

void InterpreterSelectQueryUseOptimizer::buildQueryPlan(QueryPlanExtPtr & query_plan, AnalysisPtr & analysis, bool skip_optimize)
{
    context->getOptimizerContext()->createPlanNodeIdAllocator();
    context->getOptimizerContext()->createSymbolAllocator();
    context->getOptimizerContext()->createOptimizerMetrics();

    Stopwatch stage_watch;
    stage_watch.start();
    query_ptr = QueryRewriter().rewrite(query_ptr, context);
    context->getOptimizerContext()->logOptimizerProfile(log, "Optimizer stage run time: ", "Rewrite {} ms", stage_watch.elapsedMilliseconds());
    ProfileEvents::increment(ProfileEvents::QueryRewriterTime, stage_watch.elapsedMilliseconds());

    stage_watch.restart();
    analysis = QueryAnalyzer::analyze(query_ptr, context);
    fillContextQueryAccessInfo(context, analysis);
    context->getOptimizerContext()->logOptimizerProfile(log, "Optimizer stage run time: ", "Analyzer {} ms", stage_watch.elapsedMilliseconds());
    ProfileEvents::increment(ProfileEvents::QueryAnalyzerTime, stage_watch.elapsedMilliseconds());

    stage_watch.restart();

    query_plan = PlannerExt().plan(query_ptr, *analysis, context);
    context->getOptimizerContext()->logOptimizerProfile(log, "Optimizer stage run time: ", "Planning {} ms", stage_watch.elapsedMilliseconds());
    ProfileEvents::increment(ProfileEvents::QueryPlannerTime, stage_watch.elapsedMilliseconds());

    LOG_TRACE(log, "Logical plan before optimize: \n{}", PlanPrinter::textLogicalPlan(*query_plan, context));

    if (!skip_optimize)
    {
        stage_watch.restart();
        PlanOptimizer::optimize(*query_plan, context);
        if (context->getOptimizerContext()->getSettingsRef().log_query_plan)
        {
            fillQueryPlan(context, *query_plan);
        }

        context->getOptimizerContext()->logOptimizerProfile(log, "Optimizer stage run time: ", "Optimizer {} ms", stage_watch.elapsedMilliseconds());
        ProfileEvents::increment(ProfileEvents::QueryOptimizerTime, stage_watch.elapsedMilliseconds());

        LOG_TRACE(log, "Logical plan after optimize: \n{}", PlanPrinter::textLogicalPlan(*query_plan, context));
    }
}

void InterpreterSelectQueryUseOptimizer::logUsedStorageIDs(LoggerPtr log, const std::set<StorageID> & storage_ids)
{
    LOG_DEBUG(log, "StorageIDs:");
    for (auto & storage_id : storage_ids)
        LOG_DEBUG(log, "StorageID {}", storage_id.getNameForLogs());
}

QueryPlanExt PlanNodeToNodeVisitor::convert(QueryPlanExt & query_plan)
{
    QueryPlanExt plan;
    PlanNodeToNodeVisitor visitor(plan);
    Void c;
    auto * root = VisitorUtil::accept(query_plan.getPlanNode(), visitor, c);
    plan.setRoot(root);

    for (const auto & cte : query_plan.getCTEInfo().getCTEs())
        plan.getCTENodes().emplace(cte.first, VisitorUtil::accept(cte.second, visitor, c));
    return plan;
}

QueryPlanExt::Node * PlanNodeToNodeVisitor::visitPlanNode(PlanNodeBase & node, Void & c)
{
    if (node.getChildren().empty())
    {
        auto res = QueryPlanExt::Node{.step = std::const_pointer_cast<IQueryPlanStep>(node.getStep()), .children = {}, .id = node.getId()};
        node.setStep(res.step);
        plan.addNode(std::move(res));
        return plan.getLastNode();
    }

    std::vector<QueryPlanExt::Node *> children;
    for (const auto & item : node.getChildren())
    {
        auto * child = VisitorUtil::accept(*item, *this, c);
        children.emplace_back(child);
    }
    QueryPlanExt::Node query_plan_node{
        .step = std::const_pointer_cast<IQueryPlanStep>(node.getStep()), .children = children, .id = node.getId()};
    node.setStep(query_plan_node.step);
    plan.addNode(std::move(query_plan_node));
    return plan.getLastNode();
}

PlanSegmentContext ClusterInfoFinder::find(QueryPlanExt &, ClusterInfoContext & cluster_info_context)
{
    PlanSegmentContext plan_segment_context{
        .context = cluster_info_context.context,
        .query_plan = cluster_info_context.query_plan,
        .query_id = cluster_info_context.context->getCurrentQueryId(),
        .shard_number = cluster_info_context.context->getOptimizerContext()->getWorkerSize(),
        .cluster_name = cluster_info_context.context->getOptimizerContext()->getClusterName(),
        .plan_segment_tree = cluster_info_context.plan_segment_tree.get()};
    return plan_segment_context;
}

void ExplainAnalyzeVisitor::visitExplainAnalyzeNode(QueryPlanExt::Node * node, PlanSegmentTree::Nodes & nodes)
{
    auto * explain = dynamic_cast<ExplainAnalyzeStepExt *>(node->step.get());
    PlanSegmentDescriptions plan_segment_descriptions;
    bool record_plan_detail = explain->getSetting().json && (explain->getKind() != ASTExplainQueryExt::ExplainKindExt::PipelineAnalyze);
    for (auto & segment_node : nodes)
    {
        if (explain->getKind() == ASTExplainQueryExt::ExplainKindExt::DistributedAnalyze
            || explain->getKind() == ASTExplainQueryExt::ExplainKindExt::LogicalAnalyze)
            segment_node.plan_segment->setProfileType(RReportProfileType::Enum::ReportProfileType_Enum_QueryPlanExt);
        else if (explain->getKind() == ASTExplainQueryExt::ExplainKindExt::PipelineAnalyze)
            segment_node.plan_segment->setProfileType(RReportProfileType::Enum::ReportProfileType_Enum_QueryPipeline);

        if (explain->getKind() == ASTExplainQueryExt::ExplainKindExt::DistributedAnalyze
            || explain->getKind() == ASTExplainQueryExt::ExplainKindExt::PipelineAnalyze)
            plan_segment_descriptions.emplace_back(
                PlanSegmentDescription::getPlanSegmentDescription(segment_node.plan_segment, record_plan_detail));
    }

    explain->setPlanSegmentDescriptions(plan_segment_descriptions);
}

void ExplainAnalyzeVisitor::visitNode(QueryPlanExt::Node * node, PlanSegmentTree::Nodes & nodes)
{
    for (const auto & child : node->children)
        VisitorUtil::accept(child, *this, nodes);
}

void registerInterpreterSelectQueryUseOptimizer(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSelectQueryUseOptimizer>(args.query, args.context, SelectQueryOptions());
    };
    factory.registerInterpreter("InterpreterSelectQueryUseOptimizer", create_fn);
}

}
