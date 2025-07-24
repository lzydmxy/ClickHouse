#include <Query/Processors/QueryPlan/PlanPrinter.h>

#include <Query/Analyzer/ASTEquals.h>
#include <Query/Analyzer/Analysis.h>
#include <Core/Names.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Executor/SegmentScheduler.h>
#include <Query/Optimizer/OptimizerMetrics.h>
#include <Query/Optimizer/PlanNodeSearcher.h>
#include <Query/Optimizer/PredicateConst.h>
#include <Query/Optimizer/PredicateUtils.h>
#include <Query/Optimizer/Utils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Query/Planner/GraphvizPrinter.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <magic_enum.hpp>
#include <Poco/JSON/Object.h>
#include <Query/Processors/IQueryPlanStepExt.h>
#include <Query/Processors/QueryPlan/LineageInfo.h>
#include <Query/Interpreters/InterpreterExplainQueryUseOptimizer.h>

#include <utility>
#include <vector>
#include <cstdint>

namespace DB
{
namespace
{
    template <class V>
    String join(const V & v, const String & sep, const String & prefix = {}, const String & suffix = {})
    {
        std::stringstream out;
        out << prefix;
        if (!v.empty())
        {
            auto it = v.begin();
            out << *it;
            for (++it; it != v.end(); ++it)
                out << sep << *it;
        }
        out << suffix;
        return out.str();
    }

    String getJoinAlgorithmString(JoinAlgorithm algorithm)
    {
        switch (algorithm)
        {
            case JoinAlgorithm::DEFAULT:
                return "DEFAULT";
            case JoinAlgorithm::DIRECT:
                return "DIRECT";
            case JoinAlgorithm::FULL_SORTING_MERGE:
                return "FULL_SORTING_MERGE";
            case JoinAlgorithm::AUTO:
                return "AUTO";
            case JoinAlgorithm::HASH:
                return "HASH";
            case JoinAlgorithm::PARTIAL_MERGE:
                return "PARTIAL_MERGE";
            case JoinAlgorithm::PREFER_PARTIAL_MERGE:
                return "PREFER_PARTIAL_MERGE";
            case JoinAlgorithm::PARALLEL_HASH:
                return "PARALLEL_HASH";
            case JoinAlgorithm::GRACE_HASH:
                return "GRACE_HASH";
        }
        __builtin_unreachable();
    }

    String getSerializedASTWithLimit(const IAST & ast, size_t max_text_length)
    {
        String res = serializeAST(ast);
        if (res.size() <= max_text_length)
            return res;
        else
            return res.substr(0, max_text_length);
    }
}

String PlanPrinter::textPlanNode(PlanNodeBase & node, ContextPtr context, const QueryPlanSettings & settings)
{
    PlanCostMap costs;
    StepProfiles profiles;
    TextPrinter printer{costs, context, false, {}, settings};
    bool has_children = node.getChildren().empty();
    return printer.printLogicalPlan(node, TextPrinterIntent{0, has_children}, profiles);
}

String PlanPrinter::textLogicalPlan(
    QueryPlanExt & plan, ContextMutablePtr context, PlanCostMap costs, const StepProfiles & profiles, const QueryPlanSettings & settings)
{
    TextPrinter printer{costs, context, false, {}, settings, context->getOptimizerContext()->getSettingsRef().max_predicate_text_length};
    bool has_children = !plan.getPlanNode()->getChildren().empty();
    auto output = printer.printLogicalPlan(*plan.getPlanNode(), TextPrinterIntent{0, has_children}, profiles);

    for (const auto & cte_id : plan.getCTEInfo().getCTEIds())
    {
        output += "CTEDef [" + std::to_string(cte_id) + "]\n";
        auto & cte_plan = plan.getCTEInfo().getCTEDef(cte_id);
        output += printer.printLogicalPlan(*cte_plan, TextPrinterIntent{3, !cte_plan->getChildren().empty()}, profiles);
    }

    auto magic_sets = PlanNodeSearcher::searchFrom(plan)
                          .where([](auto & node) {
                              return getQueryPlanStepType(node.getStep()) == QueryPlanStepType::JoinStepExt
                                  && dynamic_cast<const JoinStepExt &>(*node.getStep()).isMagic();
                          })
                          .count();

    if (magic_sets > 0)
        output += "note: Magic Set is applied for " + std::to_string(magic_sets) + " parts.\n";

    auto filter_nodes = PlanNodeSearcher::searchFrom(plan)
                            .where([](auto & node) { return getQueryPlanStepType(node.getStep()) == QueryPlanStepType::FilterStepExt; })
                            .findAll();

    size_t runtime_filters = 0;
    for (auto & filter : filter_nodes)
    {
        const auto * filter_step = dynamic_cast<const FilterStepExt *>(filter->getStep().get());
        auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter());
        runtime_filters += filters.first.size();
    }

    if (runtime_filters > 0)
        output += "note: Runtime Filter is applied for " + std::to_string(runtime_filters) + " times.\n";

    auto cte_nodes = PlanNodeSearcher::searchFrom(plan)
                         .where([](auto & node) { return getQueryPlanStepType(node.getStep()) == QueryPlanStepType::CTERefStepExt; })
                         .count();

    if (cte_nodes > 0)
        output += "note: CTE(Common Table Expression) is applied for " + std::to_string(cte_nodes) + " times.\n";

    auto & optimizer_metrics = context->getOptimizerContext()->getOptimizerMetrics();
    if (optimizer_metrics && !optimizer_metrics->getUsedMaterializedViews().empty())
    {
        output += "note: Materialized Views is applied for " + std::to_string(optimizer_metrics->getUsedMaterializedViews().size())
            + " times: ";
        const auto & views = optimizer_metrics->getUsedMaterializedViews();
        auto it = views.begin();
        output += it->getDatabaseName()+ "." + it->getTableName();
        for (++it; it != views.end(); ++it)
            output += ", " + it->getDatabaseName() + "." + it->getTableName();
        output += ".";
    }

    if (plan.isShortCircuit())
    {
        output += "note: Short Circuit is applied.\n";
    }

    return output;
}

String PlanPrinter::jsonLogicalPlan(
    QueryPlanExt & plan,
    std::optional<PlanNodeCost> plan_cost,
    const CostModel & cost_model,
    const StepProfiles & profiles,
    const PlanCostMap & costs,
    const QueryPlanSettings & settings)
{
    std::ostringstream os;
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);
    auto plannode_desc = NodeDescription::getPlanDescription(plan.getPlanNode());

    if (plan_cost.has_value())
    {
        auto cost = plan_cost.value();
        json->set("total_cost", cost.getCost(cost_model));
        json->set("cpu_cost_value", cost.getCpuValue());
        json->set("net_cost_value", cost.getNetValue());
        json->set("men_cost_value", cost.getMenValue());
    }

    json->set("plan", plannode_desc->jsonNodeDescription(profiles, settings.stats, costs));
    if (!plan.getCTEInfo().getCTEs().empty())
    {
        Poco::JSON::Array ctes;
        for (auto & item : plan.getCTEInfo().getCTEs())
        {
            auto cte_desc = NodeDescription::getPlanDescription(item.second);
            ctes.add(cte_desc->jsonNodeDescription(profiles, settings.stats, costs));
        }
        json->set("CTEs", ctes);
    }

    json->stringify(os, 2);
    return os.str();
}

String PlanPrinter::getPlanSegmentHeaderText(
    const PlanSegmentDescriptionPtr & segment_desc, bool print_profile, const std::unordered_map<size_t, PlanSegmentProfiles> & segment_profile)
{
    auto f = [](RExchangeMode::Enum mode) {
        switch (mode)
        {
            case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
                return "LOCAL_NO_NEED_REPARTITION";
            case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                return "LOCAL_MAY_NEED_REPARTITION";
            case RExchangeMode::BROADCAST:
                return "BROADCAST";
            case RExchangeMode::REPARTITION:
                return "REPARTITION";
            case RExchangeMode::GATHER:
                return "GATHER";
            default:
                return "UNKNOWN";
        }
    };

    std::ostringstream os;

    size_t segment_id = segment_desc->segment_id;
    os << "Segment[" << segment_id << "] [" + segment_desc->segment_type + "]\n";

    auto mode = segment_desc->mode;
    String exchange = (segment_id == 0) ? "Output" : f(mode);
    os << "   Output Exchange: " << exchange;
    if (exchange == "REPARTITION" && !segment_desc->shuffle_keys.empty()) // print shuffle keys
        os << " Shufflekeys: " << join(segment_desc->shuffle_keys, ", ");
    os << "\n";

    os << "   Parallel Size: " << segment_desc->parallel;
    os << ", Cluster Name: " << (segment_desc->cluster_name.empty() ? "server" : segment_desc->cluster_name);
    os << ", Exchange Parallel Size: " << segment_desc->exchange_parallel_size  << "\n";

    if (!segment_desc->outputs_desc.empty())
    {
        os << "   Outputs: [";
        bool first = true;
        for (auto & output : segment_desc->outputs_desc)
        {
            if (!first)
                os << "\n             ";
            os << "(SegmentId:" << output->segment_id
                << " ExchangeId:" << output->exchange_id
                << " ExchangeMode:" << magic_enum::enum_name(output->mode)
                << " ParallelSize:" << output->parallel_size
                << " KeepOrder:" << output->keep_order << ")";
            first = false;
        }
        os << "]\n";
    }

    if (!segment_desc->inputs_desc.empty())
    {
        os << "   Inputs: [";
        bool first = true;
        for (auto & input : segment_desc->inputs_desc)
        {
            if (!first)
                os << "\n             ";
            os << "(SegmentId:" << input->segment_id
                << " ExchangeId:" << input->exchange_id
                << " ExchangeMode:" << magic_enum::enum_name(input->mode)
                << " ExchangeParallelSize:" << input->exchange_parallel_size
                << " KeepOrder:" << input->keep_order
                << (input->stable ? " Stable" : "") << ")";
            first = false;
        }
        os << "]\n";
    }
    if (print_profile && !segment_profile.empty() && segment_profile.contains(segment_id))
    {
        const auto & profiles = segment_profile.at(segment_id);
        for (const auto & profile : profiles)
            os << "   " << profile->worker_address << " ReadRows: " << profile->read_rows
               << " QueryDurationTime: " << profile->query_duration_ms << "ms."
               << " IOWaitTime: " << profile->io_wait_ms << "ms.\n";
    }
    return os.str();
}

String PlanPrinter::textDistributedPlan(
    PlanSegmentDescriptions & segments_desc,
    ContextMutablePtr context,
    const std::unordered_map<PlanNodeId, double> & costs,
    const StepProfiles & profiles,
    const QueryPlanExt & query_plan,
    const QueryPlanSettings & settings,
    const std::unordered_map<size_t, PlanSegmentProfiles> & segment_profile)
{
    auto id_to_node = getPlanNodeMap(query_plan);
    for (auto & segment_desc : segments_desc)
    {
        if (segment_desc->segment_id == 0)
        {
            segment_desc->plan_node = query_plan.getPlanNode();
            continue;
        }

        if (segment_desc->root_id == 0)
            continue;

        PlanNodePtr plan_node;
        if (id_to_node.contains(segment_desc->root_id))
            plan_node = id_to_node.at(segment_desc->root_id);
        else if (segment_desc->root_child_id != 0)
            plan_node = id_to_node.at(segment_desc->root_child_id);
        else
            continue;

        segment_desc->plan_node = plan_node;
    }

    std::ostringstream os;

    auto cmp = [](const PlanSegmentDescriptionPtr & s1, const PlanSegmentDescriptionPtr & s2) { return s1->segment_id < s2->segment_id; };
    std::sort(segments_desc.begin(), segments_desc.end(), cmp);

    for (auto & segment_ptr : segments_desc)
    {
        if (settings.segment_id != UINT64_MAX && segment_ptr->segment_id != settings.segment_id)
            continue;

        os << getPlanSegmentHeaderText(segment_ptr, settings.segment_profile, segment_profile);

        if (!segment_ptr->plan_node)
            continue;

        auto analyze_node = PlanNodeSearcher::searchFrom(segment_ptr->plan_node)
                                .where([](auto & node) { return getQueryPlanStepType(node.getStep())== QueryPlanStepType::ExplainAnalyzeStepExt; })
                                .findFirst();
        if (analyze_node)
        {
            os << TextPrinter::printOutputColumns(*analyze_node.value()->getChildren()[0], TextPrinterIntent{3, false});
            TextPrinter printer{costs, context, true, segment_ptr->exchange_to_segment, settings, context->getOptimizerContext()->getSettingsRef().max_predicate_text_length};
            bool has_children = !analyze_node.value()->getChildren().empty();
            if ((getQueryPlanStepType(analyze_node.value()->getStep()) == QueryPlanStepType::CTERefStepExt
                 || getQueryPlanStepType(analyze_node.value()->getStep())== QueryPlanStepType::ExchangeStepExt))
                has_children = false;

            auto output = printer.printLogicalPlan(*analyze_node.value(), TextPrinterIntent{6, has_children}, profiles);
            os << output;
        }
        else
        {
            auto plan_root = segment_ptr->plan_node;
            os << TextPrinter::printOutputColumns(*segment_ptr->plan_node, TextPrinterIntent{3, false});
            TextPrinter printer{costs, context, true, segment_ptr->exchange_to_segment, settings};
            bool has_children = !plan_root->getChildren().empty();
            if ((getQueryPlanStepType(plan_root->getStep()) == QueryPlanStepType::CTERefStepExt
                 || getQueryPlanStepType(plan_root->getStep()) == QueryPlanStepType::ExchangeStepExt))
                has_children = false;

            auto output = printer.printLogicalPlan(*segment_ptr->plan_node, TextPrinterIntent{6, has_children}, profiles);
            os << output;
        }

        os << "\n";
    }

    return os.str();
}


String PlanPrinter::textPipelineProfile(
    PlanSegmentDescriptions & segment_descs,
    SegIdAndAddrToPipelineProfile & worker_grouped_profiles,
    const QueryPlanSettings & settings,
    const std::unordered_map<size_t, PlanSegmentProfiles> & segment_profile)
{
    std::ostringstream os;

    if (!segment_descs.empty())
    {
        auto cmp = [](const PlanSegmentDescriptionPtr & s1, const PlanSegmentDescriptionPtr & s2) { return s1->segment_id < s2->segment_id; };
        std::sort(segment_descs.begin(), segment_descs.end(), cmp);

        for (auto & segment_ptr : segment_descs)
        {
            size_t segment_id = segment_ptr->segment_id;
            if (settings.segment_id != UINT64_MAX && segment_id != settings.segment_id)
                continue;
            os << getPlanSegmentHeaderText(segment_ptr, settings.segment_profile, segment_profile);
            if (!worker_grouped_profiles.contains(segment_id) || worker_grouped_profiles.at(segment_id).empty())
                continue;

            for (auto & [address, profile] : worker_grouped_profiles.at(segment_id))
            {
                if (!profile)
                    continue;
                TextPrinterIntent print{3, false};
                os << print.print() << address << "\n";
                TextPrinter printer{{}, nullptr, true, {}, settings};
                bool has_children = !profile->children.empty();
                auto output = printer.printPipelineProfile(profile, TextPrinterIntent{3, has_children});
                os << output;
            }
            os << "\n";
        }
        return os.str();
    }

    size_t max_segment_id = 0;
    for (const auto & [segment_id, segment_profiles] : segment_profile)
        max_segment_id = std::max(segment_id, max_segment_id);
    for (size_t segment_id = 0; segment_id <= max_segment_id; ++segment_id)
    {
        if (!segment_profile.contains(segment_id))
            continue;
        auto segment_profiles = segment_profile.at(segment_id);
        os << "Segment[" << segment_id << "]\n";
        if (segment_id != 0)
        {
            for (const auto & profile : segment_profiles)
                os << profile->worker_address << " ReadRows: " << profile->read_rows << " QueryDurationTime: " << profile->query_duration_ms
                   << "ms."
                   << " TotalCpuTime: " << profile->total_cpu_ms << "ms."
                   << " IOWaitTime: " << profile->io_wait_ms << "ms.\n";
        }
        for (auto & [address, profile] : worker_grouped_profiles.at(segment_id))
        {
            if (!profile)
                continue;
            TextPrinterIntent print{3, false};
            os << print.print() << address << "\n";
            TextPrinter printer{{}, nullptr, true, {}, settings};
            bool has_children = !profile->children.empty();
            auto output = printer.printPipelineProfile(profile, TextPrinterIntent{3, has_children});
            os << output;
        }
        os << "\n";
    }
    return os.str();
}

String PlanPrinter::textQueryPipelineProfiles(ContextMutablePtr query_context)
{
    auto scheduler = query_context->getOptimizerContext()->getSegmentScheduler();
    UInt64 time_out = query_context->getOptimizerContext()->getSettingsRef().operator_profile_receive_timeout;
    auto time_start = std::chrono::system_clock::now();
    while (!scheduler->alreadyReceivedAllSegmentStatus(query_context->getCurrentQueryId()))
    {
        auto now = std::chrono::system_clock::now();
        UInt64 elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - time_start).count();
        if (elapsed >= time_out)
            break;
    }
    SegIdAndAddrToPipelineProfile worker_grouped_profiles;
    auto profiles_map = scheduler->getSegmentsProfile(query_context->getCurrentQueryId());
    for (auto & [segment_id, segment_profiles] : profiles_map)
    {
        for (auto & segment_profile : segment_profiles)
        {
            if (segment_profile->profiles.empty())
                continue;
            auto profile
                = GroupedProcessorProfile::getGroupedProfileFromMetrics(segment_profile->profiles, segment_profile->profile_root_id);
            if (profile->processor_name == "output_root" && !profile->children.empty())
                profile = profile->children[0];
            worker_grouped_profiles[segment_profile->segment_id][segment_profile->worker_address] = std::move(profile);
        }
    }
    QueryPlanSettings settings;
    PlanSegmentDescriptions plan_segment_descriptions;
    if (query_context->getOptimizerContext()->getSettingsRef().log_explain_analyze_type == LogExplainAnalyzeType::AGGREGATED_QUERY_PIPELINE)
        worker_grouped_profiles = GroupedProcessorProfile::aggregatePipelineProfileBetweenWorkers(worker_grouped_profiles);
    return PlanPrinter::textPipelineProfile(plan_segment_descriptions, worker_grouped_profiles, settings, profiles_map);
}

String PlanPrinter::jsonPipelineProfile(PlanSegmentDescriptions & segment_descs, SegIdAndAddrToPipelineProfile & worker_grouped_profiles)
{
    Poco::JSON::Object::Ptr distributed_plan = new Poco::JSON::Object(true);
    Poco::JSON::Array segments;
    for (auto & segment_desc : segment_descs)
    {
        Poco::JSON::Object::Ptr segment_json = segment_desc->jsonPlanSegmentDescription({}, true);
        if (worker_grouped_profiles.contains(segment_desc->segment_id))
        {
            Poco::JSON::Object::Ptr worker_profiles_json = new Poco::JSON::Object(true);
            for (auto [woker_ip, profile] : worker_grouped_profiles[segment_desc->segment_id])
                worker_profiles_json->set(woker_ip, profile->getJsonProfiles());
            segment_json->set("profiles", worker_profiles_json);
        }
        segments.add(segment_json);
    }
    distributed_plan->set("PipelineProfiles", segments);
    std::ostringstream os;
    distributed_plan->stringify(os, 1);
    return os.str();
}

// void PlanPrinter::getRemoteSegmentId(const QueryPlan::Node * node, std::unordered_map<PlanNodeId, size_t> & exchange_to_segment)
// {
//     auto * step = dynamic_cast<RemoteExchangeSourceStepExt *>(node->step.get());
//     if (step)
//         exchange_to_segment[node->id] = step->getInput()[0]->getPlanSegmentId();
//
//     for (const auto & child : node->children)
//         getRemoteSegmentId(child, exchange_to_segment);
// }

std::unordered_map<PlanNodeId, PlanNodePtr> PlanPrinter::getPlanNodeMap(const QueryPlanExt & query_plan)
{
    std::unordered_map<PlanNodeId, PlanNodePtr> id_to_node;
    const auto & plan = query_plan.getPlanNode();
    if (!plan)
        return id_to_node;

    id_to_node[plan->getId()] = plan;
    getPlanNodes(plan, id_to_node);

    for (const auto & cte : query_plan.getCTEInfo().getCTEs())
    {
        id_to_node[cte.second->getId()] = cte.second;
        getPlanNodes(cte.second, id_to_node);
    }

    return id_to_node;
}

void PlanPrinter::getPlanNodes(const PlanNodePtr & parent, std::unordered_map<PlanNodeId, PlanNodePtr> & id_to_node)
{
    for (const auto & child : parent->getChildren())
    {
        id_to_node[child->getId()] = child;
        if (!child->getChildren().empty())
            getPlanNodes(child, id_to_node);
    }
}

String PlanPrinter::TextPrinter::printOutputColumns(PlanNodeBase & plan_node, const TextPrinterIntent & intent)
{
    auto header = plan_node.getStep()->getOutputStream().header;

    String res;
    size_t line_feed_limit = 120;
    res += intent.print() + "Output Columns: [";

    std::vector<std::string> output_columns;
    for (auto & it : header)
    {
        output_columns.push_back(it.name);
    }
    std::sort(output_columns.begin(), output_columns.end());

    bool first = true;
    for (auto & column_name : output_columns)
    {
        if (res.length() > line_feed_limit)
        {
            res += "\n";
            res += intent.print() + String(17, ' ');
            line_feed_limit += 120;
            first = true;
        }
        if (first)
        {
            res += column_name;
            first = false;
        }
        else
        {
            res += ", ";
            res += column_name;
        }
    }
    res += "]\n";
    return res;
}

TextPrinterIntent TextPrinterIntent::forChild(bool last, bool hasChildren_) const
{
    return TextPrinterIntent{
        next_lines_prefix + (last ? LAST_PREFIX : INTERMEDIATE_PREFIX),
        next_lines_prefix + (last ? EMPTY_PREFIX : VERTICAL_LINE),
        hasChildren_};
}

TextPrinterIntent::TextPrinterIntent(String current_lines_prefix_, String next_lines_prefix_, bool hasChildren_)
    : current_lines_prefix(std::move(current_lines_prefix_)), next_lines_prefix(std::move(next_lines_prefix_)), hasChildren(hasChildren_)
{
}

String TextPrinterIntent::detailIntent() const
{
    return "\n" + next_lines_prefix + (hasChildren ? VERTICAL_LINE : EMPTY_PREFIX) + EMPTY_PREFIX;
}

String PlanPrinter::TextPrinter::printLogicalPlan(
    PlanNodeBase & plan, const TextPrinterIntent & intent, const StepProfiles & profiles) // NOLINT(misc-no-recursion)
{
    std::stringstream out;

    auto step = plan.getStep();
    if (getQueryPlanStepType(step) == QueryPlanStepType::ExplainAnalyzeStepExt)
        return printLogicalPlan(*plan.getChildren()[0], intent, profiles);

    if (profiles.empty())
    {
        if (settings.stats)
            out << intent.print() << printPrefix(plan) << step->getName() << printSuffix(plan) << " " << printStatistics(plan, intent)
                << printDetail(plan.getStep(), intent) << "\n";
        else
            out << intent.print() << printPrefix(plan) << step->getName() << printSuffix(plan) << printDetail(plan.getStep(), intent) << "\n";
    }
    else
    {
        out << intent.print() << printPrefix(plan) << step->getName() << printSuffix(plan);
        if (settings.stats)
            out << intent.detailIntent() << printStatistics(plan, intent);
        if (settings.profile && profiles.count(plan.getId()))
            out << printStepProfiles(plan, intent, profiles) << intent.detailIntent() << printQError(plan, profiles);
        out << printDetail(plan.getStep(), intent) << printAttributes(plan, intent, profiles) << "\n";
    }

    if ((getQueryPlanStepType(step) == QueryPlanStepType::CTERefStepExt || getQueryPlanStepType(step) == QueryPlanStepType::ExchangeStepExt) && is_distributed)
        return out.str();

    for (auto it = plan.getChildren().begin(); it != plan.getChildren().end();)
    {
        auto child = *it++;
        bool last = it == plan.getChildren().end();
        bool has_children = !child->getChildren().empty();
        if ((getQueryPlanStepType(child->getStep()) == QueryPlanStepType::CTERefStepExt ||  getQueryPlanStepType(child->getStep()) == QueryPlanStepType::ExchangeStepExt)
            && is_distributed)
            has_children = false;

        out << printLogicalPlan(*child, intent.forChild(last, has_children), profiles);
    }

    return out.str();
}

String PlanPrinter::TextPrinter::printPipelineProfile(GroupedProcessorProfilePtr & input_root, const TextPrinterIntent & intent)
{
    std::stringstream out;
    out << intent.print() << printPipelineProfileDetail(input_root, intent) << "\n";

    for (auto it = input_root->children.begin(); it != input_root->children.end();)
    {
        auto child = *it++;
        bool last = it == input_root->children.end();
        bool has_children = !child->children.empty() && child->children[0];
        out << printPipelineProfile(child, intent.forChild(last, has_children));
    }
    return out.str();
}

String PlanPrinter::TextPrinter::printPipelineProfileDetail(GroupedProcessorProfilePtr profile, const TextPrinterIntent & intent)
{
    std::stringstream out;
    out << profile->processor_name << " x" << profile->parallel_size
        << " ElapsedTime:" << prettySeconds(profile->sum_grouped_elapsed_us / profile->parallel_size);
    if (profile->parallel_size > 1)
        out<< "[max=" << prettySeconds(profile->max_grouped_elapsed_us) << ", min=" << prettySeconds(profile->min_grouped_elapsed_us) << "]";
    out << intent.detailIntent() << "Output: Rows:" << prettyNum(profile->grouped_output_rows, settings.pretty_num) << " ("
        << prettyBytes(profile->grouped_output_bytes) << ")";
    out << " WaitTime:" << prettySeconds(profile->sum_grouped_output_wait_elapsed_us / profile->parallel_size);
    if (profile->parallel_size > 1)
        out<< "[max=" << prettySeconds(profile->max_grouped_output_wait_elapsed_us) << ", min=" << prettySeconds(profile->min_grouped_output_wait_elapsed_us) << "]";

    out << intent.detailIntent() << "Input: Rows:" << prettyNum(profile->grouped_input_rows, settings.pretty_num) << " ("
        << prettyBytes(profile->grouped_input_bytes) << ")";
    out << " WaitTime:" << prettySeconds(profile->sum_grouped_input_wait_elapsed_us / profile->parallel_size);
    if (profile->parallel_size > 1)
        out << "[max=" << prettySeconds(profile->max_grouped_input_wait_elapsed_us)
            << ", min=" << prettySeconds(profile->min_grouped_input_wait_elapsed_us) << "]";

    return out.str();
}

String PlanPrinter::TextPrinter::printStatistics(const PlanNodeBase & plan, const TextPrinterIntent &) const
{
    if (!settings.stats)
        return "";
    std::stringstream out;
    const auto & stats = plan.getStatistics();
    out << "Est. " << (stats ? std::to_string(stats.value()->getRowCount()) : "?") << " rows";
    if (settings.cost && costs.contains(plan.getId()))
        out << ", cost " << std::scientific << costs.at(plan.getId());
    return out.str();
}

String PlanPrinter::TextPrinter::printStepProfiles(PlanNodeBase & plan, const TextPrinterIntent & intent, const StepProfiles & profiles)
{
    size_t step_id = plan.getId();
    if (profiles.count(step_id))
    {
        const auto & profile = profiles.at(step_id);
        std::stringstream out;
        out << intent.detailIntent() << "Act. WallTime: " << prettySeconds(profile->sum_elapsed_us/profile->worker_cnt);
        if (profile->worker_cnt > 1)
            out << "[max= " << prettySeconds(profile->max_elapsed_us) << ", min=" << prettySeconds(profile->min_elapsed_us) << "]";
        out << intent.detailIntent() << "     Output: " << prettyNum(profile->output_rows, settings.pretty_num) << " rows("
            << prettyBytes(profile->output_bytes) << ")";
        out << ", WaitTime: " << prettySeconds(profile->output_wait_sum_elapsed_us / profile->worker_cnt);
        if (profile->worker_cnt > 1)
            out << "[max=" << prettySeconds(profile->output_wait_max_elapsed_us)
                << ", min=" << prettySeconds(profile->output_wait_min_elapsed_us) << "]";

        int num = 1;
        if (!plan.getChildren().empty() && profile->inputs.contains(plan.getChildren()[0]->getId()))
        {
            for (auto & child : plan.getChildren())
            {
                auto input_profile = profile->inputs[child->getId()];
                if (num == 1)
                    out << intent.detailIntent() << "     Input: ";
                else
                    out << intent.detailIntent() << "            ";

                if (plan.getChildren().size() > 1)
                    out << "source[" << num << "] : ";

                out << prettyNum(input_profile.input_rows, settings.pretty_num) << " rows(" << prettyBytes(input_profile.input_bytes)
                    << ")";
                out << ", WaitTime: " << prettySeconds(input_profile.input_wait_sum_elapsed_us / profile->worker_cnt);
                if (profile->worker_cnt > 1)
                    out << "[max=" << prettySeconds(input_profile.input_wait_max_elapsed_us)
                        << ", min=" << prettySeconds(input_profile.input_wait_min_elapsed_us) << "]";
                ++num;
            }
        }
        else
        {
            for (auto & [id, input_metrics] : profile->inputs)
            {
                if (num == 1)
                    out << intent.detailIntent() << "     Input: ";
                else
                    out << intent.detailIntent() << "            ";

                if (plan.getChildren().size() > 1)
                    out << "source [" << num << "] : ";

                out << "WaitTime: " << prettySeconds(input_metrics.input_wait_sum_elapsed_us / profile->worker_cnt);
                if (profile->worker_cnt > 1)
                    out << "[max=" << prettySeconds(input_metrics.input_wait_max_elapsed_us)
                        << ", min=" << prettySeconds(input_metrics.input_wait_min_elapsed_us) << "]";
                ++num;
            }
        }

        return out.str();
    }
    return "";
}

String PlanPrinter::TextPrinter::printAttributes(PlanNodeBase & plan, const TextPrinterIntent & intent, const StepProfiles & profiles) const
{
    size_t step_id = plan.getId();
    if (!profiles.contains(step_id) || profiles.at(step_id)->address_to_attributes.empty())
        return "";
    if (!settings.query_plan_options.indexes && !settings.selected_parts)
        return "";
    std::stringstream out;
    const auto & address_to_attributes = profiles.at(step_id)->address_to_attributes;
    if (getQueryPlanStepType(plan.getStep()) == QueryPlanStepType::TableScanStepExt)
    {
        String space;
        for (const auto & [address, attribute] : address_to_attributes)
        {
            if (address_to_attributes.size() > 1)
            {
                out << intent.detailIntent() << address;
                space = "    ";
            }
            if (settings.query_plan_options.indexes && attribute.contains("Indexes"))
            {
                out << intent.detailIntent() << space << "Indexes:";
                auto index_desc = attribute.at("Indexes");
                for (const auto & desc : index_desc->name_and_detail)
                    out << intent.detailIntent() << space << "    " << desc.second;
            }
            if (settings.selected_parts)
            {
                if (attribute.contains("SelectParts"))
                    out << intent.detailIntent() << space << attribute.at("SelectParts")->description;
                if (attribute.contains("TableScanDescription"))
                    out << intent.detailIntent() << space << attribute.at("TableScanDescription")->description;
            }
        }
        return out.str();
    }
    return "";
}

String PlanPrinter::TextPrinter::prettyNum(size_t num, bool pretty_num)
{
    std::vector<std::string> suffixes{"", "K", "M", "B", "T"};
    size_t idx = 0;
    auto count = static_cast<double>(num);
    if (pretty_num)
    {
        while (count >= 1000 && idx < suffixes.size() - 1)
        {
            idx++;
            count /= static_cast<double>(1000);
        }
    }

    std::stringstream out;
    if (idx == 0)
        out << static_cast<int>(count);
    else
        out << std::fixed << std::setprecision(1) << count << suffixes[idx];
    return out.str();
}

String PlanPrinter::TextPrinter::prettySeconds(size_t us)
{
    std::vector<std::string> suffixes{"us", "ms", "s"};
    size_t idx = 0;
    auto count = static_cast<double>(us);
    while (count >= 1000 && idx < suffixes.size() - 1)
    {
        idx++;
        count /= static_cast<double>(1000);
    }

    std::stringstream out;
    out << std::fixed << std::setprecision(1) << count << suffixes[idx];
    return out.str();
}

String PlanPrinter::TextPrinter::prettyBytes(size_t bytes)
{
    std::vector<std::string> suffixes{" Bytes", " KB", " MB", " GB", " TB"};
    size_t idx = 0;
    auto count = static_cast<double>(bytes);
    while (count >= 1024 && idx < suffixes.size() - 1)
    {
        idx++;
        count /= static_cast<double>(1024);
    }

    std::stringstream out;
    out << std::fixed << std::setprecision(1) << count << suffixes[idx];
    return out.str();
}

String PlanPrinter::TextPrinter::printQError(const PlanNodeBase & plan, const StepProfiles & profiles)
{
    const auto & stats = plan.getStatistics();
    std::stringstream out;

    size_t step_id = plan.getId();
    if (profiles.count(step_id))
    {
        const auto & profile = profiles.at(step_id);
        if (plan.getChildren().size() > 1)
        {
            size_t max_input_rows = 0;
            for (const auto & p : plan.getChildren())
            {
                if (profiles.count(p->getId()) == 0)
                    continue;
                max_input_rows = std::max(max_input_rows, profiles.at(p->getId())->output_rows);
            }
            if (max_input_rows == 0)
                out << "Filtered: 0.0%";
            else
            {
                double max_rows = static_cast<double>(max_input_rows);
                double filtered = max_rows > 0 ? (((max_rows - static_cast<double>(profile->output_rows)) * static_cast<double>(100) / max_rows)) : 0.0;
                out << "Filtered: " << std::fixed << std::setprecision(1) << filtered << "%";
            }
        }
        else if (plan.getChildren().size() == 1)
        {
            if (profiles.count(plan.getChildren()[0]->getId()) == 0)
                out << "Filtered: 0.0%";
            else
            {
                auto child_input_rows = static_cast<double>(profiles.at(plan.getChildren()[0]->getId())->output_rows);
                double filtered = child_input_rows > 0 ? ((child_input_rows - static_cast<double>(profile->output_rows)) * static_cast<double>(100) / child_input_rows) : 0.0;
                out << "Filtered: " << std::fixed << std::setprecision(1) << filtered << "%";
            }
        }
        else
        {
            out << "Filtered: 0.0%";
        }

        if (stats && stats.value()->getRowCount() != 0 && profile->output_rows != 0)
        {
            if (profile->output_rows > stats.value()->getRowCount())
                out << ", QError: " << std::fixed << std::setprecision(1)
                    << static_cast<double>(profile->output_rows) / static_cast<double>(stats.value()->getRowCount());
            else
                out << ", QError: " << std::fixed << std::setprecision(1)
                    << static_cast<double>(stats.value()->getRowCount()) / static_cast<double>(profile->output_rows);
        }
        return out.str();
    }
    return "";
}

String PlanPrinter::TextPrinter::printPrefix(PlanNodeBase & plan)
{
    if (getQueryPlanStepType(plan.getStep()) == QueryPlanStepType::ExchangeStepExt)
    {
        const auto * exchange = dynamic_cast<const ExchangeStepExt *>(plan.getStep().get());
        auto f = [](RExchangeMode::Enum mode) {
            switch (mode)
            {
                case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
                case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                    return "Local ";
                case RExchangeMode::BROADCAST:
                    return "Broadcast ";
                case RExchangeMode::REPARTITION:
                    return "Repartition ";
                case RExchangeMode::GATHER:
                    return "Gather ";
                default:
                    return "";
            }
        };
        return f(exchange->getExchangeMode());
    }

    if (getQueryPlanStepType(plan.getStep()) == QueryPlanStepType::JoinStepExt)
    {
        const auto * join = dynamic_cast<const JoinStepExt *>(plan.getStep().get());
        auto f = [](JoinKind kind, JoinStrictness strictness) {
            String result;
            switch (kind)
            {
                case JoinKind::Inner:
                    result = "Inner ";
                    break;
                case JoinKind::Left:
                    result = "Left ";
                    break;
                case JoinKind::Right:
                    result = "Right ";
                    break;
                case JoinKind::Full:
                    result = "Full ";
                    break;
                case JoinKind::Cross:
                    result = "Cross ";
                    break;
                default:
                    result = "";
                    break;
            }
            switch (strictness)
            {
                case JoinStrictness::RightAny:
                    result += "RightAny ";
                    break;
                case JoinStrictness::Any:
                    result += "Any ";
                    break;
                case JoinStrictness::Asof:
                    result += "Asof ";
                    break;
                case JoinStrictness::Semi:
                    result += "Semi ";
                    break;
                case JoinStrictness::Anti:
                    result += "Anti ";
                    break;
                default:
                    break;
            }
            return result;
        };

        if (join->getJoinAlgorithm() != JoinAlgorithm::AUTO)
            return fmt::format("{}({}) ", f(join->getKind(), join->getStrictness()), getJoinAlgorithmString(join->getJoinAlgorithm()));

        return f(join->getKind(), join->getStrictness());
    }
    return "";
}


String PlanPrinter::TextPrinter::printSuffix(PlanNodeBase & plan)
{
    std::stringstream out;
    Int64 segment_id = -1;
    if (is_distributed && exchange_to_segment.contains(plan.getId()))
        segment_id = exchange_to_segment.at(plan.getId());

    if (getQueryPlanStepType(plan.getStep()) == QueryPlanStepType::TableScanStepExt)
    {
        const auto * table_scan = dynamic_cast<const TableScanStepExt *>(plan.getStep().get());
        out << " " << table_scan->getDatabase() << "." << table_scan->getOriginalTable();
    }
    else if (getQueryPlanStepType(plan.getStep()) == QueryPlanStepType::ExchangeStepExt && segment_id != -1)
    {
        out << " segment[" << exchange_to_segment.at(plan.getId()) << "]";
    }
    else if (getQueryPlanStepType(plan.getStep()) == QueryPlanStepType::CTERefStepExt)
    {
        const auto * cte = dynamic_cast<const CTERefStepExt *>(plan.getStep().get());
        out << "[" << cte->getId() << "]";
        if (segment_id != -1)
            out << " <--"
                << " segment[" << exchange_to_segment.at(plan.getId()) << "]";
    }

    out << " InputStreams size " << plan.getStep()->getInputStreams().size() << ": [ ";
    for (const auto & input_streams : plan.getStep()->getInputStreams())
    {
        out << "[";
        for (const auto & name: input_streams.header.getNames())
        {
            out << name << ", ";
        }
        out << "], ";
    }
    out << "] ";

    out << "OutputStream: [ ";

    for (const auto & name: plan.getStep()->getOutputStream().header.getNames())
    {
        out << name << ", ";
    }

    out << "] ";
    return out.str();
}

String PlanPrinter::TextPrinter::printDetail(QueryPlanStepPtr plan, const TextPrinterIntent & intent) const
{
    if (!settings.verbose)
        return "";

    std::stringstream out;
    if (getQueryPlanStepType(plan) == QueryPlanStepType::UnionStepExt)
    {
        const auto * union_step = dynamic_cast<const UnionStepExt *>(plan.get());
        out << intent.detailIntent() << "OutputToInputs: ";

        for (auto iter = union_step->getOutToInputs().begin(); iter != union_step->getOutToInputs().end(); ++iter)
        {
            if (iter != union_step->getOutToInputs().begin())
                out << ", ";
            const auto & output_to_inputs = *iter;
            out << output_to_inputs.first << " = ";
            out << join(output_to_inputs.second, ",", "[", "]");
        }
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::JoinStepExt)
    {
        const auto * join_step = dynamic_cast<const JoinStepExt *>(plan.get());
        out << intent.detailIntent() << "Condition: ";
        if (!join_step->getLeftKeys().empty())
            out << join_step->getLeftKeys()[0] << " == " << join_step->getRightKeys()[0]
                << (join_step->getKeyIdNullSafe(0) ? "(null aware)" : "");
        for (size_t i = 1; i < join_step->getLeftKeys().size(); i++)
            out << ", " << join_step->getLeftKeys()[i] << " == " << join_step->getRightKeys()[i]
                << (join_step->getKeyIdNullSafe(i) ? "(null aware)" : "");

        if (!ASTEquality::compareTree(join_step->getFilter(), PredicateConst::TRUE_VALUE))
        {
            out << intent.detailIntent() << "Filter: ";
            out << getSerializedASTWithLimit(*join_step->getFilter(), max_predicate_text_length);
        }
        if (!join_step->getRuntimeFilterBuilders().empty())
        {
            std::set<std::string> runtime_filters;
            for (const auto & item : join_step->getRuntimeFilterBuilders())
                runtime_filters.emplace(item.first);
            out << intent.detailIntent() << "Runtime Filters Builder: " << join(runtime_filters, ",", "{", "}");
        }
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::SortingStepExt)
    {
        const auto * sort = dynamic_cast<const SortingStepExt *>(plan.get());
        std::vector<String> sort_columns;
        for (const auto & desc : sort->getSortDescription())
            sort_columns.emplace_back(desc.dump());
        out << intent.detailIntent() << "Order by: " << join(sort_columns, ", ", "{", "}");

        if (!sort->getPrefixDescription().empty())
        {
            std::vector<String> prefix_sort_columns;
            for (const auto & desc : sort->getPrefixDescription())
                prefix_sort_columns.emplace_back(desc.column_name);
            out << intent.detailIntent() << "Prefix Order: " << join(prefix_sort_columns, ", ", "{", "}");
        }

        out << intent.detailIntent() << "Limit: " << sort->getLimit();
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::LimitStepExt)
    {
        const auto * limit = dynamic_cast<const LimitStepExt *>(plan.get());
        out << intent.detailIntent();
        out << "Limit: " << limit->getLimit();
        out << " Offset: " << limit->getOffset();
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::OffsetStep)
    {
        const auto * offset = dynamic_cast<const OffsetStep *>(plan.get());
        out << intent.detailIntent();
        if (offset->getOffset())
            out << " Offset: " << offset->getOffset();
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::AggregatingStepExt)
    {
        const auto * agg = dynamic_cast<const AggregatingStepExt *>(plan.get());
        auto keys = agg->getKeys();
        out << intent.detailIntent() << "Group by: " << join(keys, ", ", "{", "}");


        auto keys_not_hashed = agg->getKeysNotHashed();
        if (!keys_not_hashed.empty())
        {
            NameOrderedSet sorted_names(keys_not_hashed.begin(), keys_not_hashed.end());
            out << intent.detailIntent() << "Group by keys not hashed: " << join(sorted_names, ", ", "{", "}");
        }

        std::vector<String> aggregates;
        for (const auto & desc : agg->getAggregates())
        {
            std::stringstream ss;
            String func_name = desc.function->getName();
            auto type_name = String(typeid(desc.function.get()).name());
            if (type_name.find("AggregateFunctionNull"))
                func_name = String("AggNull(").append(std::move(func_name)).append(")");
            ss << desc.column_name << ":=" << func_name << join(desc.argument_names, ",", "(", ")");
            aggregates.emplace_back(ss.str());
        }
        if (!aggregates.empty())
            out << intent.detailIntent() << "Aggregates: " << join(aggregates, ", ");
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::ExchangeStepExt)
    {
        const auto * exchange = dynamic_cast<const ExchangeStepExt *>(plan.get());
        if (!exchange->getSchema().getColumns().empty())
        {
            auto keys = exchange->getSchema().getColumns();
            out << intent.detailIntent() << "Partition by: " << join(keys, ", ", "{", "}");
        }
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::FilterStepExt)
    {
        const auto * filter = dynamic_cast<const FilterStepExt *>(plan.get());
        auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter->getFilter());
        out << intent.detailIntent() << "Condition: " << printFilter(filter->getFilter(), max_predicate_text_length);
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::ProjectionStepExt)
    {
        const auto * projection = dynamic_cast<const ProjectionStepExt *>(plan.get());

        std::vector<String> identities;
        std::vector<String> assignments;

        for (const auto & assignment : projection->getAssignments())
            if (Utils::isIdentity(assignment))
                identities.emplace_back(assignment.first);
            else
                assignments.emplace_back(assignment.first + ":=" + getSerializedASTWithLimit(*assignment.second, max_predicate_text_length));

        std::sort(assignments.begin(), assignments.end());
        if (!identities.empty())
        {
            std::stringstream ss;
            std::sort(identities.begin(), identities.end());
            ss << join(identities, ", ", "[", "]");
            assignments.insert(assignments.begin(), ss.str());
        }

        out << intent.detailIntent() << "Expressions: " << join(assignments, ", ");
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::TableScanStepExt)
    {
        const auto * table_scan = dynamic_cast<const TableScanStepExt *>(plan.get());
        std::vector<String> identities;
        std::vector<String> assignments;
        for (const auto & name_with_alias : table_scan->getColumnAlias())
            if (name_with_alias.second == name_with_alias.first)
                identities.emplace_back(name_with_alias.second);
            else
                assignments.emplace_back(name_with_alias.second + ":=" + name_with_alias.first);

        auto query_info = table_scan->getQueryInfo();
        auto * query = query_info.query->as<ASTSelectQuery>();

        // if (query_info.partition_filter)
        // {
        //     out << intent.detailIntent() << "Partition filter: " << printFilter(query_info.partition_filter, max_predicate_text_length);
        // }

        if (query_info.input_order_info)
        {
            out << intent.detailIntent();
            out << "Input Order Info: ";

            // const auto & prefix_descs = query_info.input_order_info->order_key_prefix_descr;
            // if (!prefix_descs.empty())
            // {
            //     std::vector<String> columns;
            //     for (const auto & desc : prefix_descs)
            //         columns.emplace_back(desc.format());
            //     out << join(columns, ", ", "{", "}");
            // }
        }

        if (auto where = query->where())
            out << intent.detailIntent() << "Where: " << printFilter(where, max_predicate_text_length);
        if (auto prewhere = query->prewhere())
            out << intent.detailIntent() << "Prewhere: " << printFilter(prewhere, max_predicate_text_length);
        if (query->limitLength())
        {
            out << intent.detailIntent() << "Limit: ";
            Field converted = convertFieldToType(query->refLimitLength()->as<ASTLiteral>()->value, DataTypeUInt64());
            out << converted.safeGet<UInt64>();
        }

        if (query->sampleSize())
        {
            ASTSampleRatio * sample = query->sampleSize()->as<ASTSampleRatio>();
            out << intent.detailIntent() << "Sample Size: " << ASTSampleRatio::toString(sample->ratio);
            if (query->sampleOffset())
            {
                ASTSampleRatio * sample_offset = query->sampleOffset()->as<ASTSampleRatio>();
                out << " Offset: " << ASTSampleRatio::toString(sample_offset->ratio);
            }
        }

        std::vector<String> inline_expressions;
        for (const auto & assignment : table_scan->getInlineExpressions())
            inline_expressions.emplace_back(assignment.first + ":=" + getSerializedASTWithLimit(*assignment.second, max_predicate_text_length));
        if (!inline_expressions.empty())
            out << intent.detailIntent() << "Inline expressions: " << join(inline_expressions, ", ", "[", "]");

        if (!identities.empty())
        {
            std::stringstream ss;
            ss << join(identities, ", ", "[", "]");
            assignments.insert(assignments.begin(), ss.str());
        }

        out << intent.detailIntent() << "Outputs: " << join(assignments, ", ");

        if (table_scan->getPushdownFilter())
            out << printDetail(table_scan->getPushdownFilter(), intent);

        if (table_scan->getPushdownProjection())
            out << printDetail(table_scan->getPushdownProjection(), intent);

        if (table_scan->getPushdownAggregation())
            out << printDetail(table_scan->getPushdownAggregation(), intent);
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::TopNFilteringStepExt)
    {
        const auto *topn_filter = dynamic_cast<const TopNFilteringStepExt *>(plan.get());
        std::vector<String> sort_columns;
        for (const auto & desc : topn_filter->getSortDescription())
            sort_columns.emplace_back(desc.dump());
        out << intent.detailIntent() << "Order by: " << join(sort_columns, ", ", "{", "}");
        out << intent.detailIntent() << "Size: " << topn_filter->getSize();
        out << intent.detailIntent() << "Algorithm: " << TopNFilteringAlgorithmConverter::toString(topn_filter->getAlgorithm());
    }

    if (getQueryPlanStepType(plan) == QueryPlanStepType::TotalsHavingStepExt)
    {
        const auto * totals_having = dynamic_cast<const TotalsHavingStepExt *>(plan.get());
        if (totals_having->getHavingFilter())
            out << intent.detailIntent() << "Having: " << totals_having->getHavingFilter()->formatForErrorMessage();
    }

    return out.str();
}

String PlanPrinter::TextPrinter::printFilter(ConstASTPtr filter, size_t max_text_length)
{
    std::stringstream out;
    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter);

    if (!filters.second.empty())
        out << getSerializedASTWithLimit(*PredicateUtils::combineConjuncts(filters.second), max_text_length);

    if (!filters.first.empty())
    {
        std::set<String> runtime_filters;
        for (auto & item : filters.first)
        {
            auto desc = RuntimeFilterUtils::extractDescription(item).value();
            runtime_filters.emplace(getSerializedASTWithLimit(*desc.expr->clone(), max_text_length));
        }
        if (!filters.second.empty())
            out << " ";
        out << "Runtime Filters: " << join(runtime_filters, ", ", "{", "}");
    }

    if (filters.first.empty() && filters.second.empty())
        out << "True";
    return out.str();
}

void NodeDescription::setStepDetail(QueryPlanStepPtr step)
{
    type = getQueryPlanStepType(step);
    step_name = step->getName();
    if (getQueryPlanStepType(step) == QueryPlanStepType::UnionStepExt)
    {
        const auto * union_step = dynamic_cast<const UnionStepExt *>(step.get());
        for (const auto & output_to_inputs : union_step->getOutToInputs())
        {
            step_vector_detail["OutputToInputs"].emplace_back(output_to_inputs.first + " = " + join(output_to_inputs.second, ",", "[", "]"));
        }
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::JoinStepExt)
    {
        const auto * join_step = dynamic_cast<const JoinStepExt *>(step.get());
        auto get_kind = [](JoinKind kind) {
            switch (kind)
            {
                case JoinKind::Inner:
                    return "Inner";
                case JoinKind::Left:
                    return "Left";
                case JoinKind::Right:
                    return "Right";
                case JoinKind::Full:
                    return "Full";
                case JoinKind::Cross:
                    return "Cross";
                default:
                    return "";
            }
        };
        auto get_strictness = [](JoinStrictness strictness) {
            switch (strictness)
            {
                case JoinStrictness::RightAny:
                    return "RightAny ";
                case JoinStrictness::Any:
                    return "Any ";
                case JoinStrictness::Asof:
                    return "Asof ";
                case JoinStrictness::Semi:
                    return "Semi ";
                case JoinStrictness::Anti:
                    return "Anti";
                default:
                    return "";
            }
        };
        step_detail["JoinKind"] = get_kind(join_step->getKind());
        step_detail["Strictness"] = get_strictness(join_step->getStrictness());
        if (join_step->getJoinAlgorithm() != JoinAlgorithm::AUTO)
            step_detail["Algorithm"] = JoinAlgorithmConverter::toString(join_step->getJoinAlgorithm());

        String condition;
        for (size_t i = 0; i < join_step->getLeftKeys().size(); i++)
            step_vector_detail["Condition"].emplace_back(join_step->getLeftKeys()[i] + " == " + join_step->getRightKeys()[i]);

        if (!ASTEquality::compareTree(join_step->getFilter(), PredicateConst::TRUE_VALUE))
            step_detail["Filter"] = serializeAST(*join_step->getFilter());

        if (!join_step->getRuntimeFilterBuilders().empty())
        {
            std::set<std::string> runtime_filters;
            for (const auto & item : join_step->getRuntimeFilterBuilders())
                step_vector_detail["RuntimeFiltersBuilder"].emplace_back(item.first);
        }
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::SortingStepExt)
    {
        const auto * sort = dynamic_cast<const SortingStepExt *>(step.get());
        std::vector<String> sort_columns;
        for (const auto & desc : sort->getSortDescription())
            step_vector_detail["OrderBy"].emplace_back(
                desc.column_name + (desc.direction == -1 ? " desc" : " asc") + (desc.nulls_direction == -1 ? " nulls_last" : ""));
        step_detail["Limit"] = std::to_string(sort->getLimit());
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::LimitStepExt)
    {
        const auto * limit = dynamic_cast<const LimitStepExt *>(step.get());
        step_detail["Limit"] = std::to_string(limit->getLimit());
        step_detail["ffset"] = std::to_string(limit->getOffset());
    }

        if (getQueryPlanStepType(step) == QueryPlanStepType::OffsetStep)
    {
        const auto * offset = dynamic_cast<const OffsetStep *>(step.get());
        if (offset->getOffset())
            step_detail["Offset"] = std::to_string(offset->getOffset());
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::AggregatingStepExt)
    {
        const auto * agg = dynamic_cast<const AggregatingStepExt *>(step.get());
        auto keys = agg->getKeys();
        for (auto & key : keys)
            step_vector_detail["GroupByKeys"].emplace_back(key);

        auto keys_not_hashed = agg->getKeysNotHashed();
        if (!keys_not_hashed.empty())
        {
            for (const auto & key : keys_not_hashed)
                step_vector_detail["GroupByKeysNotHashed"].emplace_back(key);
        }

        for (const auto & desc : agg->getAggregates())
        {
            std::stringstream ss;
            String func_name = desc.function->getName();
            auto type_name = String(typeid(desc.function.get()).name());
            if (type_name.find("AggregateFunctionNull") != String::npos)
                func_name = String("AggNull(").append(std::move(func_name)).append(")");
            ss << desc.column_name << ":=" << func_name << join(desc.argument_names, ",", "(", ")");
            step_vector_detail["Aggregates"].emplace_back(ss.str());
        }
    }
    if (getQueryPlanStepType(step) == QueryPlanStepType::MergingAggregatedStepExt)
    {
        const auto * agg = dynamic_cast<const MergingAggregatedStepExt *>(step.get());
        auto keys = agg->getKeys();
        for (auto & key : keys)
            step_vector_detail["GroupByKeys"].emplace_back(key);

        for (const auto & desc : agg->getAggregates())
        {
            std::stringstream ss;
            String func_name = desc.function->getName();
            auto type_name = String(typeid(desc.function.get()).name());
            if (type_name.find("AggregateFunctionNull") != String::npos)
                func_name = String("AggNull(").append(std::move(func_name)).append(")");
            ss << desc.column_name << ":=" << func_name << join(desc.argument_names, ",", "(", ")");
            step_vector_detail["Aggregates"].emplace_back(ss.str());
        }
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::ExchangeStepExt)
    {
        const auto * exchange = dynamic_cast<const ExchangeStepExt *>(step.get());
        auto f = [](RExchangeMode::Enum mode) {
            switch (mode)
            {
                case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
                case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                    return "Local";
                case RExchangeMode::BROADCAST:
                    return "Broadcast";
                case RExchangeMode::REPARTITION:
                    return "Repartition";
                case RExchangeMode::GATHER:
                    return "Gather";
                default:
                    return "";
            }
        };
        step_detail["Mode"] = f(exchange->getExchangeMode());
        if (exchange->getExchangeMode() == RExchangeMode::REPARTITION)
        {
            for (const auto & item : (exchange->getSchema().getColumns()))
                step_vector_detail["PartitionBy"].emplace_back(item);
        }
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::FilterStepExt)
    {
        const auto * filter = dynamic_cast<const FilterStepExt *>(step.get());
        auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter->getFilter());
        step_detail["Filter"] = serializeAST(*PredicateUtils::combineConjuncts(filters.second));
        if (!filters.first.empty())
            step_detail["RuntimeFilter"] = serializeAST(*PredicateUtils::combineConjuncts(filters.first));
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::ProjectionStepExt)
    {
        const auto * projection = dynamic_cast<const ProjectionStepExt *>(step.get());

        std::vector<String> identities;
        std::vector<String> assignments;

        for (const auto & assignment : projection->getAssignments())
            if (Utils::isIdentity(assignment))
                identities.emplace_back(assignment.first);
            else
                assignments.emplace_back(assignment.first + ":=" + serializeAST(*assignment.second));

        std::sort(assignments.begin(), assignments.end());
        if (!identities.empty())
        {
            std::stringstream ss;
            std::sort(identities.begin(), identities.end());
            for (auto & identitie : identities)
                assignments.insert(assignments.begin(), identitie);
        }
        for (auto & assignment : assignments)
            step_vector_detail["Expressions"].emplace_back(assignment);
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::TableScanStepExt)
    {
        const auto * table_scan = dynamic_cast<const TableScanStepExt *>(step.get());
        std::vector<String> identities;
        std::vector<String> assignments;
        for (const auto & name_with_alias : table_scan->getColumnAlias())
            if (name_with_alias.second == name_with_alias.first)
                identities.emplace_back(name_with_alias.second);
            else
                assignments.emplace_back(name_with_alias.second + ":=" + name_with_alias.first);

        const auto & query_info = table_scan->getQueryInfo();
        auto * query = query_info.query->as<ASTSelectQuery>();

        if (auto where = query->where())
            step_detail["Where"] = PlanPrinter::TextPrinter::printFilter(where);
        if (auto prewhere = query->prewhere())
            step_detail["Prewhere"] = PlanPrinter::TextPrinter::printFilter(prewhere);
        if (query->limitLength())
        {
            Field converted = convertFieldToType(query->refLimitLength()->as<ASTLiteral>()->value, DataTypeUInt64());
            step_detail["Limit"] = std::to_string(converted.safeGet<UInt64>());
        }

        std::sort(assignments.begin(), assignments.end());
        if (query->sampleSize())
        {
            ASTSampleRatio * sample = query->sampleSize()->as<ASTSampleRatio>();
            step_detail["SampleSize"] = ASTSampleRatio::toString(sample->ratio);
            if (query->sampleOffset())
            {
                ASTSampleRatio * sample_offset = query->sampleOffset()->as<ASTSampleRatio>();
                step_detail["SampleOffset"] = ASTSampleRatio::toString(sample_offset->ratio);
            }
        }

        if (!identities.empty())
        {
            std::stringstream ss;
            for (auto & identitie : identities)
                assignments.insert(assignments.begin(), identitie);
        }

        for (auto & assignment : assignments)
            step_vector_detail["Outputs"].emplace_back(assignment);

        std::vector<String> inline_expressions;
        for (const auto & assignment : table_scan->getInlineExpressions())
            step_vector_detail["InlineExpressions"].emplace_back(assignment.first + ":=" + serializeAST(*assignment.second));

        if (table_scan->getPushdownFilter())
        {
            NodeDescriptionPtr push_down_filter_detail = std::make_shared<NodeDescription>();
            push_down_filter_detail->setStepDetail(table_scan->getPushdownFilter());
            descriptions_in_step["PushDownFilter"] = push_down_filter_detail;
        }

        if (table_scan->getPushdownProjection())
        {
            NodeDescriptionPtr push_down_projection_detail = std::make_shared<NodeDescription>();
            push_down_projection_detail->setStepDetail(table_scan->getPushdownProjection());
            descriptions_in_step["PushDownProjection"] = push_down_projection_detail;
        }

        if (table_scan->getPushdownAggregation())
        {
            NodeDescriptionPtr push_down_aggregation_detail = std::make_shared<NodeDescription>();
            push_down_aggregation_detail->setStepDetail(table_scan->getPushdownAggregation());
            descriptions_in_step["PushDownAggregation"] = push_down_aggregation_detail;
        }
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::TopNFilteringStepExt)
    {
        const auto *topn_filter = dynamic_cast<const TopNFilteringStepExt *>(step.get());
        std::vector<String> sort_columns;
        for (const auto & desc : topn_filter->getSortDescription())
            step_vector_detail["OrderBy"].emplace_back(desc.dump());
        step_detail["Size"] = std::to_string(topn_filter->getSize());
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::CTERefStepExt)
    {
        const auto * cte = dynamic_cast<const CTERefStepExt *>(step.get());
        step_detail["CTEId"] = std::to_string(cte->getId());
    }

    if (getQueryPlanStepType(step) == QueryPlanStepType::RemoteExchangeSourceStepExt)
    {
        const auto * remote_write = dynamic_cast<const RemoteExchangeSourceStepExt *>(step.get());
        auto inputs = remote_write->getInput();
        for (const auto & input : inputs)
        {
            for (const auto & column : input->getHeader())
                step_vector_detail["Segment["+ std::to_string(input->getPlanSegmentId())+"]"].emplace_back(column.name);
        }
    }
}

void NodeDescription::setStepStatistic(PlanNodePtr node)
{
    if (node->getStatistics().has_value())
    {
        NodeDescription::StatisticInfo node_stats;
        node_stats.row_count = node->getStatistics().value()->getRowCount();
        stats = node_stats;
    }
}

Poco::JSON::Object::Ptr
NodeDescription::jsonNodeDescription(const StepProfiles & node_profiles, bool print_stats, const PlanCostMap & costs)
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);
    json->set("NodeId", node_id);
    json->set("NodeType", step_name);
    for (auto & detail : step_detail)
        json->set(detail.first, detail.second);
    for (auto & vector_detail : step_vector_detail)
    {
        Poco::JSON::Array details;
        for (const auto& item : vector_detail.second)
            details.add(item);
        json->set(vector_detail.first, details);
    }

    if (stats.has_value() && print_stats)
    {
        Poco::JSON::Object::Ptr stats_json = new Poco::JSON::Object(true);
        stats_json->set("RowCount", stats.value().row_count);
        json->set("Statistic", stats_json);
    }

    if (node_profiles.contains(node_id))
    {
        const auto & profile_detail = node_profiles.at(node_id);
        Poco::JSON::Object::Ptr profiles = new Poco::JSON::Object(true);
        profiles->set("WallTimeMs", float(profile_detail->sum_elapsed_us)/profile_detail->worker_cnt/1000);
        profiles->set("MaxWallTimeMs", float(profile_detail->max_elapsed_us)/1000);
        profiles->set("MinWallTimeMs", float(profile_detail->min_elapsed_us)/1000);
        profiles->set("OutputRows", profile_detail->output_rows);
        profiles->set("OutputBytes", profile_detail->output_bytes);
        profiles->set("OutputWaitTimeMs", float(profile_detail->output_wait_sum_elapsed_us)/profile_detail->worker_cnt/1000);
        profiles->set("MaxOutputWaitTimeMs", float(profile_detail->output_wait_max_elapsed_us)/1000);
        profiles->set("MinOutputWaitTimeMs", float(profile_detail->output_wait_min_elapsed_us)/1000);
        Poco::JSON::Array inputs_profile;
        if (!children.empty() && profile_detail->inputs.contains(children[0]->node_id))
        {
            for (auto & child : children)
            {
                auto input_profile = profile_detail->inputs[child->node_id];
                Poco::JSON::Object::Ptr input = new Poco::JSON::Object(true);
                input->set("InputNodeId", child->node_id);
                input->set("InputRows", input_profile.input_rows);
                input->set("InputBytes", input_profile.input_bytes);
                input->set("InputWaitTimeMs", float(input_profile.input_wait_sum_elapsed_us)/profile_detail->worker_cnt/1000);
                input->set("MaxInputWaitTimeMs", float(input_profile.input_wait_max_elapsed_us)/1000);
                input->set("MinInputWaitTimeMs", float(input_profile.input_wait_min_elapsed_us)/1000);
                inputs_profile.add(input);
            }
        }
        else
        {
            for (auto input_profile : profile_detail->inputs)
            {
                Poco::JSON::Object::Ptr input = new Poco::JSON::Object(true);
                input->set("InputNodeId", input_profile.first);
                input->set("InputRows", input_profile.second.input_rows);
                input->set("InputBytes", input_profile.second.input_bytes);
                input->set("InputWaitTimeMs", float(input_profile.second.input_wait_sum_elapsed_us)/profile_detail->worker_cnt/1000);
                input->set("MaxInputWaitTimeMs", float(input_profile.second.input_wait_max_elapsed_us)/1000);
                input->set("MinInputWaitTimeMs", float(input_profile.second.input_wait_min_elapsed_us)/1000);
                inputs_profile.add(input);
            }
        }
        profiles->set("Inputs", inputs_profile);

        double filtered = 0.0;
        if (children.size() > 1)
        {
            size_t max_input_rows = 0;
            for (const auto & child : children)
            {
                if (!node_profiles.contains(child->node_id))
                    continue;
                max_input_rows = std::max(max_input_rows, node_profiles.at(child->node_id)->output_rows);
            }
            if (max_input_rows != 0)
            {
                double max_rows = static_cast<double>(max_input_rows);
                filtered = max_rows > 0 ? ((max_rows - static_cast<double>(profile_detail->output_rows)) * static_cast<double>(100) / max_rows) : 0;
            }
        }
        else if (children.size() == 1)
        {
            if (node_profiles.contains(children[0]->node_id))
            {
                auto child_input_rows = static_cast<double>(node_profiles.at(children[0]->node_id)->output_rows);
                filtered = child_input_rows > 0 ? ((child_input_rows - static_cast<double>(profile_detail->output_rows)) * static_cast<double>(100) / child_input_rows) : 0;
            }
        }
        profiles->set("FilteredRate", filtered);
        json->set("Profiles", profiles);
    }

    if (!descriptions_in_step.empty())
    {
        Poco::JSON::Object::Ptr descriptions = new Poco::JSON::Object(true);
        for (auto & desc : descriptions_in_step)
            descriptions->set(desc.first, desc.second->jsonNodeDescription(node_profiles, print_stats, costs));
        json->set("StepDescriptions", descriptions);
    }

    Poco::JSON::Array children_array;
    for (auto & child : children)
        children_array.add(child->jsonNodeDescription(node_profiles, print_stats, costs));

    if (!children.empty())
        json->set("Children", children_array);
    return json;
}

NodeDescriptionPtr NodeDescription::getPlanDescription(QueryPlan::Node * node)
{
    auto description = std::make_shared<NodeDescription>();
    description->node_id = node->id;
    description->setStepDetail(node->step);
    for (auto * child : node->children)
    {
        auto child_desc = getPlanDescription(child);
        description->children.emplace_back(child_desc);
    }
    return description;
}

NodeDescriptionPtr NodeDescription::getPlanDescription(PlanNodePtr node)
{
    auto description = std::make_shared<NodeDescription>();
    description->node_id = node->getId();
    description->setStepDetail(node->getStep());
    description->setStepStatistic(node);
    for (auto & child : node->getChildren())
    {
        auto child_desc = getPlanDescription(child);
        description->children.emplace_back(child_desc);
    }
    return description;
}

String PlanSegmentDescription::jsonPlanSegmentDescriptionAsString(const StepProfiles & profiles)
{
    auto json = jsonPlanSegmentDescription(profiles);
    std::ostringstream os;
    json->stringify(os, 1);
    return os.str();
}

Poco::JSON::Object::Ptr PlanSegmentDescription::jsonPlanSegmentDescription(const StepProfiles & profiles, bool is_pipeline)
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);

    auto f = [](RExchangeMode::Enum xchg_mode) {
        switch (xchg_mode)
        {
            case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
                return "LOCAL_NO_NEED_REPARTITION";
            case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                return "LOCAL_MAY_NEED_REPARTITION";
            case RExchangeMode::BROADCAST:
                return "BROADCAST";
            case RExchangeMode::REPARTITION:
                return "REPARTITION";
            case RExchangeMode::GATHER:
                return "GATHER";
            default:
                return "UNKNOWN";
        }
    };

    json->set("SegmentID", segment_id);
    json->set("SegmentType", segment_type);
    String exchange = (segment_id == 0) ? "Output" : f(mode);
    json->set("OutputExchangeMode", exchange);
    if (exchange == "REPARTITION") // print shuffle keys
    {
        Poco::JSON::Array keys;
        for (auto & key : shuffle_keys)
            keys.add(key);
        json->set("ShuffleKeys", keys);
    }
    json->set("ParallelSize", parallel);
    json->set("ClusterName", (cluster_name.empty() ? "server" : cluster_name));
    json->set("ExchangeParallelSize", exchange_parallel_size);
    if (!output_columns.empty())
    {
        Poco::JSON::Array output_array;
        for (const auto & column : output_columns)
            output_array.add(column);
        json->set("OutputColumns", output_array);
    }

    if (!outputs_desc.empty())
    {
        Poco::JSON::Array outputs;
        for (auto & output : outputs_desc)
        {
            Poco::JSON::Object::Ptr output_json = new Poco::JSON::Object(true);
            output_json->set("SegmentID", output->segment_id);
            output_json->set("PlanSegmentType",output->plan_segment_type);
            output_json->set("ExchangeId", output->exchange_id);
            output_json->set("ExchangeMode", f(output->mode));
            output_json->set("ParallelSize", output->parallel_size);
            output_json->set("KeepOrder", output->keep_order);
            outputs.add(output_json);
        }
        json->set("Outputs", outputs);
    }

    if (!inputs_desc.empty())
    {
        Poco::JSON::Array inputs;
        for (auto & input : inputs_desc)
        {
            Poco::JSON::Object::Ptr input_json = new Poco::JSON::Object(true);
            input_json->set("SegmentID", input->segment_id);
            input_json->set("ExchangeId", input->exchange_id);
            input_json->set("ExchangeMode", f(input->mode));
            input_json->set("ExchangeParallelSize", input->exchange_parallel_size);
            input_json->set("KeepOrder", input->keep_order);
            inputs.add(input_json);
        }
        json->set("Inputs", inputs);
    }

    if (node_description && !is_pipeline)
        json->set("QueryPlan", node_description->jsonNodeDescription(profiles, false));
    return json;
}

PlanSegmentDescriptionPtr PlanSegmentDescription::getPlanSegmentDescription(PlanSegmentPtr & segment, bool record_plan_detail)
{
    auto plan_segment_desc = std::make_shared<PlanSegmentDescription>();
    auto & query_plan = segment->getQueryPlan();
    plan_segment_desc->segment_id = segment->getPlanSegmentId();
    plan_segment_desc->root_id = query_plan.getRoot()->id;
    plan_segment_desc->root_child_id = query_plan.getRoot()->children.empty() ? query_plan.getRoot()->id : query_plan.getRoot()->children[0]->id;
    plan_segment_desc->query_id = segment->getQueryId();
    plan_segment_desc->cluster_name = segment->getClusterName();
    plan_segment_desc->parallel = segment->getParallelSize();
    plan_segment_desc->exchange_parallel_size = segment->getExchangeParallelSize();
    plan_segment_desc->shuffle_keys = segment->getPlanSegmentOutput()->getShufflekeys();
    plan_segment_desc->mode = segment->getPlanSegmentOutput()->getExchangeMode();
    std::unordered_map<PlanNodeId, size_t> exchange_to_segment;
    segment->getRemoteSegmentId(query_plan.getRoot(), exchange_to_segment);
    plan_segment_desc->exchange_to_segment = exchange_to_segment;

    if (plan_segment_desc->segment_id == 0)
        plan_segment_desc->segment_type = "OUTPUT";
    else if (plan_segment_desc->exchange_to_segment.empty())
        plan_segment_desc->segment_type = "SOURCE";
    else
        plan_segment_desc->segment_type = "PROCESS";

    if (plan_segment_desc->segment_id != 0 && !segment->getPlanSegmentOutputs().empty())
    {
        for (auto & output : segment->getPlanSegmentOutputs())
        {
            PlanSegmentDescription::OutputInfo output_desc;
            output_desc.segment_id = output->getPlanSegmentId();
            output_desc.plan_segment_type = planSegmentTypeToString(output->getPlanSegmentType());
            output_desc.exchange_id = output->getExchangeId();
            output_desc.mode = output->getExchangeMode();
            output_desc.parallel_size = output->getParallelSize();
            output_desc.keep_order = output->needKeepOrder();
            auto output_desc_ptr = std::make_shared<PlanSegmentDescription::OutputInfo>(output_desc);
            plan_segment_desc->outputs_desc.emplace_back(output_desc_ptr);
        }
    }

    if (!segment->getPlanSegmentInputs().empty())
    {
        for (auto & input : segment->getPlanSegmentInputs())
        {
            if (input->getPlanSegmentType() == RIPlanSegment::SOURCE)
                continue;
            PlanSegmentDescription::InputInfo input_desc;
            input_desc.segment_id = input->getPlanSegmentId();
            input_desc.exchange_id = input->getExchangeId();
            input_desc.mode = input->getExchangeMode();
            input_desc.exchange_parallel_size = input->getExchangeParallelSize();
            input_desc.keep_order = input->needKeepOrder();
            input_desc.stable = input->isStable();
            auto input_desc_ptr = std::make_shared<PlanSegmentDescription::InputInfo>(input_desc);
            plan_segment_desc->inputs_desc.emplace_back(input_desc_ptr);
        }
    }

    if (query_plan.getRoot())
    {
        const auto & header = query_plan.getRoot()->step->getOutputStream().header;
        for (const auto & it : header)
            plan_segment_desc->output_columns.push_back(it.name);
    }

    if (record_plan_detail)
        plan_segment_desc->node_description = NodeDescription::getPlanDescription(query_plan.getRoot());
    return plan_segment_desc;
}

String PlanPrinter::jsonDistributedPlan(PlanSegmentDescriptions & segment_descs, const StepProfiles & profiles)
{
    Poco::JSON::Object::Ptr distributed_plan = new Poco::JSON::Object(true);
    Poco::JSON::Array segments;
    for (auto & segment_desc : segment_descs)
        segments.add(segment_desc->jsonPlanSegmentDescription(profiles));
    distributed_plan->set("DistributedPlan", segments);
    std::ostringstream os;
    distributed_plan->stringify(os, 1);
    return os.str();
}

String PlanPrinter::jsonMetaData(
    ASTPtr & query, AnalysisPtr analysis, ContextMutablePtr context, QueryPlanExtPtr & plan, const QueryMetadataSettings & settings)
{
    Poco::JSON::Object::Ptr metadata_json = new Poco::JSON::Object(true);

    Poco::JSON::Array table_and_columns_info;
    const auto & used_columns_map = analysis->getUsedColumns();
    for (const auto & [table_ast, storage_analysis] : analysis->getStorages())
    {
        Poco::JSON::Object::Ptr used_table_info = new Poco::JSON::Object(true);

        used_table_info->set("Database", storage_analysis.database);
        used_table_info->set("Table", storage_analysis.table);

        Poco::JSON::Array used_columns;
        if (auto it = used_columns_map.find(storage_analysis.storage->getStorageID()); it != used_columns_map.end())
        {
            for (const auto & column : it->second)
                used_columns.add(column);
        }

        used_table_info->set("Columns", used_columns);

        table_and_columns_info.add(used_table_info);
    }
    metadata_json->set("UsedTablesInfo", table_and_columns_info);


    Poco::JSON::Array used_functions;
    //get used functions
    for (const auto & func_name : analysis->getUsedFunctions())
        used_functions.add(func_name);

    metadata_json->set("UsedFunctions", used_functions);

    // get settings
    Poco::JSON::Object::Ptr query_used_settings = new Poco::JSON::Object(true);
    SettingsChanges settings_changes = InterpreterExplainQueryUseOptimizer::extractSettingsFromQuery(query);
    for (const auto & setting : settings_changes)
        query_used_settings->set(setting.name, setting.value.dump());
    metadata_json->set("UsedSettings", query_used_settings);

    Poco::JSON::Array output_descs;
    ASTPtr & select_ast = query;
    if (analysis->hasOutputDescription(*select_ast))
    {
        for (const auto & desc : analysis->getOutputDescription(*select_ast))
            output_descs.add(desc.name);
    }
    metadata_json->set("OutputDescriptions", output_descs);

    // get InsertInfo
    Poco::JSON::Object::Ptr insert_table_info = new Poco::JSON::Object(true);
    if (analysis->getInsert())
    {
        auto & insert_info = analysis->getInsert().value();
        insert_table_info->set("Database", insert_info.storage_id.getDatabaseName());
        insert_table_info->set("Table", insert_info.storage_id.getTableName());

        Poco::JSON::Array insert_columns;
        for (auto & column_info : insert_info.columns)
            insert_columns.add(column_info.name);
        insert_table_info->set("columns", insert_columns);
    }
    metadata_json->set("InsertInfo", insert_table_info);

    // get FunctionsInfo
    auto function_arguments = analysis->function_arguments;
    Poco::JSON::Array functions_info;
    for (const auto & func_args : function_arguments)
    {
        Poco::JSON::Object::Ptr function_info = new Poco::JSON::Object(true);
        function_info->set("FunctionName", func_args.first);

        Poco::JSON::Array function_const_aggs;
        for (const auto & arg : func_args.second)
            function_const_aggs.add(arg);
        function_info->set("ConstantArguments", function_const_aggs);
    }
    metadata_json->set("FunctionsInfo", functions_info);

    if (plan && plan->getPlanNode() && (settings.lineage || settings.lineage_use_optimizer))
    {
        LineageInfoVisitor visitor{context, plan->getCTEInfo()};
        LineageInfoContext lineage_info_context;
        VisitorUtil::accept(plan->getPlanNode(), visitor, lineage_info_context);

        Poco::JSON::Object::Ptr lineage_info = new Poco::JSON::Object(true);

        Poco::JSON::Array table_sources_info;
        for (auto & [full_name, table_source] : visitor.table_sources)
        {
            Poco::JSON::Object::Ptr table_info = new Poco::JSON::Object(true);
            table_info->set("Database", table_source.source_tables[0].first);
            table_info->set("Table", table_source.source_tables[0].second);

            Poco::JSON::Array columns_info;
            for (auto & [id, column] : table_source.id_to_source_name)
            {
                Poco::JSON::Object::Ptr column_info = new Poco::JSON::Object(true);
                column_info->set("Id", id);
                column_info->set("Name", column);
                columns_info.add(column_info);
            }
            table_info->set("Columns", columns_info);
            table_sources_info.add(table_info);
        }
        lineage_info->set("TableSources", table_sources_info);

        Poco::JSON::Array expression_sources_info;
        for (auto & expression_source : visitor.expression_or_value_sources)
        {
            Poco::JSON::Object::Ptr expression_info = new Poco::JSON::Object(true);

            Poco::JSON::Array tables_info;
            for (auto & [database, table] : expression_source.source_tables)
            {
                Poco::JSON::Object::Ptr table_info = new Poco::JSON::Object(true);
                table_info->set("Database", database);
                table_info->set("Table", table);
                tables_info.add(table_info);
            }
            expression_info->set("Sources", tables_info);

            Poco::JSON::Array expression_list;
            for (auto & [id, name] : expression_source.id_to_source_name)
            {
                Poco::JSON::Object::Ptr expression_element = new Poco::JSON::Object(true);
                expression_element->set("Id", id);
                expression_element->set("Name", name);
                expression_list.add(expression_element);
            }
            expression_info->set("Expression", expression_list);
            expression_sources_info.add(expression_info);
        }
        lineage_info->set("ExpressionSources", expression_sources_info);

        Poco::JSON::Array lineage_dag_info;
        for (auto & [output_name, outputstream_info] : lineage_info_context.output_stream_lineages)
        {
            Poco::JSON::Object::Ptr output_info = new Poco::JSON::Object(true);

            output_info->set("Name", output_name);
            Poco::JSON::Array source_id_list;
            for (const auto & id : outputstream_info->column_ids)
                source_id_list.add(id);
            output_info->set("SourceIds", source_id_list);
            lineage_dag_info.add(output_info);
        }
        lineage_info->set("OutputLineageInfo", lineage_dag_info);

        Poco::JSON::Object::Ptr insert_info = new Poco::JSON::Object(true);
        if (visitor.insert_info)
        {
            insert_info->set("Database", visitor.insert_info->database);
            insert_info->set("Table", visitor.insert_info->table);
            Poco::JSON::Array insert_columns_info;
            for (const auto & insert_column : visitor.insert_info->insert_columns_info)
            {
                Poco::JSON::Object::Ptr insert_column_info = new Poco::JSON::Object(true);
                insert_column_info->set("InsertColumnName", insert_column.insert_column_name);
                insert_column_info->set("InputName", insert_column.input_column);
                insert_columns_info.add(insert_column_info);
            }
            insert_info->set("InsertColumnInfo", insert_columns_info);
        }
        lineage_info->set("InsertLinageInfo", insert_info);

        metadata_json->set("LineageInfo", lineage_info);
    }
    std::ostringstream os;
    metadata_json->stringify(os, 1);
    return os.str();
}
}
