#include <Query/Planner/GraphvizPrinter.h>

#include <Query/Common/PlanSegmentProfile.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Optimizer/Cascades/GroupExpression.h>
#include <Query/Processors/QueryPlan/PlanPrinter.h>
#include <Query/Optimizer/PredicateUtils.h>
#include <Query/Optimizer/Cascades/Memo.h>

#include <Interpreters/ProcessList.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ArrayJoinAction.h>
#include <DataTypes/FieldToDataType.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>

#include <filesystem>
#include <fstream>
#include <string>

#include <boost/algorithm/string/replace.hpp>


namespace DB
{

class Void;

const String GraphvizPrinter::PIPELINE_PATH = "5000_pipeline";

static std::unordered_map<QueryPlanStepType, std::string> NODE_COLORS = {
    // NOLINT(cert-err58-cpp)
    {QueryPlanStepType::ProjectionStepExt, "bisque"},
    {QueryPlanStepType::ExpandStepExt, "RosyBrown"},
    {QueryPlanStepType::FilterStepExt, "yellow"},
    {QueryPlanStepType::JoinStepExt, "orange"},
    {QueryPlanStepType::ArrayJoinStep, "orange"},
    {QueryPlanStepType::AggregatingStepExt, "chartreuse3"},
    {QueryPlanStepType::MergingAggregatedStepExt, "chartreuse3"},
    {QueryPlanStepType::WindowStep, "darkolivegreen4"},
    {QueryPlanStepType::PartitionTopNStepExt, "darkolivegreen4"},
    {QueryPlanStepType::UnionStepExt, "turquoise4"},
    {QueryPlanStepType::IntersectOrExceptStep, "turquoise4"},
    {QueryPlanStepType::ExchangeStepExt, "gold"},
    {QueryPlanStepType::RemoteExchangeSourceStepExt, "gold"},
    {QueryPlanStepType::TableScanStepExt, "deepskyblue"},
    {QueryPlanStepType::ValuesStepExt, "deepskyblue"},
    {QueryPlanStepType::OffsetStep, "gray83"},
    {QueryPlanStepType::LimitStepExt, "gray83"},
    {QueryPlanStepType::FillingStep, "gray83"},
    {QueryPlanStepType::SortingStepExt, "aliceblue"},
    {QueryPlanStepType::DistinctStepExt, "darkolivegreen4"},
    {QueryPlanStepType::ExtremesStep, "goldenrod4"},
    {QueryPlanStepType::TotalsHavingStepExt, "goldenrod4"},
    {QueryPlanStepType::ApplyStepExt, "orange"},
    {QueryPlanStepType::EnforceSingleRowStepExt, "bisque"},
    {QueryPlanStepType::AssignUniqueIdStepExt, "bisque"},
    {QueryPlanStepType::CTERefStepExt, "orange"},
    {QueryPlanStepType::ExplainAnalyzeStepExt, "orange"},
    {QueryPlanStepType::TopNFilteringStepExt, "fuchsia"},
    {QueryPlanStepType::MarkDistinctStepExt, "violet"},
    {QueryPlanStepType::IntermediateResultCacheStepExt, "darkolivegreen4"},
};

[[maybe_unused]] static auto escapeSpecialCharacters = [](String content) {
    boost::replace_all(content, "<", "\\<");
    boost::replace_all(content, ">", "\\>");
    boost::replace_all(content, "{", "\\{");
    boost::replace_all(content, "}", "\\}");
    boost::replace_all(content, "\"", "\\\">");
    return content;
};

struct PrinterContext
{
    bool is_magic = false;
};

template <class V, class Func>
static std::string join(const V & v, Func && to_string, const String & sep = ", ", const String & prefix = {}, const String & suffix = {})
{
    std::stringstream out;
    out << prefix;
    if (!v.empty())
    {
        auto it = v.begin();
        out << to_string(*it);
        for (++it; it != v.end(); ++it)
            out << sep << to_string(*it);
    }
    out << suffix;
    return out.str();
}

Void PlanNodePrinter::visitPlanNode(PlanNodeBase & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String label = step_ptr->getName() + "Node";
    String color = GraphvizPrinter::getColor(getQueryPlanStepType(step_ptr));
    printNode(node, label, StepPrinter::printStep(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitProjectionStepExtNode(ProjectionStepExtNode & node, PrinterContext & context)
{
    String label{"ProjectionNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printProjectionStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitExpandStepExtNode(ExpandStepExtNode & node, PrinterContext & context)
{
    String label{"ExpandNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printExpandStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitFilterStepExtNode(FilterStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String label{"FilterNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printFilterStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitJoinStepExtNode(JoinStepExtNode & node, PrinterContext & context)
{
    String label{"JoinNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    if (step_ptr->isMagic())
    {
        PrinterContext magic{.is_magic = true};
        printNode(node, label, StepPrinter::printJoinStepExt(*step_ptr), color, magic);
        VisitorUtil::accept(*node.getChildren()[0], *this, context); // left node is not magic
        VisitorUtil::accept(*node.getChildren()[1], *this, magic);
    }
    else
    {
        printNode(node, label, StepPrinter::printJoinStepExt(*step_ptr), color, context);
        VisitorUtil::accept(*node.getChildren()[0], *this, context);
        VisitorUtil::accept(*node.getChildren()[1], *this, context);
    }

    return Void{};
}

Void PlanNodePrinter::visitArrayJoinStepNode(ArrayJoinStepNode & node, PrinterContext & context)
{
    String label{"ArrayJoinNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printArrayJoinStep(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitAggregatingStepExtNode(AggregatingStepExtNode & node, PrinterContext & context)
{
    String label{"AggregatingNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printAggregatingStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitMarkDistinctStepExtNode(MarkDistinctStepExtNode & node, PrinterContext & context)
{
    String label{"MarkDistinctNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printMarkDistinctStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitMergingAggregatedStepExtNode(MergingAggregatedStepExtNode & node, PrinterContext & context)
{
    String label{"MergingAggregatedNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printMergingAggregatedStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitUnionStepExtNode(UnionStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"UnionNode"};
    printNode(node, label, StepPrinter::printUnionStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitIntersectOrExceptStepNode(IntersectOrExceptStepNode & node, PrinterContext & context)
{
    String label{"IntersectOrExceptNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printIntersectOrExceptStep(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitExchangeStepExtNode(ExchangeStepExtNode & node, PrinterContext & context)
{
    String label{"ExchangeNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printExchangeStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitRemoteExchangeSourceStepExtNode(RemoteExchangeSourceStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"RemoteExchangeSourceNode"};
    printNode(node, label, StepPrinter::printRemoteExchangeSourceStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitTableScanStepExtNode(TableScanStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"TableScanNode"};
    printNode(node, label, StepPrinter::printTableScanStepExt(*step_ptr), color, context);
    return Void{};
}

Void PlanNodePrinter::visitReadNothingStepNode(ReadNothingStepNode & node, PrinterContext & context)
{
    auto step_ptr = node.getStep();
    String label{node.getStep()->getName()};
    String details{"ReadNothingNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, details, color, context);
    return Void{};
}

Void PlanNodePrinter::visitReadStorageRowCountStepExtNode(ReadStorageRowCountStepExtNode & node, PrinterContext & context)
{
    String label{"ReadStorageRowCountNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printReadStorageRowCountStepExt(*step_ptr), color, context);
    return Void{};
}

Void PlanNodePrinter::visitValuesStepExtNode(ValuesStepExtNode & node, PrinterContext & context)
{
    String label{"ValuesNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printValuesStepExt(*step_ptr), color, context);
    return Void{};
}

Void PlanNodePrinter::visitLimitStepExtNode(LimitStepExtNode & node, PrinterContext & context)
{
    String label{"LimitNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printLimitStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitOffsetStepNode(OffsetStepNode & node, PrinterContext & context)
{
    String label{"OffsetNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printOffsetStep(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitLimitByStepNode(LimitByStepNode & node, PrinterContext & context)
{
    String label{"LimitByNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printLimitByStep(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitSortingStepExtNode(SortingStepExtNode & node, PrinterContext & context)
{
    String label{"SortingNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printSortingStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitMergeSortingStepExtNode(MergeSortingStepExtNode & node, PrinterContext & context)
{
    String label{"MergeSortingNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printMergeSortingStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitPartialSortingStepExtNode(PartialSortingStepExtNode & node, PrinterContext & context)
{
    String label{"PartialSortingNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printPartialSortingStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitMergingSortedStepExtNode(MergingSortedStepExtNode & node, PrinterContext & context)
{
    String label{"MergingSortedNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printMergingSortedStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitDistinctStepExtNode(DistinctStepExtNode & node, PrinterContext & context)
{
    String label{"DistinctNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printDistinctStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitExtremesStepNode(ExtremesStepNode & node, PrinterContext & context)
{
    auto step_ptr = node.getStep();
    String label{"ExtremesNode"};
    auto & step = dynamic_cast<const ExtremesStep &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printExtremesStep(step), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitTotalsHavingStepExtNode(TotalsHavingStepExtNode & node, PrinterContext & context)
{
    auto step_ptr = node.getStep();
    String label{"TotalsHavingNode"};
    auto & step = dynamic_cast<const TotalsHavingStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printTotalsHavingStepExt(step), color, context);
    return visitChildren(node, context);
}


Void PlanNodePrinter::visitFinalSampleStepExtNode(FinalSampleStepExtNode & node, PrinterContext & context)
{
    auto step_ptr = node.getStep();
    String label{"FinalSampleNode"};
    const auto & step = dynamic_cast<const FinalSampleStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printFinalSampleStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitApplyStepExtNode(ApplyStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"ApplyNode"};
    printNode(node, label, StepPrinter::printApplyStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"EnforceSingleRowNode"};
    printNode(node, label, StepPrinter::printEnforceSingleRowStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"AssignUniqueIdStep"};
    printNode(node, label, StepPrinter::printAssignUniqueIdStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitWindowStepNode(WindowStepNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"WindowNode"};
    printNode(node, label, StepPrinter::printWindowStep(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitPartitionTopNStepExtNode(PartitionTopNStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"PartitionTopNNode"};
    printNode(node, label, StepPrinter::printPartitionTopNStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitTopNFilteringStepExtNode(TopNFilteringStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"TopNFilteringNode"};
    printNode(node, label, StepPrinter::printTopNFilteringStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitIntermediateResultCacheStepExtNode(IntermediateResultCacheStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    String label{"IntermediateResultCacheNode"};
    printNode(node, label, StepPrinter::printIntermediateResultCacheStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

void PlanNodePrinter::printNode(
    const PlanNodeBase & node, const String & label, const String & details, const String & color, PrinterContext & context)
{
    out << "plannode_" << node.getId() << R"([label="{)" << escapeSpecialCharacters(label) << "|" << escapeSpecialCharacters(details);

    if (with_id)
        out << "|" << node.getId();
    
    if (node.getStatistics().isDerived())
    {
        out << "|";
        out << "Estimate Stats \\n";
        const auto & statistics = node.getStatistics();
        if (statistics)
            out << escapeSpecialCharacters(statistics.value()->toString());
        else
            out << "None";
    }

    if (!profiles.empty() && profiles.count(node.getId()))
    {
        const auto & profile = profiles.at(node.getId());
        out << "|";
        out << "Actual Stats \\n";
        out << "Output: " << PlanPrinter::TextPrinter::prettyNum(profile->output_rows) << " rows("
            << PlanPrinter::TextPrinter::prettyBytes(profile->output_bytes) << "). "
            << " Wait Time: " << PlanPrinter::TextPrinter::prettySeconds(profile->output_wait_max_elapsed_us)
            << " Wall Time: " << PlanPrinter::TextPrinter::prettySeconds(profile->max_elapsed_us) << " \\n";
        if (!node.getChildren().empty() && profile->inputs.contains(node.getChildren()[0]->getId()))
        {
            if (node.getChildren().size() == 1)
            {
                out << "Input: ";
                out << PlanPrinter::TextPrinter::prettyNum(profile->inputs[node.getChildren()[0]->getId()].input_rows) << " rows("
                    << PlanPrinter::TextPrinter::prettyBytes(profile->inputs[node.getChildren()[0]->getId()].input_bytes) << "). "
                    << "Wait Time: "
                    << PlanPrinter::TextPrinter::prettySeconds(profile->inputs[node.getChildren()[0]->getId()].input_wait_max_elapsed_us)
                    << " \\n";
            }
            else
            {
                int num = 1;
                out << "Input: \\n";
                for (const auto & child : node.getChildren())
                {
                    auto input_profile = profile->inputs[child->getId()];
                    out << "source [" << num << "] : " << PlanPrinter::TextPrinter::prettyNum(input_profile.input_rows) << " rows("
                        << PlanPrinter::TextPrinter::prettyBytes(input_profile.input_bytes) << "). "
                        << "Wait Time: " << PlanPrinter::TextPrinter::prettySeconds(input_profile.input_wait_max_elapsed_us) << " \\n";
                    ++num;
                }
            }
        }
    }

    String style = context.is_magic ? "rounded, filled, dashed" : "rounded, filled";

    out << R"(}", style=")" << style << R"(", shape=record, fillcolor=)" << color << "]"
        << ";" << std::endl;
}

Void PlanNodePrinter::visitChildren(PlanNodeBase & node, PrinterContext & context)
{
    auto children = node.getChildren();
    for (auto & iter : children)
    {
        VisitorUtil::accept(*iter, *this, context);
    }
    return Void{};
}

Void PlanNodePrinter::visitCTERefStepExtNode(CTERefStepExtNode & node, PrinterContext & context)
{
    const auto & step_ptr = node.getStep();
    String label{"CTERefNode"};
    String color{NODE_COLORS.at(getQueryPlanStepType(step_ptr))};
    printNode(node, label, StepPrinter::printCTERefStepExt(*step_ptr), color, context);

    if (cte_helper && !cte_helper->hasVisited(step_ptr->getId()))
    {
        printCTEDefNode(step_ptr->getId());
        cte_helper.value().accept(step_ptr->getId(), *this, context);
    }

    return Void{};
}

Void PlanNodePrinter::visitExplainAnalyzeStepExtNode(ExplainAnalyzeStepExtNode & node, PrinterContext & context)
{
    String label{"ExplainAnalyzeNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printExplainAnalyzeStepExt(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanNodePrinter::visitFillingStepNode(FillingStepNode & node, PrinterContext & context)
{
    String label{"FillingNode"};
    const auto & step_ptr = node.getStep();
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printFillingStep(*step_ptr), color, context);
    return visitChildren(node, context);
}

void PlanNodePrinter::printCTEDefNode(CTEId cte_id)
{
    out << "cte_" << cte_id << R"([label="{CTEDefNode|CTEId: )" << cte_id << R"(}", style="rounded, filled", shape=record];)" << std::endl;
}

Void PlanNodeEdgePrinter::visitPlanNode(PlanNodeBase & node, Void & context)
{
    auto children = node.getChildren();
    for (auto & iter : children)
    {
        printEdge(*iter, node);
        VisitorUtil::accept(*iter, *this, context);
    }
    return Void{};
}

Void PlanNodeEdgePrinter::visitCTERefStepExtNode(CTERefStepExtNode & node, Void & c)
{
    const auto & step = dynamic_cast<const CTERefStepExt &>(*node.getStep().get());
    if (cte_helper)
    {
        if (!cte_helper->hasVisited(step.getId()))
        {
            auto & cte_plan = *cte_helper.value().getCTEInfo().getCTEDef(step.getId());
            out << "plannode_" << cte_plan.getId() << " -> "
                << "cte_" << step.getId() << std::endl;
        }
        out << "cte_" << step.getId() << " -> "
            << "plannode_" << node.getId() << "[style=dashed];" << std::endl;
        cte_helper->accept(step.getId(), *this, c);
    }
    return Void{};
}

Void PlanNodeEdgePrinter::visitJoinStepExtNode(JoinStepExtNode & node, Void & context)
{
    auto children = node.getChildren();
    printEdge(*children.at(0), node);
    VisitorUtil::accept(*children.at(0), *this, context);
    printEdge(*children.at(1), node, "[color=green]");
    VisitorUtil::accept(*children.at(1), *this, context);
    return Void{};
}

void PlanNodeEdgePrinter::printEdge(PlanNodeBase & from, PlanNodeBase & to, std::string_view format)
{
    out << "plannode_" << from.getId() << " -> "
        << "plannode_" << to.getId() << format << ";" << std::endl;
}


Void PlanSegmentNodePrinter::visitNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label = step_ptr->getName() + "Node";
    String color = GraphvizPrinter::getColor(getQueryPlanStepType(step_ptr));
    printNode(node, label, StepPrinter::printStep(*step_ptr), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitProjectionStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"ProjectionNode"};
    const auto & step = dynamic_cast<const ProjectionStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printProjectionStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitExpandStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"ExpandNode"};
    const auto & step = dynamic_cast<const ExpandStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printExpandStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitFilterStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    const auto & step = dynamic_cast<const FilterStepExt &>(*step_ptr);
    String label{"FilterNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printFilterStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitJoinStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"JoinNode"};
    const auto & step = dynamic_cast<const JoinStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};

    if (step.isMagic())
    {
        PrinterContext magic{.is_magic = true};
        printNode(node, label, StepPrinter::printJoinStepExt(step), color, magic);
        VisitorUtil::accept(node->children[0], *this, context); // left node is not magic
        VisitorUtil::accept(node->children[1], *this, magic);
    }
    else
    {
        printNode(node, label, StepPrinter::printJoinStepExt(step), color, context);
        VisitorUtil::accept(node->children[0], *this, context);
        VisitorUtil::accept(node->children[1], *this, context);
    }
    return Void{};
}

Void PlanSegmentNodePrinter::visitArrayJoinStepNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"ArrayJoin"};
    const auto & step = dynamic_cast<const ArrayJoinStep &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printArrayJoinStep(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitAggregatingStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"AggregatingNode"};
    const auto & step = dynamic_cast<const AggregatingStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printAggregatingStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitMarkDistinctStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"MarkDistinctNode"};
    const auto & step = dynamic_cast<const MarkDistinctStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printMarkDistinctStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitMergingAggregatedStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"MergingAggregatedNode"};
    auto & step = dynamic_cast<const MergingAggregatedStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printMergingAggregatedStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitUnionStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const UnionStepExt &>(*step_ptr);
    String label{"UnionNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printUnionStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitExchangeStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"ExchangeNode"};
    auto & step = dynamic_cast<const ExchangeStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printExchangeStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitRemoteExchangeSourceStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const RemoteExchangeSourceStepExt &>(*step_ptr);
    String label{"RemoteExchangeSourceNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printRemoteExchangeSourceStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitTableScanStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const TableScanStepExt &>(*step_ptr);
    String label{"TableScanNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printTableScanStepExt(step), color, context);
    return Void{};
}

Void PlanSegmentNodePrinter::visitReadNothingStepNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"ReadNothingNode"};
    String details{"ReadNothingNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, details, color, context);
    return Void{};
}

Void PlanSegmentNodePrinter::visitReadStorageRowCountStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"ReadStorageRowCountNode"};
    auto & step = dynamic_cast<const ReadStorageRowCountStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printReadStorageRowCountStepExt(step), color, context);
    return Void{};
}

Void PlanSegmentNodePrinter::visitValuesStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"ValuesNode"};
    auto & step = dynamic_cast<const ValuesStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printValuesStepExt(step), color, context);
    return Void{};
}

Void PlanSegmentNodePrinter::visitLimitStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"LimitNode"};
    auto & step = dynamic_cast<const LimitStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printLimitStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitOffsetStepNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"OffsetNode"};
    auto & step = dynamic_cast<const OffsetStep &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printOffsetStep(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitLimitByStepNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"LimitByNode"};
    auto & step = dynamic_cast<const LimitByStep &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printLimitByStep(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitMergeSortingStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"MergeSortingNode"};
    auto & step = dynamic_cast<const MergeSortingStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printMergeSortingStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitSortingStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"SortingNode"};
    auto & step = dynamic_cast<const SortingStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printSortingStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitFillingStepNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"FillingNode"};
    auto & step = dynamic_cast<const FillingStep &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printFillingStep(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitIntersectOrExceptStepNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"IntersectOrExceptNode"};
    auto & step = dynamic_cast<const IntersectOrExceptStep &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printIntersectOrExceptStep(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitPartialSortingStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"PartialSortingNode"};
    auto & step = dynamic_cast<const PartialSortingStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printPartialSortingStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitMergingSortedStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"MergingSortedNode"};
    auto & step = dynamic_cast<const MergingSortedStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printMergingSortedStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitDistinctStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"DistinctNode"};
    auto & step = dynamic_cast<const DistinctStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printDistinctStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitExtremesStepNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"ExtremesNode"};
    auto & step = dynamic_cast<const ExtremesStep &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printExtremesStep(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitTotalsHavingStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"TotalsHavingNode"};
    auto & step = dynamic_cast<const TotalsHavingStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printTotalsHavingStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitFinalSampleStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    String label{"FinalSampleNode"};
    const auto & step = dynamic_cast<const FinalSampleStepExt &>(*step_ptr);
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printFinalSampleStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitApplyStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const ApplyStepExt &>(*step_ptr);
    String label{"ApplyNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printApplyStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitEnforceSingleRowStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const EnforceSingleRowStepExt &>(*step_ptr);
    String label{"EnforceSingleRowNode"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printEnforceSingleRowStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitAssignUniqueIdStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const AssignUniqueIdStepExt &>(*step_ptr);
    String label{"AssignUniqueIdStep"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printAssignUniqueIdStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitWindowStepNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const WindowStep &>(*step_ptr);
    String label{"WindowNode"};
    String color{NODE_COLORS.at(getQueryPlanStepType(step_ptr))};
    printNode(node, label, StepPrinter::printWindowStep(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitPartitionTopNStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const PartitionTopNStepExt &>(*step_ptr);
    String label{"PartitionTopNNode"};
    String color{NODE_COLORS.at(getQueryPlanStepType(step_ptr))};
    printNode(node, label, StepPrinter::printPartitionTopNStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitExplainAnalyzeStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    const auto & step = dynamic_cast<const ExplainAnalyzeStepExt &>(*step_ptr);
    String label{"ExplainAnalyzeStep"};
    String color{NODE_COLORS[getQueryPlanStepType(step_ptr)]};
    printNode(node, label, StepPrinter::printExplainAnalyzeStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitTopNFilteringStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const TopNFilteringStepExt &>(*step_ptr);
    String label{"TopNFilteringNode"};
    String color{NODE_COLORS.at(getQueryPlanStepType(step_ptr))};
    printNode(node, label, StepPrinter::printTopNFilteringStepExt(step), color, context);
    return visitChildren(node, context);
}

Void PlanSegmentNodePrinter::visitIntermediateResultCacheStepExtNode(QueryPlan::Node * node, PrinterContext & context)
{
    auto & step_ptr = node->step;
    auto & step = dynamic_cast<const IntermediateResultCacheStepExt &>(*step_ptr);
    String label{"IntermediateResultCacheNode"};
    String color{NODE_COLORS.at(getQueryPlanStepType(step_ptr))};
    printNode(node, label, StepPrinter::printIntermediateResultCacheStepExt(step), color, context);
    return visitChildren(node, context);
}

void PlanSegmentNodePrinter::printNode(
    QueryPlan::Node * node, const String & label, const String & details, const String & color, PrinterContext & context)
{
    out << "plannode_" << node->id << R"([label="{)" << escapeSpecialCharacters(label) << "|" << escapeSpecialCharacters(details);

    if (with_id)
        out << "|" << node->id;

    //    if (node.getStatistics().isDerived())
    //    {
    //        out << "|";
    //        out << "Stats \\n";
    //        auto statistics = node.getStatistics();
    //        if (statistics)
    //            out << statistics.value()->toString();
    //        else
    //            out << "None";
    //    }

    String style = context.is_magic ? "rounded, filled, dashed" : "rounded, filled";

    out << R"(}", style=")" << style << R"(", shape=record, fillcolor=)" << color << "]"
        << ";" << std::endl;
}

Void PlanSegmentNodePrinter::visitChildren(QueryPlan::Node * node, PrinterContext & context)
{
    for (auto & iter : node->children)
    {
        VisitorUtil::accept(iter, *this, context);
    }
    return Void{};
}

Void PlanSegmentEdgePrinter::visitNode(QueryPlan::Node * node, std::unordered_map<size_t, PlanSegmentPtr &> & context)
{
    std::vector<QueryPlan::Node *> & children = node->children;
    for (auto & iter : children)
    {
        printEdge(iter, node);
        VisitorUtil::accept(iter, *this, context);
    }
    return Void{};
}

Void PlanSegmentEdgePrinter::visitRemoteExchangeSourceStepExtNode(QueryPlan::Node * node, std::unordered_map<size_t, PlanSegmentPtr &> & context)
{
    auto * step = dynamic_cast<RemoteExchangeSourceStepExt *>(node->step.get());
    for (const auto & input : step->getInput())
    {
        const size_t segment_id = input->getPlanSegmentId();
        auto & plan_segment_ptr = context.at(segment_id);
        printEdge(plan_segment_ptr->getQueryPlan().getRoot(), node);
    }
    return Void{};
}

void PlanSegmentEdgePrinter::printEdge(QueryPlan::Node * from, QueryPlan::Node * to)
{
    out << "plannode_" << from->id << " -> "
        << "plannode_" << to->id << ";" << std::endl;
}

String StepPrinter::printStep(const IQueryPlanStep & step, bool include_output)
{
    std::stringstream details;
    if (include_output)
    {
        details << "Output \\n";
        for (const auto & column : step.getOutputStream().header)
        {
            details << column.name << ":";
            details << column.type->getName() << " ";
            details << (column.column ? column.column->getName() : "") << "\\n";
        }
    }
    return details.str();
}

String StepPrinter::printProjectionStepExt(const ProjectionStepExt & step, bool include_output)
{
    std::stringstream details;
    bool has_new_symbol = false;

    details << "New Assignments : \\n";
    {
        NameSet input_symbols;

        for (auto & column : step.getInputStreams()[0].header)
            input_symbols.insert(column.name);

        for (const auto & project : step.getAssignments())
        {
            if (input_symbols.find(project.first) == input_symbols.end())
            {
                has_new_symbol = true;
                String sql = serializeAST(*project.second);
                String type;
                if (auto literal = project.second->as<ASTLiteral>())
                {
                    type = applyVisitor(FieldToDataType(), literal->value)->getName();
                }
                details << project.first << ": " << sql << type << "\\n";
            }
        }
    }

    details << "|";
    details << "Full Assignments : \\n";
    for (const auto & project : step.getAssignments())
    {
        String sql = serializeAST(*project.second);
        String type;
        if (auto literal = project.second->as<ASTLiteral>())
        {
            type = applyVisitor(FieldToDataType(), literal->value)->getName();
        }
        details << project.first << ": " << sql << type << "\\n";
    }

    if (has_new_symbol && include_output)
    {
        details << "|";
        details << "Output \\n";
        for (auto & column : step.getOutputStream().header)
        {
            details << column.name << ":";
            details << column.type->getName() << "\\n";
        }
    }

    if (step.isIndexProject())
        details << "|"
                << "index";

    if (step.isFinalProject())
        details << "|"
                << "final";

    return details.str();
}

String StepPrinter::printExpandStepExt(const ExpandStepExt & step, bool)
{
    std::stringstream details;

    std::stringstream ss;
    for (const auto & element : step.getGroupIdValue())
    {
        ss << element << " ";
    }
    std::string result = ss.str();

    details << step.getGroupIdSymbol() << "[" << result << "]";
    details << "|";
    details << "Groups";
    details << "|";
    for (const auto & assignments_pre_group : step.generateAssignmentsGroups())
    {
        for (const auto & project : assignments_pre_group)
        {
            String sql = serializeAST(*project.second);
            details << project.first << ": " << sql << "\\n";
        }
        details << "|";
    }

    details << "Output \\n";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << "\\n";
    }

    return details.str();
}

String StepPrinter::printFilterStepExt(const FilterStepExt & step, bool include_output)
{
    std::stringstream details;
    details << printFilter(step.getFilter());

    if (include_output)
    {
        details << "|";
        details << "Output \\n";
        for (const auto & column : step.getOutputStream().header)
        {
            details << column.name << ":";
            details << column.type->getName() << " ";
            details << (column.column ? column.column->getName() : "") << "\\n";
        }
    }

    return details.str();
}

String StepPrinter::printJoinStepExt(const JoinStepExt & step)
{
    const Names & left = step.getLeftKeys();
    const Names & right = step.getRightKeys();
    JoinKind kind = step.getKind();
    std::stringstream details;

    auto f = [](JoinKind v) {
        switch (v)
        {
            case JoinKind::Inner:
                return "INNER";
            case JoinKind::Left:
                return "LEFT";
            case JoinKind::Right:
                return "RIGHT";
            case JoinKind::Full:
                return "FULL";
            case JoinKind::Cross:
                return "CROSS";
            case JoinKind::Comma:
                return "COMMA";
            case JoinKind::Paste:
                return "PASTE";
        }
    };

    auto strictnessf = [](JoinStrictness v) {
        switch (v)
        {
            case JoinStrictness::Unspecified:
                return "Unspecified";
            case JoinStrictness::RightAny:
                return "RightAny";
            case JoinStrictness::Any:
                return "Any";
            case JoinStrictness::Asof:
                return "Asof";
            case JoinStrictness::All:
                return "All";
            case JoinStrictness::Semi:
                return "Semi";
            case JoinStrictness::Anti:
                return "Anti";
        }
    };

    auto inequality = [](ASOFJoinInequality v) {
        switch (v)
        {
            case ASOFJoinInequality::None:
                return "None";
            case ASOFJoinInequality::Less:
                return "Less";
            case ASOFJoinInequality::Greater:
                return "Greater";
            case ASOFJoinInequality::LessOrEquals:
                return "LessOrEquals";
            case ASOFJoinInequality::GreaterOrEquals:
                return "GreaterOrEquals";
        }
    };

    if (step.isMagic())
    {
        details << "MagicSet"
                << "|";
    }

    details << "JoinKind:" << f(kind);
    details << "|";
    details << "JoinStrictness : " << strictnessf(step.getStrictness());
    if (step.getJoinAlgorithm() != JoinAlgorithm::AUTO)
    {
        details << "|";
        details << "JoinAlgorithm : " << JoinAlgorithmConverter::toString(step.getJoinAlgorithm());
    }

    details << "|";
    details << "JoinKeys\\n";
    for (int i = 0; i < static_cast<int>(left.size()); ++i)
    {
        details << left.at(i) << "=" << right.at(i) << (step.getKeyIdNullSafe(i) ? "(null aware)" : "") << "\\n";
    }
    details << "|";
    if (!PredicateUtils::isTruePredicate(step.getFilter()))
    {
        details << "JoinFilter\\n";
        details << step.getFilter()->getColumnName();
        details << "|";
    }
    details << inequality(step.getAsofInequality());
    details << "|";

    if (step.getJoinAlgorithm() == JoinAlgorithm::PARALLEL_HASH)
    {
        details << "parallel|";
    }

    if (step.getDistributionType() != DistributionType::UNKNOWN)
    {
        details << "DistributionType : ";
        if (step.getDistributionType() == DistributionType::REPARTITION)
            details << "repartition";
        else if (step.getDistributionType() == DistributionType::BROADCAST)
            details << "broadcast";
        details << "|";
    }

    if (step.isOrdered())
    {
        details << "isOrdered:" << step.isOrdered() << "|";
    }

    if (step.isHasUsing())
    {
        auto require_right_keys = step.getRequireRightKeys();
        auto using_str = require_right_keys ? fmt::format("{}", fmt::join(*require_right_keys, ",")) : "nullopt";
        details << "hasUsing:" << using_str << "|";
    }

    if (!step.getRuntimeFilterBuilders().empty())
    {
        details << "Runtime Filters \\n";
        for (const auto & runtime_filter : step.getRuntimeFilterBuilders())
            details << runtime_filter.first << ": " << runtime_filter.second.id << " "
                    << distributionToString(runtime_filter.second.distribution) << "\\n";
        details << "|";
    }

    details << "Output: \\n";
    for (const auto & item : step.getOutputStream().header)
    {
        details << item.name << ":";
        details << item.type->getName() << " ";
        details << (item.column ? item.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printArrayJoinStep(const ArrayJoinStep & step)
{
    std::stringstream details;
    details << "is left array join : " << step.arrayJoin()->is_left;
    details << "|";
    details << "Array Join columns : ";
    for (const auto & column : step.arrayJoin()->columns)
        details << column << ", ";
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printAggregatingStepExt(const AggregatingStepExt & step, bool include_output)
{
    std::stringstream details;
    details << "GroupBy:\\n";
    auto keys = step.getKeys();
    for (auto & key : keys)
    {
        details << key << "\\n";
    }
    details << "|";

    details << "KeysNotHashed:\\n";
    for (const auto & key : step.getKeysNotHashed())
    {
        details << key << "\\n";
    }
    details << "|";

    details << "Functions:\\n";
    const AggregateDescriptions & descs = step.getAggregates();
    for (const auto & desc : descs)
    {
        String func_name = desc.function->getName();
        auto type_name = String(typeid(desc.function.get()).name());
        if (type_name.find("AggregateFunctionNull") != String::npos)
        {
            func_name = String("AggNull(").append(std::move(func_name)).append(")");
        }
        details << desc.column_name << ":=" << func_name;
        details << "( ";
        details << "Argument:";
        for (const auto & argument : desc.argument_names)
        {
            details << argument << " ";
        }
        details << "Types:";
        for (const auto & type : desc.function->getArgumentTypes())
        {
            details << type->getName() << " ";
        }
        details << ")";
        details << "\\n";
        if (!desc.mask_column.empty())
        {
            details << " mask: " << desc.mask_column;
        }
        details << "\\n";
    }

    if (step.isGroupingSet())
    {
        details << "|";
        details << "Grouping Set\\n";
        for (const auto & set : step.getGroupingSetsParams())
        {
            details << "( ";
            for (const auto & name : set.used_key_names)
            {
                details << name << ", ";
            }
            details << ") ";
        }
    }

    if (!step.getGroupings().empty())
    {
        details << "|";
        details << "Grouping\\n";
        for (const auto & set : step.getGroupings())
        {
            details << set.output_name << ':';
            for (const auto & arg : set.argument_names)
            {
                details << arg << ',';
            }
            details << "; ";
        }
    }

    if (include_output)
    {
        details << "|";
        details << "Output\\n";
        for (const auto & column : step.getOutputStream().header)
        {
            details << column.name << ":";
            details << column.type->getName() << " ";
            details << (column.column ? column.column->getName() : "") << "\\n";
        }
    }

    if (step.isFinal())
        details << "|"
                << "final";
    if (step.isNoShuffle())
        details << "|"
                << "no shuffle";

    if (step.shouldProduceResultsInOrderOfBucketNumber())
    {
        details << "|";
        details << "results in order of bucket number";
    }

    if (step.isStreamingForCache())
    {
        details << "|";
        details << "streaming for cache";
    }
    //    if (step.isTotals())
    //        details << "|"
    //                << "totals";
    return details.str();
}

String StepPrinter::printMarkDistinctStepExt(const MarkDistinctStepExt & step, bool /*include_output*/)
{
    std::stringstream details;
    details << "Marker Symbol:\\n";
    details << step.getMarkerSymbol() << "\\n";
    details << "|";
    details << "Distinct Symbols :\\n";
    for (auto & symbol : step.getDistinctSymbols())
    {
        details << symbol << ',';
    }
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printMergingAggregatedStepExt(const MergingAggregatedStepExt & step)
{
    std::stringstream details;
    details << "GroupBy:\\n";
    auto keys = step.getKeys();
    for (auto & key : keys)
    {
        details << key << "\\n";
    }
    details << "|";
    details << "Functions:\\n";
    const AggregateDescriptions & descs = step.getParams().aggregates;
    for (const auto & desc : descs)
    {
        String func_name = desc.function->getName();
        auto type_name = String(typeid(desc.function.get()).name());
        if (type_name.find("AggregateFunctionNull") != String::npos)
        {
            func_name = String("AggNull(").append(std::move(func_name)).append(")");
        }
        details << desc.column_name << ":=" << func_name;
        details << "( ";
        details << "Argument:";
        for (const auto & argument : desc.argument_names)
        {
            details << argument << " ";
        }
        details << "Types:";
        for (const auto & type : desc.function->getArgumentTypes())
        {
            details << type->getName() << " ";
        }
        details << ")";
        details << "\\n";
        if (!desc.mask_column.empty())
        {
            details << " mask: " << desc.mask_column;
        }
        details << "\\n";
    }

    if (!step.getGroupings().empty())
    {
        details << "|";
        details << "Grouping\\n";
        for (const auto & set : step.getGroupings())
        {
            details << set.output_name << ':';
            for (const auto & arg : set.argument_names)
            {
                details << arg << ',';
            }
            details << "; ";
        }
    }

    if (step.isFinal())
        details << "|"
                << "final";
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << "\\n";
    }

    if (step.isMemoryEfficientAggregation())
    {
        details << "|";
        details << "memory efficient";
    }

    return details.str();
}

String StepPrinter::printUnionStepExt(const UnionStepExt & step)
{
    std::stringstream details;
    if (step.isLocal())
    {
        details << "local union"
                << "|";
    }
    details << "OutputToInputs"
            << "|";

    for (const auto & output_to_input : step.getOutToInputs())
    {
        details << output_to_input.first << ":";
        for (const auto & output : output_to_input.second)
        {
            details << output << ",";
        }
        details << "\\n";
    }
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printIntersectOrExceptStep(const IntersectOrExceptStep & step)
{
    std::stringstream details;
    details << "Operator :" << QueryPlanStepHelper::getIntersectOrExceptStepOperatorStr(step);
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printExchangeStepExt(const ExchangeStepExt & step)
{
    std::stringstream details;
    auto f = [](const RExchangeMode::Enum & mode) {
        switch (mode)
        {
            case RExchangeMode::UNKNOWN:
                return "UNKNOWN";
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
            case RExchangeMode::BUCKET_REPARTITION:
                return "BUCKET_REPARTITION";
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exchange mode");
        }
    };
    details << f(step.getExchangeMode());
    details << "|";
    details << step.getSchema().toString();

    if (step.needKeepOrder())
    {
        details << "|";
        details << "Keep Order\\n";
    }
    details << "|";
    details << "Shuffle Keys \\n";
    for (const auto & column : step.getSchema().getColumns())
    {
        details << column << " ";
    }
    details << "|";
    details << "Output \\n";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}
String StepPrinter::printRemoteExchangeSourceStepExt(const RemoteExchangeSourceStepExt & step)
{
    std::stringstream details;
    details << "Input Segments:[ ";
    auto inputs = step.getInput();
    for (const auto & input : inputs)
    {
        const size_t segment_id = input->getPlanSegmentId();
        details << segment_id << ":";

        for (const auto & column : input->getHeader())
        {
            details << column.name << " ";
        }
        details << "\\n";
    }
    details << "]";

    details << "|";
    details << "Output \\n";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printTableScanStepExt(const TableScanStepExt & step)
{
    //    auto distributed_table = dynamic_cast<StorageDistributed *>(step->getStorage().get());
    const String & database = step.getDatabase();
    const String & table = step.getTable();
    std::stringstream details;
    details << database << "." << table << "|";

    const auto & query_info = step.getQueryInfo();
    auto * query = query_info.query->as<ASTSelectQuery>();
    if (query->getExpression(ASTSelectQuery::Expression::WHERE, true))
    {
        details << "Filter : \\n";
        details << printFilter(query->refWhere());
        details << "|";
    }

    if (query->getExpression(ASTSelectQuery::Expression::PREWHERE, true))
    {
        details << "Prewhere : \\n";
        details << printFilter(query->refPrewhere());
        details << "|";
    }

    if (step.getQueryInfo().input_order_info)
    {
        const auto & input_order_info = step.getQueryInfo().input_order_info;
        details << "Input Order Info: \\n";
        const auto & prefix_descs = input_order_info->sort_description_for_merging;
        if (!prefix_descs.empty())
        {
            details << "prefix desc:  \\n";
            for (const auto & desc : prefix_descs)
            {
                details << desc.column_name << " " << desc.direction << " " << desc.nulls_direction << "\\n";
            }
        }
        details << "direction: " << input_order_info->direction << "\\n";

        details << "|";
    }

    if (query->getExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, true))
    {
        details << "Limit : \\n";
        Field converted = convertFieldToType(query->refLimitLength()->as<ASTLiteral>()->value, DataTypeUInt64());
        details << converted.safeGet<UInt64>();
        details << "|";
    }

    if (query->sampleSize())
    {
        ASTSampleRatio * sample = query->sampleSize()->as<ASTSampleRatio>();
        details << "Sample : \\n";
        details << "Sample Size : " << ASTSampleRatio::toString(sample->ratio) << "\\n";
        if (query->sampleOffset())
        {
            ASTSampleRatio * offset = query->sampleOffset()->as<ASTSampleRatio>();
            details << "Sample Offset : " << ASTSampleRatio::toString(offset->ratio) << "\\n";
        }
        details << "|";
    }

    // if (query_info.partition_filter)
    // {
    //     details << "Partition Filter : \\n";
    //     details << printFilter(query_info.partition_filter);
    //     details << "|";
    // }

    details << "Alias: \\n";
    for (const auto & assigment : step.getColumnAlias())
    {
        details << assigment.second << ": " << assigment.first << "\\n";
    }
    details << "|";

    if (step.isBucketScan())
    {
        details << "Bucket Scan |";
    }

    details << "Inline Expressions: \\n";
    for (const auto & assigment : step.getInlineExpressions())
    {
        details << assigment.first << ": " << serializeAST(*assigment.second) << "\\n";
    }
    details << "|";

    if (const auto * pushdown_filter = step.getPushdownFilterCast())
    {
        details << "Pushdown Filter |";
        details << printFilterStepExt(*pushdown_filter, false);
        details << "|";
    }

    if (const auto * pushdown_projection = step.getPushdownProjectionCast())
    {
        details << "Pushdown Projection |";
        details << printProjectionStepExt(*pushdown_projection, false);
        details << "|";
    }

    if (const auto * pushdown_aggregation = step.getPushdownAggregationCast())
    {
        details << "Pushdown Aggregation |";
        details << printAggregatingStepExt(*pushdown_aggregation, false);
        details << "|";
    }

    details << "Output \\n";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }

    return details.str();
}

String StepPrinter::printReadStorageRowCountStepExt(const ReadStorageRowCountStepExt & step)
{
    auto storage_id = step.getStorageID();
    std::stringstream details;
    details << storage_id.getFullTableName() << "|";

    auto ast = step.getQuery();
    auto * query = ast->as<ASTSelectQuery>();
    if (query && query->getExpression(ASTSelectQuery::Expression::WHERE, true))
    {
        details << "Filter : \\n";
        details << printFilter(query->refWhere());
        details << "|";
    }

    if (query && query->getExpression(ASTSelectQuery::Expression::PREWHERE, true))
    {
        details << "Prewhere : \\n";
        details << printFilter(query->refPrewhere());
        details << "|";
    }

    details << "Functions:\\n";
    auto desc = step.getAggregateDescription();
    auto type_name = String(typeid(desc.function.get()).name());
    String func_name = desc.function->getName();
    if (type_name.find("AggregateFunctionNull"))
    {
        func_name = String("AggNull(").append(std::move(func_name)).append(")");
    }
    details << desc.column_name << ":=" << func_name;
    details << "( ";
    details << "Argument:";
    for (const auto & argument : desc.argument_names)
    {
        details << argument << " ";
    }
    details << "Types:";
    for (const auto & type : desc.function->getArgumentTypes())
    {
        details << type->getName() << " ";
    }
    details << ")";
    details << "\\n";
    details << "|";

    details << "Output \\n";
    for (auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << "\\n";
    }
    return details.str();
}

String StepPrinter::printValuesStepExt(const ValuesStepExt & step)
{
    std::stringstream details;
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << "\\n";
    }
    details << "|";
    details << "Rows :" << step.getRows();
    return details.str();
}

String StepPrinter::printFinalSampleStepExt(const FinalSampleStepExt & step)
{
    std::stringstream details;
    details << "Sample Size: " << step.getSampleSize() << "\\n";
    details << "Max Chunk Size: " << step.getMaxChunkSize();
    return details.str();
}

String StepPrinter::printLimitStepExt(const LimitStepExt & step)
{
    std::stringstream details;
    details << "Limit:" << step.getLimit() << "|";
    details << "Offset:" << step.getOffset() << "|";
    details << "Output\\n";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    if (step.isPartial())
        details << "|"
                << " Partial";
    return details.str();
}

String StepPrinter::printOffsetStep(const OffsetStep & step)
{
    std::stringstream details;
    auto offset = step.getOffset();
    details << "Offset:" << offset;
    details << "|";
    details << "Output\\n";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printLimitByStep(const LimitByStep & step)
{
    std::stringstream details;
    details << "Limit value : " << step.getGroupLength();
    details << "|";
    details << "Limit columns : ";
    for (const auto & column : QueryPlanStepHelper::getLimitByStepColumns(step))
        details << column << ", ";
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printMergeSortingStepExt(const MergeSortingStepExt & step)
{
    std::stringstream details;
    details << "Order By:\\n";
    const auto & descs = step.getSortDescription();
    for (const auto & desc : descs)
    {
        details << desc.column_name << " " << desc.direction << " " << desc.nulls_direction << "\\n";
    }
    details << "|";
    details << "Limit: " << step.getLimit();
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printSortingStepExt(const SortingStepExt & step)
{
    std::stringstream details;
    details << "Order By:\\n";
    const auto & descs = step.getSortDescription();
    for (const auto & desc : descs)
    {
        details << desc.column_name << " " << desc.direction << " " << desc.nulls_direction << "\\n";
    }
    const auto & prefix_descs = step.getPrefixDescription();
    if (!prefix_descs.empty())
    {
        details << "|";
        details << "prefix desc";
        for (const auto & desc : prefix_descs)
        {
            details << desc.column_name << " " << desc.direction << " " << desc.nulls_direction << "\\n";
        }
    }
    details << "|";
    details << "Limit: " << step.getLimit();
    if (step.getStage() == SortingStepExt::Stage::FULL)
    {
        details << "|";
        details << "full";
    }
    if (step.getStage() == SortingStepExt::Stage::MERGE)
    {
        details << "|";
        details << "merge";
    }
    if (step.getStage() == SortingStepExt::Stage::PARTIAL)
    {
        details << "|";
        details << "partial";
    }
    if (step.getStage() == SortingStepExt::Stage::PARTIAL_NO_MERGE)
    {
        details << "|";
        details << "partial no merge";
    }
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printPartialSortingStepExt(const PartialSortingStepExt & step)
{
    std::stringstream details;
    details << "Order By:\\n";
    const auto & descs = step.getSortDescription();
    for (const auto & desc : descs)
    {
        details << desc.column_name << " " << desc.direction << " " << desc.nulls_direction << "\\n";
    }
    details << "|";
    details << "Limit: " << step.getLimit();

    return details.str();
}

String StepPrinter::printMergingSortedStepExt(const MergingSortedStepExt & step)
{
    std::stringstream details;
    details << "Order By:\\n";
    const auto & descs = step.getSortDescription();
    for (const auto & desc : descs)
    {
        details << desc.column_name << "\\n";
    }

    details << "|";
    details << "Limit: " << step.getLimit();

    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printDistinctStepExt(const DistinctStepExt & step)
{
    std::stringstream details;
    details << "Columns:\\n";
    for (const auto & name : step.getColumns())
    {
        details << name << "\\n";
    }
    details << "|";
    details << "limit:\\n";
    details << step.getLimitHint();
    details << "|";
    if (step.preDistinct())
    {
        details << "pre";
        details << "|";
    }
    if (!step.canToAgg())
    {
        details << "can not to agg";
        details << "|";
    }
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printApplyStepExt(const ApplyStepExt & step)
{
    auto f = [](ApplyStepExt::ApplyType v) {
        switch (v)
        {
            case ApplyStepExt::ApplyType::CROSS:
                return "CROSS";
            case ApplyStepExt::ApplyType::LEFT:
                return "LEFT";
            case ApplyStepExt::ApplyType::SEMI:
                return "SEMI";
            case ApplyStepExt::ApplyType::ANTI:
                return "ANTI";
        }
    };

    std::stringstream details;
    details << "ApplyType : " << f(step.getApplyType());
    details << "|";
    details << "Correlation \\n";
    for (const auto & name : step.getCorrelation())
    {
        details << name << " ";
    }

    details << "|";
    details << "Outer Columns\\n";
    for (const auto & name : step.getOuterColumns())
    {
        details << name << " ";
    }

    auto subquery_type = [](ApplyStepExt::SubqueryType v) {
        switch (v)
        {
            case ApplyStepExt::SubqueryType::SCALAR:
                return "SCALAR";
            case ApplyStepExt::SubqueryType::IN:
                return "IN";
            case ApplyStepExt::SubqueryType::EXISTS:
                return "EXISTS";
            case ApplyStepExt::SubqueryType::QUANTIFIED_COMPARISON:
                return "QUANTIFIED_COMPARISON";
        }
    };

    details << "|";
    details << "SubqueryType " << subquery_type(step.getSubqueryType());
    if (step.getAssignment().second)
    {
        details << "|";
        details << "Assignment \\n";
        details << step.getAssignment().first << " = " << serializeAST(*step.getAssignment().second);
    }
    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}
String StepPrinter::printEnforceSingleRowStepExt(const EnforceSingleRowStepExt & step)
{
    std::stringstream details;

    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}
String StepPrinter::printAssignUniqueIdStepExt(const AssignUniqueIdStepExt & step)
{
    std::stringstream details;
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printCTERefStepExt(const CTERefStepExt & step)
{
    std::stringstream details;
    details << "CTEId: " << step.getId() << "|";
    details << "Columns\\n";
    for (const auto & item : step.getOutputColumns())
    {
        details << item.first << ":";
        details << item.second << "\\n";
    }
    details << "Output\\n";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }

    return details.str();
}

String StepPrinter::printPartitionTopNStepExt(const PartitionTopNStepExt & step)
{
    std::stringstream details;
    details << "Partition";
    for (const auto & desc : step.getPartition())
    {
        details << desc << ", ";
    }
    details << "|";

    details << "Order by";
    for (const auto & desc : step.getOrderBy())
    {
        details << desc << ", ";
    }
    details << "|";

    details << static_cast<std::underlying_type_t<TopNModel>>(step.getModel());
    details << "|";

    details << "Limit: " << step.getLimit();
    details << "|";

    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printWindowStep(const WindowStep & step)
{
    std::stringstream details;

    const auto & window = QueryPlanStepHelper::getWindowStepWindow(step);

    details << "Partition Key\\n";
    for (const auto & pk : window.partition_by)
        details << pk.column_name << "\\n";
    details << "|";
    details << "Full Sort desc \\n";
    for (const auto & sort : window.full_sort_description)
        details << sort.column_name << "\\n";
    details << "|";
    details << "Sort Key\\n";
    for (const auto & sk : window.order_by)
        details << sk.column_name << " " << (sk.direction == 1 ? "ASC" : "DESC") << "\\n";
    details << "|";
    details << "Frame Type\\n";
    details << window.frame.toString();

    const auto & functions = QueryPlanStepHelper::getWindowStepFunctions(step);
    details << "|";
    details << "Window Functions\\n";

    for (const auto & func : functions)
    {
        details << func.column_name << ": ";
        details << func.aggregate_function->getName() << "(";
        for (const auto & arg : func.argument_names)
            details << arg << ",";
        details << ")\\n";
    }

    const auto & prefix_descs = window.full_sort_description;
    if (!prefix_descs.empty())
    {
        details << "|";
        details << "prefix desc";
        for (const auto & desc : prefix_descs)
        {
            details << desc.column_name << " " << desc.direction << " " << desc.nulls_direction << "\\n";
        }
    }

    return details.str();
}

String StepPrinter::printFilter(const ConstASTPtr & filter)
{
    auto conjuncts = PredicateUtils::extractConjuncts(filter);
    if (conjuncts.empty())
        return "";

    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings(buf, true);
    settings.hilite = false;
    settings.always_quote_identifiers = true;
    settings.identifier_quoting_style = IdentifierQuotingStyle::Backticks;
    conjuncts[0]->format(settings);
    for (size_t i = 1; i < conjuncts.size(); i++)
    {
        buf << "\\nAND ";
        conjuncts[i]->format(settings);
    }
    auto result = buf.str();
    boost::replace_all(result, "|", "!");
    return result;
}

String StepPrinter::printExplainAnalyzeStepExt(const ExplainAnalyzeStepExt & step)
{
    std::stringstream details;

    details << "ExplainAnalyzeKind: ";
    if (step.getKind() == ASTExplainQueryExt::ExplainKindExt::LogicalAnalyze)
        details << "LogicalAnalyze";
    else
        details << "DistributedAnalyze";

    details << "|";

    details << "Output |";
    for (auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printTopNFilteringStepExt(const TopNFilteringStepExt & step)
{
    std::stringstream details;
    details << "Order By:\\n";
    auto & descs = step.getSortDescription();
    for (auto & desc : descs)
    {
        details << desc.column_name << " " << desc.direction << " " << desc.nulls_direction << "\\n";
    }
    details << "|";
    details << "Size: " << step.getSize();
    details << "|";
    details << "Algorithm: " << TopNFilteringAlgorithmConverter::toString(step.getAlgorithm());

    return details.str();
}

String StepPrinter::printFillingStep(const FillingStep & step)
{
    std::stringstream details;
    details << "Order By With Fill:\\n";
    const auto & descs = step.getSortDescription();
    for (const auto & desc : descs)
    {
        details << "name: " << desc.column_name << " direction:" << desc.direction << " nulls_direction" << desc.nulls_direction;
        if (desc.with_fill)
        {
            details << " from:" << desc.fill_description.fill_from.dump() << " to:" << desc.fill_description.fill_to.dump()
                    << " step:" << desc.fill_description.fill_step.dump();
        }
        details << "\\n";
    }

    details << "|";
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printTotalsHavingStepExt(const TotalsHavingStepExt & step)
{
    std::stringstream details;
    if (step.getHavingFilter())
        details << "Having | " << step.getHavingFilter()->formatForErrorMessage() << " |";

    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printExtremesStep(const ExtremesStep & step)
{
    std::stringstream details;
    details << "Output |";
    for (const auto & column : step.getOutputStream().header)
    {
        details << column.name << ":";
        details << column.type->getName() << " ";
        details << (column.column ? column.column->getName() : "") << "\\n";
    }
    return details.str();
}

String StepPrinter::printIntermediateResultCacheStepExt(const IntermediateResultCacheStepExt & step)
{
    std::stringstream details;

    // todo: lizhuoyu, impl IntermediateResultCacheStep
    // auto cache_param = step.getCacheParam();
    // details << "CacheParam\\n";
    // details << "Digest: " << cache_param.digest << "\\n";
    // details << "Slot Mapping:\\n";
    // for (const auto & pair : cache_param.output_pos_to_cache_pos)
    // {
    //     details << "output#" << pair.first << ":"
    //             << "cache#" << pair.second << "\\n";
    // }
    // details << "Cached Table: " << cache_param.cached_table.getFullNameNotQuoted() << "\\n";
    // details << "Dependent Tables:\\n";
    // for (const auto & table : cache_param.dependent_tables)
    // {
    //     details << table.getFullNameNotQuoted() << "\\n";
    // }
    // details << "| Cache Order \\n";
    // for (const auto & column : step.getCacheOrder())
    // {
    //     details << column.name << "\\n";
    // }
    // details << "| Runtime Filters \\n";
    // if (const auto & filters = step.getIgnoredRuntimeFilters(); !filters.empty())
    // {
    //     details << "ignored runtime filters:";
    //     for (const auto & id : filters)
    //         details << " " << id;
    //     details << "\\n";
    // }
    // if (const auto & filters = step.getIncludedRuntimeFilters(); !filters.empty())
    // {
    //     details << "included runtime filters:";
    //     for (const auto & id : filters)
    //         details << " " << id;
    //     details << "\\n";
    // }
    // details << "| Output \\n";
    // for (const auto & column : step.getOutputStream().header)
    // {
    //     details << column.name << ":";
    //     details << column.type->getName() << "\\n";
    // }

    return details.str();
}

void appendAST(
    std::stringstream & out,
    ASTPtr & ast,
    const ASTPtr & parent,
    std::unordered_map<ASTPtr, UInt16> & asts,
    std::vector<std::pair<UInt16, UInt16>> & edges)
{
    String label = [&]() -> String {
        if (auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(parent))
        {
            if (ast == select_query->with())
                return "WITH";
            if (ast == select_query->select())
                return "SELECT";
            if (ast == select_query->tables())
                return "FROM";
            if (ast == select_query->prewhere())
                return "PREWHERE";
            if (ast == select_query->where())
                return "WHERE";
            if (ast == select_query->groupBy())
                return "GROUP BY";
            if (ast == select_query->having())
                return "HAVING";
            if (ast == select_query->window())
                return "WINDOW";
            if (ast == select_query->orderBy())
                return "ORDER BY";
            if (ast == select_query->limitBy())
                return "LIMIT BY";
            if (ast == select_query->limitOffset())
                return "LIMIT OFFSET";
            if (ast == select_query->limitLength())
                return "LIMIT LENGTH";
            if (ast == select_query->settings())
                return "SETTINGS";
        }

        if (auto func = std::dynamic_pointer_cast<ASTFunction>(parent))
        {
            if (ast == func->arguments)
                return "Function Args";
            if (ast == func->window_definition)
                return "Window Spec";
        }

        return ast->getID();
    }();

    bool print_sql = [&]() -> bool {
        if (ast->as<ASTExpressionList>())
            return false;
        if (ast->as<ASTTablesInSelectQuery>())
            return false;
        if (ast->as<ASTTablesInSelectQueryElement>())
            return false;
        if (ast->as<ASTTableExpression>())
            return false;
        return true;
    }();

    std::stringstream details;
    String sql = serializeAST(*ast);

    // handle escape characters that are special for graphviz
    boost::replace_all(sql, "<", "\\<");
    boost::replace_all(sql, ">", "\\>");
    boost::replace_all(sql, "\"", "\\\">");

#define MAX_PRINT_CHARACTERS 100
    if (sql.size() > MAX_PRINT_CHARACTERS)
    {
        sql.resize(MAX_PRINT_CHARACTERS);
        sql += "...";
    }
#undef MAX_PRINT_CHARACTERS

    details << "SQL:" << sql;

    String color{"bisque"};
    out << "ast_" << asts.at(ast) << R"([label="{)" << label;

    if (print_sql)
        out << "|" << details.str();

    out << R"(}", style="rounded, filled", shape=record, fillcolor=)" << color << "]"
        << ";" << std::endl;

    ASTs children = [&]() -> ASTs {
        if (auto * select_with_union = ast->as<ASTSelectWithUnionQuery>())
            return select_with_union->list_of_selects->children;
        if (auto * table_elem = ast->as<ASTTablesInSelectQueryElement>())
        {
            ASTs result;
            if (auto table_expr = std::dynamic_pointer_cast<ASTTableExpression>(table_elem->table_expression))
            {
                if (table_expr->database_and_table_name)
                    result.push_back(table_expr->database_and_table_name);
                if (auto table_subquery = std::dynamic_pointer_cast<ASTSubquery>(table_expr->subquery))
                    result.push_back(table_subquery->children[0]);
                if (table_expr->table_function)
                    result.push_back(table_expr->table_function);
                if (table_expr->sample_size)
                    result.push_back(table_expr->sample_size);
            }
            if (table_elem->table_join)
            {
                result.push_back(table_elem->table_join);
            }
            if (table_elem->array_join)
            {
                result.push_back(table_elem->array_join);
            }
            return result;
        }
        return ast->children;
    }();

    for (auto & child : children)
    {
        edges.emplace_back(asts.at(ast), asts.at(child));
        appendAST(out, child, ast, asts, edges);
    }
}

void appendASTEdge(std::stringstream & out, std::vector<std::pair<UInt16, UInt16>> & edges)
{
    for (auto & edge : edges)
    {
        out << "ast_" << edge.first << " -> "
            << "ast_" << edge.second << ";" << std::endl;
    }
}

void cleanDotFiles(const ContextMutablePtr & context)
{
    // when in the processing of sub query, DO NOT clean graphviz files.
    if (context->getOptimizerContext()->getExecuteSubQueryPath() != "")
    {
        return;
    }

    std::filesystem::path graphviz_path(context->getOptimizerContext()->getSettingsRef().graphviz_path.toString());

    try
    {
        if (!std::filesystem::exists(graphviz_path))
        {
            std::filesystem::create_directory(graphviz_path);
            return;
        }

        auto query_id = context->getInitialQueryId();

        for (auto & dir_entry : std::filesystem::directory_iterator(graphviz_path))
        {
            if (dir_entry.is_regular_file() && dir_entry.path().extension() == ".dot")
            {
                if (dir_entry.path().filename().string().find(query_id) != std::string::npos)
                {
                    continue;
                }
                std::filesystem::remove_all(dir_entry.path());
            }
        }
    }
    catch (...)
    {
    }
}

void cleanDotFiles(const ContextPtr & context)
{
    // when in the processing of sub query, DO NOT clean graphviz files.
    if (!context->getOptimizerContext()->getExecuteSubQueryPath().empty())
    {
        return;
    }

    std::filesystem::path graphviz_path(context->getOptimizerContext()->getSettingsRef().graphviz_path.toString());

    try
    {
        if (!std::filesystem::exists(graphviz_path))
        {
            std::filesystem::create_directory(graphviz_path);
            return;
        }

        auto query_id = context->getInitialQueryId();

        for (const auto & dir_entry : std::filesystem::directory_iterator(graphviz_path))
        {
            if (dir_entry.is_regular_file() && dir_entry.path().extension() == ".dot")
            {
                if (dir_entry.path().filename().string().find(query_id) != std::string::npos)
                {
                    continue;
                }
                std::filesystem::remove_all(dir_entry.path());
            }
        }
    }
    catch (...)
    {
    }
}

String GraphvizPrinter::printAST(ASTPtr ptr)
{
    std::unordered_map<ASTPtr, UInt16> asts;
    std::shared_ptr<std::atomic<UInt16>> max_node_id = std::make_unique<std::atomic<UInt16>>(0);
    std::vector<std::pair<UInt16, UInt16>> edges;

    addID(ptr, asts, max_node_id);

    std::stringstream out;
    out << "digraph ast {\n";
    out << "subgraph {\n";
    appendAST(out, ptr, nullptr, asts, edges);
    out << "}\n";
    appendASTEdge(out, edges);
    out << "}\n";
    return out.str();
}

void GraphvizPrinter::addID(ASTPtr & ast, std::unordered_map<ASTPtr, UInt16> & asts, std::shared_ptr<std::atomic<UInt16>> & max_node_id)
{
    asts.emplace(ast, (*max_node_id)++);
    ASTs & children = ast->children;
    for (auto & child : children)
    {
        addID(child, asts, max_node_id);
    }
}

String GraphvizPrinter::printLogicalPlan(PlanNodeBase & node, CTEInfo * cte_info, StepProfiles profiles)
{
    std::stringstream out;
    out << "digraph logical_plan {\n rankdir=\"BT\" \n";
    out << "subgraph {\n";
    PrinterContext printer_context{};
    PlanNodePrinter node_printer{out, true, cte_info, {}, profiles};
    VisitorUtil::accept(node, node_printer, printer_context);
    out << "}\n";
    PlanNodeEdgePrinter edge_printer{out, cte_info};
    Void context{};
    VisitorUtil::accept(node, edge_printer, context);
    out << "}\n";
    return out.str();
}

String GraphvizPrinter::printSettings(const String & color, const ContextMutablePtr & context)
{
    std::stringstream out;
    out << "context"
        << R"([label="{)"
        << "context info";

    if (context && !context->getSettingsRef().changes().empty())
    {
        out << "|";
        out << "settings \\n";
        for (auto & setting : context->getSettingsRef().changes())
        {
            out << setting.name << ":" << Settings::valueToStringUtil(setting.name, setting.value) << " \\n";
        }
    }

    String style = "rounded, filled";

    out << R"(}", style=")" << style << R"(", shape=record, fillcolor=)" << color << "]"
        << ";" << std::endl;
    return out.str();
}

void GraphvizPrinter::printLogicalPlan(PlanNodeBase & root, ContextMutablePtr & context, const String & name)
{
    if (context->getOptimizerContext()->getSettingsRef().print_graphviz)
    {
        auto const graphviz = GraphvizPrinter::printLogicalPlan(root);

        cleanDotFiles(context);

        std::stringstream path;
        path << context->getOptimizerContext()->getSettingsRef().graphviz_path.toString();
        path << context->getOptimizerContext()->getExecuteSubQueryPath() << name << "-" << context->getInitialQueryId() << ".dot";

        std::ofstream out(path.str());
        out << graphviz;
        out.close();

        // QueryStatus * process_list_elem = context->getProcessListElement();
        // if (process_list_elem)
        //     process_list_elem->addGraphviz(name, graphviz);
    }
}

void GraphvizPrinter::printLogicalPlan(QueryPlanExt & plan, ContextMutablePtr & context, const String & name, StepProfiles profiles)
{
    if (context->getOptimizerContext()->getSettingsRef().print_graphviz)
    {
        auto const graphviz = GraphvizPrinter::printLogicalPlan(*plan.getPlanNode(), &plan.getCTEInfo(), profiles);
        cleanDotFiles(context);

        std::stringstream path;
        path << context->getOptimizerContext()->getSettingsRef().graphviz_path.toString();
        path << context->getOptimizerContext()->getExecuteSubQueryPath() << name << "-" << context->getInitialQueryId() << ".dot";

        std::ofstream out(path.str());
        out << graphviz;
        out.close();

        // QueryStatus * process_list_elem = context->getProcessListElement();
        // if (process_list_elem)
        //     process_list_elem->addGraphviz(name, graphviz);
    }
}


void GraphvizPrinter::printPlanSegment(const PlanSegmentTreeUniqPtr & segment, const ContextMutablePtr & context)
{
    if (context->getOptimizerContext()->getSettingsRef().print_graphviz)
    {
        cleanDotFiles(context);

        std::stringstream path;
        path << context->getOptimizerContext()->getSettingsRef().graphviz_path.toString();
        path << context->getOptimizerContext()->getExecuteSubQueryPath() + "4000-PlanSegment"
             << "-" << context->getInitialQueryId() << ".dot";
        std::ofstream out(path.str());
        auto const graphviz = GraphvizPrinter::printPlanSegmentNodes(segment, context);
        out << graphviz;
        out.close();

        // QueryStatus * process_list_elem = context->getProcessListElement();
        // if (process_list_elem)
        //     process_list_elem->addGraphviz("4000-PlanSegment", graphviz);
    }
}

String GraphvizPrinter::printPlanSegmentNodes(const PlanSegmentTreeUniqPtr & segmentNode, const ContextMutablePtr & context)
{
    std::stringstream out;
    out << "digraph plan_segment {\n rankdir=\"BT\" \n";
    std::unordered_map<size_t, PlanSegmentPtr &> segments = segmentNode->getPlanSegmentsMap();
    std::unordered_set<PlanSegmentTree::Node *> visited_segments;
    appendPlanSegmentNodes(out, segmentNode->getRoot(), segments, visited_segments);
    out << printSettings("gray83", context);
    out << "}\n";
    return out.str();
}

void GraphvizPrinter::appendPlanSegmentNodes(
    std::stringstream & out,
    PlanSegmentTree::Node * segmentNode,
    std::unordered_map<size_t, PlanSegmentPtr &> & segments,
    std::unordered_set<PlanSegmentTree::Node *> & visited)
{
    if (!visited.emplace(segmentNode).second)
        return;

    PlanSegmentPtr & plan_segment = segmentNode->plan_segment;

    appendPlanSegmentNode(out, plan_segment);

    QueryPlan::Node * plan = plan_segment->getQueryPlan().getRoot();
    PlanSegmentEdgePrinter edge_printer{out};
    VisitorUtil::accept(plan, edge_printer, segments);

    std::vector<PlanSegmentTree::Node *> & children = segmentNode->children;
    for (auto & child : children)
    {
        appendPlanSegmentNodes(out, child, segments, visited);
    }
}

void GraphvizPrinter::appendPlanSegmentNode(std::stringstream & out, const PlanSegmentPtr & segment_ptr)
{
    out << "subgraph ";
    out << "cluster_" << segment_ptr->getPlanSegmentId();
    out << "{\n";
    auto mode = segment_ptr->getPlanSegmentOutput()->getExchangeMode();
    auto f = [](RExchangeMode::Enum mode_) {
        switch (mode_)
        {
            case RExchangeMode::UNKNOWN:
                return "UNKNOWN";
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
            case RExchangeMode::BUCKET_REPARTITION:
                return "BUCKET_REPARTITION";
            default:
                std::unreachable();
        }
    };
    size_t segment_id = segment_ptr->getPlanSegmentId();
    out << "label = \"";
    out << "segment=[ " << segment_id << " ]\n";
    if (segment_id != 0)
    {
        out << "exchange=[ " << f(mode) << " ]\n";
        out << "shufflekeys=[ ";
        for (auto & key : segment_ptr->getPlanSegmentOutput()->getShufflekeys())
        {
            out << key << " ";
        }
        out << " ]\n";
    }
    out << "parallel_size " << segment_ptr->getParallelSize() << "\n";
    out << "cluster_name " << (segment_ptr->getClusterName().empty() ? "server" : segment_ptr->getClusterName()) << "\\n";
    out << "exchange_parallel_size " << segment_ptr->getExchangeParallelSize() << "\n";

    out << "inputs:";
    for (const auto & input : segment_ptr->getPlanSegmentInputs())
    {
        out << input->getExchangeId() << "mode(" << toString(static_cast<int>(input->getExchangeMode())) << "): ";
        for (const auto & col : input->getHeader())
        {
            out << col.name << " ";
        }

        if (input->needKeepOrder())
        {
            out << "keeporder ";
        }

        if (input->isStable())
        {
            out << "stable ";
        }

        out << "\n";
    }
    out << "\n";

    out << "output:";
    for (const auto & input : segment_ptr->getPlanSegmentOutputs())
    {
        out << input->getExchangeId() << "mode(" << toString(static_cast<int>(input->getExchangeMode())) << "): ";
        for (const auto & col : input->getHeader())
        {
            out << col.name << " ";
        }
        if (input->needKeepOrder())
        {
            out << "keeporder ";
        }
        out << "hash_func:" << input->getShuffleFunctionName();

        auto visitor = FieldVisitorToString();
        out << " params:";
        for (auto item : input->getShuffleFunctionParams())
        {
            out << " " << applyVisitor(visitor, item);
        }
        out << "\n";
    }
    out << "\n";

    out << "\"";
    QueryPlan::Node * node = segment_ptr->getQueryPlan().getRoot();
    PrinterContext context{};
    PlanSegmentNodePrinter node_printer{out, true};
    VisitorUtil::accept(node, node_printer, context);
    out << "}\n";
}

static String printGroupEdges(
    const Memo & memo,
    const std::unordered_map<GroupId, std::unordered_set<GroupId>> & edge_winner,
    const std::unordered_map<GroupId, std::unordered_set<GroupId>> & cte_edge_winner)
{
    std::stringstream out;

    std::unordered_map<GroupId, std::unordered_set<GroupId>> edge_exists;
    for (const auto & group : memo.getGroups())
    {
        GroupId father_id = group->getId();
        for (const auto & expr : group->getLogicalExpressions())
        {
            for (GroupId children_id : expr->getChildrenGroups())
            {
                if (!edge_exists[children_id].contains(father_id))
                {
                    out << "group_" << children_id << "-> group_" << father_id;
                    if (edge_winner.contains(children_id) && edge_winner.at(children_id).contains(father_id))
                        out << " [penwidth = 4.0, color = red]";
                    out << ";\n";
                    edge_exists[children_id].emplace(father_id);
                }
            }
        }
    }

    for (const auto & group : memo.getGroups())
    {
        for (const auto & expr : group->getLogicalExpressions())
        {
            if (getQueryPlanStepType(expr->getStep()) == QueryPlanStepType::CTERefStepExt)
            {
                const auto * cte_step = dynamic_cast<const CTERefStepExt *>(expr->getStep().get());
                auto cte_group = memo.getCTEDefGroupByCTEId(cte_step->getId());
                out << "group_" << cte_group->getId() << "-> group_" << group->getId();
                if (cte_edge_winner.contains(cte_group->getId()) && cte_edge_winner.at(cte_group->getId()).contains(group->getId()))
                    out << " [style=dashed, penwidth = 4.0, color = red, label = shared]";
                else
                    out << " [style=dashed]";
                out << ";\n";
            }
        }
    }

    return out.str();
}

String GraphvizPrinter::printMemo(const Memo & memo, GroupId root)
{
    std::stringstream out;
    out << "digraph logical_plan {\n  rankdir=\"BT\" \n";
    out << "node[style=\"filled\", shape=record]\n";
    out << "subgraph {\n";

    std::unordered_map<GroupId, WinnerPtr> group_winner;
    std::unordered_map<GroupId, std::unordered_set<GroupId>> edge_winner;
    std::unordered_map<GroupId, std::unordered_set<GroupId>> cte_edge_winner;

    std::function<void(GroupId, const Property &)> find_group_winner = [&](GroupId group_id, const Property & required_prop) {
        auto group = memo.getGroupById(group_id);
        auto winner = group->getBestExpression(required_prop);

        if (winner->getGroupExpr() == nullptr)
            return;

        group_winner[group_id] = winner;

        const auto & required_properties = winner->getRequireChildren();
        for (size_t index = 0; index < required_properties.size(); ++index)
        {
            const auto & children_id = winner->getGroupExpr()->getChildrenGroups()[index];
            edge_winner[children_id].emplace(group_id);
            find_group_winner(children_id, required_properties[index]);
        }

        if (getQueryPlanStepType(winner->getGroupExpr()->getStep()) == QueryPlanStepType::CTERefStepExt)
        {
            const auto & cte_ref = dynamic_cast<const CTERefStepExt *>(winner->getGroupExpr()->getStep().get());
            auto cte_id = cte_ref->getId();
            auto cte_group_id = memo.getCTEDefGroupId(cte_id);
            cte_edge_winner[cte_group_id].emplace(group_id);
            find_group_winner(cte_group_id, winner->getCTEActualProperties().at(cte_id).first);
        }
    };
    if (root != UNDEFINED_GROUP)
    {
        try
        {
            find_group_winner(root, Property{Partitioning{Partitioning::Handle::SINGLE}});
        }
        catch (...)
        {
        }
    }

    for (const auto & group : memo.getGroups())
        out << printGroup(*group, group_winner);

    out << "}\n";
    out << printGroupEdges(memo, edge_winner, cte_edge_winner);
    out << "}\n";
    return out.str();
}

String GraphvizPrinter::printGroup(const Group & group, const std::unordered_map<GroupId, WinnerPtr> & group_winner)
{
    std::stringstream out;
    const IQueryPlanStep * head_step;
    if (group.getLogicalExpressions().empty())
        head_step = group.getPhysicalExpressions()[0]->getStep().get();
    else
        head_step = group.getLogicalExpressions()[0]->getStep().get();

    auto fold = [](std::string a, GroupId b) { return std::move(a) + ", " + std::to_string(b); };

    auto expr_to_str = [&](const GroupExprPtr & expr) {
        if (!expr)
            return String("");

        String result = expr->getStep()->getName();

        if (expr->getChildrenGroups().empty())
            result += String(" []");
        else
            result += " ["
                + std::accumulate(
                          std::next(expr->getChildrenGroups().begin()),
                          expr->getChildrenGroups().end(),
                          std::to_string(expr->getChildrenGroups()[0]),
                          fold)
                + "]";

        if (getQueryPlanStepType(expr->getStep()) == QueryPlanStepType::JoinStepExt)
        {
            const auto * join_step = dynamic_cast<const JoinStepExt *>(expr->getStep().get());
            for (size_t i = 0; i < join_step->getLeftKeys().size(); i++)
            {
                result += " " + escapeSpecialCharacters(join_step->getLeftKeys()[i]);
                result += "=" + escapeSpecialCharacters(join_step->getRightKeys()[i]);
            }
            if (join_step->getDistributionType() == DistributionType::REPARTITION)
            {
                result += " repartition";
            }
            if (join_step->getDistributionType() == DistributionType::BROADCAST)
            {
                result += " broadcast";
            }
            result += " " + std::to_string(hashPlanStep(*join_step, false));
        }
        if (getQueryPlanStepType(expr->getStep()) == QueryPlanStepType::CTERefStepExt)
        {
            const auto * cte_step = dynamic_cast<const CTERefStepExt *>(expr->getStep().get());
            result += " id: " + std::to_string(cte_step->getId());
        }
        result += " ";
        result += magic_enum::enum_name(expr->getProduceRule());
        result += "<BR/>";
        return result;
    };

    out << "group_" << group.getId()
        << "[label=<"
           "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";

    // type
    out << "<TR><TD COLSPAN=\"3\">" << head_step->getName() << " [" << group.getId() << "]</TD></TR>";

    out << "<TR><TD COLSPAN=\"3\">" << head_step->getName() << " [";
    for (const auto & col : head_step->getOutputStream().header)
    {
        out << col.name << " ";
    }
    out << "]</TD></TR>";

    if (getQueryPlanStepType(*head_step) == QueryPlanStepType::ReadFromStorageStep)
    {
        //        out << "<TR><TD COLSPAN=\"3\">" << dynamic_cast<const ReadFromStorageStep *>(head_step)->getTable() << "</TD></TR>";
    }

    if (getQueryPlanStepType(*head_step) == QueryPlanStepType::FilterStepExt)
    {
        out << "<TR><TD COLSPAN=\"3\">" << dynamic_cast<const FilterStepExt *>(head_step)->getFilterColumnName() << "</TD></TR>";
    }

    if (group.isJoinRoot())
    {
        out << "<TR><TD COLSPAN=\"3\">JoinRoot</TD></TR>";
    }

    if (group.getJoinRootId() != 0)
    {
        out << "<TR><TD COLSPAN=\"3\">Join Root Id: " << group.getJoinRootId() << "</TD></TR>";
    }

    if (group.isStatsDerived())
    {
        out << "<TR><TD COLSPAN=\"3\">";
        if (group.getStatistics())
        {
            auto stats = escapeSpecialCharacters(group.getStatistics().value()->toString());
            boost::replace_all(stats, "\\n", "<BR/>");
            out << stats;
        }
        else
        {
            out << "None";
        }
        out << "</TD></TR>";
    }

    // expression
    out << "<TR><TD>Logical</TD>";
    out << "<TD COLSPAN=\"2\">";
    for (auto & expr : group.getLogicalExpressions())
    {
        if (expr->isDeleted())
            out << "Deleted ";
        out << expr_to_str(expr);
    }
    out << "</TD>";
    out << "</TR>";

    out << "<TR><TD>Winner</TD>";

    // winners

    auto property_str = [&](const Property & property) {
        std::stringstream ss;
        ss << property.getNodePartitioning().toString();
        ss << "  Component:" << magic_enum::enum_name(property.getNodePartitioning().getComponent()) << "; ";
        ss << " ";
        ss << property.getCTEDescriptions().toString();
        return ss.str();
    };

    if (!group.getLowestCostExpressions().empty())
    {
        out << R"(<TD><TABLE CELLBORDER="1" BORDER="0" CELLSPACING="0">)";
        for (auto & pair : group.getLowestCostExpressions())
        {
            auto & winner = pair.second;
            bool is_winner = group_winner.contains(group.getId()) && group_winner.at(group.getId()) == winner;
            out << "<TR>";

            // property
            out << "<TD>";
            if (is_winner)
                out << "<B>";
            out << "cost: " << winner->getCost() << "<BR/>";

            for (auto cte_id : winner->getCTEAncestors())
                out << "CTE(" + std::to_string(cte_id) + ") cost: " << winner->getCTEActualProperties().at(cte_id).second << "<BR/>";

            out << "require: " << property_str(pair.first);
            if (is_winner)
                out << "<BR/>winner</B>";
            out << "</TD>";
            // property end

            // winner
            out << "<TD>";
            if (is_winner)
                out << "<B>";

            if (winner->getRemoteExchange())
            {
                if (auto exchange_step = dynamic_cast<const ExchangeStepExt *>(winner->getRemoteExchange()->getStep().get()))
                {
                    out << "enforce: ";
                    out << exchange_step->getSchema().toString();
                    out << "<BR/>";
                }
            }
            out << "actual: ";
            out << property_str(winner->getActual());
            out << "<BR/>";

            out << expr_to_str(winner->getGroupExpr());
            out << "\n";

            out << join(
                winner->getRequireChildren(), [&](const auto & item) { return property_str(item); }, ", ", "child required: ")
                << "\n";
            if (is_winner)
                out << "</B>";
            out << "</TD>";
            // winner end

            out << "</TR>";
        }
        out << "</TABLE></TD>";
    }
    // winner end

    out << "</TR>";

    out << "</TABLE>>, fillcolor=" << NODE_COLORS[getQueryPlanStepType(*head_step)] << "]"
        << ";" << std::endl;
    return out.str();
}


String GraphvizPrinter::getColor(QueryPlanStepType step)
{
    if (NODE_COLORS.count(step))
        return NODE_COLORS.at(step);
    auto step_id = static_cast<typename std::underlying_type<QueryPlanStepType>::type>(step);
    return fmt::format("\"#{:06x}\"", intHash64(step_id) & ((1U << 24) - 1));
}

void GraphvizPrinter::printAST(const ASTPtr & astPtr, ContextMutablePtr & context, const String & visitor)
{
    if (context->getOptimizerContext()->getSettingsRef().print_graphviz
        && context->getOptimizerContext()->getSettingsRef().print_graphviz_ast)
    {
        auto const graphviz = GraphvizPrinter::printAST(astPtr);

        std::stringstream path;
        path << context->getOptimizerContext()->getSettingsRef().graphviz_path.toString();
        path << visitor << "-" << context->getInitialQueryId() << ".dot";
        std::ofstream out(path.str());
        out << graphviz;
        out.close();

        // todo: zhangwanyun1, need addGraphviz from QueryStatus
        // QueryStatusPtr process_list_elem = context->getProcessListElement();
        // if (process_list_elem)
        //     process_list_elem->addGraphviz(visitor, graphviz);
    }
}

void GraphvizPrinter::printMemo(const Memo & memo, const ContextMutablePtr & context, const String & name)
{
    printMemo(memo, UNDEFINED_GROUP, context, name);
}

void GraphvizPrinter::printMemo(const Memo & memo, GroupId root_id, const ContextMutablePtr & context, const String & name)
{
    if (context->getOptimizerContext()->getSettingsRef().print_graphviz)
    {
        auto const graphviz = GraphvizPrinter::printMemo(memo, root_id);
        cleanDotFiles(context);

        std::stringstream path;
        path << context->getOptimizerContext()->getSettingsRef().graphviz_path.toString();
        path << context->getOptimizerContext()->getExecuteSubQueryPath() << name << "-" << context->getInitialQueryId() << ".dot";

        std::ofstream out(path.str());
        out << graphviz;
        out.close();

        // todo: zhangwanyun1, need addGraphviz from QueryStatus
        // auto process_list_elem = context->getProcessListElement();
        // if (process_list_elem)
        //     process_list_elem->addGraphviz(name, graphviz);
    }
}

}
