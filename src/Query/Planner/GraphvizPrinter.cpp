#include <Query/Planner/GraphvizPrinter.h>

#include <Query/Executor/PlanSegment.h>
#include <Query/ProtosHelper/ExchangeMode.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <boost/algorithm/string/replace.hpp>
#include <string>
#include <filesystem>
#include <iostream>
#include <fstream>


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
    //todo: lizhuoyu, other feat: IMPL CTEInfo
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraphvizPrinter::printGroup: not implemented");

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

void GraphvizPrinter::printLogicalPlan(PlanNodeBase &, ContextMutablePtr &, const String &)
{
    //todo: lizhuoyu, other feat: add impl
    return;
}

void GraphvizPrinter::printLogicalPlan(QueryPlan &, ContextMutablePtr &, const String &, StepProfiles)
{
    //todo: lizhuoyu, other feat: add impl
    return;
}


void GraphvizPrinter::printPlanSegment(const PlanSegmentTreeUniqPtr &, const ContextMutablePtr &)
{
    //todo: lizhuoyu, other feat: add impl
    return;
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

    // QueryPlan::Node * plan = plan_segment->getQueryPlan().getRootNode();

    //todo: lizhuoyu, other feat: add PlanSegmentEdgePrinter
    // PlanSegmentEdgePrinter edge_printer{out};
    // VisitorUtil::accept(plan, edge_printer, segments);

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

    //    out << "exchange_output_parallel_size " << segment_ptr->getExchangeOutputParallelSize() << "\n";
    out << "\"";
    // QueryPlan::Node * node = segment_ptr->getQueryPlan().getRoot();
    // PrinterContext context{};

    //todo: lizhuoyu, other feat: add PlanSegmentEdgePrinter
    // PlanSegmentNodePrinter node_printer{out, true};
    // VisitorUtil::accept(node, node_printer, context);
    out << "}\n";
}

//todo: lizhuoyu, other feat: Should imp Memo and Grou
// static String printGroupEdges(
//     const Memo & memo,
//     const std::unordered_map<GroupId, std::unordered_set<GroupId>> & edge_winner,
//     const std::unordered_map<GroupId, std::unordered_set<GroupId>> & cte_edge_winner)
// {
//     throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraphvizPrinter::printMemo: not implemented");
// }

String GraphvizPrinter::printMemo(const Memo & memo, GroupId root)
{
    //todo: lizhuoyu, other feat: Should imp Memo
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraphvizPrinter::printMemo: not implemented");
}

String GraphvizPrinter::printGroup(const Group & group, const std::unordered_map<GroupId, WinnerPtr> & group_winner)
{
    //todo: lizhuoyu, other feat: Should imp Group
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraphvizPrinter::printGroup: not implemented");
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

}
