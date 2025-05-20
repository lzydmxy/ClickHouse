#pragma once

#include <sstream>
#include <utility>
#include <IO/Operators.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Parsers/IAST.h>
#include <Query/Common/ProcessorProfile.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>


namespace DB
{
using PlanNodeId = UInt32;

struct PrinterContext;
class PlanNodeBase;

class QueryPlan;
class CTEInfo;

class Group;

class ExecutingGraph;
using ExecutingGraphPtr = std::unique_ptr<ExecutingGraph>;

class Winner;
using WinnerPtr = std::shared_ptr<Winner>;

class Memo;
using GroupId = UInt32;

class GraphvizPrinter
{
public:
    const static int PRINT_AST_INDEX = 1000;
    const static int PRINT_PLAN_BUILD_INDEX = 2000;
    const static int PRINT_PLAN_OPTIMIZE_INDEX = 3000;
    const static String MEMO_GRAPH_PATH;
    const static String PIPELINE_PATH;

    static void printAST(const ASTPtr &, ContextMutablePtr & context, const String & visitor);
    static void printLogicalPlan(PlanNodeBase &, ContextMutablePtr &, const String & name);
    static void printLogicalPlan(QueryPlanExt &, ContextMutablePtr &, const String & name, StepProfiles profiles = {});
    static void printMemo(const Memo & memo, const ContextMutablePtr & context, const String & name);
    static void printMemo(const Memo & memo, GroupId root_id, const ContextMutablePtr & context, const String & name);
    static void printPlanSegment(const PlanSegmentTreeUniqPtr &, const ContextMutablePtr &);
    static void printChunk(String transform, const Block & block, const Chunk & chunk);
    static void printPipeline(const Processors & processors, const ExecutingGraphPtr & graph, const ContextPtr & context, size_t segment_id, const String & host);
    static String getColor(QueryPlanStepType step);
    static String printSettings(const String & color, const ContextMutablePtr & context);

private:
    static String printAST(ASTPtr);
    static void addID(ASTPtr & ast, std::unordered_map<ASTPtr, UInt16> & asts, std::shared_ptr<std::atomic<UInt16>> & max_node_id);

    static String printLogicalPlan(PlanNodeBase &, CTEInfo * cte_info = nullptr, StepProfiles profiles = {});
    static String printPlanSegmentNodes(const PlanSegmentTreeUniqPtr &, const ContextMutablePtr &);
    static void appendPlanSegmentNodes(
        std::stringstream & out,
        PlanSegmentTree::Node * segmentNode,
        std::unordered_map<size_t, PlanSegmentPtr &> &,
        std::unordered_set<PlanSegmentTree::Node *> & visited);
    static void appendPlanSegmentNode(std::stringstream & out, const PlanSegmentPtr & segment_ptr);

    static String printMemo(const Memo & memo, GroupId root_id);
    static String printGroup(const Group & group, const std::unordered_map<GroupId, WinnerPtr> & group_winner);

    static String printPipeline(const Processors & processors, const ExecutingGraphPtr & graph);
    static String printGroupedPipeline(const Processors & processors, const ExecutingGraphPtr & graph);
};

}
