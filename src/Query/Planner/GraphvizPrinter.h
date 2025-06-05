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
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Optimizer/CostModel/CostCalculator.h>
#include <Query/Processors/QueryPlan/CTEVisitHelper.h>


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

class PlanNodePrinter : public PlanNodeVisitor<Void, PrinterContext>
{
public:
    explicit PlanNodePrinter(
        std::stringstream & out_,
        bool with_id_ = false,
        CTEInfo * cte_info = nullptr,
        PlanCostMap plan_cost_map_ = {},
        StepProfiles profiles_ = {})
        : out(out_)
        , cte_helper(cte_info ? std::make_optional<SimpleCTEVisitHelper<void>>(*cte_info) : std::nullopt)
        , with_id(with_id_)
        , plan_cost_map(std::move(plan_cost_map_))
        , profiles(profiles_)
    {
    }

    ~PlanNodePrinter() override = default;
    Void visitPlanNode(PlanNodeBase &, PrinterContext &) override;
    Void visitProjectionStepExtNode(ProjectionStepExtNode & node, PrinterContext & context) override;
    Void visitExpandStepExtNode(ExpandStepExtNode & node, PrinterContext & context) override;
    Void visitFilterStepExtNode(FilterStepExtNode & node, PrinterContext & context) override;
    Void visitJoinStepExtNode(JoinStepExtNode & node, PrinterContext & context) override;
    Void visitArrayJoinStepNode(ArrayJoinStepNode & node, PrinterContext & context) override;
    Void visitAggregatingStepExtNode(AggregatingStepExtNode & node, PrinterContext & context) override;
    Void visitMarkDistinctStepExtNode(MarkDistinctStepExtNode & node, PrinterContext & context) override;
    Void visitMergingAggregatedStepExtNode(MergingAggregatedStepExtNode & node, PrinterContext & context) override;
    Void visitUnionStepExtNode(UnionStepExtNode & node, PrinterContext & context) override;
    Void visitExchangeStepExtNode(ExchangeStepExtNode & node, PrinterContext & context) override;
    Void visitRemoteExchangeSourceStepExtNode(RemoteExchangeSourceStepExtNode & node, PrinterContext & context) override;
    Void visitTableScanStepExtNode(TableScanStepExtNode & node, PrinterContext & context) override;
    Void visitReadNothingStepNode(ReadNothingStepNode & node, PrinterContext & context) override;
    Void visitReadStorageRowCountStepExtNode(ReadStorageRowCountStepExtNode & node, PrinterContext & context) override;
    Void visitValuesStepExtNode(ValuesStepExtNode & node, PrinterContext & context) override;
    Void visitLimitStepExtNode(LimitStepExtNode & node, PrinterContext & context) override;
    Void visitOffsetStepNode(OffsetStepNode & node, PrinterContext & context) override;
    Void visitLimitByStepNode(LimitByStepNode & node, PrinterContext & context) override;
    Void visitSortingStepExtNode(SortingStepExtNode & node, PrinterContext & context) override;
    Void visitMergeSortingStepExtNode(MergeSortingStepExtNode & node, PrinterContext & context) override;
    Void visitPartialSortingStepExtNode(PartialSortingStepExtNode & node, PrinterContext & context) override;
    Void visitMergingSortedStepExtNode(MergingSortedStepExtNode & node, PrinterContext & context) override;
    Void visitDistinctStepExtNode(DistinctStepExtNode & node, PrinterContext & context) override;
    Void visitExtremesStepNode(ExtremesStepNode & node, PrinterContext & context) override;
    Void visitTotalsHavingStepExtNode(TotalsHavingStepExtNode & node, PrinterContext & context) override;
    Void visitFinalSampleStepExtNode(FinalSampleStepExtNode & node, PrinterContext & context) override;
    Void visitApplyStepExtNode(ApplyStepExtNode & node, PrinterContext & context) override;
    Void visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode & node, PrinterContext & context) override;
    Void visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode & node, PrinterContext & context) override;
    Void visitWindowStepNode(WindowStepNode & node, PrinterContext & context) override;
    Void visitCTERefStepExtNode(CTERefStepExtNode & node, PrinterContext & context) override;
    Void visitPartitionTopNStepExtNode(PartitionTopNStepExtNode & node, PrinterContext & context) override;
    Void visitExplainAnalyzeStepExtNode(ExplainAnalyzeStepExtNode & node, PrinterContext & context) override;
    Void visitTopNFilteringStepExtNode(TopNFilteringStepExtNode & node, PrinterContext & context) override;
    Void visitFillingStepNode(FillingStepNode & node, PrinterContext & context) override;
    Void visitIntersectOrExceptStepNode(IntersectOrExceptStepNode & node, PrinterContext & context) override;
    Void visitIntermediateResultCacheStepExtNode(IntermediateResultCacheStepExtNode & node, PrinterContext & context) override;

private:
    void printCTEDefNode(CTEId cte_id);
    std::stringstream & out;
    std::optional<SimpleCTEVisitHelper<void>> cte_helper;
    bool with_id;
    PlanCostMap plan_cost_map;
    StepProfiles profiles;
    void printNode(const PlanNodeBase & node, const String & label, const String & details, const String & color, PrinterContext & context);
    Void visitChildren(PlanNodeBase &, PrinterContext &);
    void printHints(const PlanNodeBase & node);
};

class PlanNodeEdgePrinter : public PlanNodeVisitor<Void, Void>
{
public:
    explicit PlanNodeEdgePrinter(std::stringstream & out_, CTEInfo * cte_info = nullptr)
        : out(out_), cte_helper(cte_info ? std::make_optional<SimpleCTEVisitHelper<void>>(*cte_info) : std::nullopt)
    {
    }
    Void visitPlanNode(PlanNodeBase &, Void &) override;
    Void visitCTERefStepExtNode(CTERefStepExtNode & node, Void & c) override;
    Void visitJoinStepExtNode(JoinStepExtNode & node, Void & c) override;

private:
    std::stringstream & out;
    std::optional<SimpleCTEVisitHelper<void>> cte_helper;
    void printEdge(PlanNodeBase & from, PlanNodeBase & to, std::string_view format = "");
};

class PlanSegmentNodePrinter : public NodeVisitor<Void, PrinterContext>
{
public:
    explicit PlanSegmentNodePrinter(std::stringstream & out_, bool with_id_ = false) : out(out_), with_id(with_id_) { }
    ~PlanSegmentNodePrinter() override = default;
    Void visitNode(QueryPlan::Node *, PrinterContext &) override;
    Void visitProjectionStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExpandStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitFilterStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitJoinStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitArrayJoinStepNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitAggregatingStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitMarkDistinctStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitMergingAggregatedStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitUnionStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExchangeStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitRemoteExchangeSourceStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitTableScanStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitReadNothingStepNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitReadStorageRowCountStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitValuesStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitLimitStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitOffsetStepNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitLimitByStepNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitSortingStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitMergeSortingStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitPartialSortingStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitMergingSortedStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitDistinctStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExtremesStepNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitTotalsHavingStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitFinalSampleStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitApplyStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitEnforceSingleRowStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitAssignUniqueIdStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitWindowStepNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitPartitionTopNStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExplainAnalyzeStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitTopNFilteringStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitFillingStepNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitIntersectOrExceptStepNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitIntermediateResultCacheStepExtNode(QueryPlan::Node * node, PrinterContext & context) override;

private:
    std::stringstream & out;
    bool with_id;
    void printNode(QueryPlan::Node * node, const String & label, const String & details, const String & color, PrinterContext & context);
    Void visitChildren(QueryPlan::Node *, PrinterContext &);
};

class PlanSegmentEdgePrinter : public NodeVisitor<Void, std::unordered_map<size_t, PlanSegmentPtr &>>
{
public:
    explicit PlanSegmentEdgePrinter(std::stringstream & out_) : out(out_) { }
    Void visitNode(QueryPlan::Node *, std::unordered_map<size_t, PlanSegmentPtr &> &) override;
    Void visitRemoteExchangeSourceStepExtNode(QueryPlan::Node * node, std::unordered_map<size_t, PlanSegmentPtr &> &) override;

private:
    std::stringstream & out;
    void printEdge(QueryPlan::Node * from, QueryPlan::Node * to);
};

class StepPrinter
{
public:
    static String printStep(const IQueryPlanStep & step, bool include_output = true);
    static String printProjectionStepExt(const ProjectionStepExt & step, bool include_output = true);
    static String printExpandStepExt(const ExpandStepExt & step, bool include_output = true);
    static String printFilterStepExt(const FilterStepExt & step, bool include_output = true);
    static String printJoinStepExt(const JoinStepExt & step);
    static String printArrayJoinStep(const ArrayJoinStep & step);
    static String printAggregatingStepExt(const AggregatingStepExt & step, bool include_output = true);
    static String printMarkDistinctStepExt(const MarkDistinctStepExt & step, bool include_output = true);
    static String printMergingAggregatedStepExt(const MergingAggregatedStepExt & step);
    static String printUnionStepExt(const UnionStepExt & step);
    static String printExchangeStepExt(const ExchangeStepExt & step);
    static String printRemoteExchangeSourceStepExt(const RemoteExchangeSourceStepExt & step);
    static String printFinalSampleStepExt(const FinalSampleStepExt & step);
    static String printTableScanStepExt(const TableScanStepExt & step);
    static String printReadStorageRowCountStepExt(const ReadStorageRowCountStepExt & step);
    static String printValuesStepExt(const ValuesStepExt & step);
    static String printLimitStepExt(const LimitStepExt & step);
    static String printOffsetStep(const OffsetStep & step);
    static String printLimitByStep(const LimitByStep & step);
    static String printSortingStepExt(const SortingStepExt & step);
    static String printMergeSortingStepExt(const MergeSortingStepExt & step);
    static String printPartialSortingStepExt(const PartialSortingStepExt & step);
    static String printMergingSortedStepExt(const MergingSortedStepExt & step);
    static String printDistinctStepExt(const DistinctStepExt & step);
    static String printApplyStepExt(const ApplyStepExt & step);
    static String printEnforceSingleRowStepExt(const EnforceSingleRowStepExt & step);
    static String printAssignUniqueIdStepExt(const AssignUniqueIdStepExt & step);
    static String printWindowStep(const WindowStep & step);
    static String printCTERefStepExt(const CTERefStepExt & step);
    static String printPartitionTopNStepExt(const PartitionTopNStepExt & step);
    static String printExplainAnalyzeStepExt(const ExplainAnalyzeStepExt & step);
    static String printTopNFilteringStepExt(const TopNFilteringStepExt & step);
    static String printFillingStep(const FillingStep & step);
    static String printIntersectOrExceptStep(const IntersectOrExceptStep & step);
    static String printTotalsHavingStepExt(const TotalsHavingStepExt & step);
    static String printExtremesStep(const ExtremesStep & step);
    static String printIntermediateResultCacheStepExt(const IntermediateResultCacheStepExt & step);

private:
    static String printFilter(const ConstASTPtr & filter);
};


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
