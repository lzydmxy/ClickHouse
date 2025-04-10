#pragma once

#include <Query/Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <Interpreters/QueryLog.h>
#include <Query/Processors/QueryPlan/CTEVisitHelper.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Analyzer/Analysis.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/PlanPrinter.h>

namespace DB
{
struct Analysis;
using AnalysisPtr = std::shared_ptr<Analysis>;
using PlanNodeId = UInt32;
struct QueryCacheContext;
class CTEInfo;
class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;
using PlanNodes = std::vector<PlanNodePtr>;


class InterpreterSelectQueryUseOptimizer : public IInterpreter
{
public:
    InterpreterSelectQueryUseOptimizer(const ASTPtr & query_ptr_, ContextMutablePtr context_, const SelectQueryOptions & options_)
        : InterpreterSelectQueryUseOptimizer(query_ptr_, nullptr, {}, context_, options_)
    {
    }

    InterpreterSelectQueryUseOptimizer(
        PlanNodePtr sub_plan_ptr_, CTEInfo cte_info_, ContextMutablePtr context_, const SelectQueryOptions & options_)
        : InterpreterSelectQueryUseOptimizer(nullptr, std::move(sub_plan_ptr_), std::move(cte_info_), context_, options_)
    {
    }

    InterpreterSelectQueryUseOptimizer(
        const ASTPtr & query_ptr_,
        PlanNodePtr sub_plan_ptr_,
        CTEInfo cte_info_,
        ContextMutablePtr & context_,
        const SelectQueryOptions & options_);

    QueryPlanExtPtr getQueryPlan(bool skip_optimize = false);
    void buildQueryPlan(QueryPlanExtPtr & query_plan, AnalysisPtr & analysis, bool skip_optimize = false);
    std::pair<PlanSegmentTreeUniqPtr, std::set<StorageID>> getPlanSegment();
    QueryPlanExtPtr getPlanFromCache(UInt128 query_hash);
    bool addPlanToCache(UInt128 query_hash, QueryPlanExtPtr & plan, AnalysisPtr analysis);
    static void setPlanSegmentInfoForExplainAnalyze(PlanSegmentTreeUniqPtr & plan_segment_tree, ContextMutablePtr context);
    //todo: need support query cache
    //BlockIO readFromQueryCache(ContextPtr local_context, QueryCacheContext & can_use_query_cache);

    BlockIO execute() override;

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const override
    {
        elem.query_kind = IAST::QueryKind::Select;
        //todo：need add segment_profiles in QueryLogElement
        //elem.segment_profiles = segment_profiles;
    }

    static void resetFinalSampleSize(PlanSegmentTreeUniqPtr & plan_segment_tree);

    static void fillContextQueryAccessInfo(ContextPtr context, AnalysisPtr & analysis);

    static void fillQueryPlan(ContextPtr context, QueryPlanExt & query_plan);

    Block getSampleBlock();

    static void setUnsupportedSettings(ContextMutablePtr & context);

    std::optional<std::set<StorageID>> getUsedStorageIds();

    ASTPtr & getQuery() { return query_ptr; }

private:
    ASTPtr query_ptr;
    PlanNodePtr sub_plan_ptr;
    CTEInfo cte_info;
    ContextMutablePtr context;
    SelectQueryOptions options;
    LoggerPtr log;
    bool interpret_sub_query;
    PlanSegmentTreeUniqPtr plan_segment_tree_ptr;

    std::shared_ptr<std::vector<String>> segment_profiles;

    Block block;
};

/**
 * Convert PlanNode to QueryPlan::Node.
 */
class PlanNodeToNodeVisitor : public PlanNodeVisitor<QueryPlanExt::Node *, Void>
{
public:
    static QueryPlanExt convert(QueryPlanExt &);
    explicit PlanNodeToNodeVisitor(QueryPlanExt & plan_) : plan(plan_) {  }
    QueryPlanExt::Node * visitPlanNode(PlanNodeBase & node, Void & c) override;

private:
    QueryPlanExt & plan;
};

struct ClusterInfoContext
{
    QueryPlanExt & query_plan;
    ContextMutablePtr context;
    PlanSegmentTreeUniqPtr & plan_segment_tree;
};

class ClusterInfoFinder : public PlanNodeVisitor<std::optional<PlanSegmentContext>, ClusterInfoContext>
{
public:
    static PlanSegmentContext find(QueryPlanExt & plan, ClusterInfoContext & cluster_info_context);
    explicit ClusterInfoFinder(CTEInfo & cte_info_) : cte_helper(cte_info_) { }
    std::optional<PlanSegmentContext> visitPlanNode(PlanNodeBase & node, ClusterInfoContext & cluster_info_context);
    std::optional<PlanSegmentContext> visitTableScanNode(TableScanStepExtNode & node, ClusterInfoContext & cluster_info_context);
    std::optional<PlanSegmentContext> visitCTERefNode(CTERefStepExtNode & node, ClusterInfoContext & cluster_info_context);
private:
    SimpleCTEVisitHelper<std::optional<PlanSegmentContext>> cte_helper;
};

class ExplainAnalyzeVisitor : public NodeVisitor<void, PlanSegmentTree::Nodes>
{
public:
    void visitExplainAnalyzeNode(QueryPlanExt::Node * node, PlanSegmentTree::Nodes &);
    void visitNode(QueryPlanExt::Node * node, PlanSegmentTree::Nodes &) override;
};
}
