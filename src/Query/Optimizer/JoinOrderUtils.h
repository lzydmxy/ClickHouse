#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/Context.h>
#include <Query/Processors/QueryPlan/SimplePlanRewriter.h>
#include <Query/Processors/QueryPlan/SimplePlanVisitor.h>

namespace DB
{
class JoinOrderUtils
{
public:
    static String getJoinOrder(QueryPlanExt & plan);
};

class JoinOrderExtractor : public PlanNodeVisitor<String, Void>
{
public:
    explicit JoinOrderExtractor(CTEInfo & cte_info_) : cte_helper(cte_info_) { }
    String visitPlanNode(PlanNodeBase &, Void &) override;
    String visitJoinStepExtNode(JoinStepExtNode &, Void &) override;
    String visitCTERefStepExtNode(CTERefStepExtNode & node, Void &) override;
    String visitTableScanStepExtNode(TableScanStepExtNode & node, Void &) override;

private:
    SimpleCTEVisitHelper<String> cte_helper;
};


}
