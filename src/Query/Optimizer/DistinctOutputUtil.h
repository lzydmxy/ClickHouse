#pragma once

#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Common/Void.h>

namespace DB
{
class DistinctOutputQueryUtil
{
public:
    static bool isDistinct(PlanNodeBase &);
};

class IsDistinctPlanVisitor : public PlanNodeVisitor<bool, Void>
{
public:
    bool visitPlanNode(PlanNodeBase & node, Void & c) override;
    bool visitValuesStepExtNode(ValuesStepExtNode & node, Void & context) override;
    bool visitLimitStepExtNode(LimitStepExtNode & node, Void & context) override;
    // bool visitIntersectNode(IntersectNode & node, Void & context) override;
    bool visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode & node, Void & context) override;
    bool visitAggregatingStepExtNode(AggregatingStepExtNode & node, Void & context) override;
    bool visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode & node, Void & context) override;
    bool visitFilterStepExtNode(FilterStepExtNode & node, Void & context) override;
    bool visitDistinctStepExtNode(DistinctStepExtNode & node, Void & context) override;
    // bool visitExceptNode(ExceptNode & node, Void & context) override;
    bool visitMergingSortedStepExtNode(MergingSortedStepExtNode & node, Void & context) override;
};
}
