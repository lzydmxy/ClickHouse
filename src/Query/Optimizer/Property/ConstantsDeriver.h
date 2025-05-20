#pragma once

#include <Query/Optimizer/Property/Constants.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

#include <utility>

namespace DB
{
class ConstantsDeriver
{
public:
    static Constants deriveConstants(QueryPlanStepPtr step, CTEInfo & cte_info, ContextMutablePtr & context);
    static Constants deriveConstants(QueryPlanStepPtr step, Constants & input_constants, CTEInfo & cte_info, ContextMutablePtr & context);
    static Constants
    deriveConstants(QueryPlanStepPtr step, ConstantsSet & input_constants, CTEInfo & cte_info, ContextMutablePtr & context);
    static Constants deriveConstantsFromTree(PlanNodePtr node, CTEInfo & cte_info, ContextMutablePtr & context);
};

class ConstantsDeriverContext
{
public:
    ConstantsDeriverContext(ConstantsSet input_properties_, CTEInfo & cte_info_, ContextMutablePtr & context_)
        : input_properties(std::move(input_properties_)), cte_info(cte_info_), context(context_)
    {
    }
    const ConstantsSet & getInput()
    {
        return input_properties;
    }
    CTEInfo & getCTEInfo()
    {
        return cte_info;
    }
    ContextMutablePtr & getContext()
    {
        return context;
    }

private:
    ConstantsSet input_properties;
    CTEInfo & cte_info;
    ContextMutablePtr & context;
};

class ConstantsDeriverVisitor : public StepVisitor<Constants, ConstantsDeriverContext>
{
public:
    Constants visitStep(const IQueryPlanStep &, ConstantsDeriverContext &) override;

    Constants visitFilterStepExt(const FilterStepExt &, ConstantsDeriverContext & context) override;
    Constants visitJoinStepExt(const JoinStepExt & step, ConstantsDeriverContext & context) override;
    Constants visitProjectionStepExt(const ProjectionStepExt & step, ConstantsDeriverContext & context) override;
    Constants visitMarkDistinctStepExt(const MarkDistinctStepExt & step, ConstantsDeriverContext & context) override;
    Constants visitAggregatingStepExt(const AggregatingStepExt & step, ConstantsDeriverContext & context) override;
    Constants visitUnionStepExt(const UnionStepExt & step, ConstantsDeriverContext & context) override;
    Constants visitTableScanStepExt(const TableScanStepExt &, ConstantsDeriverContext &) override;
    Constants visitReadNothingStep(const ReadNothingStep &, ConstantsDeriverContext &) override;
    Constants visitReadStorageRowCountStepExt(const ReadStorageRowCountStepExt &, ConstantsDeriverContext &) override;
    Constants visitValuesStepExt(const ValuesStepExt &, ConstantsDeriverContext &) override;
    Constants visitCTERefStepExt(const CTERefStepExt &, ConstantsDeriverContext & context) override;
};

struct ConstantsDeriverTreeVisitorContext
{
    CTEInfo & cte_info;
    ContextMutablePtr context;
};

class ConstantsDeriverTreeVisitor : public PlanNodeVisitor<Constants, ConstantsDeriverTreeVisitorContext>
{
public:
    Constants visitPlanNode(PlanNodeBase & node, ConstantsDeriverTreeVisitorContext &) override;
    Constants visitCTERefStepExtNode(CTERefStepExtNode &, ConstantsDeriverTreeVisitorContext &) override
    {
        return {};
    }
};
}
