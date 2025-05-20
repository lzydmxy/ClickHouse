#pragma once

#include <unordered_map>
#include <utility>
#include <Query/Optimizer/DataDependency/DataDependency.h>
#include <Query/Optimizer/DataDependency/DependencyUtils.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

namespace DB
{
class DataDependencyDeriver
{
public:
    static DataDependency
    deriveDataDependency(QueryPlanStepPtr step, CTEInfo & cte_info, ContextMutablePtr & context);
    static DataDependency deriveDataDependency(
        QueryPlanStepPtr step,
        DataDependency & input_property,
        CTEInfo & cte_info,
        ContextMutablePtr & context);
    static DataDependency deriveDataDependency(
        QueryPlanStepPtr step,
        DataDependencyVector & input_data_dependencies,
        CTEInfo & cte_info,
        ContextMutablePtr & context);
    static DataDependency
    deriveStorageDataDependency(const StoragePtr & storage, ContextMutablePtr & context);
};


class DataDependencyDeriverContext
{
public:
    DataDependencyDeriverContext(
        DataDependencyVector input_properties_, CTEInfo & cte_info_, ContextMutablePtr & context_)
        : input_data_dependencies(std::move(input_properties_)), cte_helper(cte_info_), context(context_)
    {
    }
    const DataDependencyVector & getInput() const
    {
        return input_data_dependencies;
    }

    auto & getCTEHelper()
    {
        return cte_helper;
    }

    ContextMutablePtr & getContext()
    {
        return context;
    }

private:
    DataDependencyVector input_data_dependencies;
    StepCTEVisitHelper<DataDependency, DataDependencyDeriverContext> cte_helper;
    ContextMutablePtr & context;
};

class DataDependencyDeriverVisitor : public StepVisitor<DataDependency, DataDependencyDeriverContext>
{
public:
    DataDependency visitStep(const IQueryPlanStep &, DataDependencyDeriverContext &) override;

    DataDependency visitProjectionStepExt(const ProjectionStepExt & step, DataDependencyDeriverContext & context) override;
    DataDependency visitJoinStepExt(const JoinStepExt & step, DataDependencyDeriverContext & context) override;
    DataDependency visitTableScanStepExt(const TableScanStepExt &, DataDependencyDeriverContext &) override;
    DataDependency visitFilterStepExt(const FilterStepExt &, DataDependencyDeriverContext & context) override;
    DataDependency visitAggregatingStepExt(const AggregatingStepExt & step, DataDependencyDeriverContext & context) override;
    DataDependency visitUnionStepExt(const UnionStepExt & step, DataDependencyDeriverContext & context) override;
    DataDependency visitExchangeStepExt(const ExchangeStepExt & step, DataDependencyDeriverContext & context) override;
    DataDependency visitLimitStepExt(const LimitStepExt &, DataDependencyDeriverContext & context) override;
    DataDependency visitSortingStepExt(const SortingStepExt &, DataDependencyDeriverContext & context) override;
    DataDependency visitCTERefStepExt(const CTERefStepExt &, DataDependencyDeriverContext & context) override;

private:
    static void visitFilterExpression(const ConstASTPtr & filter, DataDependency & data_dependency, DataDependencyDeriverContext & context);
};

}
