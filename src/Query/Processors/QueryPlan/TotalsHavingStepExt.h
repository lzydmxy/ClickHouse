#pragma once

#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

using ConstASTPtr = std::shared_ptr<const IAST>;

class TotalsHavingStepExt : public TotalsHavingStep
{
public:
    TotalsHavingStepExt(
            const DataStream & input_stream_,
            const AggregateDescriptions & aggregates_,
            bool overflow_row_,
            const ConstASTPtr & having_filter_,
            const ActionsDAGPtr & actions_dag_,
            const std::string & filter_column_,
            bool remove_filter_,
            TotalsMode totals_mode_,
            double auto_include_threshold_,
            bool final_);

    String getName() const override { return "TotalsHavingStepExt"; }
    bool isOverflowRow() const { return overflow_row; }
    String getFilterColumnName() const { return filter_column_name; }
    TotalsMode getTotalsMode() const { return totals_mode; }
    double getAutoIncludeThreshols() const { return auto_include_threshold; }
    bool isFinal() const { return final; }
    const ConstASTPtr & getHavingFilter() const { return having_filter;}
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

private:
    ConstASTPtr having_filter;
};

}

