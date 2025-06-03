#pragma once

#include <Processors/QueryPlan/FilterStep.h>
#include <Query/Parsers/ASTHelper.h>

namespace DB
{

class FilterStepExt : public FilterStep
{
public:
    FilterStepExt(
        const DataStream & input_stream_,
        ActionsDAGPtr actions_dag_,
        String filter_column_name_,
        bool remove_filter_column_);

    FilterStepExt(const DataStream & input_stream_, const ConstASTPtr & filter_, bool remove_filter_column_ = true);

    const ConstASTPtr & getFilter() const { return filter; }
    void setFilter(ConstASTPtr new_filter) { filter = std::move(new_filter); }
    String getName() const override { return "FilterStepExt"; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const;

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    static ConstASTPtr
    rewriteRuntimeFilter(const ConstASTPtr & filter, QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_context);

    static std::pair<ConstASTPtr, ConstASTPtr> splitLargeInValueList(const ConstASTPtr & filter, UInt64 limit);
    static std::vector<ConstASTPtr> removeLargeInValueList(const std::vector<ConstASTPtr> & filters, UInt64 limit);

    void toProto(Protos::FilterStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<FilterStepExt> fromProto(const Protos::FilterStepExt & proto, ContextPtr context);

    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    ConstASTPtr filter;
};

}
