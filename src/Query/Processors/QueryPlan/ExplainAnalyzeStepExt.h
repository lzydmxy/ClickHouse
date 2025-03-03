#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Query/Parsers/ASTExplainQueryExt.h>

namespace DB
{
struct PlanSegmentDescription;
using PlanSegmentDescriptionPtr = std::shared_ptr<PlanSegmentDescription>;
using PlanSegmentDescriptions = std::vector<PlanSegmentDescriptionPtr>;

// TODO: have defined in src/Interpreters/InterpreterExplainQuery.cpp
struct QueryPlanSettings
{
    QueryPlan::ExplainPlanOptions query_plan_options;

    /// Apply query plan optimizations.
    bool optimize = true;
    bool json = false;

    constexpr static char name[] = "PLAN";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings = {
        {"header", query_plan_options.header},
        {"description", query_plan_options.description},
        {"actions", query_plan_options.actions},
        {"indexes", query_plan_options.indexes},
        {"optimize", optimize},
        {"json", json},
        {"sorting", query_plan_options.sorting},
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

class ExplainAnalyzeStepExt : public ITransformingStep
{
public:
    ExplainAnalyzeStepExt(
        const DataStream & input_stream_,
        const String & output_name_,
        ASTExplainQueryExt::ExplainKindExt explain_kind_,
        ContextMutablePtr context_,
        std::shared_ptr<QueryPlan> query_plan_ptr_,
        QueryPlanSettings settings);

    String getName() const override { return "ExplainAnalyzeExt"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    bool hasPlan() const { return query_plan_ptr != nullptr; }
    const std::shared_ptr<QueryPlan> & getQueryPlan() const { return query_plan_ptr; }
    const ContextMutablePtr & getContext() const { return context; }
    const QueryPlanSettings & getSetting() const { return settings; }
    String getOutputName() const { return output_stream->header.getByPosition(0).name; }
    ASTExplainQueryExt::ExplainKindExt getKind() const { return kind; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;
    void setPlanSegmentDescriptions(PlanSegmentDescriptions & descriptions) { segment_descriptions = descriptions; }

private:
    ASTExplainQueryExt::ExplainKindExt kind;
    ContextMutablePtr context;
    std::shared_ptr<QueryPlan> query_plan_ptr;
    PlanSegmentDescriptions segment_descriptions;
    QueryPlanSettings settings;

    void updateOutputStream() override {};
};
using ExplainAnalyzeStepExtPtr = std::shared_ptr<ExplainAnalyzeStepExt>;

}
