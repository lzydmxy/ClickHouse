#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Query/Common/ExplainSettings.h>
#include <Query/Parsers/ASTExplainQueryExt.h>

namespace DB
{
struct PlanSegmentDescription;
using PlanSegmentDescriptionPtr = std::shared_ptr<PlanSegmentDescription>;
using PlanSegmentDescriptions = std::vector<PlanSegmentDescriptionPtr>;

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

    void toProto([[maybe_unused]] Protos::ExplainAnalyzeStepExt & proto, [[maybe_unused]] bool for_hash_equals = false) const;
    static std::shared_ptr<ExplainAnalyzeStepExt> fromProto(const Protos::ExplainAnalyzeStepExt & proto, ContextPtr);

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
