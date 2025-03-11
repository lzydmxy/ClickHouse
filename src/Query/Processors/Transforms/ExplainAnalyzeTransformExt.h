#pragma once

#include <Processors/ISimpleTransform.h>
#include <Query/Common/ProcessorProfile.h>
#include <Query/Processors/QueryPlan/ExplainAnalyzeStepExt.h>

namespace DB
{

using ProcessorsSet = std::unordered_set<const IProcessor *>;

class ExplainAnalyzeTransformExt : public ISimpleTransform
{
public:
    ExplainAnalyzeTransformExt(
        const Block & input_header_,
        const Block & output_header_,
        ASTExplainQueryExt::ExplainKindExt kind_,
        std::shared_ptr<QueryPlan> query_plan_ptr_,
        ContextMutablePtr context_,
        PlanSegmentDescriptions & segment_descriptions_,
        QueryPlanSettings settings = {});

    String getName() const override { return "ExplainAnalyzeTransformExt"; }

protected:
    void transform(Chunk & chunk) override;
    ISimpleTransform::Status prepare() override;

    void getProcessorProfiles(ProcessorsSet & processors_set, ProcessorProfiles & profiles, const IProcessor * processor);
    void getRemoteProcessorProfiles(std::unordered_map<size_t, std::unordered_map<String, ProcessorProfiles>> & segment_profiles);

private:
    ASTExplainQueryExt::ExplainKindExt kind;
    ContextMutablePtr context;
    std::shared_ptr<QueryPlan> query_plan_ptr;
    PlanSegmentDescriptions segment_descriptions;
    bool has_final_transform = true;
    QueryPlanSettings settings;
    String coordinator_address;
};

}
