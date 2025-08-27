#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class WindowTransform;

class WindowStepExt : public ITransformingStep
{
public:
    explicit WindowStepExt(
        const DataStream & input_stream_,
        const WindowDescription & window_description_,
        const std::vector<WindowFunctionDescription> & window_functions_,
        bool need_sort_,
        SortDescription prefix_description_ = {});

    WindowStepExt(
        const DataStream & input_stream_,
        const WindowDescription & window_description_,
        bool need_sort_,
        SortDescription prefix_description_);

    String getName() const override { return "WindowExt"; }

    const WindowDescription & getWindow() const { return window_description; }
    const std::vector<WindowFunctionDescription> & getFunctions() const { return window_functions; }
    bool needSort() const { return need_sort; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
    void toProto(Protos::WindowStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<WindowStepExt> fromProto(const Protos::WindowStepExt & proto, ContextPtr context);

    const WindowDescription & getWindowDescription() const { return window_description; }
    const SortDescription & getPrefixDescription() const { return prefix_description; }
    void setPrefixDescription(const SortDescription & prefix_description_) { prefix_description = prefix_description_; }

private:
    WindowDescription window_description;
    std::vector<WindowFunctionDescription> window_functions;
    bool need_sort;
    SortDescription prefix_description;
    void scatterByPartitionIfNeeded(QueryPipelineBuilder & pipeline);

    void updateOutputStream() override;
};

}
