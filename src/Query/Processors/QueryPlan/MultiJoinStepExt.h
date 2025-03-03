#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Optimizer/Graph.h>

namespace DB
{

class MultiJoinStepExt : public IQueryPlanStep
{
public:
    explicit MultiJoinStepExt(const DataStream & output_, const Graph & graph_) : graph(graph_) { output_stream = output_; }

    String getName() const override { return "MultiJoinStepExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const;

    const Graph & getGraph() const { return graph; }

private:
    Graph graph;
};

}
