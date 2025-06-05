#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Optimizer/Graph.h>

namespace DB
{

class MultiJoinStepExt : public IQueryPlanStep
{
public:
    explicit MultiJoinStepExt(const DataStream & output_, const Graph & graph_) : graph(graph_) { output_stream = output_; }

    String getName() const override { return "MultiJoinExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void toProto(Protos::MultiJoinStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<MultiJoinStepExt> fromProto(const Protos::MultiJoinStepExt &, ContextPtr)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "UNREACHABLE MultiJoinStepExt::fromProto()!");
    }

    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const;

    const Graph & getGraph() const { return graph; }

private:
    Graph graph;
};

}
