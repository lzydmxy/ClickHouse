#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>

#include <memory>

namespace DB
{
using CTEId = UInt32;
class SymbolMapper;

/**
 * CTE is model as two parts: CTERef and CTEDef.
 * CTERefStepExt is a source node reference to CTEDef by id.
 * CTEDef is a virtual node, the plan is stored in CTEInfo.
 */
class CTERefStepExt : public ISourceStep
{
public:
    CTERefStepExt(DataStream output_, CTEId id_, std::unordered_map<String, String> output_columns_, bool has_filter_);
    CTERefStepExt(Block header, CTEId id_, std::unordered_map<String, String> output_columns_, bool has_filter_);

    CTEId getId() const { return id; }
    const std::unordered_map<String, String> & getOutputColumns() const { return output_columns; }
    std::unordered_map<String, String> getReverseOutputColumns() const;

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported");
    }
    String getName() const override { return "CTERefExt"; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const;
    bool hasFilter() const { return has_filter; }
    void setFilter(bool has_filter_) { has_filter = has_filter_;}

    std::shared_ptr<ProjectionStepExt> toProjectionStep() const;
    PlanNodePtr toInlinedPlanNode(CTEInfo & cte_info, ContextMutablePtr & context) const;
    void toProto(Protos::CTERefStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<CTERefStepExt> fromProto(const Protos::CTERefStepExt & proto, ContextPtr context);

private:
    /**
     * CTE id reference to CTEInfo in QueryPlan.
     */
    CTEId id;

    /**
     * Map of output column name to cte column name.
     */
    std::unordered_map<String, String> output_columns;

    /**
     * CTE follows a filter.
     */
    bool has_filter;
};
}
