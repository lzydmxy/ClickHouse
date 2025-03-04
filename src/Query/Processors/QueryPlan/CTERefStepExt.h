#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <memory>

namespace DB
{
using CTEId = UInt32;
// todo: need to add CTEInfo
//class CTEInfo;
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
    String getName() const override { return "CTERefStepExt"; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const;
    bool hasFilter() const { return has_filter; }
    void setFilter(bool has_filter_) { has_filter = has_filter_;}

    // todo: need to implement ProjectionStep
    //std::shared_ptr<ProjectionStep> toProjectionStep() const;
    // todo: need to add CTEInfo
    //PlanNodePtr toInlinedPlanNode(CTEInfo & cte_info, ContextMutablePtr & context) const;

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
