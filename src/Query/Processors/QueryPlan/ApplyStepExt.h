#pragma once

#include <Core/Names.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <Query/Protos/EnumMacros.h>
#include <Query/Protos/plan_node.pb.h>
#include <Query/Parsers/ASTHelper.h>


namespace DB
{

class ApplyStepExt : public IQueryPlanStep
{
public:
    ENUM_WITH_PROTO_CONVERTER(
        ApplyType, // enum name
        Protos::ApplyStep::ApplyType, // proto enum message
        (CROSS, 0),
        (LEFT),
        (SEMI),
        (ANTI));

    ENUM_WITH_PROTO_CONVERTER(
        SubqueryType, // enum name
        Protos::ApplyStep::SubqueryType, // proto enum message
        (SCALAR, 0),
        (IN),
        (EXISTS),
        (QUANTIFIED_COMPARISON));

    ApplyStepExt(
        DataStreams input_streams_,
        Names correlation_,
        ApplyType apply_type_,
        SubqueryType subquery_type_,
        Assignment assignment_,
        NameSet outer_columns_,
        bool support_semi_anti_);

    String getName() const override { return "ApplyExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &) override;

    const Names & getCorrelation() const { return correlation; }
    ApplyType getApplyType() const { return apply_type; }
    SubqueryType getSubqueryType() const { return subquery_type; }
    const Assignment & getAssignment() const { return assignment; }
    const NameSet & getOuterColumns() const { return outer_columns; }
    void setOuterColumns(NameSet outer_columns_) { outer_columns = outer_columns_; }
    DataTypePtr getAssignmentDataType() const;
    bool supportSemiAnti() const { return support_semi_anti; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

private:
    /**
     * Correlation symbols, returned from input (outer plan) used in subquery (inner plan)
     */
    Names correlation;
    ApplyType apply_type;
    SubqueryType subquery_type;

    /**
     * Expressions that use subquery symbols.
     * <p>
     * Subquery expressions are different than other expressions
     * in a sense that they might use an entire subquery result
     * as an input (e.g: "x IN (subquery)", "x < ALL (subquery)").
     * Such expressions are invalid in linear operator context
     * (e.g: ProjectNode) in logical plan, but are correct in
     * ApplyNode context.
     * <p>
     * Example 1:
     * - expression: input_symbol_X IN (subquery_symbol_Y)
     * - meaning: if set consisting of all values for subquery_symbol_Y contains value represented by input_symbol_X
     * <p>
     * Example 2:
     * - expression: input_symbol_X < ALL (subquery_symbol_Y)
     * - meaning: if input_symbol_X is smaller than all subquery values represented by subquery_symbol_Y
     * <p>
     */
    Assignment assignment;
    NameSet outer_columns;
    bool support_semi_anti;
};

}
