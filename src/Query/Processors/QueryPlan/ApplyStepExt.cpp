#include <Query/Processors/QueryPlan/ApplyStepExt.h>

#include <Query/Interpreters/JoinUtilsExt.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

ApplyStepExt::ApplyStepExt(
    DataStreams input_streams_,
    Names correlation_,
    ApplyType apply_type_,
    SubqueryType subquery_type_,
    Assignment assignment_,
    NameSet outer_columns_,
    bool support_semi_anti_)
    : correlation(std::move(correlation_))
    , apply_type(apply_type_)
    , subquery_type(subquery_type_)
    , assignment(std::move(assignment_))
    , outer_columns(std::move(outer_columns_))
    , support_semi_anti(support_semi_anti_)
{
    input_streams = std::move(input_streams_);
    auto output = input_streams[0].header;
    output.insert(ColumnWithTypeAndName{getAssignmentDataType(), assignment.first});
    output_stream = DataStream{output};
}

DataTypePtr ApplyStepExt::getAssignmentDataType() const
{
    switch (subquery_type)
    {
        case ApplyStepExt::SubqueryType::IN: {
            auto * arguments = assignment.second->children[0]->as<ASTExpressionList>();
            auto argument_name = arguments->children[0]->as<ASTIdentifier>()->name();
            for (const auto & column : input_streams[0].header)
                if (column.name == argument_name)
                    return column.type->isNullable() ? JoinCommon::tryConvertTypeToNullable(std::make_shared<DataTypeUInt8>()) : std::make_shared<DataTypeUInt8>();
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown data type for column {}", argument_name);
        }
        case ApplyStepExt::SubqueryType::EXISTS: {
            return std::make_shared<DataTypeUInt8>();
        }
        case ApplyStepExt::SubqueryType::SCALAR: {
            for (const auto & column : input_streams[1].header)
                if (column.name == assignment.first)
                    return JoinCommon::canBecomeNullable(column.type) ? JoinCommon::tryConvertTypeToNullable(column.type) : column.type;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown data type for column {}", assignment.first);
        }
        case ApplyStepExt::SubqueryType::QUANTIFIED_COMPARISON: {
            auto argument_name = assignment.second->children[0]->as<ASTIdentifier>()->name();
            for (const auto & column : input_streams[0].header)
                if (column.name == argument_name)
                    return column.type->isNullable() ? JoinCommon::tryConvertTypeToNullable(std::make_shared<DataTypeUInt8>()) : std::make_shared<DataTypeUInt8>();
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown data type for column {}", argument_name);
        }
    }
}

QueryPipelineBuilderPtr ApplyStepExt::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ApplyStep should be rewritten into JoinStep");
}

std::shared_ptr<IQueryPlanStep> ApplyStepExt::copy(ContextPtr) const
{
    return std::make_shared<ApplyStepExt>(input_streams, correlation, apply_type, subquery_type, assignment, outer_columns, support_semi_anti);
}

}
