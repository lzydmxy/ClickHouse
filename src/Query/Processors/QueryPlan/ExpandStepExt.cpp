#include <vector>
#include <Core/NamesAndTypes.h>
#include <Query/Processors/QueryPlan/ExpandStepExt.h>
// #include <DataTypes/DataTypeHelper.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
// #include <Parsers/ASTSerDerHelper.h>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Processors/Transforms/ExpandTransformExt.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
// #include "Interpreters/join_common.h"
#include <Core/ColumnWithTypeAndName.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>

namespace DB
{

const std::string ExpandStepExt::group_id = "group_id";
const std::string ExpandStepExt::group_id_mask = "group_id_mask_";

ExpandStepExt::ExpandStepExt(
    const DataStream & input_stream_,
    Assignments assignments_,
    NameToType name_to_type_,
    String group_id_symbol_,
    std::set<Int32> group_id_value_,
    std::map<Int32, Names> group_id_non_null_symbol_)
    : ITransformingStep(input_stream_, {}, {}, true)
    , assignments(std::move(assignments_))
    , name_to_type(std::move(name_to_type_))
    , group_id_symbol(std::move(group_id_symbol_))
    , group_id_value(std::move(group_id_value_))
    , group_id_non_null_symbol(std::move(group_id_non_null_symbol_))
{
    for (const auto & item : assignments)
    {
        if (unlikely(!name_to_type[item.first]))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ExpandStepExt miss type info for column {}", item.first);
        if (unlikely(!input_stream_.header.has(item.first)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ExpandStepExt miss input column {}", item.first);
        output_stream->header.insert(ColumnWithTypeAndName{name_to_type[item.first], item.first});
    }
    auto group_id_symbol_type = std::make_shared<DataTypeInt32>();
    output_stream->header.insert(ColumnWithTypeAndName{group_id_symbol_type, group_id_symbol});
}

void ExpandStepExt::updateOutputStream()
{
    Block block;
    for (auto & input : input_streams[0].header)
        block.insert(ColumnWithTypeAndName{input.type, input.name});
    output_stream->header = block;
    auto group_id_symbol_type = std::make_shared<DataTypeInt32>();
    output_stream->header.insert(ColumnWithTypeAndName{group_id_symbol_type, group_id_symbol});
}

void ExpandStepExt::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    std::vector<ExpressionActionsPtr> expressions;
    for (auto & assignments_pre_group : generateAssignmentsGroups())
    {
        auto actions = createActions(assignments_pre_group, generateNameTypePreGroup(), Context::getGlobalContextInstance());
        auto expression = std::make_shared<ExpressionActions>(actions, settings.getActionsSettings());
        expressions.emplace_back(expression);
    }

    pipeline.addSimpleTransform([&](const Block & header)
                                { return std::make_shared<ExpandTransformExt>(header, output_stream->header, expressions); });
}

// void ExpandStepExt::prepare(const PreparedStatementContext & prepared_context)
// {
//     for (auto & assign : assignments)
//         prepared_context.prepare(assign.second);
// }

std::vector<Assignments> ExpandStepExt::generateAssignmentsGroups() const
{
    std::vector<Assignments> assignments_group;
    for (const auto & id : group_id_value)
    {
        Assignments assignments_pre_group;
        for (const auto & assignment : getAssignments())
        {
            Names non_nulls = group_id_non_null_symbol.at(id);
            auto non_nulls_exist = std::find(non_nulls.begin(), non_nulls.end(), assignment.first);

            /// if symbol exists in non_nulls list, then we need project it.
            if (non_nulls_exist != non_nulls.end())
            {
                assignments_pre_group.emplace(
                    assignment.first,
                    makeASTFunction(
                        "cast",
                        std::make_shared<ASTIdentifier>(assignment.first),
                        std::make_shared<ASTLiteral>(name_to_type.at(assignment.first)->getName())));
            }
            else /// otherwise, use null replace origin value.
            {
                assignments_pre_group.emplace(assignment.first, assignment.second);
            }
        }
        assignments_pre_group.emplace(group_id_symbol, std::make_shared<ASTLiteral>(id));

        assignments_group.emplace_back(assignments_pre_group);
    }
    return assignments_group;
}

NamesAndTypesList ExpandStepExt::generateNameTypePreGroup() const
{
    NamesAndTypesList name_type_list_pre_group;
    for (const auto & name_type : getNameToType())
        name_type_list_pre_group.push_back(NameAndTypePair{name_type.first, name_type.second});
    name_type_list_pre_group.push_back(NameAndTypePair{group_id_symbol, std::make_shared<DataTypeInt32>()});
    return name_type_list_pre_group;
}

ActionsDAGPtr ExpandStepExt::createActions(const Assignments & assignments, const NamesAndTypesList & source, ContextPtr context)
{
    ASTPtr expr_list = std::make_shared<ASTExpressionList>();

    NamesWithAliases output;
    for (const auto & item : assignments)
    {
        expr_list->children.emplace_back(item.second->clone());
        output.emplace_back(NameWithAlias{item.second->getColumnName(), item.first});
    }
    return QueryPlanStepHelper::createExpressionActions(context, source, output, expr_list);
}

std::shared_ptr<IQueryPlanStep> ExpandStepExt::copy(ContextPtr) const
{
    return std::make_shared<ExpandStepExt>(
        input_streams[0], assignments.copy(), name_to_type, group_id_symbol, group_id_value, group_id_non_null_symbol);
}

void ExpandStepExt::toProto(Protos::ExpandStepExt & proto, bool) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this ,*proto.mutable_query_plan_base());
    serializeAssignmentsToProto(assignments, *proto.mutable_assignments());
    serializeOrderedMapToProto(name_to_type, *proto.mutable_name_to_type());
    proto.set_group_id_symbol(group_id_symbol);
    for (const auto & group : group_id_value)
        proto.add_group_id_value(group);
    serializeOrderedMapToProto(group_id_non_null_symbol, *proto.mutable_group_id_non_null_symbol());
}

std::shared_ptr<ExpandStepExt> ExpandStepExt::fromProto(const Protos::ExpandStepExt & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto assignments = deserializeAssignmentsFromProto(proto.assignments());
    auto name_to_type = deserializeOrderedMapFromProto<String, DataTypePtr>(proto.name_to_type());
    String group_id_symbol = proto.group_id_symbol();
    std::set<Int32> group_id_value;
    for (const auto & group : proto.group_id_value())
        group_id_value.insert(group);
    auto group_id_non_null_symbol = deserializeOrderedMapFromProto<Int32, Names>(proto.group_id_non_null_symbol());
    auto step
        = std::make_shared<ExpandStepExt>(base_input_stream, assignments, name_to_type, group_id_symbol, group_id_value, group_id_non_null_symbol);
    step->setStepDescription(step_description);
    return step;
}
}
