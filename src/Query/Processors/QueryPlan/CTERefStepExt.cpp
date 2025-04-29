#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Parsers/ASTIdentifier.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace DB
{

CTERefStepExt::CTERefStepExt(DataStream output_, CTEId id_, std::unordered_map<String, String> output_columns_, bool has_filter_)
    : ISourceStep(std::move(output_)), id(id_), output_columns(std::move(output_columns_)), has_filter(has_filter_)
{
}

CTERefStepExt::CTERefStepExt(Block header_, CTEId id_, std::unordered_map<String, String> output_columns_, bool has_filter_)
    : ISourceStep(DataStream{.header = header_}), id(id_), output_columns(std::move(output_columns_)), has_filter(has_filter_)
{
}

std::shared_ptr<IQueryPlanStep> CTERefStepExt::copy(ContextPtr) const
{
    return std::make_shared<CTERefStepExt>(output_stream.value(), id, output_columns, has_filter);
}

std::shared_ptr<ProjectionStepExt> CTERefStepExt::toProjectionStep() const
{
    NamesAndTypes inputs;
    Assignments assignments;
    NameToType name_to_type;
    for (const auto & item : output_stream.value().header)
    {
        auto it = output_columns.find(item.name);
        if (it != output_columns.end())
        {
            assignments.emplace_back(item.name, std::make_shared<ASTIdentifier>(it->second));
            name_to_type.emplace(item.name, item.type);
            inputs.emplace_back(NameAndTypePair{it->second, item.type});
        }
    }

    ColumnsWithTypeAndName data;
    for (const auto & item : inputs)
    {
        data.emplace_back(item.type, item.name);
    }

    return std::make_shared<ProjectionStepExt>(DataStream{data}, assignments, name_to_type);
}

PlanNodePtr CTERefStepExt::toInlinedPlanNode(CTEInfo & cte_info, ContextMutablePtr & context) const
{
    auto rewrite = PlanSymbolReallocator::reallocate(cte_info.getCTEDef(id), context);

    NamesAndTypes inputs;
    Assignments assignments;
    NameToType name_to_type;

    for (const auto & item : output_stream.value().header)
    {
        auto it = output_columns.find(item.name);
        if (it != output_columns.end())
        {
            auto new_symbol = rewrite.mappings.find(it->second);
            if (new_symbol == rewrite.mappings.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "output_stream symbol not found in cte def: {}", it->second);
            assignments.emplace_back(item.name, std::make_shared<ASTIdentifier>(new_symbol->second));
            name_to_type.emplace(item.name, item.type);
            inputs.emplace_back(NameAndTypePair{it->second, item.type});
        }
    }

    ColumnsWithTypeAndName data;
    for (const auto & item : inputs)
    {
        data.emplace_back(item.type, item.name);
    }

    return PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::make_shared<ProjectionStepExt>(DataStream{data}, assignments, name_to_type), {rewrite.plan_node});
}

std::unordered_map<String, String> CTERefStepExt::getReverseOutputColumns() const
{
    std::unordered_map<String, String> reverse;
    for (const auto & item : output_columns)
        reverse.emplace(item.second, item.first);
    return reverse;
}

void CTERefStepExt::toProto(Protos::CTERefStepExt & proto, bool for_hash_equals) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    proto.set_id(id);
    serializeMapToProto(output_columns, *proto.mutable_output_columns());
    proto.set_has_filter(has_filter);
}

std::shared_ptr<CTERefStepExt> CTERefStepExt::fromProto(const Protos::CTERefStepExt & proto, ContextPtr)
{
    auto base_output_header = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto id = proto.id();
    auto output_columns = deserializeMapFromProto<String, String>(proto.output_columns());
    auto has_filter = proto.has_filter();
    auto step = std::make_shared<CTERefStepExt>(base_output_header, id, output_columns, has_filter);

    return step;
}

}
