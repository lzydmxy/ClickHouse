#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Parsers/ASTIdentifier.h>

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

    //todo: need convert from inputs to DataStream
    //return std::make_shared<ProjectionStepExt>(DataStream{inputs}, assignments, name_to_type);
    return std::make_shared<ProjectionStepExt>(DataStream{}, assignments, name_to_type);
}

//need to add CTEInfo
/*
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "output_stream symbol not found in cte def: " + it->second);
            assignments.emplace_back(item.name, std::make_shared<ASTIdentifier>(new_symbol->second));
            name_to_type.emplace(item.name, item.type);
            inputs.emplace_back(NameAndTypePair{it->second, item.type});
        }
    }
    return PlanNodeBase::createPlanNode(
        context->nextNodeId(), std::make_shared<ProjectionStep>(DataStream{inputs}, assignments, name_to_type), {rewrite.plan_node});
}
*/

std::unordered_map<String, String> CTERefStepExt::getReverseOutputColumns() const
{
    std::unordered_map<String, String> reverse;
    for (const auto & item : output_columns)
        reverse.emplace(item.second, item.first);
    return reverse;
}
}
