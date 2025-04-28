#include <algorithm>
#include <Query/Optimizer/Utils.h>
#include <Query/Optimizer/Property/ConstantsDeriver.h>
#include <Query/Common/SymbolsExtractor.h>
#include <Query/Common/PredicateUtils.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

namespace DB
{

Constants ConstantsDeriver::deriveConstants(QueryPlanStepPtr step, CTEInfo & cte_info, ContextMutablePtr & context)
{
    ConstantsSet constants_set;
    return deriveConstants(step, constants_set, cte_info, context);
}

Constants ConstantsDeriver::deriveConstants(QueryPlanStepPtr step, Constants & input, CTEInfo & cte_info, ContextMutablePtr & context)
{
    ConstantsSet input_constants;
    input_constants.emplace_back(input);
    return deriveConstants(step, input_constants, cte_info, context);
}

Constants
ConstantsDeriver::deriveConstants(QueryPlanStepPtr step, ConstantsSet & input_constants, CTEInfo & cte_info, ContextMutablePtr & context)
{
    ConstantsDeriverContext deriver_context{input_constants, cte_info, context};
    static ConstantsDeriverVisitor visitor{};
    return VisitorUtil::accept(step, visitor, deriver_context);
}

Constants ConstantsDeriver::deriveConstantsFromTree(PlanNodePtr node, CTEInfo & cte_info, ContextMutablePtr & context)
{
    ConstantsDeriverTreeVisitorContext tree_context{cte_info, context};
    ConstantsDeriverTreeVisitor visitor{};
    return VisitorUtil::accept(node, visitor, tree_context);
}

Constants ConstantsDeriverVisitor::visitStep(const IQueryPlanStep &, ConstantsDeriverContext & context)
{
    return context.getInput()[0];
}

Constants ConstantsDeriverVisitor::visitFilterStepExt(const FilterStepExt & step, ConstantsDeriverContext & context)
{
    // todo: lizhuoyu5, need optimizer
    // Predicate::DomainTranslator<String> translator{context.getContext()};
    // // TODO, remove clone. step.getFilter()->clone()
    // Predicate::ExtractionResult<String> result
    //     = translator.getExtractionResult(step.getFilter()->clone(), step.getOutputStream().header.getNamesAndTypes());
    // auto values = result.tuple_domain.extractFixedValues();
    std::map<String, FieldWithType> filter_values;
    const Constants & origin_constants = context.getInput()[0];
    for (const auto & value : origin_constants.getValues())
    {
        filter_values[value.first] = value.second;
    }
    // if (values)
    // {
    //     for (auto & value : values.value())
    //     {
    //         filter_values[value.first] = value.second;
    //     }
    // }

    for (const auto & conjunct : PredicateUtils::extractConjuncts(step.getFilter()->clone()))
    {
        const auto * func = conjunct->as<ASTFunction>();
        if (!func || func->name != "equals")
            continue;
        const auto * column = func->arguments->children[0]->as<ASTIdentifier>();
        if (!column)
            continue;
    }
    return Constants{filter_values};
}

Constants ConstantsDeriverVisitor::visitJoinStepExt(const JoinStepExt & step, ConstantsDeriverContext & context)
{
    std::unordered_map<String, String> identities;
    for (const auto & item : step.getOutputStream().header)
    {
        identities[item.name] = item.name;
    }

    Constants translated;

    if (step.getKind() == JoinKind::Inner || step.getKind() == JoinKind::Cross)
    {
        Constants left_constants = context.getInput()[0].translate(identities);
        Constants right_constants = context.getInput()[1].translate(identities);

        std::map<String, FieldWithType> filter_values;
        for (const auto & value : left_constants.getValues())
        {
            filter_values[value.first] = value.second;
        }
        for (const auto & value : right_constants.getValues())
        {
            filter_values[value.first] = value.second;
        }
        translated = Constants{filter_values};
    }

    return translated;
}

Constants ConstantsDeriverVisitor::visitProjectionStepExt(const ProjectionStepExt & step, ConstantsDeriverContext & context)
{
    // TODO@lijinzhi.zx: Extract constants from ASTIdentifer to ASTLiteral assigments.
    const auto & assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    std::unordered_map<String, String> revert_identifies;

    for (auto & item : identities)
    {
        revert_identifies[item.second] = item.first;
    }
    Constants translated;
    if (!context.getInput().empty())
    {
        translated = context.getInput()[0].translate(revert_identifies);
    }
    return translated;
}

Constants ConstantsDeriverVisitor::visitAggregatingStepExt(const AggregatingStepExt & step, ConstantsDeriverContext & context)
{
    if (step.getKeys().empty())
        return {};
    return context.getInput()[0];
}

Constants ConstantsDeriverVisitor::visitMarkDistinctStepExt(const MarkDistinctStepExt & step, ConstantsDeriverContext & context)
{
    if (step.getDistinctSymbols().empty())
        return {};
    return context.getInput()[0];
}

Constants ConstantsDeriverVisitor::visitUnionStepExt(const UnionStepExt & step, ConstantsDeriverContext & context)
{
    std::vector<Constants> transformed_children_constants;
    const auto & output_to_inputs = step.getOutToInputs();
    size_t index = 0;
    for (const auto & child_prop : context.getInput())
    {
        NameToNameMap mapping;
        for (const auto & output_to_input : output_to_inputs)
        {
            mapping[output_to_input.second[index]] = output_to_input.first;
        }
        index++;
        transformed_children_constants.emplace_back(child_prop.translate(mapping));
    }

    std::map<String, FieldWithType> filter_values;
    // If children all have a same constant, create a new constant in the union.
    for (const auto & [name_after_union, _] : output_to_inputs)
    {
        bool is_all_child_constants_same = true;
        const auto & first_constants = transformed_children_constants.at(0).getValues();
        if (!first_constants.contains(name_after_union))
            continue;
        FieldWithType first_field = first_constants.at(name_after_union);
        for (size_t i = 1; i < transformed_children_constants.size(); i++)
        {
            const auto & child_constants = transformed_children_constants.at(i).getValues();
            if (!child_constants.contains(name_after_union) || first_field != child_constants.at(name_after_union))
            {
                is_all_child_constants_same = false;
            }
        }
        if (is_all_child_constants_same)
            filter_values.emplace(name_after_union, first_field);
    }

    return Constants{filter_values};
}

Constants ConstantsDeriverVisitor::visitTableScanStepExt(const TableScanStepExt & step, ConstantsDeriverContext & /*context*/)
{
    std::map<String, FieldWithType> constants;
    auto storage_snapshot = step.getStorageSnapshot();
    GetColumnsOptions get_subcolumn_option = GetColumnsOptions::Ordinary;
    get_subcolumn_option.withExtendedObjects().withSubcolumns();

    for (const auto & [column, symbol] : step.getColumnAlias())
    {
        if (!storage_snapshot->tryGetColumn(get_subcolumn_option, column))
        {
            auto pos = column.find('.');
            if (pos != std::string::npos && pos + 1 < column.length())
            {
                const auto base_name = column.substr(0, pos);
                const auto column_in_storage = storage_snapshot->tryGetColumn(GetColumnsOptions::Ordinary, base_name);
                const auto parent_name = column.substr(0, column.rfind('.'));
                const auto parent_column = storage_snapshot->tryGetColumn(get_subcolumn_option, parent_name);

                if (column_in_storage && (!parent_column || !!dynamic_cast<const DataTypeTuple *>(parent_column->type.get())))
                {
                    // nonexists subcolumn will be always NULL
                    constants.emplace(symbol, FieldWithType{std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>()), Null{}});
                }
            }
        }
    }

    return Constants{constants};
}

Constants ConstantsDeriverVisitor::visitReadNothingStep(const ReadNothingStep &, ConstantsDeriverContext &)
{
    return {};
}

Constants ConstantsDeriverVisitor::visitReadStorageRowCountStepExt(const ReadStorageRowCountStepExt &, ConstantsDeriverContext &)
{
    return {};
}

Constants ConstantsDeriverVisitor::visitValuesStepExt(const ValuesStepExt &, ConstantsDeriverContext &)
{
    // TODO@lijinzhi.zx: Each column_name to field in ValuesStep is an identifer to literal constant.
    return {};
}

Constants ConstantsDeriverVisitor::visitCTERefStepExt(const CTERefStepExt & step, ConstantsDeriverContext & context)
{
    if (context.getInput().empty())
        return Constants{};

    std::unordered_map<String, String> revert_identifies;
    for (const auto & item : step.getOutputColumns())
    {
        revert_identifies[item.second] = item.first;
    }

    return context.getInput()[0].translate(revert_identifies);
}

Constants ConstantsDeriverTreeVisitor::visitPlanNode(PlanNodeBase & node, ConstantsDeriverTreeVisitorContext & ctx)
{
    ConstantsSet input_constants;

    for (const auto & child : node.getChildren())
        input_constants.emplace_back(VisitorUtil::accept(child, *this, ctx));

    return ConstantsDeriver::deriveConstants(node.getStep(), input_constants, ctx.cte_info, ctx.context);
}
}
