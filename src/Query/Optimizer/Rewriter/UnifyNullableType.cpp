#include <Query/Optimizer/Rewriter/UnifyNullableType.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Query/Analyzer/TypeAnalyzer.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Query/Optimizer/SymbolsExtractor.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/MergingAggregatedStepExt.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>
#include <Query/Interpreters/JoinUtilsExt.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

bool UnifyNullableType::rewrite(QueryPlanExt & plan, ContextMutablePtr context) const
{
    UnifyNullableVisitor visitor{plan.getCTEInfo(), plan.getPlanNode()};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, context);
    plan.update(result);
    return true;
}

PlanNodePtr UnifyNullableVisitor::visitPlanNode(PlanNodeBase & node, ContextMutablePtr & context)
{
    auto optimizer_context = context->getOptimizerContext();
    switch (node.getType())
    {
#define VISITOR_DEF(TYPE) \
case QueryPlanStepType::TYPE: { \
return visit##TYPE##NodeImpl(dynamic_cast<TYPE##Node &>(node), context); \
}
        APPLY_UNIFY_NULLABLE_TYPE_REWRITER_QUERY_PLAN_STEP(VISITOR_DEF)

#undef VISITOR_DEF
        default: {
            PlanNodes new_children;
            DataStreams new_inputs;

            for (auto & i : node.getChildren())
            {
                PlanNodePtr rewritten_child = VisitorUtil::accept(*i, *this, context);
                new_children.emplace_back(rewritten_child);
                new_inputs.push_back(rewritten_child->getStep()->getOutputStream());
            }

            QueryPlanStepPtr step = node.getStep();
            if (step->canUpdateInputStream())
                step->updateInputStreams(new_inputs);

            return PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(step), new_children/*, node.getStatistics()*/);

        }
    }
}

PlanNodePtr UnifyNullableVisitor::visitProjectionStepExtNodeImpl(ProjectionStepExtNode & node, ContextMutablePtr & context)
{
    PlanNodePtr child = VisitorUtil::accept(node.getChildren()[0], *this, context);
    const auto & step = *node.getStep();
    const auto & assignments = step.getAssignments();
    NameToType set_nullable;
    const auto & input_header = child->getStep()->getOutputStream().header;
    auto type_analyzer = TypeAnalyzer::create(context, input_header.getNamesAndTypes());
    for (auto & assignment : assignments)
    {
        String name = assignment.first;
        ConstASTPtr value = assignment.second;
        DataTypePtr type = type_analyzer.getType(value);
        set_nullable[name] = type;
    }

    auto expression_step = std::make_shared<ProjectionStepExt>(
        child->getStep()->getOutputStream(), assignments, set_nullable, step.isFinalProject(), step.isIndexProject());
    return ProjectionStepExtNode::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(expression_step), PlanNodes{child}/*, node.getStatistics()*/);
}

PlanNodePtr UnifyNullableVisitor::visitJoinStepExtNodeImpl(JoinStepExtNode & node, ContextMutablePtr & context)
{
    PlanNodes children;
    DataStreams inputs;
    for (const auto & item : node.getChildren())
    {
        PlanNodePtr child = VisitorUtil::accept(*item, *this, context);
        children.emplace_back(child);
        inputs.push_back(child->getStep()->getOutputStream());
    }

    const auto & join_step = *node.getStep();

    const DataStreams & input_stream = inputs;
    const DataStream & output_stream = join_step.getOutputStream();

    auto output_set_null = output_stream.header.getNamesAndTypes();
    std::unordered_map<String, DataTypePtr> left_name_to_type;
    std::unordered_map<String, DataTypePtr> right_name_to_type;
    for (const auto & left : input_stream[0].header)
    {
        left_name_to_type[left.name] = left.type;
    }
    for (const auto & right : input_stream[1].header)
    {
        right_name_to_type[right.name] = right.type;
    }

    bool make_nullable_for_left = isRightOrFull(join_step.getKind());
    bool make_nullable_for_right = isLeftOrFull(join_step.getKind());

    auto update_symbol_type = [&output_set_null](const std::unordered_map<String, DataTypePtr> & type_provider, bool make_nullable) {
        std::transform(
            output_set_null.begin(),
            output_set_null.end(),
            output_set_null.begin(),
            [&type_provider, &make_nullable](const NameAndTypePair & symbol) -> NameAndTypePair {
                if (!type_provider.contains(symbol.name))
                    return symbol;

                const auto & type = type_provider.at(symbol.name);
                if (make_nullable && JoinCommon::canBecomeNullable(type))
                    return {symbol.name, JoinCommon::convertTypeToNullable(type_provider.at(symbol.name))};
                else
                    return {symbol.name, type_provider.at(symbol.name)};
            });
    };

    update_symbol_type(left_name_to_type, make_nullable_for_left);
    update_symbol_type(right_name_to_type, make_nullable_for_right);

    ColumnsWithTypeAndName data;
    for (const auto & item : output_set_null)
    {
        data.emplace_back(item.type, item.name);
    }

    DataStream output_stream_set_null = DataStream{.header = data};
    auto join_step_set_null = std::make_shared<JoinStepExt>(
        inputs,
        output_stream_set_null,
        join_step.getKind(),
        join_step.getStrictness(),
        join_step.getMaxStreams(),
        join_step.getKeepLeftReadInOrder(),
        join_step.getLeftKeys(),
        join_step.getRightKeys(),
        join_step.getKeyIdsNullSafe(),
        join_step.getFilter(),
        join_step.isHasUsing(),
        join_step.getRequireRightKeys(),
        join_step.getAsofInequality(),
        join_step.getDistributionType(),
        join_step.getJoinAlgorithm(),
        join_step.isMagic(),
        join_step.isOrdered(),
        join_step.isSimpleReordered(),
        join_step.getRuntimeFilterBuilders());
    return JoinStepExtNode::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(join_step_set_null), children/*, node.getStatistics()*/);
}

PlanNodePtr UnifyNullableVisitor::visitAggregatingStepExtNodeImpl(AggregatingStepExtNode & node, ContextMutablePtr & context)
{
    PlanNodePtr child = VisitorUtil::accept(node.getChildren()[0], *this, context);

    const auto & step = *node.getStep();

    const AggregateDescriptions & descs = step.getAggregates();
    AggregateDescriptions descs_set_nullable;

    auto input_columns = child->getStep()->getOutputStream().header;
    for (const auto & desc : descs)
    {
        AggregateDescription desc_with_null;
        AggregateFunctionPtr fun = desc.function;
        Names argument_names = desc.argument_names;
        DataTypes types;
        for (auto & argument_name : argument_names)
        {
            for (auto & column : input_columns)
            {
                if (argument_name == column.name)
                {
                    types.emplace_back(recursiveRemoveLowCardinality(column.type));
                    break;
                }
            }
        }
        String fun_name = fun->getName();
        AggregateFunctionPtr fun_with_null = desc.function;
        // tmp fix: For AggregateFunctionNothing, the argument types may diff with
        // the ones in `descr.function->argument_types`. In this case, reconstructing aggregate description will lead
        // to a different result.
        //
        // see also similar fix in AggregatingStep.cpp
        if (fun_name != "nothing")
        {
            AggregateFunctionProperties properties;
            fun_with_null = AggregateFunctionFactory::instance().get(fun_name, NullsAction::EMPTY, types, desc.parameters, properties);
        }
        desc_with_null.function = fun_with_null;
        desc_with_null.parameters = desc.parameters;
        desc_with_null.column_name = desc.column_name;
        desc_with_null.argument_names = argument_names;
        desc_with_null.parameters = desc.parameters;
        desc_with_null.arguments = desc.arguments;
        desc_with_null.mask_column = desc.mask_column;

        descs_set_nullable.emplace_back(desc_with_null);
    }

    auto agg_step_set_null = std::make_shared<AggregatingStepExt>(
        child->getStep()->getOutputStream(),
        step.getKeys(),
        step.getKeysNotHashed(),
        descs_set_nullable,
        step.getGroupingSetsParams(),
        step.isFinal(),
        step.getStagePolicy(),
        step.getGroupBySortDescription(),
        step.getGroupings(),
        step.needOverflowRow(),
        step.shouldProduceResultsInOrderOfBucketNumber(),
        step.isNoShuffle(),
        step.isStreamingForCache());
    auto agg_node_set_null
        = AggregatingStepExtNode::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(agg_step_set_null), PlanNodes{child}/*, node.getStatistics()*/);

    return agg_node_set_null;
}

PlanNodePtr UnifyNullableVisitor::visitMergingAggregatedStepExtNodeImpl(MergingAggregatedStepExtNode & node, ContextMutablePtr & context)
{
    PlanNodePtr child = VisitorUtil::accept(node.getChildren()[0], *this, context);

    const auto & step = *node.getStep();

    const AggregateDescriptions & descs = step.getAggregates();
    AggregateDescriptions descs_set_nullable;

    auto input_columns = child->getStep()->getOutputStream().header;
    for (const auto & desc : descs)
    {
        AggregateDescription desc_with_null;
        AggregateFunctionPtr fun = desc.function;

        // get type from AggregateFunction(...);
        auto argument_types = [&] {
            auto argument_name = desc.column_name;
            auto column = input_columns.getByName(argument_name, false);
            auto partial_type = typeid_cast<std::shared_ptr<const DataTypeAggregateFunction>>(column.type);
            if (!partial_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected merge agg input type");

            return partial_type->getArgumentsDataTypes();
        }();

        String fun_name = fun->getName();
        AggregateFunctionPtr fun_with_null = desc.function;
        // tmp fix: For AggregateFunctionNothing, the argument types may diff with
        // the ones in `descr.function->argument_types`. In this case, reconstructing aggregate description will lead
        // to a different result.
        //
        // see also similar fix in AggregatingStep.cpp
        if (fun_name != "nothing")
        {
            AggregateFunctionProperties properties;
            fun_with_null = AggregateFunctionFactory::instance().get(fun_name, NullsAction::EMPTY, argument_types, desc.parameters, properties);
        }
        desc_with_null.function = fun_with_null;
        desc_with_null.parameters = desc.parameters;
        desc_with_null.column_name = desc.column_name;
        desc_with_null.argument_names = desc.argument_names;
        desc_with_null.parameters = desc.parameters;
        desc_with_null.arguments = desc.arguments;
        desc_with_null.mask_column = desc.mask_column;

        descs_set_nullable.emplace_back(desc_with_null);
    }

    const auto & agg_params = step.getParams();

    Aggregator::Params new_agg_params{
        step.getKeys(), descs_set_nullable, agg_params.overflow_row, agg_params.max_threads, agg_params.max_block_size, agg_params.min_hit_rate_to_use_consecutive_keys_optimization};

    auto merge_agg_step_set_null = std::make_shared<MergingAggregatedStepExt>(
        child->getStep()->getOutputStream(),
        step.getGroupingSetsParamsList(),
        step.getGroupings(),
        step.isFinal(),
        new_agg_params,
        step.isMemoryEfficientAggregation(),
        step.getMaxThreads(),
        step.getMemoryEfficientMergeThreads(),
        step.getMaxBlockSize(),
        step.getMemoryBoundMergingMaxBlockBytes(),
        step.getGroupBySortDescription(),
        step.getMemoryBoundMergingOfAggregationResultsEnabled());

    auto merge_agg_node_set_null = MergingAggregatedStepExtNode::createPlanNode(
        context->getOptimizerContext()->nextNodeId(), std::move(merge_agg_step_set_null), PlanNodes{child}/*, node.getStatistics()*/);

    return merge_agg_node_set_null;
}

PlanNodePtr UnifyNullableVisitor::visitUnionStepExtNodeImpl(UnionStepExtNode & node, ContextMutablePtr & context)
{
    const auto & step = *node.getStep();
    PlanNodes new_children;
    DataStreams new_inputs;
    NamesAndTypes new_output = step.getOutputStream().header.getNamesAndTypes();

    auto update_output_data_type = [&step](NamesAndTypes & output_symbols, const NamesAndTypes & new_input_symbols, int child_id) {
        for (auto & output : output_symbols)
        {
            const auto & input_name = step.getOutToInputs().at(output.name)[child_id];
            DataTypePtr input_type = nullptr;

            for (const auto & item : new_input_symbols)
            {
                if (item.name == input_name)
                {
                    input_type = item.type;
                    break;
                }
            }

            if (isNullableOrLowCardinalityNullable(input_type) && !isNullableOrLowCardinalityNullable(output.type))
            {
                output.type = JoinCommon::tryConvertTypeToNullable(output.type);
            }
        }
    };

    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        PlanNodePtr rewritten_child = VisitorUtil::accept(*node.getChildren()[i], *this, context);
        new_children.emplace_back(rewritten_child);
        new_inputs.push_back(rewritten_child->getStep()->getOutputStream());
        update_output_data_type(new_output, rewritten_child->getStep()->getOutputStream().header.getNamesAndTypes(), i);
    }

    Block new_output_header;
    for (const auto & item : new_output)
    {
        new_output_header.insert(ColumnWithTypeAndName{item.type, item.name});
    }

    // add cast projection, make Union's input stream/output stream type Nullable consistent.
    PlanNodes children_add_nullable;
    for (size_t i = 0; i < new_children.size(); i++)
    {
        Assignments add_cast;
        NameToType name_to_type;

        bool need_add_cast_projection = false;
        for (auto const & value : step.getOutToInputs())
        {
            auto output_name = value.first;
            auto output_type = new_output_header.getByName(output_name).type;

            auto input_name = value.second[i];
            auto input_type = new_children[i]->getOutputNamesToTypes().at(input_name);

            if (isNullableOrLowCardinalityNullable(output_type) && !isNullableOrLowCardinalityNullable(input_type))
            {
                need_add_cast_projection = true;
                input_type = JoinCommon::tryConvertTypeToNullable(input_type);
                Assignment assignment{
                    input_name,
                    makeASTFunction(
                        "cast", std::make_shared<ASTIdentifier>(input_name), std::make_shared<ASTLiteral>(input_type->getName()))};
                add_cast.emplace_back(assignment);
                name_to_type[input_name] = input_type;
            }
            else
            {
                Assignment assignment{input_name, std::make_shared<ASTIdentifier>(input_name)};
                add_cast.emplace_back(assignment);
                name_to_type[input_name] = input_type;
            }
        }

        if (need_add_cast_projection)
        {
            auto add_cast_step = std::make_shared<ProjectionStepExt>(new_children[i]->getStep()->getOutputStream(), add_cast, name_to_type);
            auto add_cast_node
                = std::make_shared<ProjectionStepExtNode>(context->getOptimizerContext()->nextNodeId(), std::move(add_cast_step), PlanNodes{new_children[i]});
            children_add_nullable.emplace_back(add_cast_node);
        }
        else
        {
            children_add_nullable.emplace_back(new_children[i]);
        }
    }

    DataStreams new_inputs_add_cast;
    for (auto & i : children_add_nullable)
    {
        new_inputs_add_cast.push_back(i->getStep()->getOutputStream());
    }

    auto rewritten_step = std::make_unique<UnionStepExt>(
        new_inputs_add_cast, DataStream{new_output_header}, step.getOutToInputs(), step.getMaxThreads(), step.isLocal());
    auto rewritten_node
        = UnionStepExtNode::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(rewritten_step), children_add_nullable/*, node.getStatistics()*/);
    return rewritten_node;
}

PlanNodePtr UnifyNullableVisitor::visitExchangeStepExtNodeImpl(ExchangeStepExtNode & node, ContextMutablePtr & context)
{
    const auto & step = *node.getStep();

    PlanNodes children;
    DataStreams inputs;
    for (auto & item : node.getChildren())
    {
        PlanNodePtr child = VisitorUtil::accept(*item, *this, context);
        children.emplace_back(child);
        inputs.emplace_back(child->getStep()->getOutputStream());
    }

    // update it's input/output stream types.
    auto exchange_step_set_null = std::make_unique<ExchangeStepExt>(inputs, step.getExchangeMode(), step.getSchema(), step.needKeepOrder());
    auto exchange_node_set_null
        = ExchangeStepExtNode::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(exchange_step_set_null), children/*, node.getStatistics()*/);
    return exchange_node_set_null;
}

PlanNodePtr UnifyNullableVisitor::visitCTERefStepExtNodeImpl(CTERefStepExtNode & node, ContextMutablePtr & context)
{
    auto cte_step = node.getStep();
    auto cte_id = cte_step->getId();
    auto cte_plan = cte_helper.acceptAndUpdate(cte_id, *this, context);

    const auto & cte_output_stream = cte_plan->getStep()->getOutputStream().header;

    DataStream output_stream;
    for (const auto & output : cte_step->getOutputColumns())
        output_stream.header.insert(ColumnWithTypeAndName{cte_output_stream.getByName(output.second).type, output.first});

    auto cte_ref_step = std::make_unique<CTERefStepExt>(output_stream, cte_step->getId(), cte_step->getOutputColumns(), cte_step->hasFilter());
    auto cte_ref_node = CTERefStepExtNode::createPlanNode(context->getOptimizerContext()->nextNodeId(), std::move(cte_ref_step), PlanNodes{}/*, node.getStatistics()*/);
    return cte_ref_node;
}
}
