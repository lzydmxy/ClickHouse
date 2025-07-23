#include <Query/Optimizer/Rule/Rewrite/PushPartialStepThroughExchangeRules.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/AggregateDescription.h>
#include <Query/Optimizer/ExpressionDeterminism.h>
#include <Query/Optimizer/PlanNodeCardinality.h>
#include <Query/Optimizer/ProjectionPlanner.h>
#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Property/PropertyDeriver.h>
#include <Query/Optimizer/Property/PropertyMatcher.h>
#include <Query/Optimizer/Rule/Pattern.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Optimizer/SymbolUtils.h>
#include <Query/Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Processors/Transforms/AggregatingTransformExt.h>
#include <Query/Processors/QueryPlan/Assignment.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Query/Processors/QueryPlan/SortingStepExt.h>
#include <Query/Planner/SymbolMapper.h>
#include <Poco/String.h>
#include <Poco/StringTokenizer.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>

namespace DB
{

NameSet PushPartialAggThroughExchange::BLOCK_AGGS{
    "pathcount",
    "attributionanalysis",
    "attributioncorrelationfuse",
    "attribution",
    "attributioncorrelation",
    "bitmapjoinandcard",
    "bitmapjoinandcard2",
    "bitmapjoin",
    "bitmapcount",
    "bitmapextract",
    "bitmapmulticount",
    "bitmapmulticountwithdate",
    "bitmapmaxlevel",
    "bitmapcolumndiff"};

static std::pair<bool, bool> canPushPartialWithHint(const AggregatingStepExt * step)
{
    // todo: zhangwanyun1, other feat: support hint
    // const auto & hint_list = step->getHints();
    // for (const auto & hint : hint_list)
    // {
    //     if (hint->getType() == HintCategory::PUSH_PARTIAL_AGG)
    //     {
    //         if (auto enable_hint = std::dynamic_pointer_cast<EnablePushPartialAgg>(hint))
    //             return {true, true};
    //         else if (auto disable_hint = std::dynamic_pointer_cast<DisablePushPartialAgg>(hint))
    //             return {true, false};
    //     }
    // }
    return {false, true};
}

ConstRefPatternPtr PushPartialAggThroughExchange::getPattern() const
{
    static auto pattern = Patterns::aggregating()
        .matchingStep<AggregatingStepExt>([](const AggregatingStepExt & step) { return step.getStagePolicy() != AggregateStagePolicy::MERGE; })
        .withSingle(Patterns::exchange()).result();
    return pattern;
}

std::set<String> splitToStateMerge(const AggregatingStepExt * step, PlanNodePtr exchange_child, RuleContext & context)
{
    if (!context.context->getOptimizerContext()->getSettingsRef().enable_split_countd_to_state_merge)
        return {};

    bool has_distinct = false;


    NameSet distinct_names{"uniqexact", "countdistinct"};
    for (const auto & agg : step->getAggregates())
    {
        has_distinct |= distinct_names.contains(Poco::toLower(agg.function->getName()));
    }

    std::set<String> result;
    if (has_distinct)
    {
        auto actual = PropertyDeriver::deriveProperty(exchange_child, context.context, context.cte_info, true);
        for (const auto & agg : step->getAggregates())
        {
            if (distinct_names.contains(Poco::toLower(agg.function->getName())))
            {
                Partitioning require{Partitioning::Handle::FIXED_HASH, agg.argument_names};
                if (PropertyMatcher::matchNodePartitioning(*context.context, require, actual.getNodePartitioning()))
                {
                    result.insert(agg.column_name);
                }
            }
        }
    }
    return result;
}

TransformResult split(const PlanNodePtr & node, RuleContext & context)
{
    const auto * step = dynamic_cast<const AggregatingStepExt *>(node->getStep().get());

    auto match_node_prop_agg_results = splitToStateMerge(step, node->getChildren()[0]->getChildren()[0], context);
    if (!match_node_prop_agg_results.empty())
    {
        std::map<String, AggregateFunctionPtr> name_function;
        AggregateDescriptions state_aggs;
        ASTs assignments;
        NameToNameMap agg_result_to_state;
        for (const auto & agg : step->getAggregates())
        {
            auto state_agg = agg;
            AggregateFunctionProperties properties;
            DataTypes types;
            for (const auto & name : agg.argument_names)
            {
                types.push_back(step->getInputStreams()[0].header.getByName(name).type);
            }
            state_agg.function = AggregateFunctionFactory::instance().get(
                state_agg.function->getName() + "State", NullsAction::EMPTY, types, state_agg.parameters, properties);
            state_agg.column_name += "State";
            state_aggs.emplace_back(state_agg);

            if (match_node_prop_agg_results.contains(agg.column_name))
            {
                auto init_name = context.context->getOptimizerContext()->getSymbolAllocator()->newSymbol(state_agg.column_name);
                assignments.emplace_back(makeASTFunction(
                    "initializeAggregation",
                    std::make_shared<ASTLiteral>("sumState"),
                    makeASTFunction("finalizeAggregation", std::make_shared<ASTIdentifier>(state_agg.column_name))));
            }
            else
            {
                assignments.emplace_back(std::make_shared<ASTIdentifier>(state_agg.column_name));
            }
        }

        auto state_agg = std::make_shared<AggregatingStepExt>(
            node->getChildren()[0]->getStep()->getOutputStream(),
            step->getKeys(),
            step->getKeysNotHashed(),
            state_aggs,
            step->getGroupingSetsParams(),
            true,
            AggregateStagePolicy::STATE,
            step->getGroupBySortDescription(),
            step->getGroupings(),
            step->needOverflowRow(),
            false);

        auto state_agg_node = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), state_agg, node->getChildren());
        ProjectionPlanner projection_planner(state_agg_node, context.context);
        size_t index = 0;
        for (const auto & ast : assignments)
        {
            auto [state_name, _] = projection_planner.addColumn(ast);
            agg_result_to_state[step->getAggregates()[index].column_name] = state_name;
            ++index;
        }
        auto state_projection_node = projection_planner.build();


        AggregateDescriptions merge_aggs;
        for (const auto & agg : step->getAggregates())
        {
            auto merge_agg = agg;
            AggregateFunctionProperties properties;
            merge_agg.argument_names = {agg_result_to_state[agg.column_name]};
            DataTypes types{state_projection_node->getCurrentDataStream().header.getByName(agg_result_to_state[agg.column_name]).type};

            if (match_node_prop_agg_results.contains(agg.column_name))
            {
                merge_agg.function = AggregateFunctionFactory::instance().get("sumMerge", NullsAction::EMPTY, types, merge_agg.parameters, properties);
            }
            else
            {
                merge_agg.function = AggregateFunctionFactory::instance().get(
                    merge_agg.function->getName() + "Merge", NullsAction::EMPTY, types, merge_agg.parameters, properties);
            }
            merge_aggs.emplace_back(merge_agg);
        }

        auto merge_agg = std::make_shared<AggregatingStepExt>(
            state_projection_node->getCurrentDataStream(),
            step->getKeys(),
            step->getKeysNotHashed(),
            merge_aggs,
            step->getGroupingSetsParams(),
            true,
            AggregateStagePolicy::MERGE,
            step->getGroupBySortDescription(),
            step->getGroupings(),
            step->needOverflowRow(),
            false);

        return PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), merge_agg, {state_projection_node});
    }

    QueryPlanStepPtr partial_agg = std::make_shared<AggregatingStepExt>(
        node->getChildren()[0]->getStep()->getOutputStream(),
        step->getKeys(),
        step->getKeysNotHashed(),
        step->getAggregates(),
        step->getGroupingSetsParams(),
        false,
        AggregateStagePolicy::DEFAULT,
        step->getGroupBySortDescription(),
        step->getGroupings(),
        step->needOverflowRow(),
        false,
        step->isNoShuffle(),
        step->isStreamingForCache());

    auto partial_agg_node
        = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(partial_agg), node->getChildren(), node->getStatistics());

    Names keys;
    if (!step->getGroupingSetsParams().empty())
        keys.push_back("__grouping_set");
    keys.insert(keys.end(), step->getKeys().begin(), step->getKeys().end());

    ColumnNumbers keys_positions;
    auto exchange_header = partial_agg_node->getStep()->getOutputStream().header;

    for (const auto & key : keys)
    {
        keys_positions.emplace_back(exchange_header.getPositionByName(key));
    }

    const auto & agg_params = step->getParams();
    Aggregator::Params new_params(
        std::move(keys),
        agg_params.aggregates,
        agg_params.overflow_row,
        agg_params.max_threads,
        agg_params.max_block_size,
        agg_params.min_hit_rate_to_use_consecutive_keys_optimization);

    QueryPlanStepPtr final_agg = std::make_shared<MergingAggregatedStepExt>(
        partial_agg_node->getStep()->getOutputStream(),
        step->getGroupingSetsParams(),
        step->getGroupings(),
        step->isFinal(),
        new_params,
        false,
        context.context->getSettingsRef().max_threads,
        context.context->getSettingsRef().aggregation_memory_efficient_merge_threads,
        step->getMaxBlockSize(),
        context.context->getSettingsRef().aggregation_in_order_max_block_bytes,
        SortDescription{},
        context.context->getSettingsRef().enable_memory_bound_merging_of_aggregation_results);
    auto final_agg_node
        = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(final_agg), {partial_agg_node}, node->getStatistics());
    return final_agg_node;
}

PlanNodePtr createPartial(const AggregatingStepExt * step, PlanNodePtr child, NameToNameMap & map, Context & context)
{
    auto symbol_mapper = SymbolMapper::simpleMapper(map);
    auto agg_step = symbol_mapper.map(*step);
    auto mapped_partial = PlanNodeBase::createPlanNode(context.getOptimizerContext()->nextNodeId(), agg_step, {std::move(child)});

    if (map.empty())
    {
        return mapped_partial;
    }

    Assignments assignments;
    NameToType name_to_type;
    bool is_identity = true;
    for (const auto & output : agg_step->getOutputStream().header)
    {
        auto input = symbol_mapper.map(output.name);
        assignments.emplace_back(output.name, std::make_shared<ASTIdentifier>(input));
        is_identity &= output.name == input;
        name_to_type[output.name] = output.type;
    }

    if (is_identity)
    {
        return mapped_partial;
    }
    return PlanNodeBase::createPlanNode(
        context.getOptimizerContext()->nextNodeId(), std::make_shared<ProjectionStepExt>(agg_step->getOutputStream(), assignments, name_to_type), {mapped_partial});
}

TransformResult pushPartial(const PlanNodePtr & node, RuleContext & context)
{
    const auto * step = dynamic_cast<const AggregatingStepExt *>(node->getStep().get());
    auto exchange_node = node->getChildren()[0];
    const auto * exchange_step = dynamic_cast<const ExchangeStepExt *>(exchange_node->getStep().get());

    PlanNodes partials;
    for (size_t index = 0; index < exchange_step->getInputStreams().size(); ++index)
    {
        NameToNameMap map;
        for (const auto & item : exchange_step->getOutToInputs())
        {
            if (item.first != item.second[index])
            {
                map[item.first] = item.second[index];
            }
        }

        auto projection = createPartial(step, exchange_node->getChildren()[index], map, *context.context);
        partials.emplace_back(projection);
    }

    DataStreams exchange_inputs;
    for (const auto & item : partials)
    {
        exchange_inputs.emplace_back(item->getCurrentDataStream());
    }

    return PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(),
        std::make_shared<ExchangeStepExt>(
            exchange_inputs, exchange_step->getExchangeMode(), exchange_step->getSchema(), exchange_step->needKeepOrder()),
        partials);
}

TransformResult PushPartialAggThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const AggregatingStepExt *>(node->getStep().get());
    auto [has_push_partial_hint, enable_push_partical_agg] = canPushPartialWithHint(step);
    if (has_push_partial_hint)
    {
        if (!enable_push_partical_agg)
            return {};
    }
    else if (!context.context->getOptimizerContext()->getSettingsRef().enable_push_partial_agg && !step->isGroupingSet())
        return {};

    for (const auto & agg : step->getAggregates())
    {
        if (BLOCK_AGGS.count(Poco::toLower(agg.function->getName())))
        {
            return {};
        }
    }

    if (!context.context->getOptimizerContext()->getSettingsRef().enable_push_partial_block_list.value.empty())
    {
        Poco::StringTokenizer tokenizer(context.context->getOptimizerContext()->getSettingsRef().enable_push_partial_block_list, ",");
        NameSet block_names;
        for (const auto & name : tokenizer)
        {
            block_names.emplace(name);
        }

        for (const auto & agg : step->getAggregates())
        {
            if (block_names.count(agg.function->getName()))
            {
                return {};
            }
        }
    }

    if (step->isFinal() && step->getStagePolicy() != AggregateStagePolicy::STATE)
        return split(node, context);
    else
        return pushPartial(node, context);
}

ConstRefPatternPtr PushPartialAggThroughUnion::getPattern() const
{
    static auto pattern = Patterns::aggregating()
        .matchingStep<AggregatingStepExt>([](const AggregatingStepExt & step) { return step.isPartial(); })
        .withSingle(Patterns::unionn()).result();
    return pattern;
}

TransformResult PushPartialAggThroughUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const AggregatingStepExt *>(node->getStep().get());
    auto union_node = node->getChildren()[0];
    auto [has_push_partial_hint, enable_push_partical_agg] = canPushPartialWithHint(step);
    if (has_push_partial_hint && !enable_push_partical_agg)
        return {};

    const auto * union_step = dynamic_cast<const UnionStepExt *>(union_node->getStep().get());

    PlanNodes partials;
    DataStreams union_inputs;
    for (size_t index = 0; index < union_step->getInputStreams().size(); ++index)
    {
        NameToNameMap map;
        for (const auto & item : union_step->getOutToInputs())
        {
            if (item.first != item.second[index])
            {
                map[item.first] = item.second[index];
            }
        }

        auto projection = createPartial(step, union_node->getChildren()[index], map, *context.context);

        partials.emplace_back(projection);
        union_inputs.emplace_back(projection->getCurrentDataStream());
    }

    NameToNameMap map;
    for (const auto & item : union_step->getOutToInputs())
    {
        map[item.second[0]] = item.first;
    }
    auto mapper = SymbolMapper::simpleMapper(map);

    DataStream output;
    for (const auto & item : union_inputs[0].header)
    {
        output.header.insert(ColumnWithTypeAndName{item.type, mapper.map(item.name)});
    }


    return PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(),
        std::make_shared<UnionStepExt>(union_inputs, output, OutputToInputs{}, union_step->getMaxThreads(), union_step->isLocal()),
        partials);
}

ConstRefPatternPtr PushProjectionThroughExchange::getPattern() const
{
    static auto pattern = Patterns::project().withSingle(Patterns::exchange()).result();
    return pattern;
}

TransformResult PushProjectionThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    const auto * step = dynamic_cast<const ProjectionStepExt *>(node->getStep().get());
    auto exchange_node = node->getChildren()[0];
    const auto * exchange_step = dynamic_cast<const ExchangeStepExt *>(exchange_node->getStep().get());

    if (exchange_node->getChildren().size() != 1)
    {
        return {};
    }

    // only push initializeAggregation projections
    bool has_init_state = false;
    for (const auto & assign : step->getAssignments())
    {
        if (const auto * func = assign.second->as<ASTFunction>())
        {
            has_init_state |= func->name == "initializeAggregation";
        }
    }

    if (!has_init_state)
    {
        return {};
    }

    for (const auto & item : exchange_step->getOutToInputs())
    {
        if (item.first != item.second[0])
        {
            return {};
        }
    }

    node->replaceChildren({exchange_node->getChildren()[0]});
    exchange_node->replaceChildren({node});
    return exchange_node;
}

ConstRefPatternPtr PushPartialSortingThroughExchange::getPattern() const
{
    static auto pattern
        = Patterns::sorting()
              .matchingStep<SortingStepExt>([](const SortingStepExt & step) { return step.getStage() == SortingStepExt::Stage::FULL; })
              .withSingle(Patterns::exchange().matchingStep<ExchangeStepExt>(
                  [](const ExchangeStepExt & step) { return step.getExchangeMode() == RExchangeMode::GATHER; }))
              .result();
    return pattern;
}

TransformResult PushPartialSortingThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const SortingStepExt *>(node->getStep().get());
    auto old_exchange_node = node->getChildren()[0];
    const auto * old_exchange_step = dynamic_cast<const ExchangeStepExt *>(old_exchange_node->getStep().get());

    PlanNodes exchange_children;
    for (size_t index = 0; index < old_exchange_node->getChildren().size(); index++)
    {
        auto exchange_child = old_exchange_node->getChildren()[index];
        if (dynamic_cast<SortingStepExtNode *>(exchange_child.get()))
        {
            return {};
        }

        SortDescription new_sort_desc;
        for (const auto & desc : step->getSortDescription())
        {
            auto new_desc = desc;
            const auto & out_to_inputs = old_exchange_step->getOutToInputs();
            if (!out_to_inputs.contains(desc.column_name) || out_to_inputs.at(desc.column_name).size() <= index)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "PushPartialSortingThroughExchange: Can not find {} in out_to_inputs.", desc.column_name);
            new_desc.column_name = old_exchange_step->getOutToInputs().at(desc.column_name).at(index);
            new_sort_desc.emplace_back(new_desc);
        }

        auto before_exchange_sort = std::make_unique<SortingStepExt>(
            exchange_child->getStep()->getOutputStream(), new_sort_desc, step->getLimit(), SortingStepExt::Stage::PARTIAL, SortDescription{});
        PlanNodes children{exchange_child};
        auto before_exchange_sort_node
            = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(before_exchange_sort), children, node->getStatistics());
        exchange_children.emplace_back(before_exchange_sort_node);
    }

    auto exchange_step = old_exchange_step->copy(context.context);
    dynamic_cast<ExchangeStepExt *>(exchange_step.get())->setKeepOrder(true);
    auto exchange_node = PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(), std::move(exchange_step), exchange_children, old_exchange_node->getStatistics());

    QueryPlanStepPtr final_sort = step->copy(context.context);
    dynamic_cast<SortingStepExt *>(final_sort.get())->setStage(SortingStepExt::Stage::MERGE);
    PlanNodes exchange{exchange_node};
    auto final_sort_node
        = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(final_sort), exchange, node->getStatistics());
    return final_sort_node;
}

ConstRefPatternPtr PushPartialSortingThroughUnion::getPattern() const
{
    static auto pattern
        = Patterns::sorting()
              .matchingStep<SortingStepExt>([](const SortingStepExt & step) { return step.getStage() == SortingStepExt::Stage::PARTIAL; })
              .withSingle(Patterns::unionn())
              .result();
    return pattern;
}

TransformResult PushPartialSortingThroughUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const SortingStepExt *>(node->getStep().get());
    auto union_node = node->getChildren()[0];
    const auto * union_step = dynamic_cast<const UnionStepExt *>(union_node->getStep().get());

    PlanNodes union_inputs;
    for (size_t index = 0; index < union_node->getChildren().size(); index++)
    {
        auto exchange_child = union_node->getChildren()[index];
        if (dynamic_cast<SortingStepExtNode *>(exchange_child.get()))
            return {};

        SortDescription new_sort_desc;
        for (const auto & desc : step->getSortDescription())
        {
            auto new_desc = desc;
            const auto & out_to_inputs = union_step->getOutToInputs();
            if (!out_to_inputs.contains(desc.column_name) || out_to_inputs.at(desc.column_name).size() <= index)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "PushPartialSortingThroughUnion: Can not find {} in out_to_inputs.", desc.column_name);
            new_desc.column_name = union_step->getOutToInputs().at(desc.column_name).at(index);
            new_sort_desc.emplace_back(new_desc);
        }

        auto partial_sorting = std::make_unique<SortingStepExt>(
            exchange_child->getStep()->getOutputStream(), new_sort_desc, step->getLimit(), SortingStepExt::Stage::PARTIAL_NO_MERGE, SortDescription{});
        PlanNodes children{exchange_child};
        auto before_exchange_sort_node
            = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(partial_sorting), children, node->getStatistics());
        union_inputs.emplace_back(before_exchange_sort_node);
    }

    auto merging_sorted = std::make_unique<SortingStepExt>(
        step->getOutputStream(), step->getSortDescription(), step->getLimit(), SortingStepExt::Stage::MERGE, SortDescription{});

    return PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(),
        std::move(merging_sorted),
        {PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), union_node->getStep(), union_inputs)});
}

static bool isLimitNeeded(const LimitStepExt & limit, const PlanNodePtr & node)
{
    auto range = PlanNodeCardinality::extractCardinality(*node);
    return !limit.hasPreparedParam() && range.upper_bound > limit.getLimit() + limit.getOffset();
}

ConstRefPatternPtr PushPartialLimitThroughExchange::getPattern() const
{
    static auto pattern = Patterns::limit().withSingle(Patterns::exchange()).result();
    return pattern;
}

TransformResult PushPartialLimitThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    auto old_exchange_node = node->getChildren()[0];
    const auto * old_exchange_step = dynamic_cast<const ExchangeStepExt *>(old_exchange_node->getStep().get());

    PlanNodes exchange_children;
    bool should_apply = false;
    for (const auto & exchange_child : old_exchange_node->getChildren())
    {
        if (isLimitNeeded(*step, exchange_child))
        {
            auto partial_limit = std::make_unique<LimitStepExt>(
                exchange_child->getStep()->getOutputStream(),
                step->getLimit() + step->getOffset(),
                size_t{0},
                false,
                false,
                step->getSortDescription(),
                true);
            auto partial_limit_node = PlanNodeBase::createPlanNode(
                context.context->getOptimizerContext()->nextNodeId(), std::move(partial_limit), PlanNodes{exchange_child}, node->getStatistics());
            exchange_children.emplace_back(partial_limit_node);

            should_apply = true;
        }
    }

    if (!should_apply)
        return {};

    auto exchange_step = old_exchange_step->copy(context.context);
    auto exchange_node = PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(), std::move(exchange_step), exchange_children, old_exchange_node->getStatistics());

    node->replaceChildren({exchange_node});
    return node;
}

ConstRefPatternPtr PushPartialDistinctThroughExchange::getPattern() const
{
    static auto pattern = Patterns::distinct()
        .matchingStep<DistinctStepExt>([](const DistinctStepExt & step) { return !step.preDistinct(); })
        .withSingle(Patterns::exchange()).result();
    return pattern;
}

TransformResult PushPartialDistinctThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const DistinctStepExt *>(node->getStep().get());
    auto old_exchange_node = node->getChildren()[0];
    const auto * old_exchange_step = dynamic_cast<const ExchangeStepExt *>(old_exchange_node->getStep().get());
    if (dynamic_cast<const DistinctStepExt *>(old_exchange_node->getChildren()[0]->getStep().get()))
    {
        return {};
    }

    PlanNodes exchange_children;
    for (const auto & exchange_child : old_exchange_node->getChildren())
    {
        auto partial_limit = std::make_unique<DistinctStepExt>(
            exchange_child->getStep()->getOutputStream(),
            step->getSetSizeLimits(),
            step->getLimitHint(),
            step->getColumns(),
            true,
            context.context->getSettingsRef().optimize_distinct_in_order,
            step->canToAgg());
        auto partial_limit_node = PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(), std::move(partial_limit), PlanNodes{exchange_child}, node->getStatistics());
        exchange_children.emplace_back(partial_limit_node);
    }
    auto exchange_step = old_exchange_step->copy(context.context);
    auto exchange_node = PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(), std::move(exchange_step), exchange_children, old_exchange_node->getStatistics());

    node->replaceChildren({exchange_node});
    return node;
}

ConstRefPatternPtr PushPartialTopNDistinctThroughExchange::getPattern() const
{
    static auto pattern = Patterns::limit()
        .withSingle(
            Patterns::sorting().withSingle(Patterns::distinct()
                                               .matchingStep<DistinctStepExt>([](const DistinctStepExt & step) { return !step.preDistinct(); })
                                               .withSingle(Patterns::exchange())))
        .result();
    return pattern;
}

TransformResult PushPartialTopNDistinctThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * limit_step = dynamic_cast<const LimitStepExt *>(node->getStep().get());
    const auto * sort_step = dynamic_cast<const SortingStepExt *>(node->getChildren()[0]->getStep().get());

    const auto * step = dynamic_cast<const DistinctStepExt *>(node->getChildren()[0]->getChildren()[0]->getStep().get());
    auto old_exchange_node = node->getChildren()[0]->getChildren()[0]->getChildren()[0];
    const auto * old_exchange_step = dynamic_cast<const ExchangeStepExt *>(old_exchange_node->getStep().get());
    PlanNodes sorting_children;
    for (const auto & exchange_child : old_exchange_node->getChildren())
    {
        auto partial_distinct = std::make_unique<DistinctStepExt>(
            exchange_child->getStep()->getOutputStream(), SizeLimits{}, 0, step->getColumns(), true, context.context->getSettingsRef().optimize_distinct_in_order, step->canToAgg());
        auto partial_distinct_node = PlanNodeBase::createPlanNode(
            context.context->getOptimizerContext()->nextNodeId(), std::move(partial_distinct), PlanNodes{exchange_child}, node->getStatistics());
        sorting_children.emplace_back(partial_distinct_node);
    }

    auto partial_sort = std::make_unique<SortingStepExt>(
        sorting_children[0]->getStep()->getOutputStream(),
        sort_step->getSortDescription(),
        0u,
        SortingStepExt::Stage::PARTIAL,
        SortDescription{});
    auto partial_sort_node
        = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(partial_sort), sorting_children, node->getStatistics());

    auto distinct_limit = step->copy(context.context);
    dynamic_cast<DistinctStepExt *>(distinct_limit.get())->setLimitHint(limit_step->getLimit() + limit_step->getOffset());
    auto before_exchange = PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(),
        limit_step->copy(context.context),
        {PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), distinct_limit, {partial_sort_node})});


    auto exchange_step = old_exchange_step->copy(context.context);
    dynamic_cast<ExchangeStepExt *>(exchange_step.get())->setKeepOrder(true);
    auto exchange_node = PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(), std::move(exchange_step), {before_exchange}, old_exchange_node->getStatistics());

    QueryPlanStepPtr final_sort = sort_step->copy(context.context);
    dynamic_cast<SortingStepExt *>(final_sort.get())->setStage(SortingStepExt::Stage::MERGE);
    dynamic_cast<SortingStepExt *>(final_sort.get())->setLimit(0);
    PlanNodes exchange{exchange_node};
    auto final_sort_node
        = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(final_sort), exchange, node->getStatistics());


    return PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(),
        limit_step->copy(context.context),
        {PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), step->copy(context.context), {final_sort_node})});
}

ConstRefPatternPtr MarkTopNDistinctThroughExchange::getPattern() const
{
    static auto pattern = Patterns::limit()
        .withSingle(Patterns::sorting().withSingle(
            Patterns::distinct().matchingStep<DistinctStepExt>([](const DistinctStepExt & step) { return !step.preDistinct() && step.canToAgg(); })))
        .result();
    return pattern;
}

TransformResult MarkTopNDistinctThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto distinct_node = node->getChildren()[0]->getChildren()[0];
    const auto * step = dynamic_cast<const DistinctStepExt *>(distinct_node->getStep().get());

    PlanNodes exchange_children;
    auto distinct_step = std::make_unique<DistinctStepExt>(
        distinct_node->getChildren()[0]->getCurrentDataStream(),
        step->getSetSizeLimits(),
        step->getLimitHint(),
        step->getColumns(),
        step->preDistinct(),
        context.context->getSettingsRef().optimize_distinct_in_order,
        false);
    auto new_distinct = PlanNodeBase::createPlanNode(
        context.context->getOptimizerContext()->nextNodeId(), std::move(distinct_step), distinct_node->getChildren(), node->getStatistics());

    node->getChildren()[0]->replaceChildren({new_distinct});
    return node;
}
}
