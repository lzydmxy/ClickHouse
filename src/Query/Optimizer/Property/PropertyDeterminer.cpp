#include <set>
#include <Query/Optimizer/Property/PropertyDeterminer.h>

#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Utils.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/Processors/QueryPlan/DistinctStepExt.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/TotalsHavingStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>
#include <Processors/QueryPlan/WindowStep.h>

namespace DB
{
PropertySets PropertyDeterminer::determineRequiredProperty(QueryPlanStepPtr step, const Property & property, Context & context)
{
    DeterminerContext ctx{property, context};
    DeterminerVisitor visitor{};
    PropertySets input_properties = VisitorUtil::accept(step, visitor, ctx);
    if (!property.getCTEDescriptions().empty() || !property.getTableLayout().empty())
    {
        for (auto & property_set : input_properties)
        {
            for (auto & prop : property_set)
            {
                prop.setCTEDescriptions(property.getCTEDescriptions());
                prop.setTableLayout(property.getTableLayout());
            }
        }
    }
    return input_properties;
}


PropertySets DeterminerVisitor::visitStep(const IQueryPlanStep &, DeterminerContext & context)
{
    return {{context.getRequired()}};
}

PropertySets DeterminerVisitor::visitMultiJoinStepExt(const MultiJoinStepExt & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

PropertySets DeterminerVisitor::visitBufferStepExt(const BufferStepExt & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

PropertySets DeterminerVisitor::visitPartitionTopNStepExt(const PartitionTopNStepExt &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

PropertySets DeterminerVisitor::visitLocalExchangeStepExt(const LocalExchangeStepExt & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

PropertySets DeterminerVisitor::visitOffsetStep(const OffsetStep &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

// todo lizhuoyu5 , add FinishSortingStep
// PropertySets DeterminerVisitor::visitFinishSortingStep(const FinishSortingStep &, DeterminerContext &)
// {
//     return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
// }

PropertySets DeterminerVisitor::visitFinalSampleStepExt(const FinalSampleStepExt & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

PropertySets DeterminerVisitor::visitProjectionStepExt(const ProjectionStepExt & step, DeterminerContext & ctx)
{
    if (step.isFinalProject() && (ctx.getRequired().getNodePartitioning().getComponent() != Component::WORKER))
        return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
    const auto & assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    auto translated = ctx.getRequired().translate(identities);
    if (!step.getInputStreams()[0].header)
        return {{Property{}}};
    translated.setPreferred(true);
    return {{translated}};
}

PropertySets DeterminerVisitor::visitArrayJoinStep(const ArrayJoinStep &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

PropertySets DeterminerVisitor::visitFilterStepExt(const FilterStepExt &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

// TODO property expand @jingpeng
PropertySets DeterminerVisitor::visitJoinStepExt(const JoinStepExt & step, DeterminerContext & context)
{
    const Names & left_keys = step.getLeftKeys();
    const Names & right_keys = step.getRightKeys();

    auto enforce_round_robine = context.getContext().getOptimizerContext()->getSettingsRef().enforce_round_robin;
    // process ASOF join, it is different with normal join.
    if (step.getStrictness() == JoinStrictness::Asof)
    {
        Names left_keys_asof;
        Names right_keys_asof;
        for (size_t i = 0; i < left_keys.size() - 1; ++i)
        {
            left_keys_asof.emplace_back(left_keys[i]);
            right_keys_asof.emplace_back(right_keys[i]);
        }

        Partitioning left_stream{PartitioningHandle::FIXED_HASH, left_keys_asof};
        Partitioning right_stream{PartitioningHandle::FIXED_HASH, right_keys_asof};

        Property left{Partitioning{PartitioningHandle::FIXED_HASH, left_keys_asof, false, 0, nullptr, enforce_round_robine}, left_stream};
        Property right{Partitioning{PartitioningHandle::FIXED_HASH, right_keys_asof, false, 0, nullptr, false}, right_stream};
        PropertySet set;
        set.emplace_back(left);
        set.emplace_back(right);
        return {set};
    }

    if (step.getDistributionType() == DistributionType::BROADCAST)
    {
        auto left_require = context.getRequired();
        left_require.setPreferred(true);
        return {{left_require, Property{Partitioning{PartitioningHandle::FIXED_BROADCAST}}}};
    }

    if (left_keys.empty() && right_keys.empty())
    {
        Property left{Partitioning{PartitioningHandle::SINGLE}};
        Property right{Partitioning{PartitioningHandle::SINGLE}};
        PropertySet set;
        set.emplace_back(left);
        set.emplace_back(right);
        return {set};
    }

    std::vector<std::tuple<String, String>> join_key_pairs;
    for (size_t i = 0; i < left_keys.size(); ++i)
    {
        join_key_pairs.emplace_back(std::make_tuple(left_keys[i], right_keys[i]));
    }

    PropertySets result;
    if (join_key_pairs.size() <= context.getContext().getOptimizerContext()->getSettingsRef().max_expand_join_key_size)
    {
        for (auto & set : Utils::powerSet(join_key_pairs))
        {
            Names sub_left_keys;
            Names sub_right_keys;
            for (const auto & item : set)
            {
                sub_left_keys.emplace_back(std::get<0>(item));
                sub_right_keys.emplace_back(std::get<1>(item));
            }

            Partitioning left_stream{PartitioningHandle::FIXED_HASH, sub_left_keys};
            Partitioning right_stream{PartitioningHandle::FIXED_HASH, sub_right_keys};
            Property left{Partitioning{PartitioningHandle::FIXED_HASH, sub_left_keys, false, 0, nullptr, enforce_round_robine}, left_stream};
            Property right{Partitioning{PartitioningHandle::FIXED_HASH, sub_right_keys, false, 0, nullptr, false}, right_stream};
            PropertySet prop_set;
            prop_set.emplace_back(left);
            prop_set.emplace_back(right);
            result.emplace_back(prop_set);
        }
    }
    else
    {
        Partitioning left_stream{PartitioningHandle::FIXED_HASH, left_keys};
        Partitioning right_stream{PartitioningHandle::FIXED_HASH, right_keys};
        Property left{Partitioning{PartitioningHandle::FIXED_HASH, left_keys, false, 0, nullptr, enforce_round_robine}, left_stream};
        Property right{Partitioning{PartitioningHandle::FIXED_HASH, right_keys, false, 0, nullptr, false}, right_stream};
        PropertySet prop_set;
        prop_set.emplace_back(left);
        prop_set.emplace_back(right);
        result.emplace_back(prop_set);
    }
    return result;
}

PropertySets DeterminerVisitor::visitAggregatingStepExt(const AggregatingStepExt & step, DeterminerContext & context)
{
    if (!context.getContext().getOptimizerContext()->getSettingsRef().enable_shuffle_before_state_func && step.getAggregates().size() > 0)
    {
        bool all_state_agg = true;
        for (const auto & agg : step.getAggregates())
        {
            if (!agg.function->getName().ends_with("State"))
            {
                all_state_agg = false;
                break;
            }
        }
        if (all_state_agg)
        {
            auto require = context.getRequired();
            require.setPreferred(true);
            return {{require}};
        }
    }
    if (!step.isFinal())
    {
        auto require = context.getRequired();
        require.setPreferred(true);
        return {{require}};
    }

    auto keys = step.getKeys();
    if (keys.empty())
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{PartitioningHandle::SINGLE}});
        return {set};
    }

    PropertySets sets;
    auto required_keys = context.getRequired().getNodePartitioning().getColumns();
    if (context.getContext().getOptimizerContext()->getSettingsRef().enable_merge_require_property && !required_keys.empty() && keys.size() > required_keys.size())
    {
        std::set<String> keys_set(keys.begin(), keys.end());
        bool contain_all = true;
        for (auto & required_key : required_keys)
        {
            if (!keys_set.contains(required_key))
            {
                contain_all = false;
                break;
            }
        }

        if (contain_all)
            sets.emplace_back(
                PropertySet{Property{context.getRequired().getNodePartitioning(), context.getRequired().getStreamPartitioning()}});
    }

    if (keys.size() <= context.getContext().getOptimizerContext()->getSettingsRef().max_expand_agg_key_size)
    {
        for (const auto & sub_keys : Utils::powerSet(keys))
        {
            Property prop{
                Partitioning{PartitioningHandle::FIXED_HASH, sub_keys}, Partitioning{PartitioningHandle::FIXED_HASH, sub_keys}};
            sets.emplace_back(PropertySet{prop});
        }
    }
    else
    {
        sets.emplace_back(PropertySet{
            Property{Partitioning{PartitioningHandle::FIXED_HASH, keys}, Partitioning{PartitioningHandle::FIXED_HASH, keys}}});
    }

    if (step.isGroupingSet())
    {
        keys.emplace_back("__grouping_set");
        return {PropertySet{
            Property{Partitioning{PartitioningHandle::FIXED_HASH, keys, false, 0, nullptr, true, Component::ANY, true}}}};
    }

    return sets;
}

PropertySets DeterminerVisitor::visitTotalsHavingStepExt(const TotalsHavingStepExt &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitMarkDistinctStepExt(const MarkDistinctStepExt & step, DeterminerContext &)
{
    auto keys = step.getDistinctSymbols();
    if (keys.empty())
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{PartitioningHandle::SINGLE}});
        return {set};
    }

    PropertySets sets;

    sets.emplace_back(PropertySet{Property{Partitioning{
        PartitioningHandle::FIXED_HASH,
        keys,
    }}});

    return sets;
}

PropertySets DeterminerVisitor::visitMergingAggregatedStepExt(const MergingAggregatedStepExt & step, DeterminerContext &)
{
    auto keys = step.getKeys();
    if (keys.empty())
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{PartitioningHandle::SINGLE}});
        return {set};
    }
    std::vector<String> group_bys;
    group_bys.reserve(keys.size());
    for (const auto & key : keys)
    {
        group_bys.emplace_back(key);
    }
    PropertySet set;
    set.emplace_back(Property{
        Partitioning{
            PartitioningHandle::FIXED_HASH,
            group_bys,
        },
        Partitioning{
            PartitioningHandle::FIXED_HASH,
            group_bys,
        }

    });
    return {set};
}

PropertySets DeterminerVisitor::visitUnionStepExt(const UnionStepExt & step, DeterminerContext & context)
{
    PropertySet set;
    for (size_t i = 0; i < step.getInputStreams().size(); ++i)
    {
        std::unordered_map<String, String> mapping;
        for (const auto & output_to_input : step.getOutToInputs())
        {
            mapping[output_to_input.first] = output_to_input.second[i];
        }
        Property translated = context.getRequired().translate(mapping);
        translated.setPreferred(true);
        set.emplace_back(translated);
    }
    return {set};
}

PropertySets DeterminerVisitor::visitIntersectOrExceptStep(const IntersectOrExceptStep & node, DeterminerContext &)
{
    PropertySet set;
    for (const auto & input : node.getInputStreams())
    {
        set.emplace_back(Property{Partitioning{
            PartitioningHandle::FIXED_HASH,
            input.header.getNames(),
        }});
    }

    return {set};
}

PropertySets DeterminerVisitor::visitExchangeStepExt(const ExchangeStepExt &, DeterminerContext &)
{
    return {{Property{}}};
}

PropertySets DeterminerVisitor::visitRemoteExchangeSourceStepExt(const RemoteExchangeSourceStepExt & node, DeterminerContext & context)
{
    return visitStep(node, context);
}

PropertySets DeterminerVisitor::visitTableScanStepExt(const TableScanStepExt &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitReadNothingStep(const ReadNothingStep &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitReadStorageRowCountStepExt(const ReadStorageRowCountStepExt &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitValuesStepExt(const ValuesStepExt &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitLimitStepExt(const LimitStepExt & step, DeterminerContext & context)
{
    if (step.isPartial())
        return visitStep(step, context);
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitLimitByStep(const LimitByStep &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitSortingStepExt(const SortingStepExt &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

// // todo lizhuoyu5, add SortingStep
// PropertySets DeterminerVisitor::visitMergeSortingStep(const MergeSortingStep &, DeterminerContext &)
// {
//     return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
// }
//
// PropertySets DeterminerVisitor::visitPartialSortingStep(const PartialSortingStep &, DeterminerContext &)
// {
//     return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
// }
//
// PropertySets DeterminerVisitor::visitMergingSortedStep(const MergingSortedStep &, DeterminerContext &)
// {
//     return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
// }

PropertySets DeterminerVisitor::visitDistinctStepExt(const DistinctStepExt &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitExtremesStep(const ExtremesStep &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}, Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitWindowStep(const WindowStep & step, DeterminerContext & context)
{
    const auto & keys = QueryPlanStepHelper::getWindowStepWindow(step).partition_by;
    if (keys.empty())
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{PartitioningHandle::SINGLE}});
        return {set};
    }
    std::vector<String> group_bys;
    for (const auto & key : keys)
    {
        group_bys.emplace_back(key.column_name);
    }
    PropertySets sets;
    if (keys.size() <= context.getContext().getOptimizerContext()->getSettingsRef().max_expand_agg_key_size)
    {
        for (const auto & sub_keys : Utils::powerSet(group_bys))
        {
            Property prop{Partitioning{PartitioningHandle::FIXED_HASH, sub_keys}};
            sets.emplace_back(PropertySet{prop});
        }
    }
    else
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{PartitioningHandle::FIXED_HASH, group_bys, false}});
        sets.emplace_back(set);
    }
    return sets;
}

PropertySets DeterminerVisitor::visitApplyStepExt(const ApplyStepExt &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}, Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitEnforceSingleRowStepExt(const EnforceSingleRowStepExt &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitAssignUniqueIdStepExt(const AssignUniqueIdStepExt & node, DeterminerContext & context)
{
    return visitStep(node, context);
}

PropertySets DeterminerVisitor::visitCTERefStepExt(const CTERefStepExt &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitExplainAnalyzeStepExt(const ExplainAnalyzeStepExt &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitTopNFilteringStepExt(const TopNFilteringStepExt &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

PropertySets DeterminerVisitor::visitFillingStep(const FillingStep &, DeterminerContext &)
{
    return {{Property{Partitioning{PartitioningHandle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitExpandStepExt(const ExpandStepExt &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

PropertySets DeterminerVisitor::visitIntermediateResultCacheStepExt(const IntermediateResultCacheStepExt & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

}
