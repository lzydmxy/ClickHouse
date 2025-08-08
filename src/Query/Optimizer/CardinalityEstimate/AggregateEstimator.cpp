#include <Query/Optimizer/CardinalityEstimate/AggregateEstimator.h>
#include <Query/Common/OptimizerContext.h>


namespace DB
{
static double estimateGroupBy(
    std::unordered_map<String, SymbolStatisticsPtr> & symbol_statistics, PlanNodeStatisticsPtr & child_stats, const Names & group_keys, const NameSet & keys_not_hashed, double multi_agg_keys_correlated_coefficient, ContextMutablePtr context)
{
    double row_count = 1;
    bool all_unknown = true;
    for (size_t i = 0; i < group_keys.size(); i++)
    {
        const auto & key = group_keys[i];
        if (child_stats->getSymbolStatistics().contains(key) && !child_stats->getSymbolStatistics(key)->isUnknown())
        {
            auto key_stats = child_stats->getSymbolStatistics(key)->copy();
            if (!keys_not_hashed.contains(key)) // `keys not hashed` don't affect the calculation result of agg and should be ignored when estimating the cardinality.
            {
                int null_rows
                    = child_stats->getRowCount() == 0 || (double(key_stats->getNullsCount()) / child_stats->getRowCount() == 0.0) ? 0 : 1;
                if (key_stats->getNdv() > 0)
                {
                    double cndv = static_cast<double>(key_stats->getNdv()) + null_rows;
                    if (i != 0)
                        row_count *= std::max(1.0, multi_agg_keys_correlated_coefficient * cndv);
                    else
                        row_count *= cndv;
                }
            }

            symbol_statistics[key] = key_stats;
            all_unknown = false;
        }
    }

    if (!group_keys.empty() && all_unknown)
    {
        row_count = child_stats->getRowCount();
        if (context->getOptimizerContext()->getSettingsRef().enable_estimate_without_symbol_statistics)
        {
            row_count *= context->getOptimizerContext()->getSettingsRef().stats_estimator_first_agg_key_filter_coefficient;
            row_count *= std::pow(context->getOptimizerContext()->getSettingsRef().stats_estimator_remaining_agg_keys_filter_coefficient, group_keys.size() - 1);
            row_count = std::max(1.0, row_count);
        }
    }

    row_count = std::min(row_count, double(child_stats->getRowCount()));

    for (auto & item : symbol_statistics)
    {
        auto & stat = item.second;
        double adjust_row_count = double(row_count) / child_stats->getRowCount();
        stat = stat->applySelectivity(adjust_row_count, 1.0);
    }

    return row_count;
}

PlanNodeStatisticsPtr AggregateEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const AggregatingStepExt & step, ContextMutablePtr context)
{
    if (!child_stats)
    {
        return nullptr;
    }
    std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics;
    
    Names group_keys;
    for (const auto & key : step.getKeys())
        group_keys.push_back(key);

    UInt64 row_count = static_cast<UInt64>(estimateGroupBy(symbol_statistics, child_stats, group_keys, step.getKeysNotHashed(), context->getOptimizerContext()->getSettingsRef().multi_agg_keys_correlated_coefficient, context));

    std::unordered_map<String, DataTypePtr> name_to_type;
    for (const auto & item : step.getOutputStream().header)
    {
        name_to_type[item.name] = item.type;
    }
    const AggregateDescriptions & agg_descs = step.getAggregates();
    for (const auto & agg_desc : agg_descs)
    {
        symbol_statistics[agg_desc.column_name]
            = AggregateEstimator::estimateAggFun(agg_desc.function, agg_desc.argument_names, row_count, name_to_type[agg_desc.column_name], child_stats);
    }

    return std::make_shared<PlanNodeStatistics>(row_count, std::move(symbol_statistics));
}

PlanNodeStatisticsPtr AggregateEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const MergingAggregatedStepExt & step, ContextMutablePtr context)
{
    if (!child_stats)
    {
        return nullptr;
    }

    std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics;
    const Names & group_keys = step.getKeys();
    UInt64 row_count = static_cast<UInt64>(estimateGroupBy(symbol_statistics, child_stats, group_keys, {}, 1.0, context));

    const AggregateDescriptions & agg_descs = step.getAggregates();
    std::unordered_map<String, DataTypePtr> name_to_type;
    for (const auto & item : step.getOutputStream().header)
    {
        name_to_type[item.name] = item.type;
    }
    for (const auto & agg_desc : agg_descs)
    {
        symbol_statistics[agg_desc.column_name]
            = AggregateEstimator::estimateAggFun(agg_desc.function, agg_desc.argument_names, row_count, name_to_type[agg_desc.column_name], child_stats);
    }

    return std::make_shared<PlanNodeStatistics>(row_count, std::move(symbol_statistics));
}

PlanNodeStatisticsPtr AggregateEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const DistinctStepExt & step, ContextMutablePtr context)
{
    if (!child_stats)
    {
        return nullptr;
    }

    std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics;
    const auto & columns = step.getColumns();
    UInt64 limit = step.getSetSizeLimits().max_rows;
    double row_count = estimateGroupBy(symbol_statistics, child_stats, columns, {}, 1.0, context);

    // distinct limit
    if (limit != 0)
    {
        row_count = std::min(row_count, double(limit));
    }

    return std::make_shared<PlanNodeStatistics>(row_count, std::move(symbol_statistics));
}

SymbolStatisticsPtr AggregateEstimator::estimateAggFun(AggregateFunctionPtr fun, const Names & args, UInt64 row_count, DataTypePtr data_type, PlanNodeStatisticsPtr & /*child_stats*/)
{
    // FIXME(lizhuoyu5), Remove unreasonable max/min estimations after aggregation, as this cause bad cases for TPC-H Q18.
    // Float64 min = 0;
    // Float64 max = 0;
    // if (fun->getName() == "sum" && !args.empty() && child_stats->getSymbolStatistics(args[0]))
    // {
    //     min = child_stats->getSymbolStatistics(args[0])->getMin();
    //     max = child_stats->getSymbolStatistics(args[0])->getMax();
    // }
    SymbolStatistics statistics{row_count, 0, 0, 0, 0, {}, data_type, fun->getName(), false};
    return std::make_shared<SymbolStatistics>(statistics);
}

}
