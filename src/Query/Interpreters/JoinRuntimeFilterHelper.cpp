#include <Query/Interpreters/JoinRuntimeFilterHelper.h>

#include <DataTypes/DataTypeLowCardinality.h>

#include <Interpreters/HashJoin.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/JoinSwitcher.h>

#include <Query/Interpreters/TableJoinExt.h>

#include <Query/Executor/RuntimeFilter/RuntimeFilterConsumer.h>


namespace DB
{

template <typename T, bool equal_null>
static bool procNumericBlock(BloomFilterWithRange & bf_with_range, const IColumn * column)
{
    const auto * nullable = checkAndGetColumn<ColumnNullable>(column);

    if (nullable)
    {
        const auto col = checkAndGetColumn<ColumnVector<T>>(nullable->getNestedColumn());
        if (!col)
            return false;

        if constexpr (equal_null)
        {
            for (size_t i = 0; i < nullable->size(); ++i)
            {
                if (nullable->isNullAt(i))
                    bf_with_range.addNull();
                else
                    bf_with_range.addKey(col->getData()[i]);
            }
        }
        else
        {
            for (size_t i = 0; i < nullable->size(); ++i)
            {
                if (!nullable->isNullAt(i))
                    bf_with_range.addKey(col->getData()[i]);
            }
        }
    }
    else
    {
        const auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;

        std::for_each(
            col->getData().cbegin(), col->getData().cend(),
            [&](const auto x) { bf_with_range.addKey(x); });
    }

    return true;
}

template<bool equal_null>
static void buildOneBlock(BloomFilterWithRange & bf_with_range, WhichDataType which, const IColumn * column)
{
    bool ret = false;
#define DISPATCH(TYPE) \
if (which.idx == TypeIndex::TYPE) ret = procNumericBlock<TYPE, equal_null>(bf_with_range, column);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        ret = procNumericBlock<UInt16, equal_null>(bf_with_range, column);
    else if (which.idx == TypeIndex::Date32)
        ret = procNumericBlock<Int32, equal_null>(bf_with_range, column);
    else if (which.idx == TypeIndex::DateTime)
        ret = procNumericBlock<UInt32, equal_null>(bf_with_range, column);

    if (!ret)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "buildOneBlock unexpected type of column: {}", column->getName());
}

void JoinRuntimeFiltersHelper::tryBuildRuntimeFilters(HashJoin & hash_join)
{
    auto table_join_ext = std::static_pointer_cast<TableJoinExt>(hash_join.table_join);
    if (!table_join_ext)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin need hold TableJoinExt to support RuntimeFilters");

    auto runtime_filter_consumer = table_join_ext->getRuntimeFilterConsumer();
    if (!runtime_filter_consumer)
        return;

    auto & runtime_filters = runtime_filter_consumer->getRuntimeFilters();
    if (runtime_filters.empty())
        return;

    size_t ht_size = hash_join.getTotalRowCount();
    if (ht_size == 0 && runtime_filter_consumer->getLocalSteamParallel() == 1)
    {
        bypassRuntimeFilters(hash_join, BypassType::BYPASS_EMPTY_HT, 0);
        return;
    }

    if (runtime_filter_consumer->getLocalSteamParallel() > 1)
    {
        if (runtime_filter_consumer->isBypassed())
            return; /// already bypassed

        if (runtime_filter_consumer->addBuildParams(ht_size, &hash_join.data->blocks))
        {
            /// last one, start build
            size_t total_size = runtime_filter_consumer->totalHashTableSize();
            if (total_size > table_join_ext->getInBuildThreshold() && total_size > table_join_ext->getBloomBuildThreshold())
            {
                bypassRuntimeFilters(hash_join, BypassType::BYPASS_LARGE_HT, total_size);
                return;
            }
            else if (total_size == 0)
            {
                /// edge case all stream is empty
                bypassRuntimeFilters(hash_join, BypassType::BYPASS_EMPTY_HT, 0);
                return;
            }
            Stopwatch stopwatch;
            std::vector<const BlocksList*> && all_blocks = runtime_filter_consumer->buildParamsBlocks();
            buildAllRF(hash_join, total_size, all_blocks, runtime_filter_consumer);
            runtime_filter_consumer->finalize();
            LOG_TRACE(&Poco::Logger::get("JoinRuntimeFiltersHelper"), "build rf total rows:{} all cost: {} ms", total_size, stopwatch.elapsedMilliseconds());
            return;
        }

        size_t total_size = runtime_filter_consumer->totalHashTableSize();
        if (!runtime_filter_consumer->isBypassed() && total_size > table_join_ext->getInBuildThreshold() && total_size > table_join_ext->getBloomBuildThreshold())
        {
            bypassRuntimeFilters(hash_join, BypassType::BYPASS_LARGE_HT, total_size);
            return;
        }
    }
    else
    {
        if (ht_size > table_join_ext->getInBuildThreshold() && ht_size > table_join_ext->getBloomBuildThreshold())
        {
            bypassRuntimeFilters(hash_join, BypassType::BYPASS_LARGE_HT, ht_size);
            return;
        }
        Stopwatch stopwatch;
        buildAllRF(hash_join, ht_size, {&hash_join.data->blocks}, runtime_filter_consumer);
        runtime_filter_consumer->finalize();
        LOG_TRACE(&Poco::Logger::get("JoinRuntimeFiltersHelper"), "build local rf runtime filter total rows:{} cost: {} ms", ht_size, stopwatch.elapsedMilliseconds());
    }
}

void JoinRuntimeFiltersHelper::tryBuildRuntimeFilters(JoinPtr join)
{
    if (auto hash_join = std::dynamic_pointer_cast<HashJoin>(join))
    {
        tryBuildRuntimeFilters(*hash_join);
    }
    else if (auto concurrent_hash_join = std::dynamic_pointer_cast<ConcurrentHashJoin>(join))
    {
        size_t total_rows = 0;
        for (const auto & hash_join : concurrent_hash_join->hash_joins)
        {
            total_rows += hash_join->data->getTotalRowCount();
        }

        if (total_rows == 0)
        {
            // need bypass
            bypassRuntimeFilters(*concurrent_hash_join->hash_joins.front()->data, BypassType::BYPASS_EMPTY_HT, 0);
            return ;
        }

        auto table_join_ext = std::static_pointer_cast<TableJoinExt>(hash_join->table_join);
        if (!table_join_ext)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin need hold TableJoinExt to support RuntimeFilters");

        if (total_rows > table_join_ext->getInBuildThreshold() && total_rows > table_join_ext->getBloomBuildThreshold())
        {
            // need bypass
            bypassRuntimeFilters(*concurrent_hash_join->hash_joins.front()->data, BypassType::BYPASS_LARGE_HT, total_rows);
            return ;
        }

        for (const auto & hash_join : concurrent_hash_join->hash_joins)
        {
            tryBuildRuntimeFilters(*hash_join->data);
        }
    }
    if (auto join_switcher = std::dynamic_pointer_cast<JoinSwitcher>(join))
    {
        tryBuildRuntimeFilters(join_switcher->join);
    }

}

void JoinRuntimeFiltersHelper::bypassRuntimeFilters(HashJoin & hash_join, BypassType type, size_t total_size)
{
    auto table_join_ext = std::static_pointer_cast<TableJoinExt>(hash_join.table_join);
    if (!table_join_ext)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin need hold TableJoinExt to support RuntimeFilters");
    const auto & runtime_filter_consumer = table_join_ext->getRuntimeFilterConsumer();
    if (!runtime_filter_consumer)
        return;
    LOG_DEBUG(&Poco::Logger::get("JoinRuntimeFiltersHelper"), "going build rf bypass rf: {}, size:{}", bypassTypeToString(type), total_size);
    runtime_filter_consumer->bypass(type);
}

void JoinRuntimeFiltersHelper::buildAllRF(HashJoin & hash_join, size_t total_size, const std::vector<const BlocksList *> & all_blocks, RuntimeFilterConsumerPtr rf_consumer)
{
    const auto & runtime_filters = rf_consumer->getRuntimeFilters();

    auto table_join_ext = std::static_pointer_cast<TableJoinExt>(hash_join.table_join);
    if (!table_join_ext)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin need hold TableJoinExt to support RuntimeFilters");

    if (total_size <= table_join_ext->getInBuildThreshold())
    {
        /// only build in filter
        for (const auto & runtime_filter : runtime_filters)
        {
            if (!hash_join.right_table_keys.has(runtime_filter.first)) /// shouldn't happen
                continue ;
            buildValueSetRF(runtime_filter.second, runtime_filter.first, all_blocks, rf_consumer);
        }

        return;
    }

    if (total_size <= table_join_ext->getBloomBuildThreshold())
    {
        /// just build bloom filter
        for (const auto & rf : runtime_filters)
        {
            if (!hash_join.right_table_keys.has(rf.first)) /// shouldn't happen
                continue ;

            buildBloomFilterRF(hash_join, rf.second, rf.first, total_size, all_blocks, rf_consumer);
        }
        return;
    }
}

void JoinRuntimeFiltersHelper::buildValueSetRF(const RuntimeFilter & rf_info, const String & name, const std::vector<const BlocksList *> & blocks,
                               RuntimeFilterConsumerPtr rf_consumer)
{
    DataTypePtr type;
    for (const auto & lists : blocks)
    {
        if (lists != nullptr && !lists->empty())
        {
            const auto & col = lists->front().getByName(name);
            type = removeNullable(recursiveRemoveLowCardinality(col.type));
            break;
        }
    }

    ValueSetWithRangePtr vs_with_range = std::make_shared<ValueSetWithRange>(type);
    for (const auto & lists : blocks)
    {
        if (lists == nullptr || lists->empty())
            continue;
        for (const auto & block : *lists)
        {
            const auto & col = block.getByName(name).column;
            Field field;
            for (size_t i = 0; i < block.rows(); ++i)
            {
                col->get(i, field);
                if (!field.isNull())
                    vs_with_range->insert(field);
            }
        }
    }
    rf_consumer->addFinishRF(std::move(vs_with_range), rf_info.id, rf_info.distribution == RRuntimeFilter::LOCAL);
}

void JoinRuntimeFiltersHelper::buildBloomFilterRF(
    HashJoin & hash_join, const RuntimeFilter & rf_info, const String & name, size_t ht_size, const std::vector<const BlocksList *> & blocks,
    RuntimeFilterConsumerPtr rf_consumer)
{
    auto is_equal_null = [&hash_join](const String & name)
    {
        const auto & clause = hash_join.table_join->getOnlyClause();
        if (clause.nullsafe_compare_key_indexes.empty())
            return false;

        for (const auto & index : clause.nullsafe_compare_key_indexes)
        {
            if (name == clause.key_names_right[index])
                return true;
        }
        return false;
    };

    bool is_null_safe = is_equal_null(name);

    DataTypePtr type;
    for (const auto & lists : blocks)
    {
        if (lists != nullptr && !lists->empty())
        {
            const auto & col = lists->front().getByName(name);
            type = removeNullable(recursiveRemoveLowCardinality(col.type));
            break;
        }
    }
    WhichDataType which(type);

    /// try enlarge ndv for none-shuffle-aware grf
    size_t pre_enlarge_size = ht_size;
    bool pre_enlarge = true;
    if (rf_consumer->isDistributed(name))
    {
        pre_enlarge_size *= rf_consumer->getParallelWorkers();
        // if (table_join->getShuffleAwareNDVThreshold() && pre_enlarge_size > table_join->getShuffleAwareNDVThreshold())
        // {
        //     pre_enlarge = false;
        //     pre_enlarge_size = ht_size;
        // } // TODO: Yuanning RuntimeFilter
    }

    BloomFilterWithRangePtr bf_with_range
        = std::make_shared<BloomFilterWithRange>(pre_enlarge_size, type);
    bf_with_range->is_pre_enlarged = pre_enlarge;
    for (const auto & lists : blocks)
    {
        if (lists == nullptr || lists->empty())
            continue;
        for (const auto & block : *lists)
        {
            const auto & col = block.getByName(name).column;
            const auto * nullable = checkAndGetColumn<ColumnNullable>(col.get());
            if (nullable)
            {
                const auto * nest_col = nullable->getNestedColumnPtr().get();
                if (nest_col->isNumeric())
                {
                    if (is_null_safe)
                        buildOneBlock<true>(*bf_with_range, which, nullable);
                    else
                        buildOneBlock<false>(*bf_with_range, which, nullable);
                }
                else if (nest_col->getDataType() == TypeIndex::Map || nest_col->getDataType() == TypeIndex::Tuple)
                {
                    for (size_t i = 0; i < block.rows(); ++i)
                    {
                        if (nullable->isNullAt(i))
                        {
                            if (is_null_safe)
                                bf_with_range->addNull();
                        }
                        else
                        {
                            bf_with_range->addKey<true>(*nest_col, i);
                        }
                    }
                }
                else
                {
                    for (size_t i = 0; i < block.rows(); ++i)
                    {
                        if (nullable->isNullAt(i))
                        {
                            if (is_null_safe)
                                bf_with_range->addNull();
                        }
                        else
                        {
                            bf_with_range->addKey<false>(*nest_col, i);
                        }
                    }
                }
            }
            else
            {
                if (col->isNumeric())
                {
                    if (is_null_safe)
                        buildOneBlock<true>(*bf_with_range, which, col.get());
                    else
                        buildOneBlock<false>(*bf_with_range, which, col.get());
                }
                else if (col->getDataType() == TypeIndex::Map || col->getDataType() == TypeIndex::Tuple)
                {
                    for (size_t i = 0; i < block.rows(); ++i)
                    {
                        bf_with_range->addKey<true>(*col, i);
                    }
                }
                else
                {
                    for (size_t i = 0; i < block.rows(); ++i)
                    {
                        bf_with_range->addKey<false>(*col, i);
                    }
                }
            }
        }
    }
    rf_consumer->addFinishRF(std::move(bf_with_range), rf_info.id, rf_info.distribution == RRuntimeFilter::LOCAL);
}


}
