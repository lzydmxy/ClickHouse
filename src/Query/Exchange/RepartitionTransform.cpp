#include "RepartitionTransform.h"
#include <utility>
#include <Common/WeakHash.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context_fwd.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
RepartitionTransform::RepartitionTransform(
    const Block & header_, size_t partition_num_, ColumnNumbers repartition_keys_, ExecutableFunctionPtr repartition_func_)
    : ISimpleTransform(header_, header_, true)
    , partition_num(partition_num_)
    , repartition_keys(std::move(repartition_keys_))
    , repartition_func(std::move(repartition_func_))
    , logger(getLogger("RepartitionTransform"))
{
}

void RepartitionTransform::transform(Chunk & chunk)
{
    IColumn::Selector partition_selector;
    RepartitionTransform::PartitionStartPoints partition_start_points;
    std::tie(partition_selector, partition_start_points)
        = doRepartition(partition_num, chunk, getInputPort().getHeader(), repartition_keys, repartition_func, REPARTITION_FUNC_RESULT_TYPE);
    ChunkInfoPtr repartion_info = std::make_shared<RepartitionChunkInfo>(
        std::move(partition_selector), std::move(partition_start_points), std::move(chunk.getChunkInfo()));
    LOG_TRACE(logger, "RepartitionTransform transform");
    chunk.setChunkInfo(std::move(repartion_info));
}

std::pair<IColumn::Selector, RepartitionTransform::PartitionStartPoints> RepartitionTransform::doRepartition(
    size_t partition_num,
    const Chunk & chunk,
    const Block & header,
    const ColumnNumbers & repartition_keys,
    ExecutableFunctionPtr repartition_func,
    const DataTypePtr & result_type)
{
    auto log = getLogger("RepartitionTransform");
    size_t input_rows_count = chunk.getNumRows();
    auto selector_column = ColumnUInt64::create(input_rows_count);
    const Columns & columns = chunk.getColumns();
    //Need remove partition_row_idx_start_points, only use selector
    PartitionStartPoints partition_row_idx_start_points(partition_num + 1, 0);
    IColumn::Selector repartition_selector(input_rows_count, 0);
    //PODArrayWithStackMemory<UInt32, 32> partition_index(input_rows_count, 0);

    bool all_null = false;
    for (size_t key_idx : repartition_keys)
    {
        auto column = columns[key_idx];
        if (column->onlyNull())
            all_null = true;
    }
    if (!all_null)
    {
        ColumnsWithTypeAndName arguments;
        arguments.reserve(repartition_keys.size());
        for (size_t key_idx : repartition_keys)
        {
            const auto & type_and_name = header.getByPosition(key_idx);
            auto column = columns[key_idx];
            arguments.emplace_back(ColumnWithTypeAndName(column, type_and_name.type, type_and_name.name));
        }
        ColumnPtr hash_result = repartition_func->execute(arguments, result_type, input_rows_count, false);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto partition = hash_result->get64(i) % partition_num;
            //partition_index[i] = partition;
            repartition_selector[i] = partition;
        }
        if (hash_result->isNullable())
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (hash_result->isNullAt(i))
                {
                    //partition_index[i] = 0;
                    repartition_selector[i] = 0;
                }
            }
        }
    }
    else
    {
        for (size_t i = 0; i < input_rows_count; ++i)
        {
                repartition_selector[i] = 0;
        }
    }

    // for (size_t i = 0; i < input_rows_count; ++i)
    // {
    //     partition_row_idx_start_points[partition_index[i]]++;
    // }

    // // make partition_row_idx_start_points[partition_num] = input_rows_count
    // for (size_t i = 1; i <= partition_num; ++i)
    // {
    //     partition_row_idx_start_points[i] += partition_row_idx_start_points[i - 1];
    // }

    // for (size_t i = 0; i <= partition_num; i++)
    // {
    //     LOG_TRACE(log, "Partition index {}, row index {}", i, partition_row_idx_start_points[i]);
    // }

    // for (size_t i = input_rows_count; i-- > 0;)
    // {
    //     //repartition_selector[partition_row_idx_start_points[partition_index[i]] - 1] = i;
    //     partition_row_idx_start_points[partition_index[i]]--;
    //     auto row_part = partition_index[i]; //row -> partition
    //     auto part_row_start = partition_row_idx_start_points[partition_index[i]];
    //     LOG_TRACE(log, "Row index row_index {}, row_partition {}, partition_row_start {}", i, row_part, part_row_start);
    // }
    return std::make_pair(std::move(repartition_selector), std::move(partition_row_idx_start_points));
}

ExecutableFunctionPtr RepartitionTransform::getDefaultRepartitionFunction(const ColumnsWithTypeAndName & arguments, ContextPtr context)
{
    FunctionOverloadResolverPtr func_builder = FunctionFactory::instance().get(REPARTITION_FUNC, context);
    FunctionBasePtr function_base = func_builder->build(arguments);
    return function_base->prepare(arguments);
}

ExecutableFunctionPtr RepartitionTransform::getRepartitionHashFunction(const String & func_name, const ColumnsWithTypeAndName & arguments, ContextPtr context, const Array & params)
{
    FunctionOverloadResolverPtr func_builder = FunctionFactory::instance().get(func_name, context);
    FunctionBasePtr function_base = func_builder->build(arguments);
    //TODO: Need add prepareWithParameters in IFunction interface
    //return params.empty() ? function_base->prepare(arguments) : function_base->prepareWithParameters(arguments, params);
    return function_base->prepare(arguments);
}

const DataTypePtr RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE = std::make_shared<DataTypeUInt64>();
const DataTypePtr RepartitionTransform::REPARTITION_FUNC_NULLABLE_RESULT_TYPE = std::make_shared<DataTypeNullable>(RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE);
}
