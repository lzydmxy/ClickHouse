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
   IColumn::Selector partition_selector
        = doRepartition(partition_num, chunk, getInputPort().getHeader(), repartition_keys, repartition_func, REPARTITION_FUNC_RESULT_TYPE);
    ChunkInfoPtr repartion_info = std::make_shared<RepartitionChunkInfo>(
        std::move(partition_selector), std::move(chunk.getChunkInfo()));
    LOG_TRACE(logger, "RepartitionTransform transform");
    chunk.setChunkInfo(std::move(repartion_info));
}

IColumn::Selector RepartitionTransform::doRepartition(
    size_t partition_num,
    const Chunk & chunk,
    const Block & header,
    const ColumnNumbers & repartition_keys,
    ExecutableFunctionPtr repartition_func,
    const DataTypePtr & result_type)
{
    size_t input_rows_count = chunk.getNumRows();
    auto selector_column = ColumnUInt64::create(input_rows_count);
    const Columns & columns = chunk.getColumns();

    ColumnsWithTypeAndName arguments;
    arguments.reserve(repartition_keys.size());
    for (size_t key_idx : repartition_keys)
    {
        const auto & type_and_name = header.getByPosition(key_idx);
        arguments.emplace_back(ColumnWithTypeAndName(columns[key_idx], type_and_name.type, type_and_name.name));
    }

    ColumnPtr hash_result = repartition_func->execute(arguments, result_type, input_rows_count, false);

    PartitionStartPoints partition_row_idx_start_points(partition_num + 1, 0);

    IColumn::Selector repartition_selector(input_rows_count, 0);

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        repartition_selector[i] = hash_result->get64(i) % partition_num;
    }

    if (hash_result->isNullable())
    {
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (hash_result->isNullAt(i))
                repartition_selector[i] = 0;
        }
    }

    return repartition_selector;
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
