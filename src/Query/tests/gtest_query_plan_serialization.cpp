#include <DataTypes/DataTypeFactory.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

using namespace DB;

Block createBlock()
{
    ColumnWithTypeAndName column;
    column.name = "RES";

    DataTypePtr type = DataTypeFactory::instance().get("UInt8");
    column.column = type->createColumnConst(1, Field(1));
    column.type = type;

    ColumnsWithTypeAndName columns;
    columns.push_back(column);

    return Block(columns);
}

DataStream createDataStream()
{
    return DataStream{.header = createBlock()};
}

SortDescription createSortDescription()
{
    SortDescription sort_desc;

    Names keys{"key1", "key2", "key3", "key4"};
    for (const auto & key_name : keys)
    {
        auto sort = SortColumnDescription(key_name, 1, 1);
        sort.fill_description.fill_from = Field("field_from");
        sort_desc.emplace_back(sort);
    }

    return sort_desc;
}

SizeLimits createSizeLimits()
{
    return SizeLimits();
}

AggregatorExt::Params createAggregatorExtParams()
{
    ColumnNumbers keys;
    AggregateDescriptions aggregates;

    return AggregatorExt::Params(
        Block(),
        keys,
        aggregates,
        false,
        1,
        OverflowMode::ANY,
        2,
        3,
        4,
        false,
        4,
        true,
        nullptr,
        5,
        6,
        false,
        7,
        0,
        false,
        false,
        true,
        0.5,
        {});
}

QueryPlanStepPtr createAggregatingStepExt()
{
    DataStream input_stream{.header = Block()};

    AggregatorExt::Params params = createAggregatorExtParams();

    SortDescriptionWithPositions group_by_sort_description;

    return make_unique<AggregatingStepExt>(
        input_stream,
        params,
        NameSet{},
        GroupingSetsParamsExtList{},
        true,
        AggregateStagePolicy::DEFAULT,
        8,
        9,
        10,
        true,
        nullptr,
        std::move(group_by_sort_description),
        true);
}

QueryPlanStepPtr serializeQueryPlanStep(QueryPlanStepPtr & step)
{
    /**
      * serialize to buffer
      */
    WriteBufferFromOwnString write_buffer;
    serializePlanStep(step, write_buffer);

    /**
      * deserialize from buffer
      */
    const auto & context = getContext().context;

    ReadBufferFromString read_buffer(write_buffer.str());
    return deserializePlanStep(read_buffer, context);
}

TEST(QueryPlanTest, QueryPlanSerialization)
{
    auto agg_step = createAggregatingStepExt();
    auto new_agg_step = serializeQueryPlanStep(agg_step);
    std::cout << new_agg_step->getName() << std::endl;
    EXPECT_EQ(agg_step->getName(), new_agg_step->getName());
    EXPECT_EQ(
        dynamic_cast<AggregatingStepExt *>(agg_step.get())->getParams().src_header.dumpStructure(),
        dynamic_cast<AggregatingStepExt *>(new_agg_step.get())->getParams().src_header.dumpStructure());
}
