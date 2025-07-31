#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/Processors/QueryPlan/DistinctStepExt.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/FinishSortingStepExt.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Query/Processors/QueryPlan/MergeSortingStepExt.h>
#include <Query/Processors/QueryPlan/MergingAggregatedStepExt.h>
#include <Query/Processors/QueryPlan/MergingSortedStepExt.h>
#include <Query/Processors/QueryPlan/PartialSortingStepExt.h>
#include <Query/Processors/QueryPlan/TotalsHavingStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

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

Aggregator::Params createAggregatorParams()
{
    Names keys;
    AggregateDescriptions aggregates;

    return Aggregator::Params(keys, aggregates, false, 5, 0, 0.5);
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
    // std::cout << new_agg_step->getName() << std::endl;
    EXPECT_EQ(agg_step->getName(), new_agg_step->getName());
    EXPECT_EQ(
        dynamic_cast<AggregatingStepExt *>(agg_step.get())->getParams().src_header.dumpStructure(),
        dynamic_cast<AggregatingStepExt *>(new_agg_step.get())->getParams().src_header.dumpStructure());
}

void TestSingleSimpleStep(QueryPlanStepPtr step)
{
    auto new_step = serializeQueryPlanStep(step);
    std::cout << new_step->getName() << std::endl;
    EXPECT_EQ(step->getName(), new_step->getName());
}

QueryPlanStepPtr createReadNothingStep()
{
    Block block = createBlock();
    return std::make_unique<ReadNothingStep>(block);
}

QueryPlanStepPtr createPartialSortingStepExt()
{
    DataStream stream = createDataStream();
    SortDescription desc = createSortDescription();
    SizeLimits limits = createSizeLimits();
    return std::make_unique<PartialSortingStepExt>(stream, desc, 0, limits);
}

QueryPlanStepPtr createOffsetStep()
{
    DataStream stream = createDataStream();
    return std::make_unique<OffsetStep>(stream, 0);
}

QueryPlanStepPtr createMergingSortedStepExt()
{
    DataStream stream = createDataStream();
    SortDescription desc = createSortDescription();
    return std::make_unique<MergingSortedStepExt>(stream, desc, 0, 0);
}

QueryPlanStepPtr createMergeSortingStepExt()
{
    DataStream stream = createDataStream();
    SortDescription desc = createSortDescription();
    return std::make_unique<MergeSortingStepExt>(stream, desc, 0, 0, 0, 0, 0, nullptr, 0);
}

QueryPlanStepPtr createLimitStepExt()
{
    DataStream stream = createDataStream();
    return std::make_unique<LimitStepExt>(stream, UInt64(0), UInt64(0));
}

QueryPlanStepPtr createLimitByStep()
{
    DataStream stream = createDataStream();
    Names columns;
    return std::make_unique<LimitByStep>(stream, 0, 0, columns);
}

QueryPlanStepPtr createFinishSortingStepExt()
{
    DataStream stream = createDataStream();
    SortDescription desc1 = createSortDescription();
    SortDescription desc2 = createSortDescription();
    return std::make_unique<FinishSortingStepExt>(stream, desc1, desc2, 0, 0);
}

QueryPlanStepPtr createFillingStep()
{
    DataStream stream = createDataStream();
    stream.has_single_port = true;
    SortDescription desc1 = createSortDescription();
    SortDescription desc2 = createSortDescription();
    return std::make_unique<FillingStep>(stream, desc1, desc2, nullptr, false);
}

QueryPlanStepPtr createExtremesStep()
{
    DataStream stream = createDataStream();
    return std::make_unique<ExtremesStep>(stream);
}

QueryPlanStepPtr createDistinctStepExt()
{
    DataStream stream = createDataStream();
    SizeLimits limits = createSizeLimits();
    Names columns;
    return std::make_unique<DistinctStepExt>(stream, limits, 0, columns, false, true, false);
}

QueryPlanStepPtr createUnionStepExt()
{
    DataStreams streams;
    streams.push_back(createDataStream());
    streams.push_back(createDataStream());
    return std::make_unique<UnionStepExt>(streams);
}

QueryPlanStepPtr createMergingAggregatedStepExt()
{
    DataStream stream = createDataStream();
    Names keys;
    GroupingSetsParamsExtList grouping_sets_params;
    GroupingDescriptions groupings;
    Aggregator::Params params = createAggregatorParams();
    SortDescription desc = createSortDescription();
    return std::make_unique<MergingAggregatedStepExt>(
        stream, grouping_sets_params, groupings, false, params, false, 0, 0, 0, 0, desc, false);
}

// todo: hongzhigao1, other feat: if migrate CubeStep or not
// QueryPlanStepPtr createCubeStep()
// {
//     DataStream stream = createDataStream();
//     AggregatingTransformParamsExtPtr params = std::make_shared<AggregatingTransformParamsExt>(createAggregatorExtParams(), true);
//     return std::make_unique<CubeStep>(stream, params, false, false);
// }

// todo: hongzhigao1, other feat: if migrate RollupStep or not
// QueryPlanStepPtr createRollupStep()
// {
//     DataStream stream = createDataStream();
//     AggregatingTransformParamsExtPtr params = std::make_shared<AggregatingTransformParamsExt>(createAggregatorExtParams(), true);
//     return std::make_unique<RollupStep>(stream, params);
// }

TEST(QueryPlanTest, SimpleStepTest)
{
    TestSingleSimpleStep(createReadNothingStep());
    TestSingleSimpleStep(createPartialSortingStepExt());
    TestSingleSimpleStep(createOffsetStep());
    TestSingleSimpleStep(createMergeSortingStepExt());
    TestSingleSimpleStep(createMergingSortedStepExt());
    TestSingleSimpleStep(createLimitStepExt());
    TestSingleSimpleStep(createLimitByStep());
    TestSingleSimpleStep(createFinishSortingStepExt());
    TestSingleSimpleStep(createFillingStep());
    TestSingleSimpleStep(createExtremesStep());
    TestSingleSimpleStep(createDistinctStepExt());
    TestSingleSimpleStep(createUnionStepExt());

    TestSingleSimpleStep(createMergingAggregatedStepExt());
    // TestSingleSimpleStep(createCubeStep());
    // TestSingleSimpleStep(createRollupStep());
}

ActionsDAGPtr createActionsDAG()
{
    auto actions_dag = std::make_shared<ActionsDAG>();
    const auto & context = getContext().context;

    tryRegisterFunctions();
    auto & factory = FunctionFactory::instance();
    auto function_builder = factory.get("lower", context);

    ColumnWithTypeAndName column;
    column.name = "TEST";

    DataTypePtr type = DataTypeFactory::instance().get("String");
    column.column = type->createColumnConst(1, Field("TEST CONSTANT"));
    column.type = type;

    actions_dag->addColumn(column);

    ActionsDAG::NodeRawConstPtrs children;
    children.push_back(&actions_dag->getNodes().back());
    actions_dag->addFunction(function_builder, std::move(children), "lower()");

    return actions_dag;
}

void TestSingleActionsStep(QueryPlanStepPtr step)
{
    auto new_step = serializeQueryPlanStep(step);
    std::cout << new_step->getName() << std::endl;

    EXPECT_EQ(step->getName(), new_step->getName());

    // todo test for others
}

QueryPlanStepPtr createExpressionStep()
{
    DataStream stream = createDataStream();
    ActionsDAGPtr actions = createActionsDAG();

    return std::make_unique<ExpressionStep>(stream, std::move(actions));
}

QueryPlanStepPtr createFilterStepExt()
{
    DataStream stream = createDataStream();
    ActionsDAGPtr actions = createActionsDAG();

    return std::make_unique<FilterStepExt>(stream, std::move(actions), "RES", false);
}

QueryPlanStepPtr createTotalsHavingStepExt()
{
    DataStream stream = createDataStream();
    ActionsDAGPtr actions = createActionsDAG();

    return std::make_unique<TotalsHavingStepExt>(
        stream, AggregateDescriptions{}, false, nullptr, std::move(actions), "TEST", false, TotalsMode::AFTER_HAVING_AUTO, 1.0, false);
}

QueryPlanStepPtr createArrayJoinStep()
{
    const auto & context = getContext().context;

    auto val = ColumnUInt32::create();
    auto off = ColumnUInt64::create();

    ColumnsWithTypeAndName columns;
    columns.emplace_back(ColumnWithTypeAndName(
        ColumnArray::create(std::move(val), std::move(off)), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()), "Array"));

    return std::make_unique<ArrayJoinStep>(
        DataStream{.header = Block(columns)}, std::make_shared<ArrayJoinAction>(NameSet{"Array"}, false, context));
}

// actions dag is not supported in protobuf
// TEST(QueryPlanTest, ActionsStepTest)
// {
//     TestSingleActionsStep(createExpressionStep());
//     TestSingleActionsStep(createFilterStepExt());
//     TestSingleActionsStep(createTotalsHavingStepExt());
//
//     TestSingleActionsStep(createArrayJoinStep());
// }
