#include <random>
#include <Query/Core/tests/gtest_protobuf_common.h>
#include <Query/Protos/plan_node.pb.h>

#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>

#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>

#include <Query/ProtosHelper/ExchangeMode.h>

using namespace DB;
using namespace DB::UnitTest;

TEST_F(ProtobufTest, AssignUniqueIdStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto unique_id = fmt::format("text{}", eng() % 100);
        auto result = std::make_shared<AssignUniqueIdStepExt>(base_input_stream, unique_id);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::AssignUniqueIdStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = AssignUniqueIdStepExt::fromProto(pb, ProtobufTest::context);
    // re-serialize to protobuf
    Protos::AssignUniqueIdStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, EnforceSingleRowStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto result = std::make_shared<EnforceSingleRowStepExt>(base_input_stream);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::EnforceSingleRowStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::EnforceSingleRowStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ExtremesStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto result = std::make_shared<ExtremesStep>(base_input_stream);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::ExtremesStep pb;
    QueryPlanStepHelper::toProto(*step, pb);
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ExtremesStep pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, FillingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        base_input_stream.has_single_port = true; // make ctor happy
        SortDescription sort_description;
        SortDescription fill_description;
        for (int i = 0; i < 2; ++i)
        {
            auto desc = generateSortColumnDescription(eng);
            fill_description.emplace_back(desc);
            sort_description.emplace_back(desc);
        }

        auto result = std::make_shared<FillingStep>(base_input_stream, sort_description, fill_description, nullptr, false);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::FillingStep pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::FillingStep pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

// todo
TEST_F(ProtobufTest, FinishSortingStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription prefix_description;
        for (int i = 0; i < 2; ++i)
            prefix_description.emplace_back(generateSortColumnDescription(eng));
        SortDescription result_description;
        for (int i = 0; i < 2; ++i)
            result_description.emplace_back(generateSortColumnDescription(eng));
        auto max_block_size = eng() % 1000;
        auto limit = eng() % 1000;
        auto result = std::make_shared<FinishSortingStepExt>(base_input_stream, prefix_description, result_description, max_block_size, limit);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::FinishSortingStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::FinishSortingStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, LimitByStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto group_length = eng() % 1000;
        auto group_offset = eng() % 1000;
        Names columns;
        for (int i = 0; i < 10; ++i)
            columns.emplace_back(fmt::format("text{}", eng() % 100));
        auto result = std::make_shared<LimitByStep>(base_input_stream, group_length, group_offset, columns);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::LimitByStep pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::LimitByStep pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, LimitStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto limit = eng() % 1000;
        auto offset = eng() % 1000;
        auto always_read_till_end = eng() % 2 == 1;
        auto with_ties = eng() % 2 == 1;
        SortDescription description;
        for (int i = 0; i < 2; ++i)
            description.emplace_back(generateSortColumnDescription(eng));
        auto partial = eng() % 2 == 1;
        auto result = std::make_shared<LimitStepExt>(base_input_stream, limit, offset, always_read_till_end, with_ties, description, partial);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::LimitStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::LimitStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, MarkDistinctStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto marker_symbol = fmt::format("text{}", eng() % 100);
        std::vector<String> distinct_symbols;
        for (int i = 0; i < 10; ++i)
            distinct_symbols.emplace_back(fmt::format("text{}", eng() % 100));
        auto result = std::make_shared<MarkDistinctStepExt>(base_input_stream, marker_symbol, distinct_symbols);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::MarkDistinctStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::MarkDistinctStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, MergingSortedStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription sort_description;
        for (int i = 0; i < 2; ++i)
            sort_description.emplace_back(generateSortColumnDescription(eng));
        auto max_block_size = eng() % 1000;
        auto limit = eng() % 1000;
        auto result = std::make_shared<MergingSortedStepExt>(base_input_stream, sort_description, max_block_size, limit);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::MergingSortedStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::MergingSortedStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, OffsetStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto offset = eng() % 1000;
        auto result = std::make_shared<OffsetStep>(base_input_stream, offset);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::OffsetStep pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::OffsetStep pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, SortingStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription result_description;
        for (int i = 0; i < 2; ++i)
            result_description.emplace_back(generateSortColumnDescription(eng));
        auto limit = eng() % 1000;
        SortDescription prefix_description;
        for (int i = 0; i < 2; ++i)
            prefix_description.emplace_back(generateSortColumnDescription(eng));
        auto result = std::make_shared<SortingStepExt>(base_input_stream, result_description, limit, SortingStepExt::Stage::FULL, prefix_description);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::SortingStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::SortingStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ValuesStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto base_output_header = generateBlock(eng);
        Fields fields;
        for (int i = 0; i < 2; ++i)
            fields.emplace_back(generateField(eng));
        auto rows = eng() % 1000;
        auto result = std::make_shared<ValuesStepExt>(base_output_header, fields, rows);

        return result;
    }();

    // serialize to protobuf
    Protos::ValuesStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ValuesStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, UnionStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto [base_input_streams, base_output_stream, output_to_inputs] = generateSetOperationStep(eng);
        auto max_threads = eng() % 1000;
        auto local = eng() % 2 == 1;
        auto result = std::make_shared<UnionStepExt>(base_input_streams, base_output_stream, output_to_inputs, max_threads, local);

        return result;
    }();

    // serialize to protobuf
    Protos::UnionStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::UnionStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ExchangeStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        DataStreams input_streams;
        for (int i = 0; i < 2; ++i)
            input_streams.emplace_back(generateDataStream(eng));
        auto exchange_type = static_cast<RExchangeMode::Enum>(eng() % 3);
        auto schema = generatePartitioning(eng);
        auto keep_order = eng() % 2 == 1;
        auto result = std::make_shared<ExchangeStepExt>(input_streams, exchange_type, schema, keep_order);

        return result;
    }();

    // serialize to protobuf
    Protos::ExchangeStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ExchangeStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, CTERefStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto base_output_header = generateBlock(eng);
        auto id = eng() % 1000;
        std::unordered_map<String, String> output_columns;
        for (int i = 0; i < 10; ++i)
            output_columns[fmt::format("text{}", eng() % 100)] = fmt::format("text{}", eng() % 100);
        auto has_filter = eng() % 2 == 1;
        auto result = std::make_shared<CTERefStepExt>(base_output_header, id, output_columns, has_filter);

        return result;
    }();

    // serialize to protobuf
    Protos::CTERefStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::CTERefStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, DistinctStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto set_size_limits = generateSizeLimits(eng);
        auto limit_hint = eng() % 1000;
        Names columns;
        for (int i = 0; i < 10; ++i)
            columns.emplace_back(fmt::format("text{}", eng() % 100));
        auto pre_distinct = eng() % 2 == 1;
        auto result = std::make_shared<DistinctStepExt>(base_input_stream, set_size_limits, limit_hint, columns, pre_distinct, true, true);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::DistinctStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::DistinctStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, PartialSortingStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription sort_description;
        for (int i = 0; i < 2; ++i)
            sort_description.emplace_back(generateSortColumnDescription(eng));
        auto limit = eng() % 1000;
        auto size_limits = generateSizeLimits(eng);
        auto result = std::make_shared<PartialSortingStepExt>(base_input_stream, sort_description, limit, size_limits);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::PartialSortingStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::PartialSortingStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, PartitionTopNStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        Names partition;
        for (int i = 0; i < 10; ++i)
            partition.emplace_back(fmt::format("text{}", eng() % 100));
        Names order_by;
        for (int i = 0; i < 10; ++i)
            order_by.emplace_back(fmt::format("text{}", eng() % 100));
        auto limit = eng() % 1000;
        auto model = static_cast<TopNModel>(eng() % 3);
        auto result = std::make_shared<PartitionTopNStepExt>(base_input_stream, partition, order_by, limit, model);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::PartitionTopNStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::PartitionTopNStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ReadNothingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto base_output_header = generateBlock(eng);
        auto result = std::make_shared<ReadNothingStep>(base_output_header);

        return result;
    }();

    // serialize to protobuf
    Protos::ReadNothingStep pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ReadNothingStep pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, TopNFilteringStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription sort_description;
        for (int i = 0; i < 2; ++i)
            sort_description.emplace_back(generateSortColumnDescription(eng));
        auto size = eng() % 1000;
        auto model = static_cast<TopNModel>(eng() % 3);
        auto res = std::make_shared<TopNFilteringStepExt>(base_input_stream, sort_description, size, model);
        res->setStepDescription(step_description);
        return res;
    }();

    // serialize to protobuf
    Protos::TopNFilteringStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::TopNFilteringStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, FilterStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto filter = generateAST(eng);
        auto remove_filter_column = eng() % 2 == 1;
        auto s = std::make_shared<FilterStepExt>(base_input_stream, filter, remove_filter_column);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::FilterStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::FilterStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    ASSERT_EQ(pb2.filter().blob(), pb.filter().blob());
    ASSERT_EQ(pb2.filter().text(), pb.filter().text());

    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ProjectionStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto assignments = generateAssignments(eng);
        NameToType name_to_type;
        for (const auto & [k, v] : assignments)
        {
            name_to_type[k] = test_data_types[eng() % 3];
            (void)v;
        }
        auto final_project = eng() % 2 == 1;
        auto s = std::make_shared<ProjectionStepExt>(base_input_stream, assignments, name_to_type, final_project);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::ProjectionStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ProjectionStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, JoinStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        DataStreams input_streams;
        for (int i = 0; i < 2; ++i)
            input_streams.emplace_back(generateDataStream(eng));
        DataStream output_stream;
        output_stream = generateDataStream(eng);
        auto step_description = fmt::format("text{}", eng() % 100);
        auto kind = static_cast<JoinKind>(eng() % 3);
        auto strictness = static_cast<JoinStrictness>(eng() % 3);
        auto max_streams = eng() % 1000;
        auto keep_left_read_in_order = eng() % 2 == 1;
        Names left_keys;
        for (int i = 0; i < 10; ++i)
            left_keys.emplace_back(fmt::format("text{}", eng() % 100));
        Names right_keys;
        for (int i = 0; i < 10; ++i)
            right_keys.emplace_back(fmt::format("text{}", eng() % 100));
        std::vector<bool> key_ids_null_safe;
        for (size_t i = 0; i < left_keys.size(); ++i)
            key_ids_null_safe.emplace_back(eng() % 2 == 0);
        auto filter = generateAST(eng);
        auto has_using = eng() % 2 == 1;
        std::optional<std::vector<bool>> require_right_keys;
        if (eng() % 2 == 0)
            require_right_keys = std::vector<bool>({true, false});
        auto asof_inequality = static_cast<ASOFJoinInequality>(eng() % 3);
        auto distribution_type = static_cast<DistributionType>(eng() % 3);
        auto join_algorithm = static_cast<JoinAlgorithm>(eng() % 3);
        auto is_magic = eng() % 2 == 1;
        auto is_ordered = eng() % 2 == 1;
        auto simple_reordered = false;
        LinkedHashMap<String, RuntimeFilter> runtime_filter_builders;
        runtime_filter_builders.emplace("a", generateRuntimeFilterBuildInfos(eng));
        runtime_filter_builders.emplace("b", generateRuntimeFilterBuildInfos(eng));
        auto s = std::make_shared<JoinStepExt>(
            input_streams,
            output_stream,
            kind,
            strictness,
            max_streams,
            keep_left_read_in_order,
            left_keys,
            right_keys,
            key_ids_null_safe,
            filter,
            has_using,
            require_right_keys,
            asof_inequality,
            distribution_type,
            join_algorithm,
            is_magic,
            is_ordered,
            simple_reordered);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::JoinStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::JoinStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

// todo: lizhuoyu, need optimizer MergeSortingStepExt
// TEST_F(ProtobufTest, MergeSortingStepExt)
// {
//     std::default_random_engine eng(42);
//     // construct valid step
//     auto step = [&eng] {
//         std::string step_description = fmt::format("description {}", eng() % 100);
//         auto base_input_stream = generateDataStream(eng);
//         SortDescription description;
//         for (int i = 0; i < 2; ++i)
//             description.emplace_back(generateSortColumnDescription(eng));
//         auto max_merged_block_size = eng() % 1000;
//         auto limit = eng() % 1000;
//         auto max_bytes_before_remerge = eng() % 1000;
//         auto remerge_lowered_memory_bytes_ratio = eng() % 100 * 0.01;
//         auto max_bytes_before_external_sort = eng() % 1000;
//         auto tmp_volume = context ? context->getTemporaryVolume() : nullptr;
//         auto min_free_disk_space = eng() % 1000;
//         auto enable_auto_spill = eng() % 2;
//         auto s = std::make_shared<MergeSortingStepExt>(
//             base_input_stream,
//             description,
//             max_merged_block_size,
//             limit,
//             max_bytes_before_remerge,
//             remerge_lowered_memory_bytes_ratio,
//             max_bytes_before_external_sort,
//             tmp_volume,
//             min_free_disk_space,
//             enable_auto_spill);
//         s->setStepDescription(step_description);
//         return s;
//     }();
//
//     // serialize to protobuf
//     Protos::MergeSortingStepExt pb;
//     QueryPlanStepHelper::toProto(*step, pb);;
//     // deserialize from protobuf
//     auto step2 = QueryPlanStepHelper::fromProto(pb, context);
//     // re-serialize to protobuf
//     Protos::MergeSortingStepExt pb2;
//     QueryPlanStepHelper::toProto(*step2, pb2);
//     compareProto(pb, pb2);
//     compareStep(step, step2);
// }

TEST_F(ProtobufTest, MergingAggregatedStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        NameSet distinct_keys;
        for (int i = 0; i < 10; ++i)
            distinct_keys.emplace(fmt::format("text{}", eng() % 100));
        Names keys{distinct_keys.begin(), distinct_keys.end()};
        GroupingSetsParamsExtList grouping_sets_params;
        for (int i = 0; i < 2; ++i)
            grouping_sets_params.emplace_back(generateGroupingSetsParams(eng));
        GroupingDescriptions groupings;
        for (int i = 0; i < 2; ++i)
            groupings.emplace_back(generateGroupingDescription(eng));
        auto params = generateAggregatorParams(base_input_stream.header, eng);
        auto final = eng() % 2 == 1;
        auto memory_efficient_aggregation = eng() % 2 == 1;
        auto max_threads = eng() % 1000;
        auto memory_efficient_merge_threads = eng() % 1000;
        auto s = std::make_shared<MergingAggregatedStepExt>(
            base_input_stream,
            grouping_sets_params,
            groupings,
            final,
            params,
            memory_efficient_aggregation,
            max_threads,
            memory_efficient_merge_threads,
            8196,
            0,
            SortDescription{},
            false);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::MergingAggregatedStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::MergingAggregatedStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, AggregatingStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        NameSet distinct_keys;
        for (int i = 0; i < 10; ++i)
            distinct_keys.emplace(fmt::format("text{}", eng() % 100));
        Names keys{distinct_keys.begin(), distinct_keys.end()};
        NameSet keys_not_hashed;
        for (int i = 0; i < 10; ++i)
            keys_not_hashed.emplace(fmt::format("text{}", eng() % 100));
        auto params = generateAggregatorParamsExt(eng);
        GroupingSetsParamsExtList grouping_sets_params;
        for (int i = 0; i < 2; ++i)
            grouping_sets_params.emplace_back(generateGroupingSetsParams(eng));
        auto final = eng() % 2 == 1;
        auto max_block_size = eng() % 1000;
        auto merge_threads = eng() % 1000;
        auto temporary_data_merge_threads = eng() % 1000;
        auto storage_has_evenly_distributed_read = eng() % 2 == 1;
        auto group_by_info = generateInputOrderInfo(eng);
        SortDescriptionWithPositions group_by_sort_description;
        for (int i = 0; i < 2; ++i)
            group_by_sort_description.emplace_back(generateSortColumnDescription(eng), 0);
        GroupingDescriptions groupings;
        for (int i = 0; i < 2; ++i)
            groupings.emplace_back(generateGroupingDescription(eng));
        auto should_produce_results_in_order_of_bucket_number = eng() % 2 == 1;
        auto s = std::make_shared<AggregatingStepExt>(
            base_input_stream,
            keys,
            keys_not_hashed,
            params,
            grouping_sets_params,
            final,
            AggregateStagePolicy::DEFAULT,
            max_block_size,
            merge_threads,
            temporary_data_merge_threads,
            storage_has_evenly_distributed_read,
            group_by_info,
            group_by_sort_description,
            groupings,
            false,
            should_produce_results_in_order_of_bucket_number);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::AggregatingStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::AggregatingStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ArrayJoinStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng, true);
        auto array_join = generateArrayJoinAction(eng);
        auto s = std::make_shared<ArrayJoinStep>(base_input_stream, array_join);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::ArrayJoinStep pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ArrayJoinStep pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, TableScanStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        auto storage_id = test_storage_ids[eng() % 3];
        NamesWithAliases column_alias = {{"a", "aaa"}, {"b", "bbb"}};
        auto query_info = generateSelectQueryInfo(eng);
        auto max_block_size = eng() % 1000;
        std::shared_ptr<AggregatingStepExt> pushdown_aggregation = nullptr;
        std::shared_ptr<ProjectionStepExt> pushdown_projection = nullptr;
        std::shared_ptr<ProjectionStepExt> pushdown_index_projection = nullptr;
        std::shared_ptr<FilterStepExt> pushdown_filter = nullptr;

        auto s = std::make_shared<TableScanStepExt>(
            context,
            storage_id,
            column_alias,
            query_info,
            max_block_size,
            String{} /*alias*/,
            false,

            Assignments{},
            pushdown_aggregation,
            pushdown_projection,
            pushdown_filter);

        return s;
    }();

    // serialize to protobuf
    Protos::TableScanStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::TableScanStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, RemoteExchangeSourceStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        auto input_stream = generateDataStream(eng);
        auto step_description = fmt::format("text{}", eng() % 100);
        auto inputs = std::vector{generatePlanSegmentInput(eng)};
        auto s = std::make_shared<RemoteExchangeSourceStepExt>(inputs, input_stream, false, false);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::RemoteExchangeSourceStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::RemoteExchangeSourceStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, FinalSampleStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto sample_size = eng() % 1000;
        auto max_chunk_size = eng() % 1000;
        auto s = std::make_shared<FinalSampleStepExt>(base_input_stream, sample_size, max_chunk_size);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::FinalSampleStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::FinalSampleStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ReadStorageRowCountStepExt)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        auto base_output_header = generateBlock(eng);
        auto storage_id = test_storage_ids[eng() % 3];
        auto query = generateAST(eng);
        auto agg_desc = generateAggregateDescription(eng, 0);
        // auto num_rows = eng() % 1000;
        auto is_final_agg = false;
        auto s = std::make_shared<ReadStorageRowCountStepExt>(base_output_header, query, agg_desc, is_final_agg, storage_id, nullptr);

        return s;
    }();

    // serialize to protobuf
    Protos::ReadStorageRowCountStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ReadStorageRowCountStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, WindowStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        WindowDescription desc;
        Names originalColumns = {
            "a",
            "b",
            "c",
        };
        Names countOutputs = {
            "c",
        };
        Names markers = {
            "d",
        };
        String row_number_symbol("e");
        for (const auto & column : originalColumns)
        {
            desc.partition_by.push_back(SortColumnDescription(column, 1 /* direction */, 1 /* nulls_direction */));
        }
        for (const auto & column : originalColumns)
        {
            desc.full_sort_description.push_back(SortColumnDescription(column, 1 /* direction */, 1 /* nulls_direction */));
        }
        WindowFrame default_frame{
            true,
            WindowFrame::FrameType::RANGE,
            WindowFrame::BoundaryType::Unbounded,
            0,
            true,
            WindowFrame::BoundaryType::Current,
            0,
            false};
        desc.frame = default_frame;
        auto output_stream = generateDataStream(eng);

        std::vector<WindowFunctionDescription> functions;
        for (size_t i = 0; i < markers.size(); i++)
        {
            String output = countOutputs.at(i);

            Names argument_names = {markers[i]};
            DataTypes types{std::make_shared<DataTypeUInt8>()};
            Array params;
            AggregateFunctionProperties properties;
            AggregateFunctionPtr aggregate_function = AggregateFunctionFactory::instance().get("sum", NullsAction::EMPTY, types, params, properties);
            WindowFunctionDescription function{output, nullptr, aggregate_function, params, types, argument_names};
            functions.emplace_back(function);
        }
        {
            Names argument_names;
            DataTypes types;
            Array params;
            AggregateFunctionProperties properties;

            AggregateFunctionPtr aggregate_function = AggregateFunctionFactory::instance().get("row_number", NullsAction::EMPTY, types, params, properties);
            WindowFunctionDescription function{row_number_symbol, nullptr, aggregate_function, params, types, argument_names};
            functions.emplace_back(function);
        }

        auto s = std::make_shared<WindowStep>(output_stream, desc, functions, true);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::WindowStep pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::WindowStep pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, IntersectOrExceptStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        DataStreams input_streams;
        for (int i = 0; i < 2; ++i)
            input_streams.emplace_back(generateDataStream(eng));
        auto current_operator = static_cast<IntersectOrExceptStep::Operator>(eng() % 3);
        auto max_threads = eng() % 1000;
        auto s = std::make_shared<IntersectOrExceptStep>(input_streams, current_operator, max_threads);
        return s;
    }();

    // serialize to protobuf
    Protos::IntersectOrExceptStep pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::IntersectOrExceptStep pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, BufferStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto s = std::make_shared<BufferStepExt>(base_input_stream);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::BufferStepExt pb;
    QueryPlanStepHelper::toProto(*step, pb);;
    // deserialize from protobuf
    auto step2 = QueryPlanStepHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::BufferStepExt pb2;
    QueryPlanStepHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}
