#include <Query/Core/tests/gtest_protobuf_common.h>
#include <Query/Protos/plan_node.pb.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/ProtosHelper/FieldHelper.h>


using namespace DB;
using namespace DB::UnitTest;

ContextMutablePtr ProtobufTest::session_context;
ContextMutablePtr ProtobufTest::context;
DataTypes ProtobufTest::test_data_types;
std::vector<NameAndTypePair> ProtobufTest::test_name_and_type_pairs;
std::vector<StorageID> ProtobufTest::test_storage_ids;

TEST_F(ProtobufTest, Field)
{
    std::default_random_engine eng(42);
    for (int i = 0; i < 10; ++i)
    {
        auto obj = generateField(eng);
        Protos::Field pb;
        FieldToProto(obj, pb);
        Field obj2;
        FieldFillFromProto(obj2, pb);
        ASSERT_EQ(obj, obj2);
        Protos::Field pb2;
        FieldToProto(obj2, pb2);
        compareProto(pb, pb2);
    }
}

TEST_F(ProtobufTest, FillColumnDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateFillColumnDescription(eng);
    // serialize to protobuf
    Protos::FillColumnDescription pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    FillColumnDescription obj2;
    ProtosSerDerHelper::fillFromProto(obj2, pb);
    // re-serialize to protobuf
    Protos::FillColumnDescription pb2;
    ProtosSerDerHelper::toProto(obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, SortColumnDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateSortColumnDescription(eng);
    // serialize to protobuf
    Protos::SortColumnDescription pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    SortColumnDescription obj2;
    ProtosSerDerHelper::fillFromProto(obj2, pb);
    // re-serialize to protobuf
    Protos::SortColumnDescription pb2;
    ProtosSerDerHelper::toProto(obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, Block)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateBlock(eng);
    // serialize to protobuf
    Protos::Block pb;
    serializeHeaderToProto(obj, pb);
    // deserialize from protobuf
    Block obj2 = deserializeHeaderFromProto(pb);
    // re-serialize to protobuf
    Protos::Block pb2;
    serializeHeaderToProto(obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, DataStream)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateDataStream(eng);
    // serialize to protobuf
    Protos::DataStream pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    DataStream obj2;
    ProtosSerDerHelper::fillFromProto(obj2, pb);
    // re-serialize to protobuf
    Protos::DataStream pb2;
    ProtosSerDerHelper::toProto(obj2, pb2);
    std::string str, str2;
    pb.SerializeToString(&str);
    pb2.SerializeToString(&str2);
    std::string dbg_str;
    std::string dbg_str2;
    google::protobuf::TextFormat::PrintToString(pb, &dbg_str);
    google::protobuf::TextFormat::PrintToString(pb2, &dbg_str2);
    ASSERT_EQ(dbg_str, dbg_str2);
    ASSERT_EQ(str, str2);
}

TEST_F(ProtobufTest, SizeLimits)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateSizeLimits(eng);
    // serialize to protobuf
    Protos::SizeLimits pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    SizeLimits obj2;
    ProtosSerDerHelper::fillFromProto(obj2, pb);
    // re-serialize to protobuf
    Protos::SizeLimits pb2;
    ProtosSerDerHelper::toProto(obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, Partitioning)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generatePartitioning(eng);
    // serialize to protobuf
    Protos::Partitioning pb;
    obj.toProto(pb);
    // deserialize from protobuf
    DB::Partitioning obj2 = DB::Partitioning::fromProto(pb);
    // re-serialize to protobuf
    Protos::Partitioning pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, NameAndTypePair)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateNameAndTypePair(eng);
    // serialize to protobuf
    Protos::NameAndTypePair pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    NameAndTypePair obj2;
    ProtosSerDerHelper::fillFromProto(obj2, pb);
    // re-serialize to protobuf
    Protos::NameAndTypePair pb2;
    ProtosSerDerHelper::toProto(obj2, pb2);
    compareProto(pb, pb2);
}


TEST_F(ProtobufTest, GroupingSetsParamsExt)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateGroupingSetsParams(eng);
    // serialize to protobuf
    Protos::GroupingSetsParamsExt pb;
    obj.toProto(pb);
    // deserialize from protobuf
    GroupingSetsParamsExt obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::GroupingSetsParamsExt pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, GroupingDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateGroupingDescription(eng);
    // serialize to protobuf
    Protos::GroupingDescription pb;
    obj.toProto(pb);
    // deserialize from protobuf
    GroupingDescription obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::GroupingDescription pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, AggregateDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateAggregateDescription(eng, 6);
    // serialize to protobuf
    Protos::AggregateDescription pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    AggregateDescription obj2;
    ProtosSerDerHelper::fillFromProto(obj2, pb);
    // re-serialize to protobuf
    Protos::AggregateDescription pb2;
    ProtosSerDerHelper::toProto(obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, AggregatorParams)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateAggregatorParams(eng);

    // serialize to protobuf
    Protos::AggregatorExtParams pb;
    step.toProto(pb);
    // deserialize from protobuf
    auto step2 = AggregatorExt::Params::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::AggregatorExtParams pb2;
    step2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, AggregatingTransformParams)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateAggregatingTransformParams(eng);

    // serialize to protobuf
    Protos::AggregatingTransformParamsExt pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = AggregatingTransformParamsExt::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::AggregatingTransformParamsExt pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, ArrayJoinAction)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateArrayJoinAction(eng);

    // serialize to protobuf
    Protos::ArrayJoinAction pb;
    ProtosSerDerHelper::toProto(*step, pb);
    // deserialize from protobuf
    auto step2 = ProtosSerDerHelper::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ArrayJoinAction pb2;
    ProtosSerDerHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, SelectQueryInfo)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateSelectQueryInfo(eng);
    // serialize to protobuf
    Protos::SelectQueryInfo pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    SelectQueryInfo obj2;
    ProtosSerDerHelper::fillFromProto(obj2, pb);
    // re-serialize to protobuf
    Protos::SelectQueryInfo pb2;
    ProtosSerDerHelper::toProto(obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, AddressInfo)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateAddressInfo(eng);
    // serialize to protobuf
    Protos::AddressInfo pb;
    obj.toProto(pb);
    // deserialize from protobuf
    AddressInfo obj2;
    obj2.fromProto(pb);
    // re-serialize to protobuf
    Protos::AddressInfo pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, PlanSegmentInput)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generatePlanSegmentInput(eng);
    // serialize to protobuf
    Protos::PlanSegmentInput pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = std::make_shared<PlanSegmentInput>();
    step2->fromProto(pb, context);
    // re-serialize to protobuf
    Protos::PlanSegmentInput pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, PlanSegmentOutput)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto output = generatePlanSegmentOutput(eng);
    // serialize to protobuf
    Protos::PlanSegmentOutput pb;
    output->toProto(pb);
    // deserialize from protobuf
    auto output2 = std::make_shared<PlanSegmentOutput>();
    output2->fromProto(pb);
    // re-serialize to protobuf
    Protos::PlanSegmentOutput pb2;
    output2->toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, InputOrderInfo)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateInputOrderInfo(eng);

    // serialize to protobuf
    Protos::InputOrderInfo pb;
    ProtosSerDerHelper::toProto(*step, pb);
    // deserialize from protobuf
    auto step2 = ProtosSerDerHelper::fillFromProto(pb);
    // re-serialize to protobuf
    Protos::InputOrderInfo pb2;
    ProtosSerDerHelper::toProto(*step2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, WindowFunctionDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateWindowFunctionDescription(eng);
    // serialize to protobuf
    Protos::WindowFunctionDescription pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    auto obj2 = ProtosSerDerHelper::fillFromProto(pb);
    // re-serialize to protobuf
    Protos::WindowFunctionDescription pb2;
    ProtosSerDerHelper::toProto(*obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, WindowDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateWindowDescription(eng);
    // serialize to protobuf
    Protos::WindowDescription pb;
    ProtosSerDerHelper::toProto(obj, pb);
    // deserialize from protobuf
    auto obj2 = ProtosSerDerHelper::fillFromProto(pb);
    // re-serialize to protobuf
    Protos::WindowDescription pb2;
    ProtosSerDerHelper::toProto(*obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, RuntimeFilterBuildInfos)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateRuntimeFilterBuildInfos(eng);
    // serialize to protobuf
    Protos::RuntimeFilter pb;
    obj.toProto(pb);
    // deserialize from protobuf
    auto obj2 = RuntimeFilter::fromProto(pb);
    // re-serialize to protobuf
    Protos::RuntimeFilter pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}
