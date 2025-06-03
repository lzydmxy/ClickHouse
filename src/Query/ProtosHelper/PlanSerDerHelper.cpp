#include "PlanSerDerHelper.h"

#include <google/protobuf/util/message_differencer.h>
#include <Common/SipHash.h>
#include <Common/ClickHouseRevision.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/queryToString.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/TableJoin.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/ASTSerDerHelper.h>
#include <Query/ProtosHelper/DataTypeHelper.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/ProtosHelper/ProgressHelper.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Protos/ReadWriteProtobuf.h>


//#include <Protos/ReadWriteProtobuf.h>
// #include <Processors/QueryPlan/AggregatingStep.h>
// #include <Processors/QueryPlan/ApplyStep.h>
// #include <Processors/QueryPlan/ArrayJoinStep.h>
// #include <Processors/QueryPlan/AssignUniqueIdStep.h>
// #include <Processors/QueryPlan/BufferStep.h>
// #include <Processors/QueryPlan/CTERefStep.h>
// #include <Processors/QueryPlan/CreatingSetsStep.h>
// #include <Processors/QueryPlan/CubeStep.h>
// #include <Processors/QueryPlan/DistinctStep.h>
// #include <Processors/QueryPlan/EnforceSingleRowStep.h>
// #include <Processors/QueryPlan/ExceptStep.h>
// #include <Processors/QueryPlan/ExchangeStep.h>
// #include <Processors/QueryPlan/ExplainAnalyzeStep.h>
// #include <Processors/QueryPlan/ExpressionStep.h>
// #include <Processors/QueryPlan/ExtremesStep.h>
// #include <Processors/QueryPlan/FillingStep.h>
// #include <Processors/QueryPlan/FilterStep.h>
// #include <Processors/QueryPlan/FinalSampleStep.h>
// #include <Processors/QueryPlan/FinishSortingStep.h> 
// #include <Processors/QueryPlan/ISourceStep.h>
// #include <Processors/QueryPlan/ITransformingStep.h>
// #include <Processors/QueryPlan/IntersectOrExceptStep.h>
// #include <Processors/QueryPlan/IntersectStep.h>
// #include <Processors/QueryPlan/JoinStep.h>
// #include <Processors/QueryPlan/LimitByStep.h>
// #include <Processors/QueryPlan/LimitStep.h>
// #include <Processors/QueryPlan/LocalExchangeStep.h>
// #include <Processors/QueryPlan/MarkDistinctStep.h>
// #include <Processors/QueryPlan/MergeSortingStep.h>
// #include <Processors/QueryPlan/MergingAggregatedStep.h>
// #include <Processors/QueryPlan/MergingSortedStep.h>
// #include <Processors/QueryPlan/MultiJoinStep.h>
// #include <Processors/QueryPlan/ExpandStep.h>
// #include <Processors/QueryPlan/OffsetStep.h>
// #include <Processors/QueryPlan/OutfileWriteStep.h>
// #include <Processors/QueryPlan/OutfileFinishStep.h>
// #include <Processors/QueryPlan/PartialSortingStep.h>
// #include <Processors/QueryPlan/PartitionTopNStep.h>
// #include <Processors/QueryPlan/PlanSegmentSourceStep.h>
// #include <Processors/QueryPlan/ProjectionStep.h>
// #include <Processors/QueryPlan/ReadFromMergeTree.h>
// #include <Processors/QueryPlan/ReadFromPreparedSource.h>
// #include <Processors/QueryPlan/ReadNothingStep.h>
// #include <Processors/QueryPlan/ReadStorageRowCountStep.h>
// #include <Processors/QueryPlan/RemoteExchangeSourceStep.h>
// #include <Processors/QueryPlan/IntermediateResultCacheStep.h>
// #include <Processors/QueryPlan/RollupStep.h>
// #include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
// #include <Processors/QueryPlan/SortingStep.h>
// #include <Processors/QueryPlan/TableFinishStep.h>
// #include <Processors/QueryPlan/TableScanStep.h>
// #include <Processors/QueryPlan/TableWriteStep.h>
// #include <Processors/QueryPlan/TopNFilteringStep.h>
// #include <Processors/QueryPlan/TotalsHavingStep.h>
// #include <Processors/QueryPlan/UnionStep.h>
// #include <Processors/QueryPlan/ValuesStep.h>
// #include <Processors/QueryPlan/WindowStep.h>
// #include <Processors/QueryPlan/Assignment.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int PROTOBUF_BAD_CAST;
}

void serializeColumn(const ColumnPtr & column, const DataTypePtr & data_type, WriteBuffer & buf)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */

    writeBinary(isColumnConst(*column), buf);

    ColumnPtr full_column = column->convertToFullColumnIfConst();

    serializeDataType(data_type, buf);
    writeBinary(full_column->size(), buf);

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&buf](ISerialization::SubstreamPath) -> WriteBuffer * { return &buf; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0; //-V1048

    auto serialization = data_type->getDefaultSerialization();

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(*full_column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*full_column, 0, 0, settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);
}

ColumnPtr deserializeColumn(ReadBuffer & buf)
{
    bool is_const_column;
    readBinary(is_const_column, buf);

    auto data_type = deserializeDataType(buf);
    size_t rows;
    readBinary(rows, buf);

    ColumnPtr column = data_type->createColumn();

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &buf; };
    settings.avg_value_size_hint = 0;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    auto serialization = data_type->getDefaultSerialization();

    serialization->deserializeBinaryBulkStatePrefix(settings, state);
    serialization->deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data when deserialize column. Rows read: {}. Rows expected: {}.", column->size(), rows);

    if (is_const_column)
        column = ColumnConst::create(column, rows);

    return column;
}

void serializeBlock(const Block & block, WriteBuffer & buf)
{
    auto server_revision = ClickHouseRevision::getVersionRevision();
    auto block_out = std::make_unique<NativeWriter>(buf, server_revision, block);
    block_out->write(block);
}

Block deserializeBlock(ReadBuffer & buf)
{
    auto server_revision = ClickHouseRevision::getVersionRevision();
    auto block_in = std::make_shared<NativeReader>(buf, server_revision);
    return block_in->read();
}

QueryPlanStepPtr deserializePlanStep(ReadBuffer & buf, ContextPtr context)
{
    String blob;
    readBinary(blob, buf);
    RQueryPlanStep proto;
    proto.ParseFromString(blob);
    auto step = deserializeQueryPlanStepFromProto(proto, context);
    return step;
}

void serializePlanStep(const QueryPlanStepPtr & step, WriteBuffer & buf)
{
    RQueryPlanStep proto;
    serializeQueryPlanStepToProto(step, proto);
    String blob;
    proto.SerializeToString(&blob);
    writeBinary(blob, buf);
}

void serializeAssignmentsToProto(const Assignments & assignments, RAssignments & proto)
{
    for (const auto & [k, v] : assignments)
    {
        auto *pair = proto.add_pairs();
        pair->set_key(k);
        serializeASTToProto(v, *pair->mutable_value());
    }
}

Assignments deserializeAssignmentsFromProto(const RAssignments & proto)
{
    Assignments res;
    for (const auto & pair : proto.pairs())
    {
        auto k = pair.key();
        auto v = deserializeASTFromProto(pair.value());
        res.emplace_back(k, v);
    }
    return res;
}

void nameAndTypePairToProto(const NameAndTypePair & pair, RNameAndTypePair & proto)
{
    proto.set_name(pair.name);
    serializeDataTypeToProto(pair.type, *proto.mutable_type());
    //serializeDataTypeToProto(type_in_storage, *proto.mutable_type_in_storage());
    // if (subcolumn_delimiter_position.has_value())
    //     proto.set_subcolumn_delimiter_position(subcolumn_delimiter_position.value());
}

NameAndTypePair nameAndTypePairFromProto(const RNameAndTypePair & proto)
{
    NameAndTypePair pair;
    pair.name = proto.name();
    pair.type = deserializeDataTypeFromProto(proto.type());
    // type_in_storage = deserializeDataTypeFromProto(proto.type_in_storage());
    // if (proto.has_subcolumn_delimiter_position())
    //     subcolumn_delimiter_position = proto.subcolumn_delimiter_position();
    return pair;
}

void serializeHeaderToProto(const Block & block, RBlock & proto)
{
    for (const auto & pair : block.getNamesAndTypes())
        ProtosSerDerHelper::toProto(pair, *proto.add_names_and_types());
        // nameAndTypePairToProto(pair, *proto.add_names_and_types());
}

Block deserializeHeaderFromProto(const RBlock & proto)
{
    std::vector<NameAndTypePair> pairs;
    for (const auto & pair_pb : proto.names_and_types())
    {
        NameAndTypePair pair;
        ProtosSerDerHelper::fillFromProto(pair, pair_pb);
        pairs.emplace_back(std::move(pair));
    }

    ColumnsWithTypeAndName data;

    for (const auto & item : pairs)
    {
        data.emplace_back(item.type, item.name);
    }
    return Block(std::move(data));
}

void serializeAggregateFunctionToProto(
    AggregateFunctionPtr function, const Array & parameters, const DataTypes & arg_types, RAggregateFunction & proto)
{
    proto.set_func_name(function->getName());
    for (const auto & arg_type : arg_types)
        serializeDataTypeToProto(arg_type, *proto.add_arg_types());
    serializeFieldVectorToProto(parameters, *proto.mutable_parameters());
}

void serializeAggregateFunctionToProto(AggregateFunctionPtr function, const Array & parameters, RAggregateFunction & proto)
{
    // use arg type in agg_func
    // removal of low_card info is possible
    serializeAggregateFunctionToProto(function, parameters, function->getArgumentTypes(), proto);
}

std::tuple<AggregateFunctionPtr, Array, DataTypes> deserializeAggregateFunctionFromProto(const RAggregateFunction & proto)
{
    auto func_name = proto.func_name();
    DataTypes arg_types;
    for (auto & proto_element : proto.arg_types())
    {
        auto element = deserializeDataTypeFromProto(proto_element);
        arg_types.emplace_back(element);
    }

    auto & factory = AggregateFunctionFactory::instance();
    NullsAction action = NullsAction::EMPTY;
    auto parameters = deserializeFieldVectorFromProto<Array>(proto.parameters());
    AggregateFunctionProperties properties;
    auto function = factory.get(func_name, action, arg_types, parameters, properties);
    return {std::move(function), std::move(parameters), std::move(arg_types)};
}

template <typename Step, typename ProtoType>
inline void serializeQueryPlanStepToProtoImpl(const QueryPlanStepPtr & origin_step, ProtoType & proto)
{
    auto step = std::dynamic_pointer_cast<Step>(origin_step);
    if (!step)
    {
        throw Exception(ErrorCodes::PROTOBUF_BAD_CAST, "Step type unmatched");
    }
    step->toProto(proto);
}

void serializeQueryPlanStepToProto(const QueryPlanStepPtr & step, RQueryPlanStep & proto)
{
    QueryPlanStepHelper::toProto(*step.get(), proto);
}

template <typename Step, typename ProtoType>
inline QueryPlanStepPtr deserializeQueryPlanStepFromProtoImpl(const ProtoType & proto, ContextPtr context)
{
    auto step = Step::fromProto(proto, context);
    return step;
}

QueryPlanStepPtr deserializeQueryPlanStepFromProto(const RQueryPlanStep & proto, ContextPtr context)
{
    return QueryPlanStepHelper::fromProto(proto, context);
}

template <typename StepType, typename ProtoType>
bool isPlanStepEqualImpl(const IQueryPlanStep & a, const IQueryPlanStep & b)
{
    const auto & sa = reinterpret_cast<const StepType &>(a);
    const auto & sb = reinterpret_cast<const StepType &>(b);
    ProtoType pb_a;
    ProtoType pb_b;
    sa.toProto(pb_a, true);
    sb.toProto(pb_b, true);

    auto is_equal = google::protobuf::util::MessageDifferencer::Equals(pb_a, pb_b);
    return is_equal;
}

bool isPlanStepEqualImpl(const IQueryPlanStep & a, const IQueryPlanStep & b)
{
    RQueryPlanStep pb_a;
    RQueryPlanStep pb_b;
    QueryPlanStepHelper::toProto(a, pb_a, true);
    QueryPlanStepHelper::toProto(b, pb_b, true);

    auto is_equal = google::protobuf::util::MessageDifferencer::Equals(pb_a, pb_b);
    return is_equal;
}

bool isPlanStepEqual(const IQueryPlanStep & a, const IQueryPlanStep & b)
{
    if (getQueryPlanStepType(a) != getQueryPlanStepType(b))
        return false;

    switch (getQueryPlanStepType(a))
    {
#define CASE_DEF(TYPE, VAR_NAME) \
    case QueryPlanStepType::TYPE: { \
        return isPlanStepEqualImpl(a, b); \
    }

        APPLY_PROTOBUF_STEP_TYPES_AND_NAMES(CASE_DEF)

        default:
            throw Exception(ErrorCodes::PROTOBUF_BAD_CAST, "Unsupported step {}", a.getName());
#undef CASE_DEF
    }
}

template <typename StepType, typename ProtoType>
UInt64 hashPlanStepImpl(const IQueryPlanStep & raw_step, bool ignore_output_stream)
{
    const auto & step = reinterpret_cast<const StepType &>(raw_step);
    ProtoType proto;
    step.toProto(proto, ignore_output_stream);

    auto res = sipHash64Protobuf(proto);
    return res;
}

UInt64 hashPlanStepImpl(const IQueryPlanStep & step, bool ignore_output_stream)
{
    RQueryPlanStep proto;
    QueryPlanStepHelper::toProto(step, proto, ignore_output_stream);
    auto res = sipHash64Protobuf(proto);
    return res;
}

UInt64 hashPlanStep(const IQueryPlanStep & step, bool ignore_output_stream)
{
    switch (getQueryPlanStepType(step))
    {
#define CASE_DEF(TYPE, VAR_NAME) \
    case QueryPlanStepType::TYPE: { \
        return hashPlanStepImpl(step, ignore_output_stream); \
    }

        APPLY_PROTOBUF_STEP_TYPES_AND_NAMES(CASE_DEF)

        default:
            throw Exception(ErrorCodes::PROTOBUF_BAD_CAST, "Unsupported step");
#undef CASE_DEF
    }
}

//todo: need to delete
/*
void serializeHeaderToProto(const Block & block, Protos::Block & proto)
{
    for (const auto & pair : block.getNamesAndTypes())
    {
        ProtosSerDerHelper::toProto(pair, *proto.add_names_and_types());
    }
}

Block deserializeHeaderFromProto(const Protos::Block & proto)
{
    std::vector<NameAndTypePair> pairs;
    for (const auto & pair_pb : proto.names_and_types())
    {
        NameAndTypePair pair;
        ProtosSerDerHelper::fillFromProto(pair, pair_pb);
        pairs.emplace_back(std::move(pair));
    }

    ColumnsWithTypeAndName data;

    for (const auto & item : pairs)
    {
        data.emplace_back(item.type, item.name);
    }
    return Block(std::move(data));
}

*/
}
