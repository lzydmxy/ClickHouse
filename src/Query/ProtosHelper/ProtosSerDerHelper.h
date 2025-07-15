#pragma once
#include <Interpreters/AggregateDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/WindowDescription.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/StorageID.h>

namespace DB
{

namespace Protos
{
class DataStream;
class NameAndTypePair;
class SortColumnDescription;
class FillColumnDescription;
class SendLogsRequest;
}

class ProtosSerDerHelper
{
public:
    ProtosSerDerHelper() = default;
    ~ProtosSerDerHelper() = default;

    static void serializeToProtoBase(const ITransformingStep & step, Protos::ITransformingStep & proto);
    static std::pair<String, DataStream> deserializeFromProtoBase(const Protos::ITransformingStep & proto);

    static void toProto(const DataStream & data_stream, Protos::DataStream & proto);
    static void fillFromProto(DataStream & data_stream, const Protos::DataStream & proto);

    static void toProto(const NameAndTypePair & pair, Protos::NameAndTypePair & proto);
    static void fillFromProto(NameAndTypePair & pair, const Protos::NameAndTypePair & proto);

    static void toProto(const SortColumnDescription & sort_column_description, Protos::SortColumnDescription & proto);
    static void fillFromProto(SortColumnDescription & sort_column_description, const Protos::SortColumnDescription & proto);

    static void toProto(const FillColumnDescription & fill_column_description, Protos::FillColumnDescription & proto);
    static void fillFromProto(FillColumnDescription & fill_column_description, const Protos::FillColumnDescription & proto);

    static void toProto(const AggregateDescription & aggregate_description, Protos::AggregateDescription & proto);
    static void fillFromProto(AggregateDescription & aggregate_description, const Protos::AggregateDescription & proto);

    static void toProto(const InputOrderInfo & input_order_info, Protos::InputOrderInfo & proto);
    static std::shared_ptr<InputOrderInfo> fillFromProto(const Protos::InputOrderInfo & proto);

    static void toProto(
        const SortColumnDescriptionWithColumnIndex & sort_column_description_with_column_index,
        Protos::SortColumnDescriptionWithColumnIndex & proto);
    static void fillFromProto(
        SortColumnDescriptionWithColumnIndex & sort_column_description_with_column_index,
        const Protos::SortColumnDescriptionWithColumnIndex & proto);

    static void toProto(const WindowFrame & window_frame, Protos::WindowFrame & proto);
    static std::shared_ptr<WindowFrame> fillFromProto(const Protos::WindowFrame & proto);

    static void toProto(const WindowDescription & window_frame, Protos::WindowDescription & proto);
    static std::shared_ptr<WindowDescription> fillFromProto(const Protos::WindowDescription & proto);

    static void toProto(const WindowFunctionDescription & window_frame, Protos::WindowFunctionDescription & proto);
    static std::shared_ptr<WindowFunctionDescription> fillFromProto(const Protos::WindowFunctionDescription & proto);

    static void toProto(const Field & field, Protos::Field & proto);
    static std::shared_ptr<Field> fillFromProto(const Protos::Field & proto);

    static void toProto(const SizeLimits & fill_column_description, Protos::SizeLimits & proto);
    static void fillFromProto(SizeLimits & fill_column_description, const Protos::SizeLimits & proto);

    static void toProto(const SettingChange & fill_column_description, Protos::SettingChange & proto);
    static void fillFromProto(SettingChange & fill_column_description, const Protos::SettingChange & proto);

    static void toProto(const SettingsChanges & fill_column_description, Protos::SettingsChanges & proto);
    static void fillFromProto(SettingsChanges & fill_column_description, const Protos::SettingsChanges & proto);

    static void toProto(const StorageID & storage_id, Protos::StorageID & proto);
    static std::shared_ptr<StorageID> fromProto(const Protos::StorageID & proto, ContextPtr context);
    static std::shared_ptr<StorageID> tryFromProto(const Protos::StorageID & proto, ContextPtr context);

    static void serializeToProtoBase(const ISourceStep & step, Protos::ISourceStep & proto);
    static Block deserializeFromProtoBase(const Protos::ISourceStep & proto);
    static void toProto(const Aggregator::Params & agg_params, Protos::AggregatorParams & proto);
    static Aggregator::Params fromProto(const Protos::AggregatorParams & proto, ContextPtr context);

    static void toProto(const ArrayJoinAction & array_join_action, Protos::ArrayJoinAction & proto);
    static std::shared_ptr<ArrayJoinAction> fromProto(const Protos::ArrayJoinAction & proto, ContextPtr context);

    static void toProto(const SelectQueryInfo & select_query_info, Protos::SelectQueryInfo & proto);
    static void fillFromProto(SelectQueryInfo & select_query_info, const Protos::SelectQueryInfo & proto);

    static void toProto(const Block & log_block, Protos::SendLogsRequest & request);
    static void fillFromProto(Block & log_block, const Protos::SendLogsRequest & request);

};

}
