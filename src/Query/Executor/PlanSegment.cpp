#include "PlanSegment.h"

#include <sstream>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Parsers/queryToString.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/ExchangeMode.h>
#include <Query/ProtosHelper/RPCHelpers.h>
#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void IPlanSegment::serialize(WriteBuffer & buf) const
{
    serializeBlock(header, buf);
    writeBinary(UInt8(type), buf);
    writeBinary(UInt8(exchange_mode), buf);
    writeBinary(exchange_id, buf);
    writeBinary(exchange_parallel_size, buf);
    writeBinary(name, buf);
    writeBinary(segment_id, buf);

    writeBinary(shuffle_keys.size(), buf);
    for (auto & key : shuffle_keys)
        writeBinary(key, buf);
}

void IPlanSegment::deserialize(ReadBuffer & buf, ContextPtr)
{
    header = deserializeBlock(buf);

    UInt8 read_type;
    readBinary(read_type, buf);
    type = RIPlanSegment::Enum(read_type);

    UInt8 read_mode;
    readBinary(read_mode, buf);
    exchange_mode = static_cast<RExchangeMode::Enum>(read_mode);

    readBinary(exchange_id, buf);
    readBinary(exchange_parallel_size, buf);
    readBinary(name, buf);
    readBinary(segment_id, buf);

    size_t key_size;
    readBinary(key_size, buf);
    shuffle_keys.resize(key_size);
    for (size_t i = 0; i < key_size; ++i)
        readBinary(shuffle_keys[i], buf);
}

void IPlanSegment::toProtoBase(RIPlanSegment & proto) const
{
    serializeHeaderToProto(header, *proto.mutable_header());

    proto.set_type(type);
    proto.set_exchange_mode(exchange_mode);
    proto.set_exchange_id(exchange_id);
    proto.set_exchange_parallel_size(exchange_parallel_size);
    proto.set_name(name);
    proto.set_segment_id(segment_id);
    for (const auto & element : shuffle_keys)
        proto.add_shuffle_keys(element);
}

void IPlanSegment::fromProtoBase(const RIPlanSegment & proto)
{
    header = deserializeHeaderFromProto(proto.header());

    type = proto.type();
    exchange_mode = proto.exchange_mode();
    exchange_id = proto.exchange_id();
    exchange_parallel_size = proto.exchange_parallel_size();
    name = proto.name();
    segment_id = proto.segment_id();
    for (const auto & element : proto.shuffle_keys())
        shuffle_keys.emplace_back(element);
}

String IPlanSegment::toString(size_t indent) const
{
    std::ostringstream ostr;
    String indent_str(indent, ' ');

    ostr << indent_str << "[segment_id: " << segment_id << "\n";
    ostr << indent_str << "name: " << name << "\n";
    ostr << indent_str << "header: " << header.dumpStructure() << "\n";
    ostr << indent_str << "type: " << planSegmentTypeToString(type) << "\n";
    ostr << indent_str << "exchange_mode: " << exchangeModeToString(exchange_mode) << "\n";
    ostr << indent_str << "exchange_id: " << exchange_id << "\n";
    ostr << indent_str << "exchange_parallel_size: " << exchange_parallel_size << "\n";
    ostr << indent_str << "shuffle_keys: " << "\n";
    ostr << indent_str;
    for (auto & key : shuffle_keys)
        ostr << key << ", ";
    ostr << "]\n";
    return ostr.str();
}

void PlanSegmentInput::serialize(WriteBuffer & buf) const
{
    // TODO replace plansegment* serde to protobuf
    RPlanSegmentInput proto;
    this->toProto(proto);
    auto str = proto.SerializeAsString();
    writeBinary(str, buf);
}

void PlanSegmentInput::deserialize(ReadBuffer & buf, ContextPtr context)
{
    // TODO replace plansegment* serde to protobuf
    String str;
    readBinary(str, buf);
    RPlanSegmentInput proto;
    proto.ParseFromString(str);
    this->fromProto(proto, context);
}

void PlanSegmentInput::toProto(RPlanSegmentInput & proto) const
{
    IPlanSegment::toProtoBase(*proto.mutable_base_plan_segment());
    proto.set_parallel_index(parallel_index);
    proto.set_keep_order(keep_order);
    for (const auto & element : source_addresses)
        element.toProto(*proto.add_source_addresses());

    //TODO: Need toProto/fromProto in StorageIDHelper class
    // if (type == RIPlanSegment::SOURCE && storage_id.has_value())
    // {
    //     storage_id.value().toProto(*proto.mutable_storage_id());
    // }
}

void PlanSegmentInput::fromProto(const RPlanSegmentInput & proto, ContextPtr context)
{
    IPlanSegment::fromProtoBase(proto.base_plan_segment());
    parallel_index = proto.parallel_index();
    keep_order = proto.keep_order();
    for (const auto & proto_element : proto.source_addresses())
    {
        AddressInfo element;
        element.fromProto(proto_element);
        source_addresses.emplace_back(std::move(element));
    }

    //TODO: Need toProto/fromProto in StorageIDHelper class
    // if (type == RIPlanSegment::SOURCE && proto.has_storage_id())
    // {
    //     storage_id = StorageID::fromProto(proto.storage_id(), context);
    // }
}

String PlanSegmentInput::toString(size_t indent) const
{
    std::ostringstream ostr;
    String indent_str(indent, ' ');

    ostr << IPlanSegment::toString(indent) << "\n";
    ostr << indent_str << "parallel_index: " << parallel_index << "\n";
    ostr << indent_str << "keep_order: " << keep_order << "\n";
    ostr << indent_str << "storage_id: " << (type == RIPlanSegment::SOURCE && storage_id.has_value() ? storage_id->getNameForLogs() : "") << "\n";
    ostr << indent_str << "source_addresses: " << "\n";
    ostr << indent_str << "isStable: " << isStable() << "\n";
    for (auto & address : source_addresses)
        ostr << indent_str << indent_str << address.toString() << "\n";

    return ostr.str();
}

void PlanSegmentOutput::serialize(WriteBuffer & buf) const
{
    IPlanSegment::serialize(buf);
    writeBinary(shuffle_function_name, buf);
    writeBinary(parallel_size, buf);
    writeBinary(keep_order, buf);
}

void PlanSegmentOutput::deserialize(ReadBuffer & buf, ContextPtr context)
{
    IPlanSegment::deserialize(buf, context);
    readBinary(shuffle_function_name, buf);
    readBinary(parallel_size, buf);
    readBinary(keep_order, buf);
}

void PlanSegmentOutput::toProto(RPlanSegmentOutput & proto)
{
    IPlanSegment::toProtoBase(*proto.mutable_base_plan_segment());
    proto.set_shuffle_hash_function(shuffle_function_name);
    proto.set_parallel_size(parallel_size);
    proto.set_keep_order(keep_order);
    if(!shuffle_func_params.empty())
    {
        serializeFieldVectorToProto(shuffle_func_params, *proto.mutable_shuffle_function_parameters());
    }
}

void PlanSegmentOutput::fromProto(const RPlanSegmentOutput & proto)
{
    IPlanSegment::fromProtoBase(proto.base_plan_segment());
    shuffle_function_name = proto.shuffle_hash_function();
    parallel_size = proto.parallel_size();
    keep_order = proto.keep_order();
    if (proto.has_shuffle_function_parameters())
    {
        shuffle_func_params = deserializeFieldVectorFromProto<Array>(proto.shuffle_function_parameters());
    }
}

String PlanSegmentOutput::toString(size_t indent) const
{
    std::ostringstream ostr;
    String indent_str(indent, ' ');

    ostr << IPlanSegment::toString(indent) << "\n";
    ostr << indent_str << "shuffle_function_name: " << shuffle_function_name << "\n";
    if (!shuffle_func_params.empty())
    {
        ostr << indent_str << "shuffle_parameters: ";
        for (auto & field : shuffle_func_params)
        {
            ostr << DB::toString(field) << " ";
        }
        ostr << "\n";
    }
    ostr << indent_str << "parallel_size: " << parallel_size << "\n";
    ostr << indent_str << "keep_order: " << keep_order;

    return ostr.str();
}


void PlanSegment::setPlanSegmentToQueryPlan(QueryPlanExt::Node * node, ContextPtr & context)
{
    if (!node)
        return;
    if (auto * remote_step = dynamic_cast<RemoteExchangeSourceStepExt *>(node->step.get()))
        remote_step->setPlanSegment(this, context);
    else
    {
        for (auto & child : node->children)
        {
            setPlanSegmentToQueryPlan(child, context);
        }
    }
}

void PlanSegment::serialize(WriteBuffer & buf) const
{
    writeBinary(segment_id, buf);
    writeBinary(query_id, buf);

    // query_plan.serialize(buf);
    // TODO: change to the following when implement plan segment rpc
    // TODO: here don't change for compatibility
    // Protos::QueryPlan proto;
    // query_plan.toProto(proto);
    // auto blob = proto.SerializeAsString();
    // writeBinary(blob, buf);

    writeBinary(inputs.size(), buf);
    for (const auto & input : inputs)
        input->serialize(buf);

    if (outputs.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find output when serialize PlanSegment");
    writeBinary(outputs.size(), buf);
    for (const auto & output : outputs)
        output->serialize(buf);

    coordinator_address.serialize(buf);

    writeBinary(cluster_name, buf);
    writeBinary(parallel, buf);
    writeBinary(exchange_parallel_size, buf);
    writeBinary(runtime_filters.size(), buf);
    for (const auto & id : runtime_filters)
        writeBinary(id, buf);

    writeBinary(parallel_index, buf);
}

void PlanSegment::deserialize(ReadBuffer & buf, ContextMutablePtr context)
{
    readBinary(segment_id, buf);
    readBinary(query_id, buf);

    query_plan.addInterpreterContext(context);

    // query_plan.deserialize(buf);
    // TODO: change to the following when implement plan segment rpc
    // TODO: here don't change for compatibility
    // std::string blob;
    // readBinary(blob, buf);
    // Protos::QueryPlan proto;
    // proto.ParseFromString(blob);
    // query_plan.fromProto(proto);

    size_t input_size;
    readBinary(input_size, buf);
    for (size_t i = 0; i < input_size; ++i)
    {
        auto input = std::make_shared<PlanSegmentInput>();
        input->deserialize(buf, context);
        inputs.push_back(input);
    }

    size_t output_size;
    readBinary(output_size, buf);
    for (size_t i = 0; i < output_size; ++i)
    {
        auto cur_output = std::make_shared<PlanSegmentOutput>();
        cur_output->deserialize(buf, context);
        outputs.emplace_back(std::move(cur_output));
    }

    coordinator_address.deserialize(buf);

    readBinary(cluster_name, buf);
    readBinary(parallel, buf);
    readBinary(exchange_parallel_size, buf);

    size_t runtime_filters_size;
    readBinary(runtime_filters_size, buf);
    for (size_t i = 0; i < runtime_filters_size; ++i)
    {
        RuntimeFilterId id;
        readBinary(id, buf);
        runtime_filters.emplace(id);
    }

    readBinary(parallel_index, buf);
}

void PlanSegment::toProto(RPlanSegment & plan_segment_proto)
{
    auto plan_ptr = std::make_unique<RQueryPlan>();
    
    // TODO: Add toProto function for QueryPlan
    query_plan.toProto(*plan_ptr);
    plan_segment_proto.set_allocated_query_plan(plan_ptr.release());
    plan_segment_proto.set_cluster_name(cluster_name);
    plan_segment_proto.set_parallel(parallel);
    plan_segment_proto.set_exchange_parallel_size(exchange_parallel_size);
    if (outputs.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find output when serialize PlanSegment");
    for (auto & output : outputs)
    {
        RPlanSegmentOutput output_proto;
        output->toProto(output_proto);
        *plan_segment_proto.add_outputs() = std::move(output_proto);
    }

    for (const auto & id : runtime_filters)
        plan_segment_proto.add_runtime_filter_id(id);
    //TODO:
    //plan_segment_proto.set_profile_type(ReportProfileTypeConverter::toProto(profile_type));
}

void PlanSegment::fromProto(const RPlanSegment & proto, ContextMutablePtr context_)
{
    query_plan.addInterpreterContext(context_);
    //TODO: Add fromProto function for QueryPlan
    query_plan.fromProto(proto.query_plan());
    cluster_name = proto.cluster_name();
    parallel = proto.parallel();
    exchange_parallel_size = proto.exchange_parallel_size();
    query_id = proto.query_id();
    segment_id = proto.segment_id();
    coordinator_address = AddressInfo{proto.coordinator_address()};

    for(const auto & output_proto: proto.outputs())
    {
        auto cur_output = std::make_shared<PlanSegmentOutput>();
        cur_output->fromProto(output_proto);
        outputs.emplace_back(std::move(cur_output));
    }

    for(auto runtime_filter_id: proto.runtime_filter_id())
    {   
        runtime_filters.emplace(runtime_filter_id);
    }
    //TODO:
    //profile_type = ReportProfileTypeConverter::fromProto(proto.profile_type());
}

/**
 * update plansegemnt if
 * 1. a segment is deserialized
 * 2. before final segment executed on coordinator
 */
void PlanSegment::update(ContextPtr context)
{
    setPlanSegmentToQueryPlan(query_plan.getRootNode(), context);
    has_local_input
        = std::find_if(inputs.begin(), inputs.end(), [](const auto & input) { return isLocalExchange(input->getExchangeMode()); })
        != inputs.end();
    has_local_output
        = std::find_if(outputs.begin(), outputs.end(), [](const auto & output) { return isLocalExchange(output->getExchangeMode()); })
        != outputs.end();
}

PlanSegmentPtr PlanSegment::deserializePlanSegment(ReadBuffer & buf, ContextMutablePtr context)
{
    auto plan_segment = std::make_unique<PlanSegment>();
    plan_segment->deserialize(buf, context);
    plan_segment->update(std::move(context));
    return plan_segment;
}

String PlanSegment::toString()
{
    std::ostringstream ostr;

    ostr << "segment_id: " << segment_id << "\n";
    ostr << "query_id: " << query_id << "\n";
    ostr << "parallel_index: " << parallel_index << "\n";

    WriteBufferFromOwnString plan_str;
    query_plan.explainPlan(plan_str, QueryPlanExt::ExplainPlanOptions{true, true, true, true});
    ostr << plan_str.str() << "\n";

    ostr << "inputs: " << "\n";
    for (auto & input : inputs)
        ostr << input->toString(4) << "\n";
    ostr << "outputs: " << "\n";
    for (auto & output : outputs)
        ostr << output->toString(4) << "\n";

    ostr << "coordinator_address: " << coordinator_address.toString() << "\n";
    ostr << "cluster_name: " << cluster_name << "\n";
    ostr << "parallel: " << parallel << ", exchange_parallel_size: " << exchange_parallel_size;

    return ostr.str();
}

void PlanSegment::getRemoteSegmentId(const QueryPlanExt::Node * node, std::unordered_map<PlanNodeId, size_t> & exchange_to_segment)
{
    // TODO:Need Step
    // auto * step = dynamic_cast<RemoteExchangeSourceStepExt *>(node->step.get());
    // if (step)
    //     exchange_to_segment[node->id] = step->getInput()[0]->getPlanSegmentId();

    for (const auto & child : node->children)
        getRemoteSegmentId(child, exchange_to_segment);
}

std::unordered_map<size_t, PlanSegmentPtr &> PlanSegmentTree::getPlanSegmentsMap()
{
    std::unordered_map<size_t, PlanSegmentPtr &> all_segments;
    Nodes & all_nodes = getNodes();
    for(auto & node : all_nodes)
    {
        all_segments.emplace(node.plan_segment->getPlanSegmentId(), node.plan_segment);
    }
    return all_segments;
}

String PlanSegmentTree::toString() const
{
    std::ostringstream ostr;

    std::queue<Node *> print_queue;
    print_queue.push(root);

    while (!print_queue.empty())
    {
        auto current = print_queue.front();
        print_queue.pop();

        for (auto & child : current->children)
            print_queue.push(child);

        ostr << current->plan_segment->toString() << "\n";
        ostr << " ------------------ " << "\n";
    }

    return ostr.str();
}

}
