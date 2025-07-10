#include "AddressInfo.h"
#include <string>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Query/Common/QueryCommon.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/ProtosHelper/HostWithPorts.h>

namespace DB
{

AddressInfo getLocalAddress(const ContextPtr & context)
{
    return *(getLocalAddressPtr(context).get());
}

AddressInfoPtr getLocalAddressPtr(const ContextPtr & context)
{
    const auto & host = getFQDNOrHostName();
    auto tcp_port = context->getTCPPort();
    auto rpc_port = context->getOptimizerContext()->getRPCPort();
    const ClientInfo & info = context->getClientInfo();
    return std::make_shared<AddressInfo>(host, tcp_port, info.current_user, "", rpc_port); // TODO wujianchao add password
}

// AddressInfo getRemoteAddress(HostWithPorts host_with_ports, ContextPtr & query_context)
// {
//     if(query_context->getSettingsRef().enable_internal_communication_user)
//     {
//         // Trick for avoiding RBAC performace loss
//         static auto [user, password] = query_context->getCnchInterserverCredentials();
//         return AddressInfo(
//             host_with_ports.host,
//             host_with_ports.tcp_port,
//             user,
//             password,
//             host_with_ports.rpc_port);
//     }

//     const ClientInfo & info = query_context->getClientInfo();
//     return AddressInfo(
//         host_with_ports.host,
//         host_with_ports.tcp_port,
//         info.current_user,
//         info.current_password,
//         host_with_ports.rpc_port);

// }


AddressInfo::AddressInfo(const String &host_name_, UInt16 port_, const String &user_, const String &password_, 
    UInt16 exchange_port_)
    : host_name(host_name_), port(port_), user(user_), password(password_), exchange_port(exchange_port_) 
{
}

AddressInfo::AddressInfo(const RAddressInfo & proto) 
        : host_name(proto.host_name()), port(proto.port()), exchange_port(proto.exchange_port())
{
    user = proto.user();
    password = proto.password();
}

AddressInfo AddressInfo::create(const Cluster::Address & cluster_address)
{
    return AddressInfo{cluster_address.host_name, cluster_address.port, cluster_address.user, cluster_address.password, cluster_address.rpc_port};
}

void AddressInfo::serialize(WriteBuffer &buf) const
{
    // TODO: remove this when PlanSegment Protobuf is ready
    writeBinary(host_name, buf);
    writeBinary(port, buf);
    writeBinary(user, buf);
    writeBinary(password, buf);
    writeBinary(exchange_port, buf);
}

void AddressInfo::deserialize(ReadBuffer &buf)
{
    // TODO: remove this when PlanSegment Protobuf is ready
    readBinary(host_name, buf);
    readBinary(port, buf);
    readBinary(user, buf);
    readBinary(password, buf);
    readBinary(exchange_port, buf);
}

void AddressInfo::toProto(RAddressInfo & proto) const
{
    proto.set_host_name(host_name);
    proto.set_port(port);
    proto.set_user(user);
    proto.set_password(password);
    proto.set_exchange_port(exchange_port);
}

void AddressInfo::fromProto(const RAddressInfo & proto)
{
    host_name = proto.host_name();
    port = proto.port();
    user = proto.user();
    password = proto.password();
    exchange_port = proto.exchange_port();
}

String AddressInfo::toString() const
{
    return fmt::format("host_name: {}, port: {}, exchange_port: {} user: {}", host_name, port, exchange_port, user);
}

String AddressInfo::toShortString() const
{
    return fmt::format("{}:{}/{}", host_name, port, exchange_port);
}

void PlanSegmentPartitionSource::toProto(RPlanSegmentPartitionSource & proto) const
{
    proto.set_exchange_id(exchange_id);
    address->toProto(*proto.mutable_address());
    for (auto p_id : partition_ids)
        proto.add_partition_ids(p_id);
}

void PlanSegmentPartitionSource::fromProto(const RPlanSegmentPartitionSource & proto)
{
    exchange_id = proto.exchange_id();
    address = std::make_shared<AddressInfo>();
    address->fromProto(proto.address());
    partition_ids.reserve(proto.partition_ids().size());
    for (auto p_id : proto.partition_ids())
    {
        partition_ids.emplace_back(p_id);
    }
}

String PlanSegmentPartitionSource::toString() const
{
    return fmt::format(
        "source[{} - partition_ids:{} - exchange_id:{}]",
        address->toShortString(),
        containerToString<std::vector<UInt32>>(partition_ids),
        exchange_id);
}

}
