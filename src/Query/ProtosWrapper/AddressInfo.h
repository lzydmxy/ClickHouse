#pragma once

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Query/ProtosWrapper/QueryProto.h>
#include <Query/ProtosWrapper/HostWithPorts.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

// see Cluster::Address
class AddressInfo
{
public:
    AddressInfo() = default;
    AddressInfo(const String & host_name_, UInt16 port_, const String & user_, const String & password_);
    AddressInfo(const String & host_name_, UInt16 port_, const String & user_, const String & password_, UInt16 exchange_port_);
    AddressInfo(const RAddressInfo & proto_);

    void serialize(WriteBuffer &) const;
    void deserialize(ReadBuffer &);
    void toProto(RAddressInfo & proto) const;
    void fromProto(const RAddressInfo & proto);

    const String & getHostName() const { return host_name; }
    UInt16 getPort() const { return port; }
    UInt16 getExchangePort() const { return exchange_port;}
    const String & getUser() const { return user; }
    const String & getPassword() const { return password; }

    String toString() const;
    String toShortString() const;
    inline bool operator == (AddressInfo const& rhs) const
    {
        return (this->host_name == rhs.host_name && this->port == rhs.port);
    }
    inline bool operator < (AddressInfo const& rhs) const
    {
        int ret = host_name.compare(rhs.host_name);
        if (ret)
            return ret < 0;
        return port < rhs.port;
    }
    class Hash
    {
    public:
        size_t operator()(const AddressInfo & key) const
        {
            return std::hash<std::string_view>{}(key.host_name) + static_cast<size_t>(key.port);
        }
    };

private:
    String host_name;
    UInt16 port;
    String user;
    String password;
    UInt16 exchange_port;
};

using AddressInfos = std::vector<AddressInfo>;
using AddressInfoPtr = std::shared_ptr<AddressInfo>;


AddressInfo getLocalAddress(const Context & query_context);

AddressInfo getRemoteAddress(HostWithPorts host_with_ports, ContextPtr & query_context);

inline String extractHostPort(const AddressInfo & address)
{ 
    return createHostPortString(address.getHostName(), address.getPort()); 
}

inline String extractExchangeHostPort(const AddressInfo & address) 
{
    return createHostPortString(address.getHostName(), toString(address.getExchangePort())); 
}

struct PlanSegmentPartitionSource
{
    UInt64 exchange_id;
    AddressInfoPtr address;
    std::vector<UInt32> partition_ids;
    void toProto(RPlanSegmentPartitionSource & proto) const;
    void fromProto(const RPlanSegmentPartitionSource & proto);
    String toString() const;
};

}
