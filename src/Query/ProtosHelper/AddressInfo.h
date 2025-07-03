#pragma once

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/HostWithPorts.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;
class AddressInfo;
using AddressInfos = std::vector<AddressInfo>;
using AddressInfoPtr = std::shared_ptr<AddressInfo>;

/// Address information of distributed tables, exchanges and other RPC services
/// see Cluster::Address
class AddressInfo
{
public:
    AddressInfo() = default;
    AddressInfo(const String & host_name_, UInt16 port_, const String & user_, const String & password_, UInt16 exchange_port_);
    AddressInfo(const RAddressInfo & proto_);

    AddressInfoPtr getAddressInfoPtr() const
    {
        return std::make_shared<AddressInfo>(this->getHostName(), this->getPort(), this->getUser(), this->getPassword(), this->getExchangePort());
    }

    static AddressInfo create(const Cluster::Address & cluster_address);
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
    friend class ProtosSerDerHelper;

private:
    String host_name;
    UInt16 port;
    String user;
    String password;
    // Same as rpc port
    UInt16 exchange_port{0};
};

/// Get local RPC address with host and port
AddressInfo getLocalAddress(const ContextPtr & context);
/// Get local RPC address with host and port
AddressInfoPtr getLocalAddressPtr(const ContextPtr & context);

AddressInfoPtr getRemoteAddress(HostWithPorts host_with_ports, ContextPtr & query_context);

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
