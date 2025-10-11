#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/parseAddress.h>
#include <Interpreters/Context.h>
#include <Query/ProtosHelper/HostWithPorts.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

    HostWithPorts::HostWithPorts(const std::string & host_, UInt16 rpc_port_, UInt16 tcp_port_, UInt16 http_port_, std::string id_)
        : host{removeBracketsIfIpv6(host_)}
        , rpc_port{rpc_port_}
        , tcp_port{tcp_port_}
        , http_port{http_port_}
        , id{std::move(id_)}
    {
    }

std::string HostWithPorts::toDebugString() const
{
    WriteBufferFromOwnString wb;

    wb << '{';
    if (!id.empty())
        wb << id << " ";
    if (!host.empty())
        wb << host << " ";
    if (rpc_port != 0)
        wb << " rpc/" << rpc_port;
    if (tcp_port != 0)
        wb << " tcp/" << tcp_port;
    wb << '}';

    return wb.str();
}

HostWithPorts HostWithPorts::fromRPCAddress(const std::string & s)
{
    std::pair<std::string, UInt16> host_port = parseAddress(s, 0);
    HostWithPorts res{std::string{removeBracketsIfIpv6(host_port.first)}};
    res.rpc_port = host_port.second;
    return res;
}

bool HostWithPorts::isExactlySameVec(const HostWithPortsVec & lhs, const HostWithPortsVec & rhs)
{
    return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), HostWithPorts::IsSameEndpoint{});
}

std::ostream & operator<<(std::ostream & os, const HostWithPorts & host_ports)
{
    os << host_ports.toDebugString();
    return os;
}

namespace
{
std::string getFromEnvOrConfig(ContextPtr context, const std::string & name)
{
    char * ret = std::getenv(name.c_str());
    if (ret)
        return ret;

    return context->getConfigRef().getString(name, "");
}
} /// end namespace


std::string getVirtualWareHouseID(ContextPtr context)
{
    static std::string virtual_warehouse_id = getFromEnvOrConfig(context, "VIRTUAL_WAREHOUSE_ID");
    return virtual_warehouse_id;
}

/// for some system getIPOrFQDNOrHostName() returns ip address with network interface at the end like fe80::f24c:28af:6150:e261%enp1s0f0
std::string truncateNetworkInterfaceIfHas(const std::string & s)
{
    auto pos = s.find('%');
    if (pos != std::string::npos)
    {
        std::string truncated = s.substr(0, pos);
        return truncated;
    }
    return s;
}


HostWithPorts HostWithPorts::createHostWithPorts(const RHostWithPorts & hp)
{
    return HostWithPorts{
        hp.host(),
        uint16_t(hp.rpc_port()),
        uint16_t(hp.tcp_port()),
        uint16_t(hp.http_port()),
        hp.hostname()
    };
}

void HostWithPorts::fillHostWithPorts(const HostWithPorts & hp, RHostWithPorts & pb_hp)
{
    pb_hp.set_host(hp.getHost());
    pb_hp.set_rpc_port(hp.rpc_port);
    pb_hp.set_tcp_port(hp.tcp_port);
    pb_hp.set_http_port(hp.http_port);
    pb_hp.set_hostname(hp.id);
}

}
