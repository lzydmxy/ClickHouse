#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Interpreters/Context.h>
#include <Query/Common/HostWithPorts.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
    if (exchange_port != 0)
        wb << " exc/" << exchange_port;
    if (exchange_status_port != 0)
        wb << " exs/" << exchange_status_port;
    if (real_id)
        wb << " real_id/" << *real_id;
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

std::string getWorkerID(ContextPtr context)
{
    auto get_worker_id_lambda = [] (ContextPtr c) {
        std::string worker_id = getFromEnvOrConfig(c, "WORKER_ID");
        if (worker_id.empty())
            worker_id = getHostIPFromEnv();
        return worker_id;
    };

    static std::string worker_id = get_worker_id_lambda(context);
    return worker_id;
}

std::string getWorkerGroupID(ContextPtr context)
{
    static std::string worker_group_id = getFromEnvOrConfig(context, "WORKER_GROUP_ID");
    return worker_group_id;
}

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

}
