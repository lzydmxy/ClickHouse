#pragma once
#include <cstdint>
#include <functional>
#include <optional>
#include <ostream>
#include <string>
#include <vector>
#include <fmt/core.h>
#include <base/getFQDNOrHostName.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Cluster.h>
#include <Core/Types.h>
#include <Query/ProtosHelper/QueryProto.h>

namespace DB
{
class HostWithPorts;
using HostWithPortsVec = std::vector<HostWithPorts>;

std::string truncateNetworkInterfaceIfHas(const std::string & s);

inline const std::string & getHostIPFromEnv()
{
    const auto get_host_ip_lambda = [] () -> std::string
    {
        {
            const char * byted_ipv6 = getenv("BYTED_HOST_IPV6");
            if (byted_ipv6 && byted_ipv6[0])
                return byted_ipv6;
        }

        {
            const char * my_ipv6 = getenv("MY_HOST_IPV6");
            if (my_ipv6 && my_ipv6[0])
                return my_ipv6;
        }

        {
            const char * byted_ipv4 = getenv("BYTED_HOST_IP");
            if (byted_ipv4 && byted_ipv4[0])
                return byted_ipv4;
        }

        {
            const char * my_ipv4 = getenv("MY_HOST_IP");
            if (my_ipv4 && my_ipv4[0])
                return my_ipv4;
        }

        return truncateNetworkInterfaceIfHas(getFQDNOrHostName());
    };

    static std::string host_ip = get_host_ip_lambda();
    return host_ip;
}

inline const char * getLoopbackIPFromEnv()
{
    const auto get_loopback_ip_lambda = [] () -> const char *
    {
        {
            const char * byted_ipv6 = getenv("BYTED_HOST_IPV6");
            if (byted_ipv6 && byted_ipv6[0])
                return "::1";
        }

        {
            const char * my_ipv6 = getenv("MY_HOST_IPV6");
            if (my_ipv6 && my_ipv6[0])
                return "::1";
        }

        {
            const char * byted_ipv4 = getenv("BYTED_HOST_IP");
            if (byted_ipv4 && byted_ipv4[0])
                return "127.0.0.1";
        }

        {
            const char * my_ipv4 = getenv("MY_HOST_IP");
            if (my_ipv4 && my_ipv4[0])
                return "127.0.0.1";
        }

        return "127.0.0.1";
    };

    static const char * loopback_ip = get_loopback_ip_lambda();
    return loopback_ip;
}

inline const char * getConsulIPFromEnv()
{
    const auto get_consul_ip_lambda = []() -> const char * {
        {
            const char * consul_http_ipv6 = getenv("CONSUL_HTTP_HOST");
            if (consul_http_ipv6 && consul_http_ipv6[0])
                return consul_http_ipv6;
        }

        {
            const char * byted_ipv6 = getenv("BYTED_HOST_IPV6");
            if (byted_ipv6 && byted_ipv6[0])
                return byted_ipv6;
        }

        {
            const char * my_ipv6 = getenv("MY_HOST_IPV6");
            if (my_ipv6 && my_ipv6[0])
                return my_ipv6;
        }

        return "::1";
    };

    static const char * consul_ip = get_consul_ip_lambda();
    return consul_ip;
}

inline std::string addBracketsIfIpv6(const std::string & host_name)
{
    if (host_name.find_first_of(':') != std::string::npos && !host_name.empty() && host_name.back() != ']')
        return fmt::format("[{}]", host_name);
    else
        return host_name;
}

inline std::string createHostPortString(const std::string & host, UInt16 port)
{
    return fmt::format("{}:{}", addBracketsIfIpv6(host), port);
}

inline std::string createHostPortString(const std::string & host, const std::string & port)
{
    return fmt::format("{}:{}", addBracketsIfIpv6(host), port);
}

std::string getWorkerID(ContextPtr context);
std::string getWorkerGroupID(ContextPtr context);
std::string getVirtualWareHouseID(ContextPtr context);

inline std::string_view removeBracketsIfIpv6(const std::string & host_name)
{
    if (host_name.find_first_of(':') != std::string::npos &&
        !host_name.empty() &&
        host_name.back() == ']' &&
        host_name.front() == '['
    )
        return std::string_view(host_name.data() + 1, host_name.size() - 2);
    return std::string_view(host_name.c_str());
}

inline bool isSameHost(const std::string & lhs, const std::string & rhs)
{
    if (lhs == rhs)
        return true;
    return removeBracketsIfIpv6(lhs) == removeBracketsIfIpv6(rhs);
}

/// The host and port to be managed and controlled
class HostWithPorts
{
public:
    HostWithPorts() = default;
    HostWithPorts(const std::string & host_, UInt16 rpc_port_ = 0, UInt16 tcp_port_ = 0, UInt16 http_port_ = 0, std::string id_ = {});

    std::string host;
    UInt16 rpc_port{0};
    UInt16 tcp_port{0};
    UInt16 http_port{0};// TODO wujianchao remove
    std::string id;

    static HostWithPorts createHostWithPorts(const RHostWithPorts & hp);
    static void fillHostWithPorts(const HostWithPorts & hp, RHostWithPorts & pb_hp);

    bool empty() const { return host.empty() || (rpc_port == 0 && tcp_port == 0); }

    std::string getRPCAddress() const { return fmt::format("{}:{}", addBracketsIfIpv6(host), std::to_string(rpc_port)); }
    std::string getTCPAddress() const { return fmt::format("{}:{}", addBracketsIfIpv6(host), std::to_string(tcp_port)); }
    std::string getHTTPAddress() const { return fmt::format("{}:{}", addBracketsIfIpv6(host), std::to_string(http_port)); }
    std::string getExchangeAddress() const { return getRPCAddress(); }

    bool operator<(const HostWithPorts & rhs) const { return id < rhs.getId(); }
    const std::string & getHost() const { return host; }
    UInt16 getTCPPort() const { return tcp_port; }
    UInt16 getHTTPPort() const { return http_port; }
    UInt16 getRPCPort() const { return rpc_port; }
    std::string toDebugString() const;
    void replaceId(const String & id_) { id = id_; }
    String getId() const { return id; }

    static HostWithPorts fromRPCAddress(const std::string & s);

    /// NOTE: PLEASE DO NOT implement any comparison operator which is a kind of bad code style

    struct IsSameEndpoint
    {
        bool operator()(const HostWithPorts & lhs, const HostWithPorts & rhs) const
        {
            return isSameHost(lhs.host, rhs.host) && lhs.rpc_port == rhs.rpc_port && lhs.tcp_port == rhs.tcp_port;
        }
    };

    struct IsExactlySame
    {
        bool operator()(const HostWithPorts & lhs, const HostWithPorts & rhs) const
        {
            return lhs.id == rhs.id && isSameHost(lhs.host, rhs.host) && lhs.rpc_port == rhs.rpc_port && lhs.tcp_port == rhs.tcp_port
                && lhs.http_port == rhs.http_port;
        }
    };

    bool isSameEndpoint(const HostWithPorts & rhs) const
    {
        return IsSameEndpoint{}(*this, rhs);
    }

    bool isExactlySame(const HostWithPorts & rhs) const { return IsExactlySame{}(*this, rhs); }

    static bool isExactlySameVec(const HostWithPortsVec & lhs, const HostWithPortsVec & rhs);
};

std::ostream & operator<<(std::ostream & os, const HostWithPorts & host_ports);

}

namespace std
{

template <>
struct hash<DB::HostWithPorts>
{
    std::size_t operator()(const DB::HostWithPorts & hp) const
    {
        return std::hash<string>()(DB::addBracketsIfIpv6(hp.getHost())) ^ std::hash<UInt16>()(hp.rpc_port) ^ (std::hash<UInt16>()(hp.tcp_port) << 16);
    }
};

template <>
struct equal_to<DB::HostWithPorts>
{
    bool operator()(const DB::HostWithPorts & lhs, const DB::HostWithPorts & rhs) const
    {
        return DB::HostWithPorts::IsSameEndpoint{}(lhs, rhs);
    }
};

}
