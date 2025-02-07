#pragma once
#include <unordered_set>

namespace DB
{

struct HostID
{
    String host_fqdn;      /// current host domain name
    String host_fqdn_id;   /// host_name:port
    const String toString() const
    {
        if (!host_fqdn_id.empty())
            return host_fqdn_id;
        else
            return host_fqdn;
    }

    inline bool operator==(HostID const & rhs) const
    {
        return (host_fqdn_id == rhs.host_fqdn_id && host_fqdn == rhs.host_fqdn);
    }
};

struct HostIDHash
{
    std::size_t operator()(const HostID & hostID) const
    {
        return std::hash<String>()(hostID.toString());
    }
};

using HostIDSet = std::unordered_set<HostID, HostIDHash>;

}
