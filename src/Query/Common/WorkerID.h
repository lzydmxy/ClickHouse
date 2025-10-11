#pragma once
#include <unordered_set>

namespace DB
{

struct WorkerID
{
    WorkerID(std::string host_, UInt16 rpc_port_) : host(host_), rpc_port(rpc_port_) { }
    WorkerID() = default;

    std::string host;
    UInt16 rpc_port{0};

    String toString() const
    {
        return host + ":" + std::to_string(rpc_port);
    }

    inline bool operator==(WorkerID const & rhs) const
    {
        return host == rhs.host && rpc_port == rhs.rpc_port;
    }
};

struct WorkerIDHash
{
    std::size_t operator()(const WorkerID & worker_id) const
    {
        return std::hash<String>()(worker_id.toString());
    }
};

using WorkerNodeSet = std::unordered_set<WorkerID, WorkerIDHash>;
}
