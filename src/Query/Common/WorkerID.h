#pragma once
#include <unordered_set>

namespace DB
{

struct WorkerID
{
    String cluster;
    String id;
    const String toString() const
    {
        return cluster + "." + id;
    }

    inline bool operator==(WorkerID const & rhs) const
    {
        return (cluster == rhs.cluster && id == rhs.id);
    }
};

struct WorkerIDHash
{
    std::size_t operator()(const WorkerID & workderID) const
    {
        return std::hash<String>()(workderID.toString());
    }
};

using HostIDSet = std::unordered_set<WorkerID, WorkerIDHash>;

}
