#pragma once

#include<Query/ProtosHelper/HostWithPorts.h>

namespace DB
{

struct WorkerNodeResourceData
{
    HostWithPorts host_ports;
    UInt32 query_num;

    double cpu_usage;
    double cpu_usage_1min;
    double cpu_usage_10sec;
    double memory_usage;
    double memory_usage_1min;
    UInt64 memory_available;
    UInt64 disk_space;

    UInt64 cpu_limit;
    UInt64 memory_limit;

    UInt64 register_time;
    UInt64 last_update_time;

    UInt64 reserved_memory_bytes;
    UInt32 reserved_cpu_cores;
    UInt64 last_status_create_time {0};


    WorkerNodeResourceData() = default;
    WorkerNodeResourceData(const Protos::WorkerNodeResourceData & resource_info);

    void fillProto(Protos::WorkerNodeResourceData & resource_info) const;
    static WorkerNodeResourceData createFromProto(const Protos::WorkerNodeResourceData & resource_info);
    inline String toDebugString() const
    {
        std::stringstream ss;
        ss << "{ host:" << host_ports.getHost() << ", rpc_port:" << host_ports.getRPCPort() << ", register_time: " << register_time
           << ", cpu_usage:" << cpu_usage << ", memory_usage:" << memory_usage
           << ", memory_available:" << formatReadableSizeWithDecimalSuffix(memory_available) << ", query_num:" << query_num << " }";
        return ss.str();
    }
};
}
