#include <Query//Common/WorkerNodeResourceData.h>

namespace DB
{
WorkerNodeResourceData::WorkerNodeResourceData(const Protos::WorkerNodeResourceData & resource_info)
{
    host_ports = HostWithPorts::createHostWithPorts(resource_info.host_ports());

    cpu_usage = resource_info.cpu_usage();
    cpu_usage_1min = resource_info.cpu_usage_1min();
    cpu_usage_10sec = resource_info.cpu_usage_10sec();
    memory_usage = resource_info.memory_usage();
    memory_usage_1min = resource_info.memory_usage_1min();
    memory_available = resource_info.memory_available();
    disk_space = resource_info.disk_space();
    query_num = resource_info.query_num();

    if (resource_info.has_cpu_limit())
        cpu_limit = resource_info.cpu_limit();
    if (resource_info.has_memory_limit())
        memory_limit = resource_info.memory_limit();

    if (resource_info.has_last_update_time())
        last_update_time = resource_info.last_update_time();

    if (resource_info.has_register_time())
        register_time = resource_info.register_time();

    if (resource_info.has_last_status_create_time())
        last_status_create_time = resource_info.last_status_create_time();
}

WorkerNodeResourceData WorkerNodeResourceData::createFromProto(const Protos::WorkerNodeResourceData & resource_info)
{
    WorkerNodeResourceData res;
    res.host_ports = HostWithPorts::createHostWithPorts(resource_info.host_ports());

    res.cpu_usage = resource_info.cpu_usage();
    res.cpu_usage_1min = resource_info.cpu_usage_1min();
    res.cpu_usage_10sec = resource_info.cpu_usage_10sec();
    res.memory_usage = resource_info.memory_usage();
    res.memory_usage_1min = resource_info.memory_usage_1min();
    res.memory_available = resource_info.memory_available();
    res.disk_space = resource_info.disk_space();
    res.query_num = resource_info.query_num();

    if (resource_info.has_cpu_limit())
        res.cpu_limit = resource_info.cpu_limit();
    if (resource_info.has_memory_limit())
        res.memory_limit = resource_info.memory_limit();

    if (resource_info.has_last_update_time())
        res.last_update_time = resource_info.last_update_time();

    if (resource_info.has_reserved_memory_bytes())
        res.reserved_memory_bytes = resource_info.reserved_memory_bytes();
    if (resource_info.has_reserved_cpu_cores())
        res.reserved_cpu_cores = resource_info.reserved_cpu_cores();
    if (resource_info.has_register_time())
        res.register_time = resource_info.register_time();

    return res;
}

void WorkerNodeResourceData::fillProto(Protos::WorkerNodeResourceData & resource_info) const
{
    HostWithPorts::fillHostWithPorts(host_ports, *resource_info.mutable_host_ports());

    resource_info.set_cpu_usage(cpu_usage);
    resource_info.set_cpu_usage_1min(cpu_usage_1min);
    resource_info.set_memory_usage(memory_usage);
    resource_info.set_memory_usage_1min(memory_usage_1min);
    resource_info.set_memory_available(memory_available);
    resource_info.set_disk_space(disk_space);
    resource_info.set_query_num(query_num);

    if (cpu_limit && memory_limit)
    {
        resource_info.set_cpu_limit(cpu_limit);
        resource_info.set_memory_limit(memory_limit);
    }

    if (last_update_time)
        resource_info.set_last_update_time(last_update_time);

    if (reserved_memory_bytes)
        resource_info.set_reserved_memory_bytes(reserved_memory_bytes);
    if (reserved_cpu_cores)
        resource_info.set_reserved_cpu_cores(reserved_cpu_cores);
    if (register_time)
        resource_info.set_register_time(register_time);

    if (last_status_create_time)
        resource_info.set_last_status_create_time(last_status_create_time);
    resource_info.set_cpu_usage_10sec(cpu_usage_10sec);
}

}
