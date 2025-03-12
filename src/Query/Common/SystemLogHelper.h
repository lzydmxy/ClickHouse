#pragma once
#include <QueryPipeline/QueryPipeline.h>
#include <Interpreters/ProcessorsProfileLog.h>

namespace DB
{

/// Processor profile log, query log etc.
class SystemLogHelper
{
public:
    static void addProcessorsProfileLog(
        std::shared_ptr<ProcessorsProfileLog> processor_profile_log,
        const QueryPipeline *pipeline,
        const String& query_id,
        std::chrono::time_point<std::chrono::system_clock> finish_time,
        int segment_id);
};

}
