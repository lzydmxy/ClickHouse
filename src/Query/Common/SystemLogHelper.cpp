#include "SystemLogHelper.h"
#include <Common/DateLUT.h>

namespace DB
{

void SystemLogHelper::addProcessorsProfileLog(
    std::shared_ptr<ProcessorsProfileLog> processors_profile_log,
    const QueryPipeline *pipeline,
    const String& query_id,
    std::chrono::time_point<std::chrono::system_clock> finish_time,
    int segment_id = 0)
{
    ProcessorProfileLogElement processor_elem;
    processor_elem.event_time = timeInSeconds(finish_time);
    processor_elem.event_time_microseconds = timeInMicroseconds(finish_time);
    processor_elem.query_id = query_id;

    auto get_proc_id = [](const IProcessor & proc) -> UInt64
    {
        return reinterpret_cast<std::uintptr_t>(&proc);
    };
    for (const auto & processor : pipeline->getProcessors())
    {
        std::vector<UInt64> parents;
        for (const auto & port : processor->getOutputs())
        {
            if (!port.isConnected())
                continue;
            const IProcessor & next = port.getInputPort().getProcessor();
            parents.push_back(get_proc_id(next));
        }

        processor_elem.id = get_proc_id(*processor);
        processor_elem.parent_ids = std::move(parents);
        processor_elem.plan_step = reinterpret_cast<std::uintptr_t>(processor->getQueryPlanStep());
        /// plan_group is set differently to community CH,
        /// which is processor->getQueryPlanStepGroup();
        /// here, it is combined with the segment_id and 
        /// invoking count for visualizing processors in the profiling website
        processor_elem.plan_group = processor->getQueryPlanStepGroup() | (segment_id << 16);

        processor_elem.processor_name = processor->getName();

        processor_elem.elapsed_us = processor->getElapsedUs();
        processor_elem.input_wait_elapsed_us = processor->getInputWaitElapsedUs();
        processor_elem.output_wait_elapsed_us = processor->getOutputWaitElapsedUs();

        auto stats = processor->getProcessorDataStats();
        processor_elem.input_rows = stats.input_rows;
        processor_elem.input_bytes = stats.input_bytes;
        processor_elem.output_rows = stats.output_rows;
        processor_elem.output_bytes = stats.output_bytes;

        processors_profile_log->add(processor_elem);
    }
}

}
