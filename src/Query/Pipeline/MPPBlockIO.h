#pragma once
#include <QueryPipeline/BlockIO.h>

namesapce DB 
{
/**
 * MPPBlockIO is not needed. Since ClickHouse 21.11, BlockIO has removed many redundant fields.
 * Keep this code for now and remove it after the official release.
 */
struct MPPBlockIO : public BlockIO 
{
    // BlockOutputStreamPtr out;
    // BlockInputStreamPtr in;
    std::shared_ptr<MPPQueryCoordinator> coordinator;
    std::shared_ptr<ProcessListEntry> process_list_entry;
    std::shared_ptr<PlanSegmentProcessListEntry> plan_segment_process_entry;
    /// NOTE: make sure it's destructed after streams and pipeline.
    ConnectionPtr remote_execution_conn;
}

}
