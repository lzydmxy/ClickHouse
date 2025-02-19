#pragma once
#include <QueryPipeline/BlockIO.h>

namesapce DB 
{

struct MPPBlockIO : public BlockIO 
{
    std::shared_ptr<MPPQueryCoordinator> coordinator;
    std::shared_ptr<ProcessListEntry> process_list_entry;
    std::shared_ptr<PlanSegmentProcessListEntry> plan_segment_process_entry;
    /// NOTE: make sure it's destructed after streams and pipeline.
    ConnectionPtr remote_execution_conn;
}

}
