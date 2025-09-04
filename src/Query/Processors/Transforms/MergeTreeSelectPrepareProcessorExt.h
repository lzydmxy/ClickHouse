#pragma once

#include <Processors/ISource.h>
#include <Query/Processors/QueryPlan/TableScanStepExt.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>


namespace DB
{

class MergeTreeSelectPrepareProcessorExt : public ISource
{
public:
    MergeTreeSelectPrepareProcessorExt(
        TableScanStepExt & step_,
        const BuildQueryPipelineSettingsExt & build_settings,
        Block header,
        const std::vector<RuntimeFilterId> & ids_,
        UInt64 wait_time);

    String getName() const override
    {
        return "MergeTreeSelectPrepareProcessorExt";
    }

    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

private:
    TableScanStepExt & step;
    BuildQueryPipelineSettingsExt settings;
    Processors processors;
    UInt64 rf_wait_time_ns;
    std::vector<std::string> runtime_filters;
    Stopwatch timing;
    bool poll_done = false;
    bool start_poll = false;
    bool start_expand = false;
};

}
