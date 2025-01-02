#pragma once

#include <Common/Logger.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <IO/Progress.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/QueryLog.h>
#include <Query/Executor/AddressInfo.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/RuntimeSegmentsStatus.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Query/Protos/plan_segment_manager.pb.h>

#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Interpreters/profile/PlanSegmentProfile.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/QueryPipeline.h>
#include <Poco/Logger.h>
#include <common/types.h>

namespace DB
{
class ThreadGroupStatus;
struct BlockIO;

struct SenderMetrics
{
    std::unordered_map<size_t, std::vector<std::pair<UInt64, size_t>>> bytes_sent;
};

class PlanSegmentExecutor
{
public:
    explicit PlanSegmentExecutor(
        PlanSegmentInstancePtr plan_segment_instance_,
        ContextMutablePtr context_,
        PlanSegmentProcessList::EntryPtr process_plan_segment_entry_ = nullptr);
    explicit PlanSegmentExecutor(
        PlanSegmentInstancePtr plan_segment_instance_,
        ContextMutablePtr context_,
        PlanSegmentProcessList::EntryPtr process_plan_segment_entry_,
        ExchangeOptions options_);

    ~PlanSegmentExecutor() noexcept;

    struct ExecutionResult
    {
        AddressInfo coordinator_address;
        RuntimeSegmentStatus runtime_segment_status;
        Protos::SenderMetrics sender_metrics;
        PlanSegmentProfilePtr segment_profile;
    };
    std::optional<ExecutionResult> execute();
    BlockIO lazyExecute(bool add_output_processors = false);

    static void registerAllExchangeReceivers(LoggerPtr log, const QueryPipeline & pipeline, UInt32 register_timeout_ms);

protected:
    void doExecute();
    QueryPipelinePtr buildPipeline();
    void buildPipeline(QueryPipelinePtr & pipeline, BroadcastSenderPtrs & senders);

private:
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry;

    ContextMutablePtr context;
    PlanSegmentInstancePtr plan_segment_instance;
    PlanSegment * plan_segment;
    PlanSegmentOutputs plan_segment_outputs;
    ExchangeOptions options;
    LoggerPtr logger;
    RuntimeSegmentsMetrics metrics;
    std::unique_ptr<QueryLogElement> query_log_element;
    SenderMetrics sender_metrics;
    Progress progress;
    Progress final_progress;
    PlanSegmentProfilePtr segment_profile;

    Processors buildRepartitionExchangeSink(BroadcastSenderPtrs & senders, bool keep_order, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    Processors buildBroadcastExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    Processors buildLoadBalancedExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    void collectSegmentQueryRuntimeMetric(const QueryStatus * query_status);
    void prepareSegmentInfo() const;
    void sendProgress();
};

}
