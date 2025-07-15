#pragma once
#include <vector>
#include <unordered_map>
#include <base/types.h>
#include <Common/logger_useful.h>
#include <IO/Progress.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Interpreters/QueryLogExt.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Query/Common/PlanSegmentProfile.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/RuntimeSegmentsStatus.h>
#include <Query/Executor/PlanSegmentProcessList.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/ExchangeUtils.h>

namespace DB
{
class ThreadGroupStatus;
struct BlockIO;

class QueryPipeline;
using QueryPipelinePtr = std::unique_ptr<QueryPipeline>;

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
        AddressInfoPtr coordinator_address;
        RuntimeSegmentStatus runtime_segment_status;
        RSenderMetrics sender_metrics;
        PlanSegmentProfilePtr segment_profile;
    };

    std::optional<ExecutionResult> execute();
    BlockIO lazyExecute(bool add_output_processors = false);

    static void registerAllExchangeReceivers(LoggerPtr log, const QueryPipeline & pipeline, UInt32 register_timeout_ms);

protected:
    void doExecute();
    QueryPipeline buildPipeline();
    QueryPipeline buildPipeline(BroadcastSenderPtrs & senders);

private:
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry;

    ContextMutablePtr context;
    OptimizerContextPtr optimizer_context;
    PlanSegmentInstancePtr plan_segment_instance;
    PlanSegment * plan_segment;
    PlanSegmentOutputs plan_segment_outputs;
    ExchangeOptions options;
    LoggerPtr logger;
    RuntimeSegmentsMetrics metrics;
    std::unique_ptr<QueryLogElementExt> query_log_element;
    SenderMetrics sender_metrics;
    Progress progress;
    Progress final_progress;
    PlanSegmentProfilePtr segment_profile;
    InternalTextLogsQueuePtr non_initial_node_logs_queue;  // used for distributed query

    Processors buildRepartitionExchangeSink(BroadcastSenderPtrs & senders, bool keep_order, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    Processors buildBroadcastExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    Processors buildLoadBalancedExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    void collectSegmentQueryRuntimeMetric(const QueryStatus * query_status);
    void prepareSegmentInfo() const;
    void sendProgress();
    void sendLogs();
};

}
