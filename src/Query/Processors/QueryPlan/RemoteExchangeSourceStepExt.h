#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Common/MultiPathBoundedQueue.h>
#include <Query/Exchange/Local/LocalChannelOptions.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Query/Exchange/bRPC/BrpcRemoteBroadcastReceiver.h>

#include <memory>


namespace DB
{
class PlanSegmentInput;
using PlanSegmentInputPtr = std::shared_ptr<PlanSegmentInput>;
using PlanSegmentInputs = std::vector<PlanSegmentInputPtr>;

class PlanSegment;
using PlanSegmentSharedPtr = std::shared_ptr<PlanSegment>;

class RemoteExchangeSourceStepExt : public ISourceStep
{
public:
    explicit RemoteExchangeSourceStepExt(PlanSegmentInputs inputs_, DataStream input_stream_, bool is_add_totals_, bool is_add_extremes_);

    String getName() const override { return "RemoteExchangeSourceStepExt"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    PlanSegmentInputs getInput() const { return inputs; }
    void setInputs(PlanSegmentInputs inputs_)
    {
        inputs = std::move(inputs_);
    }
    void setInputStream(DataStream input_stream_)
    {
        input_streams = {std::move(input_stream_)};
    }

    void setPlanSegment(const PlanSegmentSharedPtr & plan_segment_, ContextPtr context_);
    PlanSegmentSharedPtr getPlanSegment() const { return plan_segment; }
    size_t getPlanSegmentId() const { return plan_segment_id; }


    void describePipeline(FormatSettings & settings) const override;

    void setExchangeOptions(ExchangeOptions options_) { options = options_; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

    bool isAddTotals() const { return is_add_totals; }
    bool isAddExtremes() const  { return is_add_extremes; }

private:
    // TODO: if bsp_mode is required, then add other codes
    BroadcastReceiverPtr createReceiver(
        bool is_local_exchange,
        const LocalChannelOptions & local_options,
        size_t write_plan_segment_id,
        size_t exchange_id,
        size_t partition_id,
        ExchangeDataKeyPtr data_key,
        const Block & exchange_header,
        bool keep_order,
        bool enable_metrics,
        const String & write_address_info,
        MultiPathQueuePtr collector,
        std::shared_ptr<QueryExchangeLog> query_exchange_log);
    PlanSegmentInputs inputs;
    PlanSegmentSharedPtr plan_segment;
    LoggerPtr logger;
    size_t plan_segment_id;
    String query_id;
    String coordinator_address;
    AddressInfo read_address_info;
    ContextPtr context;
    ExchangeOptions options;
    bool is_add_totals;
    bool is_add_extremes;
};
}
