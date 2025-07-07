#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>

#include <Interpreters/Context_fwd.h>
#include <Query/ProtosHelper/ExchangeMode.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentProcessList.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/DataTrans/LocalBroadcastChannel.h>
#include <Query/Exchange/DataTrans/MultiPathReceiver.h>
#include <Query/Exchange/DataTrans/DeserializeBufTransform.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Processors/Exchange/ExchangeSourceExt.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Common/Exception.h>
#include <Query/Exchange/QueryExchangeLog.h>
#include <Query/Executor/sendPlanSegment.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

#include <memory>
#include <string>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RemoteExchangeSourceStepExt::RemoteExchangeSourceStepExt(PlanSegmentInputs inputs_, DataStream input_stream_, bool is_add_totals_, bool is_add_extremes_)
    : ISourceStep(DataStream{.header = inputs_[0]->getHeader()}), inputs(std::move(inputs_)), is_add_totals(is_add_totals_), is_add_extremes(is_add_extremes_)
{
    input_streams.emplace_back(std::move(input_stream_));
    logger = getLogger("RemoteExchangeSourceStepExt");
}

void RemoteExchangeSourceStepExt::toProto(Protos::RemoteExchangeSourceStepExt & proto, bool) const
{
    // NOTE: this step is ISourceStep but not using serde of ISourceStep
    // maybe a bug, but here just follow the original serde anyway
    ProtosSerDerHelper::toProto(input_streams[0], *proto.mutable_input_stream());
    proto.set_step_description(step_description);
    for (auto & element : inputs)
    {
        if (!element)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PlanSegmentInput cannot be nullptr");
        element->toProto(*proto.add_inputs());
    }
    proto.set_is_add_totals(is_add_totals);
    proto.set_is_add_extremes(is_add_extremes);
}

std::shared_ptr<RemoteExchangeSourceStepExt> RemoteExchangeSourceStepExt::fromProto(const Protos::RemoteExchangeSourceStepExt & proto, ContextPtr context)
{
    DataStream input_stream;
    ProtosSerDerHelper::fillFromProto(input_stream, proto.input_stream());
    auto step_description = proto.step_description();

    PlanSegmentInputs inputs;
    for (auto & proto_element : proto.inputs())
    {
        auto element = std::make_shared<PlanSegmentInput>();
        element->fromProto(proto_element, context);
        inputs.emplace_back(std::move(element));
    }

    bool is_add_totals = proto.has_is_add_totals() ? proto.is_add_totals(): false;
    bool is_add_extremes = proto.has_is_add_extremes() ? proto.is_add_extremes(): false;

    auto step = std::make_unique<RemoteExchangeSourceStepExt>(inputs, input_stream, is_add_totals, is_add_extremes);
    step->setStepDescription(step_description);

    return step;
}

std::shared_ptr<IQueryPlanStep> RemoteExchangeSourceStepExt::copy(ContextPtr) const
{
    return std::make_shared<RemoteExchangeSourceStepExt>(inputs, input_streams[0], is_add_totals, is_add_extremes);
}

void RemoteExchangeSourceStepExt::setPlanSegment(PlanSegment * plan_segment_, ContextPtr context_)
{
    context = context_;
    plan_segment = plan_segment_;
    plan_segment_id = plan_segment->getPlanSegmentId();
    /// only plan segment at server needs to set totals source or extremes source
    if (plan_segment_id != 0)
    {
        is_add_totals = false;
        is_add_extremes = false;
    }
    query_id = plan_segment->getQueryId();
    coordinator_address = extractExchangeHostPort(plan_segment->getCoordinatorAddress());
    read_address_info = getLocalAddress(context);
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Plan segment not set context");
    options = ExchangeUtils::getExchangeOptions(context);
}

void RemoteExchangeSourceStepExt::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    //todo: zhangwanyun, other feat: need query_unique_id from getCurrentTransactionID
    if (!plan_segment)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Should setPlanSegment before initializePipeline!");

    Pipe pipe;
    size_t source_num = 0;

    const auto & settings_ext = BuildQueryPipelineSettingsExt::cast(settings);
    auto optimizer_context = context->getOptimizerContext();
    UInt64 current_tx_id = optimizer_context->getTransactionID(context->getInitialQueryId());

    bool keep_order = optimizer_context->getSettingsRef().exchange_enable_force_keep_order ||  optimizer_context->getSettingsRef().enable_shuffle_with_order;
    if (!keep_order)
    {
        for (const auto & input : inputs)
        {
            if (input->needKeepOrder())
            {
                keep_order = input->needKeepOrder();
                break;
            }
        }
    }

    const Block & exchange_header = getOutputStream().header;
    Block source_header;
    if (keep_order)
        source_header = exchange_header;

    LOG_TRACE(logger, "Initialize pipeline: keeper order {}, is_add_totals {}, is_add_extremes {}, inputs size {}, enable multi receiver {}, source header columns {}, exchange header columns {}",
        keep_order, is_add_totals, is_add_extremes, inputs.size(),
        optimizer_context->getSettingsRef().exchange_enable_multipath_receiver,
        source_header.columns(), exchange_header.columns());

    ExchangeTotalsSourcePtr totals_source;
    if (is_add_totals)
        totals_source = std::make_shared<ExchangeTotalsSourceExt>(source_header);

    ExchangeExtremesSourcePtr extremes_source;
    if (is_add_extremes)
        extremes_source = std::make_shared<ExchangeExtremesSourceExt>(source_header);
    auto enable_metrics = optimizer_context->getSettingsRef().log_query_exchange;
    auto query_exchange_log = enable_metrics ? context->getOptimizerContext()->getQueryExchangeLog(): nullptr;
    auto register_mode = BrpcExchangeReceiverRegistryService::BRPC;
    //todo: zhangwanyun, other feat: need bsp_mode context->getSettingsRef().bsp_mode ? context->getDiskExchangeDataManager()
    auto disk_exchange_mgr = nullptr;
    size_t local_queue_size = optimizer_context->getSettingsRef().exchange_local_receiver_queue_size;
    size_t remote_queue_size = optimizer_context->getSettingsRef().exchange_remote_receiver_queue_size;
    size_t multi_path_queue_size = optimizer_context->getSettingsRef().exchange_multi_path_receiver_queue_size;
    std::shared_ptr<MemoryController> memory_controller;
    auto weak_segment_process_list_entry = optimizer_context->getPlanSegmentProcessListEntry();
    if (weak_segment_process_list_entry)
        memory_controller = weak_segment_process_list_entry->getMemoryController();

    size_t input_index = 0;
    for (const auto & input : inputs)
    {
        size_t write_plan_segment_id = input->getPlanSegmentId();
        size_t exchange_parallel_size = input->getExchangeParallelSize();
        UInt32 exchange_id = input->getExchangeId();
        UInt32 parallel_id = optimizer_context->getPlanSegmentInstanceID().parallel_index;
        auto exchange_mode = input->getExchangeMode();
        if (exchange_mode == RExchangeMode::LOCAL_NO_NEED_REPARTITION || exchange_mode == RExchangeMode::LOCAL_MAY_NEED_REPARTITION)
            parallel_id = 0;
        else if (exchange_mode == RExchangeMode::BROADCAST)
            exchange_parallel_size = 1;
        size_t partition_id_start = parallel_id * exchange_parallel_size;
        LocalChannelOptions local_options{
            .queue_size = local_queue_size, .max_timeout_ts = options.exchange_timeout_ts, .enable_metrics = enable_metrics};
        auto iter = settings_ext.sources.find(exchange_id);
        if (input->getSourceAddress().empty() && !settings_ext.distributed_settings.is_explain && iter == settings_ext.sources.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No source address for segment {}'s input segment {}, parallel id is {}",
                    optimizer_context->getPlanSegmentInstanceID().segment_id, write_plan_segment_id,
                    optimizer_context->getPlanSegmentInstanceID().parallel_index);
        bool enable_block_compress = optimizer_context->getSettingsRef().exchange_enable_block_compress;
        BroadcastReceiverPtrs receivers;
        MultiPathQueuePtr collector = nullptr;
        if (optimizer_context->getSettingsRef().exchange_enable_multipath_receiver && !keep_order)
            collector = std::make_shared<MultiPathBoundedQueue>(multi_path_queue_size, memory_controller);
        //todo: zhangwanyun, other feat: need bsp_mode context->getSettingsRef().bsp_mode
        bool is_final_plan_segment = false;
        if (plan_segment_id == 0)
            is_final_plan_segment = true;
        for (const auto & source_address : input->getSourceAddress())
        {
            auto write_address = extractExchangeHostPort(source_address);
            for (size_t i = 0; i < exchange_parallel_size; ++i)
            {
                UInt32 partition_id = partition_id_start + i;
                ExchangeDataKeyPtr data_key;
                //todo: zhangwanyun, other feat: if bsp_mode is required, then add other codes
                data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id);

                // TODO wujianchao support local exchange optimization
                // bool is_local_exchange = ExchangeUtils::isLocalExchange(read_address_info, source_address); // TODO wujianchao read_address_info is hostname source_address is localhost
                bool is_local_exchange = false;

                LOG_TRACE(logger, "Initialize pipeline input index {}, exchange data key {}, is local exchange {} for receiver," \
                    "read address {}, write address {}, collector is null {}",
                    input_index, *data_key, is_local_exchange, read_address_info.toShortString(), source_address.toShortString(),
                    collector == nullptr);

                BroadcastReceiverPtr receiver = createReceiver(disk_exchange_mgr, is_local_exchange, local_options, write_plan_segment_id,
                    exchange_id, partition_id, data_key, exchange_header, keep_order, enable_metrics, write_address, collector,
                    register_mode, query_exchange_log);
                receivers.emplace_back(std::move(receiver));
            }
            // input_index++;
        }

        if (optimizer_context->getSettingsRef().exchange_enable_multipath_receiver && !keep_order)
        {
            if (settings_ext.distributed_settings.is_explain)
            {
                ExchangeDataKeyPtr data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id_start);
                String name = BrpcRemoteBroadcastReceiver::generateName(
                            exchange_id, write_plan_segment_id, plan_segment_id, partition_id_start, coordinator_address);
                auto queue = std::make_shared<MultiPathBoundedQueue>(remote_queue_size, memory_controller);

                auto brpc_receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(std::move(data_key), "", context, exchange_header,
                    keep_order, name, std::move(queue), register_mode, query_exchange_log);

                brpc_receiver->setEnableReceiverMetrics(enable_metrics);
                BroadcastReceiverPtr receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(brpc_receiver);
                receivers.emplace_back(std::move(receiver));
                source_num++;
            }
            String receiver_name = MultiPathReceiver::generateName(
                exchange_id, write_plan_segment_id, plan_segment_id, coordinator_address);
            auto multi_path_options
                = MultiPathReceiverOptions{.enable_block_compress = enable_block_compress, .enable_metrics = enable_metrics};
            auto multi_path_receiver = std::make_shared<MultiPathReceiver>(
                collector, std::move(receivers), exchange_header, receiver_name, std::move(multi_path_options), context);

            // LOG_DEBUG(logger, "Create multi receiver name {}, source_header columns {}, struct {}",
            //     multi_path_receiver->getName(), source_header.columns(), source_header.dumpStructure());

            auto source = std::make_shared<ExchangeSourceExt>(source_header, std::move(multi_path_receiver), options, is_final_plan_segment,
                totals_source, extremes_source);
            pipe.addSource(std::move(source));
            source_num++;
        }
        else
        {
            if (settings_ext.distributed_settings.is_explain)
            {
                ExchangeDataKeyPtr data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id_start);
                String name = BrpcRemoteBroadcastReceiver::generateName(
                            exchange_id, write_plan_segment_id, plan_segment_id, partition_id_start, coordinator_address);
                auto brpc_receiver = std::make_shared<BrpcRemoteBroadcastReceiver>( std::move(data_key), "", context, exchange_header,
                    keep_order, name, std::make_shared<MultiPathBoundedQueue>(remote_queue_size, memory_controller));
                BroadcastReceiverPtr receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(brpc_receiver);
                // LOG_DEBUG(logger, "Create single receiver name {}, source_header columns {}", receiver->getName(), source_header.columns());
                auto source = std::make_shared<ExchangeSourceExt>(source_header, std::move(receiver), options, is_final_plan_segment, totals_source, extremes_source);
                pipe.addSource(std::move(source));
                source_num++;
            }
            for (auto & receiver : receivers)
            {
                auto source = std::make_shared<ExchangeSourceExt>(source_header, std::move(receiver), options, is_final_plan_segment);
                pipe.addSource(std::move(source));
                source_num++;
            }
        }
        input_index ++;
    }

    if (is_add_totals)
        pipe.addTotalsSource(std::move(totals_source));
    if (is_add_extremes)
        pipe.addExtremesSource(std::move(extremes_source));

    LOG_TRACE(logger, "Initialize pipeline pipe processors size {}", pipe.getProcessors().size());

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));

    if (!keep_order)
    {
        pipeline.resize(optimizer_context->getSettingsRef().exchange_source_pipeline_threads);
        pipeline.addSimpleTransform([enable_compress = optimizer_context->getSettingsRef().exchange_enable_block_compress, header = exchange_header](
                                        const Block &) { return std::make_shared<DeserializeBufTransform>(header, enable_compress); });
    }

    auto prev_pipe_threads = pipeline.getNumThreads();

    pipeline.setMaxThreads(source_num);

    LOG_DEBUG(logger, "Initialize pipeline, total source_num {}, pipeline threads {}, prev {}", source_num, pipeline.getNumThreads(), prev_pipe_threads);
}

BroadcastReceiverPtr RemoteExchangeSourceStepExt::createReceiver(
    DiskExchangeDataManagerPtr disk_mgr,
    bool is_local_exchange,
    const LocalChannelOptions & local_options,
    size_t write_plan_segment_id,
    size_t exchange_id,
    size_t partition_id,
    ExchangeDataKeyPtr data_key,
    const Block & exchange_header,
    bool keep_order,
    bool enable_metrics,
    const String & write_address,
    MultiPathQueuePtr collector,
    BrpcExchangeReceiverRegistryService::RegisterMode register_mode,
    std::shared_ptr<QueryExchangeLog> query_exchange_log)
{
    BroadcastReceiverPtr receiver;
    auto optimizer_context = context->getOptimizerContext();
    size_t remote_queue_size = optimizer_context->getSettingsRef().exchange_remote_receiver_queue_size;
    std::shared_ptr<MemoryController> memory_controller;
    auto weak_segment_process_list_entry = optimizer_context->getPlanSegmentProcessListEntry();
    if (weak_segment_process_list_entry)
        memory_controller = weak_segment_process_list_entry->getMemoryController();
    if (is_local_exchange)
    {
        if (!options.force_remote_mode)
        {
            LOG_TRACE(logger, "Create local exchange source : {}@{} for plansegment {}->{}",
                *data_key, write_address, write_plan_segment_id, plan_segment_id);

            String name = LocalBroadcastChannel::generateName(
                exchange_id, write_plan_segment_id, plan_segment_id, partition_id, coordinator_address);
            auto queue = collector ? collector : std::make_shared<MultiPathBoundedQueue>(local_options.queue_size, memory_controller);
            auto local_channel = std::make_shared<LocalBroadcastChannel>(data_key, local_options, name, std::move(queue), context);
            receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(local_channel);
            //todo: zhangwanyun, other feat: if bsp_mode is required, then add other codes
        }
        else
        {
            String localhost_address = optimizer_context->getHostWithPorts().getExchangeAddress();
            LOG_TRACE(logger, "Force local exchange use remote source : {}@{} for plansegment {}->{}",
                *data_key, localhost_address, write_plan_segment_id, plan_segment_id);

            String name = BrpcRemoteBroadcastReceiver::generateName(
                exchange_id, write_plan_segment_id, plan_segment_id, partition_id, coordinator_address);
            auto queue = collector ? collector
                                   : std::make_shared<MultiPathBoundedQueue>(remote_queue_size, memory_controller);
            auto brpc_receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(std::move(data_key), localhost_address, context,
                exchange_header, keep_order, name, std::move(queue), register_mode, query_exchange_log, coordinator_address);
            brpc_receiver->setEnableReceiverMetrics(enable_metrics);
            receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(brpc_receiver);
        }
    }
    else
    {
        LOG_TRACE(logger, "Create remote exchange source : {}@{} for plansegment {}->{}", *data_key, write_address, write_plan_segment_id,
            plan_segment_id);

        String name = BrpcRemoteBroadcastReceiver::generateName(
            exchange_id, write_plan_segment_id, plan_segment_id, partition_id, coordinator_address);
        auto queue = collector ? collector
                               : std::make_shared<MultiPathBoundedQueue>(remote_queue_size, memory_controller);
        auto brpc_receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(std::move(data_key), write_address, context, exchange_header,
            keep_order, name, std::move(queue), register_mode, query_exchange_log, coordinator_address);

        brpc_receiver->setEnableReceiverMetrics(enable_metrics);
        receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(brpc_receiver);
    }
    return receiver;
}

void RemoteExchangeSourceStepExt::describePipeline(FormatSettings & settings) const
{
    if (!inputs.empty())
        settings.out << String(settings.offset, settings.indent_char) << "Source segment_id : [ " << std::to_string(inputs.back().get()->getPlanSegmentId()) << " ]\n";
    ISourceStep::describePipeline(settings);
}

}
