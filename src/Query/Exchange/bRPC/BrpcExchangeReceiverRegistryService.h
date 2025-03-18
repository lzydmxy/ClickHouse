#pragma once
#include <brpc/server.h>
#include <brpc/stream.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Query/Protos/registry.pb.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Common/OptimizerSettings.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/bRPC/BrpcServiceDefines.h>

namespace DB
{
class BrpcExchangeReceiverRegistryService : public Protos::RegistryService
{
public:
    enum RegisterMode
    {
        BRPC = 0,
        //BSP mode support DISK_READER
        //DISK_READER = 1
    };
    explicit BrpcExchangeReceiverRegistryService(ContextMutablePtr context_)
        : context(std::move(context_)), max_buf_size(context->getOptimizerContext()->getSettingsRef().exchange_stream_max_buf_size)
    {
    }
    explicit BrpcExchangeReceiverRegistryService(int max_buf_size_) : max_buf_size(max_buf_size_)
    {
    }

    /// register the brpc sender of pipeline mode,
    /// exchange data is loaded from in-memory pipeline executions
    void registry(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::RegistryRequest * request,
        ::DB::Protos::RegistryResponse * response,
        ::google::protobuf::Closure * done) override;

    /// register the brpc sender of bsp mode,
    /// exchange data is loaded from disk dumped by previous execution
    /// Not supported BSP mode now, See registerBRPCSenderFromDisk method in BC
    void registerSenderFromDisk(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::RegistryDiskSenderRequest * request,
        ::DB::Protos::RegistryResponse * response,
        ::google::protobuf::Closure * done) override;

    /// cancel exchange data reader(only bsp mode)
    /// Not supported BSP mode now
    void cancelExchangeDataReader(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::CancelExchangeDataReaderRequest * request,
        ::DB::Protos::CancelExchangeDataReaderResponse * response,
        ::google::protobuf::Closure * done) override;

    /// Not supported BSP mode now
    void cleanupExchangeData(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::CleanupExchangeDataRequest * request,
        ::DB::Protos::CleanupExchangeDataResponse * response,
        ::google::protobuf::Closure * done) override;

    void sendExchangeDataHeartbeat(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::ExchangeDataHeartbeatRequest * request,
        ::DB::Protos::ExchangeDataHeartbeatResponse * response,
        ::google::protobuf::Closure * done) override;

    void setContext(ContextMutablePtr context_)
    {
        context = context_;
    }

private:
    ContextMutablePtr context;
    int max_buf_size;
    LoggerPtr log = getLogger("BrpcExchangeReceiverRegistryService");

    /// stream will be accepted, but the host socket of the accpeted stream
    /// is not really set yet until done->Run() is called
    void acceptStream(
        brpc::Controller * cntl,
        uint64_t accept_timeout_ms,
        BroadcastSenderProxyPtr sender,
        const String & query_id,
        brpc::StreamId & sender_stream_id);

    /// proxy will become real sender in this method
    void registerSenderToProxy(
        const DiskExchangeDataManagerPtr & mgr,
        const BroadcastSenderProxyPtr & sender_proxy,
        const String & query_id,
        const brpc::StreamId & sender_stream_id,
        Processors processors,
        const ExchangeDataKeyPtr & key,
        const String & coordinator_addr,
        bool read_from_disk);
};

REGISTER_SERVICE_IMPL(BrpcExchangeReceiverRegistryService);
}
