#pragma once
#include <atomic>
#include <optional>
#include <vector>
#include <brpc/stream.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Query/Common/MultiPathBoundedQueue.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Exchange/QueryExchangeLog.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/bRPC/AsyncRegisterResult.h>
#include <Query/Exchange/bRPC/BrpcExchangeReceiverRegistryService.h>
#include <Query/Exchange/bRPC/BrpcRemoteBroadcastSender.h>

namespace DB
{

class StreamHandler;
using StreamHandlerPtr = std::shared_ptr<StreamHandler>;

class BrpcRemoteBroadcastReceiver : public std::enable_shared_from_this<BrpcRemoteBroadcastReceiver>, public IBroadcastReceiver
{
public:
    BrpcRemoteBroadcastReceiver(
        ExchangeDataKeyPtr trans_key_,
        String registry_address_,
        ContextPtr context_,
        Block header_,
        bool keep_order_,
        const String &name_,
        BrpcExchangeReceiverRegistryService::RegisterMode mode_ = BrpcExchangeReceiverRegistryService::RegisterMode::BRPC);

    BrpcRemoteBroadcastReceiver(
        ExchangeDataKeyPtr trans_key_,
        String registry_address_,
        ContextPtr context_,
        Block header_,
        bool keep_order_,
        const String & name_,
        MultiPathQueuePtr queue_,
        BrpcExchangeReceiverRegistryService::RegisterMode mode_ = BrpcExchangeReceiverRegistryService::RegisterMode::BRPC,
        std::shared_ptr<QueryExchangeLog> query_exchange_log_ = nullptr,
        String coordinator_address_ = "");

    ~BrpcRemoteBroadcastReceiver() override;

    void registerToSenders(UInt32 timeout_ms) override;
    RecvDataPacket recv(TimePoint timeout_ms) override;
    BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;
    String getName() const override;
    void pushReceiveQueue(MultiPathDataPacket packet);
    void setSendDoneFlag() { send_done_flag.test_and_set(std::memory_order_release); }

    static String generateName(size_t exchange_id, size_t write_segment_id, size_t read_segment_id, size_t parallel_index,
        const String & co_host_port)
    {
        return fmt::format("BrpcReciver[{}_{}_{}_{}_{}]",
            write_segment_id, read_segment_id, parallel_index, exchange_id, co_host_port);
    }

    static String generateNameForTest()
    {
        return generateName(0, 0, 0, 0, "");
    }

    AsyncRegisterResult registerToSendersAsync(UInt32 timeout_ms);
private:
    String name;
    LoggerPtr log = getLogger("BrpcRemoteBroadcastReceiver");
    ExchangeDataKeyPtr trans_key;
    String registry_address;
    ContextPtr context;
    OptimizerContextPtr optimizer_context;
    Block header;
    brpc::StreamOptions stream_options;
    std::atomic<BroadcastStatusCode> finish_status_code{BroadcastStatusCode::RUNNING};
    std::atomic_flag send_done_flag = ATOMIC_FLAG_INIT;
    MultiPathQueuePtr queue;
    brpc::StreamId stream_id{brpc::INVALID_STREAM_ID};
    bool keep_order;
    String initial_query_id;
    BrpcExchangeReceiverRegistryService::RegisterMode mode;
    std::shared_ptr<QueryExchangeLog> query_exchange_log;
    String coordinator_address;

    void sendRegisterRPC(
        Protos::RegistryService_Stub & stub,
        brpc::Controller & cntl,
        Protos::RegistryRequest * request,
        Protos::RegistryResponse * response,
        google::protobuf::Closure * done);
};

using BrpcRemoteBroadcastReceiverShardPtr = std::shared_ptr<BrpcRemoteBroadcastReceiver>;
using BrpcRemoteBroadcastReceiverWeakPtr = std::weak_ptr<BrpcRemoteBroadcastReceiver>;
using BrpcReceiverPtrs = std::vector<BrpcRemoteBroadcastReceiverShardPtr>;
}
