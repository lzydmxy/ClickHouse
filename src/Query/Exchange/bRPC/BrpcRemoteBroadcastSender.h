#pragma once
#include <atomic>
#include <mutex>
#include <vector>
#include <brpc/stream.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/bRPC/WriteBufferFromBrpc.h>

namespace DB
{

class BrpcRemoteBroadcastSender : public IBroadcastSender
{
public:
    BrpcRemoteBroadcastSender(ExchangeDataKeyPtr trans_key_, brpc::StreamId stream_id, ContextPtr context_, Block header_);
    ~BrpcRemoteBroadcastSender() override;

    BroadcastStatus sendImpl(Chunk chunk) override;
    BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;

    /// Merge another BrpcRemoteBroadcastSender to this sender, to simplify code, we assume that no member method is called concurrently
    void merge(IBroadcastSender && sender) override;
    String getName() const override;
    BroadcastSenderType getType() override { return BroadcastSenderType::Brpc; }

private:
    LoggerPtr log = getLogger("BrpcRemoteBroadcastSender");
    ExchangeDataKeyPtrs trans_keys;
    ContextPtr context;
    OptimizerContextPtr optimizer_context;
    Block header;
    std::vector<brpc::StreamId> sender_stream_ids;

    BroadcastStatus sendIOBuffer(const butil::IOBuf & io_buffer, brpc::StreamId stream_id, const ExchangeDataKey & data_key);
    void serializeChunkToIoBuffer(Chunk chunk, WriteBufferFromBrpc & out) const;
};

}
