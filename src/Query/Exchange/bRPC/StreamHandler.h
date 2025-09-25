#pragma once
#include <brpc/channel.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Query/Common/BoundedDataQueue.h>
#include <Query/Exchange/bRPC/BrpcRemoteBroadcastReceiver.h>

namespace DB
{
class StreamHandler : public brpc::StreamInputHandler
{
public:
    StreamHandler(const ContextPtr & context_, BrpcRemoteBroadcastReceiverShardPtr receiver_, Block header_, bool keep_order_)
        : context(context_), optimizer_context(context_->getOptimizerContext()), receiver(receiver_)
        , header(std::move(header_)), keep_order(keep_order_)
    {
    }

    int on_received_messages(brpc::StreamId id, butil::IOBuf * const * messages, size_t size) noexcept override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

    void on_failed(brpc::StreamId id, int32_t error_code, const std::string& error_text) override;

    void on_finished(brpc::StreamId id, int32_t finish_status_code) override;
private:
    ContextPtr context;
    OptimizerContextPtr optimizer_context;
    LoggerPtr log = getLogger("StreamHandler");
    BrpcRemoteBroadcastReceiverWeakPtr receiver;
    Block header;
    bool keep_order;
};

using StreamHandlerPtr = std::shared_ptr<StreamHandler>;

}
