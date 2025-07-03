#include "BroadcastSenderProxy.h"
#include <algorithm>
#include <mutex>
#include <optional>
#include <Common/Exception.h>
#include <base/types.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Common/OptimizerSettings.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
}

BroadcastSenderProxy::BroadcastSenderProxy(ExchangeDataKeyPtr data_key_, SenderProxyOptions options)
    : data_key(std::move(data_key_)), wait_timeout_ms(options.wait_timeout_ms), logger(getLogger("BroadcastSenderProxy"))
{
}

BroadcastSenderProxy::~BroadcastSenderProxy()
{
    try
    {
        BroadcastSenderProxyRegistry::instance().remove(data_key);
    }
    catch (...)
    {
        tryLogCurrentException(logger);
    }
}

BroadcastStatus BroadcastSenderProxy::sendImpl(Chunk chunk)
{
    if (!has_real_sender.load(std::memory_order_acquire))
        waitBecomeRealSender(wait_timeout_ms);
    return real_sender->send(std::move(chunk));
}

BroadcastStatus BroadcastSenderProxy::finish(BroadcastStatusCode status_code, String message)
{
    LOG_TRACE(logger, "BroadcastSenderProxy::finish key {}, status {}, message {}",
        *data_key, toString(status_code), message);

    if (!has_real_sender.load(std::memory_order_acquire))
    {
        // No need to waitBecomeRealSender since receiver can infer finish status as SEND_UNKNOWN_ERROR
        // if no finish code is received.
        if (status_code > BroadcastStatusCode::RUNNING)
        {
            std::lock_guard lock(mutex);
            if (!real_sender)
            {
                // Wakeup all pending call for waitBecomeRealSender and waitAccept
                closed = true;
                wait_accept.notify_all();
                wait_become_real.notify_all();
                LOG_ERROR(logger, "Proxy {} does not contain a real sender", *data_key);
                return BroadcastStatus(BroadcastStatusCode::SEND_NOT_READY, false, "Sender not ready");
            }
        }

        waitBecomeRealSender(wait_timeout_ms);
    }
    return real_sender->finish(status_code, message);
}

void BroadcastSenderProxy::merge(IBroadcastSender && sender)
{
    if (!has_real_sender.load(std::memory_order_relaxed))
        waitBecomeRealSender(wait_timeout_ms);

    BroadcastSenderProxy * other = dynamic_cast<BroadcastSenderProxy *>(&sender);
    if (!other)
        real_sender->merge(std::move(sender));
    else
    {
        if (!other->has_real_sender)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't merge proxy has no real sender {}", *data_key);

        real_sender->merge(std::move(*other->real_sender));
        other->has_real_sender.store(false, std::memory_order_release);

        std::unique_lock lock(other->mutex);
        other->context = ContextPtr();
        other->header = Block();
    }
}

String BroadcastSenderProxy::getName() const
{
    String prefix = "[Proxy]";
    return real_sender ? prefix + real_sender->getName() : prefix + data_key->toString();
}

void BroadcastSenderProxy::waitAccept(UInt32 timeout_ms)
{
    std::unique_lock lock(mutex);
    if (context)
        return;

    LOG_TRACE(logger, "BroadcastSenderProxy::waitAccept {}", *data_key);

    if (!wait_accept.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] {
            return this->header.operator bool() || closed;
        }))
        throw Exception(ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION, "Wait accept timeout for {}, timeout ms {}", *data_key, timeout_ms);
    else if (closed)
        throw Exception(ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION, "Interrput accept for {}", *data_key);
}

void BroadcastSenderProxy::accept(ContextPtr context_, Block header_)
{
    LOG_TRACE(logger, "BroadcastSenderProxy::accept {}, header {}", *data_key, header_.operator bool());

    std::unique_lock lock(mutex);
    if (header || context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't call accept twice for {}", *data_key);
    context = std::move(context_);
    header = std::move(header_);
    auto optimizer_context = context->getOptimizerContext();
    wait_timeout_ms = optimizer_context->getSettingsRef().exchange_wait_accept_max_timeout_ms + 
    optimizer_context->getSettingsRef().wait_runtime_filter_timeout + 3000; // 3000 is send planSegment timeout
    wait_accept.notify_all();
}

void BroadcastSenderProxy::becomeRealSender(BroadcastSenderPtr sender)
{
    std::lock_guard lock(mutex);
    if (closed)
        throw Exception(ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION,
            "becomeRealSender failed, BroadcastSenderProxy {} already closed", *data_key);
    if (real_sender)
    {
        if (real_sender != sender)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't set set real sender twice for {}", *data_key);
        return;
    }

    LOG_TRACE(logger, "Proxy become real sender: {}", sender->getName());
    real_sender = std::move(sender);
    has_real_sender.store(true, std::memory_order_release);
    wait_become_real.notify_all();
}

void BroadcastSenderProxy::waitBecomeRealSender(UInt32 timeout_ms)
{
    std::unique_lock lock(mutex);
    if (real_sender)
        return;
    if (!wait_become_real.wait_for(
            lock, std::chrono::milliseconds(timeout_ms), [this] { return this->real_sender.operator bool() || closed; }))
        throw Exception(ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION, "Wait become real sender timeout for {}, timeout {}", *data_key, timeout_ms);
    else if (closed)
        throw Exception(ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION, "Interrput waitBecomeRealSender for {}, timeout {}", *data_key, timeout_ms);
}

BroadcastSenderType BroadcastSenderProxy::getType()
{
    if (!has_real_sender.load(std::memory_order_relaxed))
        waitBecomeRealSender(wait_timeout_ms);
    return real_sender->getType();
}

ContextPtr BroadcastSenderProxy::getContext() const
{
    std::lock_guard lock(mutex);
    return context;
}

Block BroadcastSenderProxy::getHeader() const
{
    std::lock_guard lock(mutex);
    return header;
}

ExchangeDataKeyPtr BroadcastSenderProxy::getDataKey() const
{
    return data_key;
}

}
