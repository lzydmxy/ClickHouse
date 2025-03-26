#include "BroadcastSenderProxyRegistry.h"
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>

namespace DB
{
BroadcastSenderProxyRegistry::BroadcastSenderProxyRegistry()
    : log(getLogger("BroadcastSenderProxyRegistry"))
{
}

BroadcastSenderProxyPtr BroadcastSenderProxyRegistry::get(ExchangeDataKeyPtr data_key)
{
    std::lock_guard lock(mutex);
    auto it = proxies.find(*data_key);
    if (it != proxies.end())
    {
        auto channel_ptr = it->second.lock();
        if (channel_ptr)
            return channel_ptr;
    }
    return nullptr;
}

BroadcastSenderProxyPtr BroadcastSenderProxyRegistry::getOrCreate(ExchangeDataKeyPtr data_key)
{
    return getOrCreate(std::move(data_key), SenderProxyOptions{.wait_timeout_ms = 5000});
}

BroadcastSenderProxyPtr BroadcastSenderProxyRegistry::getOrCreate(ExchangeDataKeyPtr data_key, SenderProxyOptions options)
{
    std::lock_guard lock(mutex);
    auto it = proxies.find(*data_key);
    if (it != proxies.end())
    {
        auto channel_ptr = it->second.lock();
        if (channel_ptr)
            return channel_ptr;
    }

    LOG_TRACE(log, "Register sender proxy with key {}", *data_key);
    auto channel_ptr = std::shared_ptr<BroadcastSenderProxy>(new BroadcastSenderProxy(std::move(data_key), std::move(options)));
    proxies.emplace(*channel_ptr->getDataKey(), BroadcastSenderProxyEntry(channel_ptr));
    return channel_ptr;
}

void BroadcastSenderProxyRegistry::remove(ExchangeDataKeyPtr data_key)
{
    std::lock_guard lock(mutex);
    auto result = proxies.erase(*data_key);
    LOG_TRACE(log, "Remove proxy {} with result: {} ", *data_key, result);
}

size_t BroadcastSenderProxyRegistry::countProxies()
{
    std::lock_guard lock(mutex);
    return proxies.size();
}

}
