#pragma once
#include <mutex>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <base/types.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
class BroadcastSenderProxy;
struct SenderProxyOptions;

class BroadcastSenderProxyRegistry final : private boost::noncopyable
{
public:
    static BroadcastSenderProxyRegistry & instance()
    {
        static BroadcastSenderProxyRegistry * instance = new BroadcastSenderProxyRegistry;
        return *instance;
    }

    BroadcastSenderProxyPtr get(ExchangeDataKeyPtr data_key);

    BroadcastSenderProxyPtr getOrCreate(ExchangeDataKeyPtr data_key);

    BroadcastSenderProxyPtr getOrCreate(ExchangeDataKeyPtr data_key, SenderProxyOptions options);

    void remove(ExchangeDataKeyPtr data_key);

    size_t countProxies();

private:
    BroadcastSenderProxyRegistry();
    mutable std::mutex mutex;
    using BroadcastSenderProxyEntry = std::weak_ptr<BroadcastSenderProxy>;
    std::unordered_map<ExchangeDataKey, BroadcastSenderProxyEntry, ExchangeDataKeyHashFunc> proxies;
    LoggerPtr log;
};

}
