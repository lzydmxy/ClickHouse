#include "LoadBalancedExchangeSink.h"
#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>

namespace DB
{
class RoundRobinSelector : public LoadBalancedExchangeSink::LoadBalanceSelector
{
public:
    explicit RoundRobinSelector(size_t partition_num_) : LoadBalanceSelector(partition_num_) { }
    virtual size_t selectNext() override { return count++ % partition_num; }

private:
    UInt32 count = rand(); // NOLINT
};

LoadBalancedExchangeSink::LoadBalancedExchangeSink(Block header_, BroadcastSenderPtrs senders_, const String &name_)
    : IExchangeSink(std::move(header_))
    , name(name_)
    , senders(std::move(senders_))
    , partition_selector(std::make_unique<RoundRobinSelector>(senders.size()))
    , logger(getLogger("LoadBalancedExchangeSink"))
{
}

LoadBalancedExchangeSink::~LoadBalancedExchangeSink() = default;


void LoadBalancedExchangeSink::consume(Chunk chunk)
{
    if (!has_input)
    {
        finish();
        return;
    }
    auto status = ExchangeUtils::sendAndCheckReturnStatus(*senders[partition_selector->selectNext()], std::move(chunk));
    if (status.code != BroadcastStatusCode::RUNNING)
        finish();
}

void LoadBalancedExchangeSink::onFinish()
{
    LOG_TRACE(logger, "LoadBalancedExchangeSink finish");
}

void LoadBalancedExchangeSink::onCancel()
{
    LOG_TRACE(logger, "LoadBalancedExchangeSink cancel");
    for (const BroadcastSenderPtr & sender : senders)
        sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
}

}
