#pragma once
#include <Common/logger_useful.h>
#include <Processors/IProcessor.h>
#include <Query/Exchange/IExchangeSink.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
/// Sink which send data to ExchangeSource with LoadBalanceSelector.
class LoadBalancedExchangeSink : public IExchangeSink
{
public:
    class LoadBalanceSelector : private boost::noncopyable
    {
    public:
        explicit LoadBalanceSelector(size_t partition_num_) : partition_num(partition_num_) { }
        virtual size_t selectNext() = 0;
        virtual ~LoadBalanceSelector() = default;

    protected:
        size_t partition_num;
    };
    using LoadBalanceSelectorPtr = std::unique_ptr<LoadBalanceSelector>;

    explicit LoadBalancedExchangeSink(Block header_, BroadcastSenderPtrs senders_, const String &name_);
    virtual ~LoadBalancedExchangeSink() override;
    virtual String getName() const override { return name; }

    static String generateName(size_t exchange_id)
    {
        return fmt::format("LoadBalancedExchangeSink[{}]", exchange_id);
    }

    static String generateNameForTest()
    {
        return fmt::format("LoadBalancedExchangeSink[{}]", -1);
    }


protected:
    virtual void consume(Chunk) override;
    virtual void onFinish() override;
    virtual void onCancel() override;

private:
    String name;
    Block header = getPort().getHeader();
    BroadcastSenderPtrs senders;
    LoadBalanceSelectorPtr partition_selector;
    LoggerPtr logger;
};

}
