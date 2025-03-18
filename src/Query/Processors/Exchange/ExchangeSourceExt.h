#pragma once

#include <Processors/ISource.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Common/Logger.h>
#include <Query/Exchange/ChunkInfo.h>

#include <atomic>

namespace DB
{

class ExchangeTotalsSourceExt;
using ExchangeTotalsSourcePtr = std::shared_ptr<ExchangeTotalsSourceExt>;
class ExchangeExtremesSourceExt;
using ExchangeExtremesSourcePtr = std::shared_ptr<ExchangeExtremesSourceExt>;

/// Read chunk from ExchangeSink.
class ExchangeSourceExt : public ISource
{
public:
    ExchangeSourceExt(
        Block header_,
        BroadcastReceiverPtr receiver_,
        ExchangeOptions options_,
        ExchangeTotalsSourcePtr totals_source_ = nullptr,
        ExchangeExtremesSourcePtr extremes_source_ = nullptr);
    ExchangeSourceExt(
        Block header_,
        BroadcastReceiverPtr receiver_,
        ExchangeOptions options_,
        bool fetch_exception_from_scheduler_,
        ExchangeTotalsSourcePtr totals_source_ = nullptr,
        ExchangeExtremesSourcePtr extremes_source_ = nullptr);
    ~ExchangeSourceExt() override;

    IProcessor::Status prepare() override;
    String getName() const override;
    String getClassName() const;
    BroadcastReceiverPtr & getReceiver() { return receiver; }

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    BroadcastReceiverPtr receiver;
    ExchangeOptions options;
    ExchangeTotalsSourcePtr totals_source;
    ExchangeExtremesSourcePtr extremes_source;
    std::atomic<bool> was_query_canceled {false};
    std::atomic<bool> was_receiver_finished {false};
    LoggerPtr logger;
    void checkBroadcastStatus(const BroadcastStatus & status) const;
};

class ExchangeTotalsSourceExt : public ISource
{
public:
    explicit ExchangeTotalsSourceExt(const Block& header);
    ~ExchangeTotalsSourceExt() override;

    String getName() const override { return "ExchangeTotalsSourceExt"; }
    void setTotals(Chunk chunk);

protected:
    Chunk generate() override;

private:
    Chunk totals;
};

class ExchangeExtremesSourceExt : public ISource
{
public:
    explicit ExchangeExtremesSourceExt(const Block& header);
    ~ExchangeExtremesSourceExt() override;

    String getName() const override { return "ExchangeExtremesSourceExt"; }
    void setExtremes(Chunk chunk);

protected:
    Chunk generate() override;

private:
    Chunk extremes;
};

}
