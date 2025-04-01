#include "IExchangeSink.h"
#include <atomic>
#include <Common/Exception.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>

namespace DB
{
IExchangeSink::IExchangeSink(Block header_) : ISink(std::move(header_))
{
}

IExchangeSink::Status IExchangeSink::prepare()
{
    LOG_TRACE(getLogger("IExchangeSink"), "IExchangeSink::prepare, was_on_start_called {}, was_on_finish_called {}, is_finished {}, has_input {}",
        was_on_start_called, was_on_finish_called, is_finished.load(std::memory_order_relaxed), has_input);

    if (is_finished.load(std::memory_order_relaxed))
    {
        onFinish();
        input.close();
        return Status::Finished;
    }
    return ISink::prepare();
}

void IExchangeSink::onStart()
{
    ISink::onStart();
}

void IExchangeSink::onFinish()
{
    is_finished.store(true, std::memory_order_relaxed);
    ISink::onFinish();
}

}
