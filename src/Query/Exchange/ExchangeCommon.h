#pragma once

#include <base/types.h>
#include <Interpreters/Context.h>

namespace DB {

struct ExchangeOptions
{
    Poco::Timespan exchange_timeout_ts;
    UInt64 send_threshold_in_bytes;
    UInt64 send_threshold_in_row_num;
    bool force_remote_mode = false;
    bool force_use_buffer = false;
};

static inline ExchangeOptions getExchangeOptions(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    auto exchange_timeout = settings.max_execution_time.totalSeconds() * 1000;
    return {
        //.exchange_timeout_ts = context->getQueryExpirationTimeStamp(),
        .exchange_timeout_ts = exchange_timeout,
        .send_threshold_in_bytes = settings.exchange_buffer_send_threshold_in_bytes,
        .send_threshold_in_row_num = settings.exchange_buffer_send_threshold_in_row,
        .force_remote_mode = settings.exchange_enable_force_remote_mode,
        .force_use_buffer = settings.exchange_force_use_buffer};
}

}
