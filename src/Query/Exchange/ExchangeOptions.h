#pragma once
#include <base/types.h>

namespace DB 
{

struct ExchangeOptions
{
    timespec exchange_timeout_ts;
    UInt64 send_threshold_in_bytes;
    UInt64 send_threshold_in_row_num;
    bool force_remote_mode = false;
    bool force_use_buffer = false;
};

}
