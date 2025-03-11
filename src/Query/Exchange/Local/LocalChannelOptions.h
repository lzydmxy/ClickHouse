#pragma once

#include <base/types.h>

namespace DB {

    struct LocalChannelOptions{
        size_t queue_size;
        timespec max_timeout_ts;
        bool enable_metrics;
    };
}
