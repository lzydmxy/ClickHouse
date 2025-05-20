#pragma once
#include <Interpreters/Context.h>
#include <Query/Common/QueryCommon.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/ExchangeDataKey.h>

namespace DB
{

struct ExchangeOptions
{
    TimePoint exchange_timeout_ts;
    UInt64 send_threshold_in_bytes{0};
    UInt64 send_threshold_in_row_num{0};
    bool force_remote_mode = false;
    bool force_use_buffer = false;
};

struct LocalChannelOptions
{
    size_t queue_size;
    TimePoint max_timeout_ts;
    bool enable_metrics;
};

class ExchangeUtils
{
public:
    static bool isLocalExchange(const AddressInfo & read_address_info, const AddressInfo & write_address_info);
    static ExchangeOptions getExchangeOptions(const ContextPtr & context);
    static BroadcastStatus sendAndCheckReturnStatus(IBroadcastSender & sender, Chunk chunk);
    static void mergeSenders(BroadcastSenderPtrs & senders);
    static void transferGlobalMemoryToThread(Int64 bytes);
};

}
