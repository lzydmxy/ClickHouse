#pragma once

#include <Interpreters/Context.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/ExchangeDataKey.h>

namespace DB
{

struct ExchangeOptions
{
    Poco::Timespan exchange_timeout_ts;
    UInt64 send_threshold_in_bytes;
    UInt64 send_threshold_in_row_num;
    bool force_remote_mode = false;
    bool force_use_buffer = false;
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
