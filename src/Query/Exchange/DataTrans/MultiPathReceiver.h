#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <boost/core/noncopyable.hpp>
#include <butil/iobuf.h>
#include <Common/Stopwatch.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/bRPC/AsyncRegisterResult.h>
#include <Query/Common/MultiPathBoundedQueue.h>


namespace DB
{

struct MultiPathReceiverOptions
{
    bool enable_block_compress;
    bool enable_metrics;
};

class MultiPathReceiver final : public IBroadcastReceiver,
                                public std::enable_shared_from_this<MultiPathReceiver>,
                                private boost::noncopyable
{
public:
    explicit MultiPathReceiver(
        MultiPathQueuePtr collector_,
        BroadcastReceiverPtrs sub_receivers_,
        Block header_,
        String name_,
        MultiPathReceiverOptions options_,
        ContextPtr context_);
    ~MultiPathReceiver() override;
    void registerToSenders(UInt32 timeout_ms) override;

    void registerToLocalSenders(UInt32 timeout_ms);
    void registerToSendersAsync(UInt32 timeout_ms);
    void registerToSendersJoin();

    RecvDataPacket recv(TimePoint timeout_ts) override;
    BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;
    String getName() const override;

    static String generateName(
        size_t exchange_id, size_t write_segment_id, size_t read_segment_id, String& co_host_port)
    {
        return fmt::format(
            "MultiPathReceiver[{}_{}_{}_{}_{}]",
            write_segment_id,
            read_segment_id,
            0, // parallel_index
            exchange_id,
            co_host_port
        );
    }

private:
    std::atomic_bool registering{false};
    std::atomic_bool inited{false};

    BroadcastStatus init_fin_status{BroadcastStatusCode::RUNNING, false, "init"};
    std::atomic<BroadcastStatus *> fin_status {&init_fin_status};

    std::vector<AsyncRegisterResult> async_results;

    mutable std::mutex running_receiver_mutex;
    mutable std::mutex wait_register_mutex;
    std::condition_variable wait_register_cv;
    std::map<String, size_t> running_receiver_names;
    MultiPathQueuePtr collector;
    BroadcastReceiverPtrs sub_receivers;
    Block header;
    String name;
    LoggerPtr logger;
    Stopwatch register_s;
    ContextPtr context;

};

}
