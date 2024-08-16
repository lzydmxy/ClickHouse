#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <Common/ThreadPool.h>
#include <Storages/Consensus/Settings.h>
#include <Storages/Consensus/NuRaft.h>
#include <Storages/Consensus/Settings.h>
#include <Storages/Consensus/RaftLogSegment.h>


namespace DB
{
    class RaftServer;
}

namespace Consensus
{

using nuraft::int32;
using nuraft::int64;
using nuraft::ulong;

class LogEntryQueue
{
public:
    LogEntryQueue() : batch(0), start_index(0), last_index(0),
        log(&(Poco::Logger::get("LogEntryQueue"))) { }
    NuLogEntryPtr getEntry(const UInt64 & index);
    void putEntry(UInt64 & index, NuLogEntryPtr entry);
    UInt64 startIndex() const { return start_index; }
    UInt64 lastIndex() const { return last_index; }
    void clear();
private:
    static constexpr UInt8 BIT_SIZE = 16;
    static constexpr UInt32 MAX_VECTOR_SIZE = 65536; //2^16
    UInt64 batch;
    UInt64 start_index;
    UInt64 last_index;
    Poco::Logger * log;
    NuLogEntryPtr entry_vec[MAX_VECTOR_SIZE];
    std::shared_mutex queue_mutex;
};

class RaftLogStorage : public nuraft::log_store
{
    __nocopy__(RaftLogStorage)
public :
    RaftLogStorage(const String & log_dir_, const SettingsPtr & settings_);
    ~RaftLogStorage() override;
    
    ulong append(NuLogEntryPtr & entry) override;
    void write_at(ulong index, NuLogEntryPtr & entry) override;

    NuLogEntryPtr entry_at(ulong index) override;
    NuLogEntriesPtr log_entries(ulong start, ulong end) override;
    NuLogEntriesPtr log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes = 0) override;
    void end_of_append_batch(ulong start, ulong cnt) override;

    ulong next_slot() const override;
    ulong start_index() const override;
    NuLogEntryPtr last_entry() const override;
    
    ulong term_at(ulong index) override;

    bool is_conf(ulong index) override;

    NuBufferPtr pack(ulong index, int32 cnt) override;

    void apply_pack(ulong index, NuBuffer & pack) override;

    bool compact(ulong last_log_index) override;

    bool flush() override;

    ulong last_durable_index() override;

    void shutdown();

    void setServer(DB::RaftServer * server);
private:
    const SettingsPtr & settings;
    Poco::Logger * log;
    LogEntryQueue log_queue;
    LogSegmentStorePtr segment_store;
    NuLogEntryPtr last_log_entry;
    DB::RaftServer * raft_server;
    std::shared_mutex append_mutex;
    std::atomic<bool> shutdown_called{false};
};

}
