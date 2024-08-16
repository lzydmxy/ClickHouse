#include <cassert>
#include <memory>
#include <unistd.h>
#include <base/sleep.h>
#include <Common/setThreadName.h>
#include <Storages/Consensus/RaftLogStorage.h>
#include <Storages/Consensus/RaftServer.h>


namespace Consensus
{

NuLogEntryPtr LogEntryQueue::getEntry(const UInt64 & index)
{
    std::shared_lock read_lock(queue_mutex);
    if (index > last_index || index < start_index)
        return nullptr;
    if (index >> BIT_SIZE == batch || index >> BIT_SIZE == batch - 1)
    {
        auto entry = entry_vec[index & (MAX_VECTOR_SIZE - 1)];
        LOG_TRACE(log, "get log index {}, star index {}, last index {}, queue index {}, batch {}, term {}, val type {}, buf size {}",
            index, start_index, last_index, index & (MAX_VECTOR_SIZE - 1), batch,
            entry->get_term(), entry->get_val_type(), entry->get_buf().size()
        );
        return entry;
    }
    else
        return nullptr;
}

void LogEntryQueue::putEntry(UInt64 & index, NuLogEntryPtr entry)
{
    std::lock_guard write_lock(queue_mutex);
    entry_vec[index & (MAX_VECTOR_SIZE - 1)] = entry;
    batch = std::max(batch, index >> BIT_SIZE);
    last_index = index;
    if ( start_index == 0 || last_index < start_index)
        start_index = last_index;
    else
        start_index = (last_index - start_index < MAX_VECTOR_SIZE) ? start_index : start_index + 1;

    LOG_TRACE(log, "put log index {}, start index {}, last index {}, queue index {}, batch {}, term {}, val type {}, buf size {}",
        index, start_index, last_index, index & (MAX_VECTOR_SIZE - 1), batch,
        entry->get_term(), entry->get_val_type(), entry->get_buf().size());
}

void LogEntryQueue::clear()
{
    LOG_DEBUG(log, "clear log queue.");
    std::lock_guard write_lock(queue_mutex);
    batch = 0;
    start_index = 0;
    last_index = 0;
    for (size_t i = 0; i < MAX_VECTOR_SIZE; ++i)
        entry_vec[i] = nullptr;
}

RaftLogStorage::RaftLogStorage(const std::string & log_dir_, const SettingsPtr & settings_)
    : settings(settings_), log(&(Poco::Logger::get("RaftLogStorage")))
{
    LOG_DEBUG(log, "Init raft log storage, log dir {}", log_dir_);
    segment_store = LogSegmentStore::getInstance(log_dir_, settings);
    segment_store->init();

    if (segment_store->lastLogIndex() >= 1)
        last_log_entry = segment_store->getEntry(segment_store->lastLogIndex());
    else
        last_log_entry = RaftNew<NuLogEntry>(0, NuBuffer::alloc(0));
}

void RaftLogStorage::setServer(DB::RaftServer * server)
{
    raft_server = server;
}

void RaftLogStorage::shutdown()
{
    shutdown_called = true;
}

RaftLogStorage::~RaftLogStorage()
{
    shutdown();
}

ulong RaftLogStorage::append(NuLogEntryPtr & entry)
{
    LOG_DEBUG(log, "Append log term {}, val type {}, buf size {}",
        entry->get_term(), entry->get_val_type(),entry->get_buf().size());

    auto log_index = segment_store->appendEntry(entry);
    log_queue.putEntry(log_index, entry);
    last_log_entry = entry;
    LOG_DEBUG(log, "End append log index {}", log_index);
    return log_index;
}

void RaftLogStorage::write_at(ulong index, NuLogEntryPtr & entry)
{
    LOG_DEBUG(log, "write log at index {}, term {}, val type {}, buf size {}",
        index, entry->get_term(), entry->get_val_type(),entry->get_buf().size());

    if (segment_store->writeAt(index, entry) == index)
    {
        log_queue.clear();
        log_queue.putEntry(index, entry);
        last_log_entry = entry;
    }
}

NuLogEntriesPtr RaftLogStorage::log_entries(ulong start, ulong end)
{
    auto ret = RaftNew<NuLogEntries>();
    for (auto i = start; i < end; i++)
        ret->push_back(entry_at(i));
    LOG_TRACE(log, "Log entries, start {} end {}", start, end);
    return ret;
}

NuLogEntriesPtr RaftLogStorage::log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes)
{
    auto ret = RaftNew<NuLogEntries>();
    int64 get_size = 0;
    int64 entry_size = 0;
    for (auto i = start; i < end; i++)
    {
        auto entry_ptr = entry_at(i);
        entry_size = entry_ptr->get_buf().size() + sizeof(ulong) + sizeof(char);
        if (batch_size_hint_in_bytes > 0 && get_size + entry_size > batch_size_hint_in_bytes)
            break;
        ret->push_back(entry_ptr);
        get_size += entry_size;
    }
    LOG_TRACE(log, "Log entries ext, start {} end {}, real size {}, max size {}", start, end, get_size, batch_size_hint_in_bytes);
    return ret;
}

NuLogEntryPtr RaftLogStorage::entry_at(ulong index)
{
    auto entry = log_queue.getEntry(index);
    if (!entry)
        entry = segment_store->getEntry(index);
    LOG_TRACE(log, "get entry index {}", index);
    if (entry)
        return entry;
    else
        return nullptr;
}

ulong RaftLogStorage::term_at(ulong index)
{
    auto entry = entry_at(index);
    if (entry) return entry->get_term();
    else
        return 0;
}

bool RaftLogStorage::is_conf(ulong index)
{
    auto entry = entry_at(index);
    return entry && entry->get_val_type() == nuraft::conf;
}

NuBufferPtr RaftLogStorage::pack(ulong index, int32 cnt)
{
    auto entries = log_entries(index, index + cnt);

    NuBuffers logs;
    size_t size_total = 0;
    for (auto it = entries->begin(); it != entries->end(); it++)
    {
        auto buf = (*it)->serialize();
        size_total += buf->size();
        logs.push_back(buf);
    }
    //entry count,  buffer size, buffer
    auto buf_out = NuBuffer::alloc(sizeof(int32) + cnt * sizeof(int32) + size_total);
    buf_out->pos(0);
    buf_out->put(cnt);

    for (auto & entry : logs)
    {
        buf_out->put(static_cast<int32>(entry->size()));
        buf_out->put(*entry);
    }
    LOG_TRACE(log, "Pack log start {}, count {}", index, cnt);
    return buf_out;
}

void RaftLogStorage::apply_pack(ulong index, NuBuffer & pack)
{
    pack.pos(0);
    int32 log_count = pack.get_int();
    for (auto log_index = 0; log_index < log_count; log_index++)
    {
        ulong cur_index = index + log_index;
        int32 buf_size = pack.get_int();

        auto buf_local = NuBuffer::alloc(buf_size);
        pack.get(buf_local);
        auto cur_entry = NuLogEntry::deserialize(*buf_local);

        LOG_TRACE(log, "Current index {}, last_log_index {}", cur_index, segment_store->lastLogIndex());
        log_queue.putEntry(cur_index, cur_entry);
    }
    LOG_TRACE(log, "Apply pack {}", index);
}

//last_log_index : last removed log index (include index)
bool RaftLogStorage::compact(ulong last_log_index)
{
    segment_store->removeSegment(last_log_index + 1);
    log_queue.clear();
    LOG_DEBUG(log, "Compact last_log_index {}", last_log_index);
    return true;
}

ulong RaftLogStorage::next_slot() const
{
    return segment_store->lastLogIndex() + 1;
}

ulong RaftLogStorage::start_index() const
{
    LOG_TRACE(log, "Start index {}", segment_store->firstLogIndex());
    return segment_store->firstLogIndex();
}

NuLogEntryPtr RaftLogStorage::last_entry() const
{
    return last_log_entry;
}

void RaftLogStorage::end_of_append_batch(ulong start, ulong cnt)
{
    LOG_TRACE(log, "End of append batch start {}, count {}", start, cnt);
    if (settings->raft_settings->log_fsync_mode == FsyncMode::FSYNC)
    {
        flush();
    }
}

bool RaftLogStorage::flush()
{
    LOG_TRACE(log, "Flush last index {}", segment_store->lastLogIndex());
    segment_store->flush();
    return true;
}

ulong RaftLogStorage::last_durable_index()
{
    return segment_store->lastLogIndex();
}

}
