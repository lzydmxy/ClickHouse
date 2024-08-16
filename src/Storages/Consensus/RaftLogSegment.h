#pragma once

#include <fstream>
#include <iostream>
#include <map>
#include <vector>
#include <shared_mutex>
#include <Poco/DateTime.h>
#include <Common/logger_useful.h>
#include <Storages/Consensus/Settings.h>
#include <Storages/Consensus/NuRaft.h>
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/Consensus/NuLogEntry.h>


namespace Consensus
{

using nuraft::int64;

enum class LogVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with ctime mtime
};

struct VersionLogEntry
{
    LogVersion version;
    NuLogEntryPtr entry;
};

static constexpr auto CURRENT_LOG_VERSION = LogVersion::V1;

class LogSegment;
class LogSegmentStore;

using LogSegmentPtr = std::shared_ptr<LogSegment>;
using LogSegments = std::vector<LogSegmentPtr>;
using LogSegmentStorePtr = std::shared_ptr<LogSegmentStore>;

// SegmentLog layout:
//      log_1_1000_createtime: closed segment
//      log_1001_open_createtime: open segment
class LogSegment
{
public:
    LogSegment(const std::string & log_dir_, UInt64 first_index_, const std::string & file_name_ = "", const std::string & create_time_ = "")
        : log_dir(log_dir_), first_index(first_index_), last_index(first_index_ - 1), file_name(file_name_)
        , create_time(create_time_), seg_fd(-1), file_size(0), is_open(true), log(&(Poco::Logger::get("LogSegment")))
        , version(CURRENT_LOG_VERSION)
    {
    }

    LogSegment(const std::string & log_dir_, UInt64 first_index_, UInt64 last_index_, const std::string file_name_ = "")
        : log_dir(log_dir_), first_index(first_index_), last_index(last_index_), file_name(file_name_)
        , seg_fd(-1), file_size(0), is_open(false), log(&(Poco::Logger::get("LogSegment")))
    {
    }

    ~LogSegment() { }

    void create();
    void load();
    void close(bool is_full);
    void remove();
    inline UInt64 flush() const;

    bool isOpen() const { return is_open; }
    off_t loadVersion();
    LogVersion getVersion() const { return version; }

    // serialize entry, and append to open segment,return new start index
    UInt64 appendEntry(NuLogEntryPtr entry, std::atomic<UInt64> & last_log_index);
    // truncate segment to last_index_kept
    void truncate(UInt64 last_index_kept);

    // get entry by index
    NuLogEntryPtr getEntry(UInt64 log_index);
    // get entry's term by index
    UInt64 getTerm(UInt64 log_index) const;

    UInt64 getFileSize() const { return file_size.load(std::memory_order_consume); }
    UInt64 firstIndex() const { return first_index; }
    UInt64 lastIndex() const { return last_index.load(std::memory_order_consume); }
    std::string getFileName();
    Poco::DateTime getCreateTime() { return create_dt; }

private:
    struct LogMeta
    {
        off_t offset;
        size_t length;
        UInt64 term;
    };
    void writeFileHeader();
    std::string getOpenFileName();
    std::string getOpenPath();
    std::string getFinishFileName();
    std::string getFinishPath();
    std::string getPath();

    void openFile();
    void closeFile();

    //Get log index
    int getMeta(UInt64 log_index, LogMeta * meta) const;
    int loadHeader(int fd, off_t offset, LogEntryHeader * head) const;
    int loadEntry(int fd, off_t offset, LogEntryHeader * head, NuLogEntryPtr & entry) const;

    int truncateMetaAndGetLast(UInt64 last);
private:
    std::string log_dir;
    const UInt64 first_index;
    std::atomic<UInt64> last_index;
    std::string file_name;
    Poco::DateTime create_dt;
    std::string create_time;
    int seg_fd;
    std::atomic<UInt64> file_size;
    bool is_open;
    Poco::Logger * log;
    mutable std::shared_mutex log_mutex;
    //file offset
    std::vector<std::pair<UInt64 /*offset*/, UInt64 /*term*/>> offset_term;
    LogVersion version;
};


// LogSegmentStore use segmented append-only file, all data in disk, all index in memory.
// append one log entry, only cause one disk write, every disk write will call fsync().
class LogSegmentStore
{
public:
    static constexpr int LOAD_THREAD_NUM = 8;

public:
    LogSegmentStore(const std::string & log_dir_, const SettingsPtr & settings_);
    virtual ~LogSegmentStore() { }

public:
    static LogSegmentStorePtr getInstance(const std::string & log_dir, const SettingsPtr & settings_, bool force_new = false);

    // init log store, check consistency and integrity
    void init();
    void close();
    UInt64 flush();

    UInt64 firstLogIndex() { return first_log_index.load(std::memory_order_acquire); }
    UInt64 lastLogIndex() { return last_log_index.load(std::memory_order_acquire); }
    void setLastLogIndex(UInt64 index) { last_log_index.store(index, std::memory_order_release); }

    // append entry to log
    UInt64 appendEntry(NuLogEntryPtr entry);
    UInt64 writeAt(UInt64 index, const NuLogEntryPtr entry);

    // get logentry by index
    NuLogEntryPtr getEntry(UInt64 index);

    void getEntries(UInt64 start_index, UInt64 end_index, ptr<std::vector<NuLogEntryPtr>> & entries);

    void getEntriesExt(UInt64 start_idx, UInt64 end_idx, int64 batch_size_hint_in_bytes, ptr<std::vector<NuLogEntryPtr>> & entries);

    // get logentry's term by index
    UInt64 getTerm(UInt64 index);

    // truncate old log segment when segment count reach max_segment_count
    void removeSegmentThread();

    // delete segment from storage's head, [1, first_index_kept) will be discarded
    // The log segment to which first_index_kept belongs whill not be deleted.
    void removeSegment(UInt64 first_index_kept);

    // delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
    void truncateLog(UInt64 last_index_kept);

    void reset(UInt64 next_log_index);

    //closed segments, unclude open segment
    LogSegments & getSegments() { return segments; }

    LogVersion getVersion(UInt64 index);
private:
    //for LogSegmentStore init
    void listFiles(std::vector<std::string> & seg_files);
    void openSegment();
    void listSegments();
    void loadSegments();

    //get LogSegment by log index
    void getSegment(UInt64 log_index, ptr<LogSegment> & ptr);

private:
    static LogSegmentStorePtr segment_store;
    std::string log_dir;
    const SettingsPtr settings;
    std::atomic<UInt64> first_log_index;
    std::atomic<UInt64> last_log_index;
    LogSegments segments;
    mutable std::shared_mutex seg_mutex;
    LogSegmentPtr open_segment;
    bool shutdown_called { false };
    GlobalThreadPtr remove_thread;
    Poco::Logger * log;
};

}
