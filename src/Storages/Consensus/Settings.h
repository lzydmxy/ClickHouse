#pragma once

#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Defines.h>
#include <Core/SettingsEnums.h>
#include <IO/WriteBufferFromString.h>
#include <Coordination/LoggerWrapper.h>


namespace Consensus
{

using namespace DB;

/// Raft log fsync mode.
enum FsyncMode
{
    /// The leader can do log replication and log persisting in parallel, thus it can reduce the latency of write operation path.
    /// In this mode data is safety.
    FSYNC_PARALLEL,
    /// The leader and follower do log persisting synchronously. In this mode data is safety.
    FSYNC,
    /// The leader and follower do log persisting asynchronously and in batch. In this mode data is less safety.
    FSYNC_BATCH
};

namespace FsyncModeNS
{

FsyncMode parseFsyncMode(const String & in);
String toString(FsyncMode mode);

}

/// Consume data to mergetree or others.
enum ConsumeMode
{
    /// Sync consume single data to mergetree
    SYNC_SINGLE,
    /// Aync consume batch data to mergetree
    ASYNC_BATCH,
    /// Sync consume batch data to mergetree, not support now.
    SYNC_BATCH
};

namespace ConsumeModeNS
{

ConsumeMode parseConsumeMode(const String & in);
String toString(ConsumeMode mode);

}

struct Settings;
using SettingsPtr = std::shared_ptr<Settings>;
struct RaftSettings;
using RaftSettingsPtr = std::shared_ptr<RaftSettings>;

struct Settings
{
public:
    Settings();
    static SettingsPtr loadFromConfig(const Poco::Util::AbstractConfiguration & config);
    void dump(WriteBufferFromOwnString & buf) const;

    static constexpr int NOT_EXIST = -1;

    int server_id;
    String host;
    // forward and data port between leader and followers
    int server_port;
    int internal_port;

    String log_dir;
    String state_dir;
    int logfile_segment_mb;
    int logfile_keep_count;
    int logfile_keep_minutes;
    int snapshot_interval_seconds;

    int forward_thread_count;
    int request_thread_count;
    int request_queue_size;

    ConsumeMode consume_mode;
    int consume_batch_size;
    int consume_queue_size;
    int consume_waittime_ms;

    RaftSettingsPtr raft_settings;
};

struct RaftSettings
{
    /// Default client operation timeout
    Int32 operation_timeout_ms;
    /// Heartbeat interval between quorum nodes
    Int32 heart_beat_interval_ms;
    /// Lower bound of election timer (avoid too often leader elections)
    Int32 election_timeout_lower_bound_ms;
    /// Lower bound of election timer (avoid too often leader elections)
    Int32 election_timeout_upper_bound_ms;
    /// How many log items to store (don't remove during compaction)
    Int32 reserved_log_items;
    /// How many log items we have to collect to write new snapshot00
    Int32 snapshot_distance;
    /// How many snapshots we want to store
    UInt64 max_stored_snapshots;
    /// How many time we will until RAFT shutdown
    UInt64 shutdown_timeout;
    /// How many time we will until RAFT to start
    UInt64 startup_timeout;
    /// Log internal RAFT logs into main server log level. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal'
    LogsLevel raft_logs_level;
    /// NuRaft thread pool size
    UInt64 nuraft_thread_size;
    /// When node became fresh
    UInt64 fresh_log_gap;
    /// How many times we will try to apply configuration change (add/remove server) to the cluster
    UInt64 configuration_change_tries_count;
    /// Max batch size for append_entries
    UInt64 max_batch_size;
    /// Raft log fsync mode
    FsyncMode log_fsync_mode;
    /// How many logs do once fsync when async_fsync is false
    UInt64 log_fsync_interval;

    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    static RaftSettingsPtr getDefault();
};

}
