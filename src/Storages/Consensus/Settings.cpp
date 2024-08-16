#include <filesystem>
#include <IO/WriteHelpers.h>
#include <Core/SettingsEnums.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Storages/Consensus/Settings.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}

}

namespace Consensus
{

namespace FsyncModeNS
{

FsyncMode parseFsyncMode(const String & in)
{
    if (in == "fsync_parallel")
        return FsyncMode::FSYNC_PARALLEL;
    else if (in == "fsync")
        return FsyncMode::FSYNC;
    else if (in == "fsync_batch")
        return FsyncMode::FSYNC_BATCH;
    else
        throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown config 'log_fsync_mode'.");
}

String toString(FsyncMode mode)
{
    if (mode == FsyncMode::FSYNC_PARALLEL)
        return "fsync_parallel";
    else if (mode == FsyncMode::FSYNC)
        return "fsync";
    else if (mode == FsyncMode::FSYNC_BATCH)
        return "fsync_batch";
    else
        throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown config 'log_fsync_mode'.");
}

}

namespace ConsumeModeNS
{

ConsumeMode parseConsumeMode(const String & in)
{
    if (in == "sync_single")
        return ConsumeMode::SYNC_SINGLE;
    else if (in == "sync_batch")
        return ConsumeMode::SYNC_BATCH;
    else if (in == "async_batch")
        return ConsumeMode::ASYNC_BATCH;
    else
        throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown config 'consum_mode' '{}'", in);
}

String toString(ConsumeMode mode)
{
    if (mode == ConsumeMode::SYNC_SINGLE)
        return "sync_single";
    else if (mode == ConsumeMode::SYNC_BATCH)
        return "sync_batch";
    else if (mode == ConsumeMode::ASYNC_BATCH)
        return "async_batch";
    else
        throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown config 'consum_mode'.");
}

}

Settings::Settings()
: server_id(NOT_EXIST)
, raft_settings(RaftSettings::getDefault())
{
}

SettingsPtr Settings::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    SettingsPtr ret = std::make_shared<Settings>();

    ret->server_id = config.getInt("raft_server.server_id");
    ret->host = config.getString("raft_server.host", "0.0.0.0");

    ret->server_port = config.getInt("raft_server.server_port");
    ret->internal_port = config.getInt("raft_server.internal_port");

    ret->log_dir = config.getString("raft_server.log_storage_path", "./raft_log");
    ret->state_dir = config.getString("raft_server.state_storage_path", "./raft_state");

    ret->logfile_segment_mb = config.getInt("raft_server.logfile_segment_mb", 10000);               //10GB
    ret->logfile_keep_count = config.getInt("raft_server.logfile_keep_count", 100);                 //100
    ret->logfile_keep_minutes = config.getInt("raft_server.logfile_keep_minutes", 1440);            //1 day
    ret->snapshot_interval_seconds = config.getInt("raft_server.snapshot_interval_seconds", 1800);  //1800 secondss

    ret->request_thread_count = config.getInt("raft_server.request_thread_count", 16);
    ret->forward_thread_count = config.getInt("raft_server.forward_thread_count", 8);

    /// Use 300KB/req * 10K = 3GB memory space
    ret->request_queue_size = config.getInt("raft_server.request_queue_size", 8192);

    ret->consume_mode = ConsumeModeNS::parseConsumeMode(config.getString("raft_server.consume_mode","sync_single"));
    ret->consume_batch_size = config.getInt("raft_server.consume_batch_size", 100);      //100 * 500 = 5w
    ret->consume_queue_size = config.getInt("raft_server.consume_queue_size", 100);      //100 * 100 * 1K = 10M
    ret->consume_waittime_ms = config.getInt("raft_server.consume_waittime_ms", 100);    //

    ret->raft_settings->loadFromConfig("raft_server.raft_configuration", config);

    return ret;
}

void Settings::dump(WriteBufferFromOwnString & buf) const
{
    auto write_int = [&buf](Int64 value)
    {
        writeIntText(value, buf);
        buf.write('\n');
    };

    writeText("server_id=", buf);
    write_int(server_id);

    writeText("host=", buf);
    writeText(host, buf);
    buf.write('\n');

    writeText("server_port=", buf);
    write_int(server_port);

    writeText("internal_port=", buf);
    write_int(internal_port);

    writeText("log_dir=", buf);
    writeText(log_dir, buf);
    buf.write('\n');

    writeText("state_dir=", buf);
    writeText(state_dir, buf);
    buf.write('\n');

    writeText("request_thread_count=", buf);
    write_int(request_thread_count);
    buf.write('\n');

    writeText("forward_thread_count=", buf);
    write_int(forward_thread_count);
    buf.write('\n');

    writeText("request_queue_size=", buf);
    write_int(request_queue_size);
    buf.write('\n');

    /// raft_settings
    writeText("operation_timeout_ms=", buf);
    write_int(raft_settings->operation_timeout_ms);

    writeText("heart_beat_interval_ms=", buf);
    write_int(raft_settings->heart_beat_interval_ms);
    writeText("election_timeout_lower_bound_ms=", buf);
    write_int(raft_settings->election_timeout_lower_bound_ms);
    writeText("election_timeout_upper_bound_ms=", buf);
    write_int(raft_settings->election_timeout_upper_bound_ms);

    writeText("reserved_log_items=", buf);
    write_int(raft_settings->reserved_log_items);
    writeText("snapshot_distance=", buf);
    write_int(raft_settings->snapshot_distance);
    writeText("max_stored_snapshots=", buf);
    write_int(raft_settings->max_stored_snapshots);

    writeText("shutdown_timeout=", buf);
    write_int(raft_settings->shutdown_timeout);
    writeText("startup_timeout=", buf);
    write_int(raft_settings->startup_timeout);

    writeText("raft_logs_level=", buf);
    write_int(static_cast<Int64>(raft_settings->raft_logs_level));

    writeText("log_fsync_mode=", buf);
    writeText(FsyncModeNS::toString(raft_settings->log_fsync_mode), buf);
    buf.write('\n');
    writeText("log_fsync_interval=", buf);
    write_int(raft_settings->log_fsync_interval);

    writeText("nuraft_thread_size=", buf);
    write_int(raft_settings->nuraft_thread_size);
    writeText("fresh_log_gap=", buf);
    write_int(raft_settings->fresh_log_gap);
}

void RaftSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        auto get_key = [&config_elem] (String key)-> String
        {
            return config_elem + "." + key;
        };

        operation_timeout_ms = config.getUInt(get_key("operation_timeout_ms"), 20000);
        heart_beat_interval_ms = config.getUInt(get_key("heart_beat_interval_ms"), 500);
        election_timeout_lower_bound_ms = config.getUInt(get_key("election_timeout_lower_bound_ms"), 1000);
        election_timeout_upper_bound_ms = config.getUInt(get_key("election_timeout_upper_bound_ms"), 2000);
        reserved_log_items = config.getUInt(get_key("reserved_log_items"), 1000000);
        snapshot_distance = config.getUInt(get_key("snapshot_distance"), 3000000);
        max_stored_snapshots = config.getUInt(get_key("max_stored_snapshots"), 5);
        startup_timeout = config.getUInt(get_key("startup_timeout"), 6000000);
        shutdown_timeout = config.getUInt(get_key("shutdown_timeout"), 5000);
        //raft_logs_level = Poco::Logger::parseLevel(config.getString(get_key("raft_logs_level"), "information"));
        //raft_logs_level = fromString(config.getString(get_key("raft_logs_level"), "information"));
        raft_logs_level = SettingFieldLogsLevelTraits::fromString(config.getString(get_key("raft_logs_level"), "information"));
        /// TODO set a value according to CPU size
        nuraft_thread_size = config.getUInt(get_key("nuraft_thread_size"), 32);
        fresh_log_gap = config.getUInt(get_key("fresh_log_gap"), 200);
        configuration_change_tries_count = config.getUInt(get_key("configuration_change_tries_count"), 30);
        max_batch_size = config.getUInt(get_key("max_batch_size"), 1000);
        log_fsync_mode = FsyncModeNS::parseFsyncMode(config.getString(get_key("log_fsync_mode"), "fsync_parallel"));
        log_fsync_interval = config.getUInt(get_key("log_fsync_interval"), 1000);

    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in configuration.");
        throw;
    }
}

RaftSettingsPtr RaftSettings::getDefault()
{
    RaftSettingsPtr settings = std::make_shared<RaftSettings>();
    settings->operation_timeout_ms = 20000;
    settings->heart_beat_interval_ms = 500;
    settings->election_timeout_lower_bound_ms = 10000;
    settings->election_timeout_upper_bound_ms = 20000;
    settings->reserved_log_items = 10000000;
    settings->snapshot_distance = 3000000;
    settings->max_stored_snapshots = 5;
    settings->shutdown_timeout = 5000;
    settings->startup_timeout = 6000000;

    settings->raft_logs_level = LogsLevel::information;
    settings->nuraft_thread_size = 32;
    settings->fresh_log_gap = 200;
    settings->configuration_change_tries_count = 30;
    settings->max_batch_size = 1000;
    settings->log_fsync_interval = 1000;
    settings->log_fsync_mode = FsyncMode::FSYNC_PARALLEL;

    return settings;
}

}
