#pragma once
#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/ClientInfo.h>
#include <Common/ProfileEvents.h>

namespace DB
{
struct QueryExchangeLogElement
{
    String initial_query_id{"-1"};
    UInt64 exchange_id{std::numeric_limits<UInt64>::max()};
    UInt64 partition_id{std::numeric_limits<UInt64>::max()};
    UInt64 parallel_index{std::numeric_limits<UInt64>::max()};
    String coordinator_address{};
    time_t event_time{};

    Int32 finish_code{};
    Int8 is_modifier{};
    String message;
    String type;

    /**
     * sender metrics
     * if brpc senders are merged, only first sender will have valid metrics
     */ 
    UInt64 send_time_ms{};
    UInt64 send_rows{};
    /// io buf bytes, for DiskPartitionWriter will be zero
    UInt64 send_bytes{};
    /// chunk bytes, for some data types like `ColumnAggregateFunction`, send_uncompressed_bytes can be inaccurate
    UInt64 send_uncompressed_bytes{};
    UInt64 num_send_times{};
    UInt64 ser_time_ms{};
    UInt64 send_retry{};
    UInt64 send_retry_ms{};
    UInt64 overcrowded_retry{};

    /**
     * disk partition writer metrics
     */
    UInt64 disk_partition_writer_create_file_ms{}; /// create initial file cost
    UInt64 disk_partition_writer_pop_ms{}; /// bg thread pop cost
    UInt64 disk_partition_writer_write_ms{}; /// push to queue cost
    UInt64 disk_partition_writer_write_num{}; /// how many times write op is invoked
    UInt64 disk_partition_writer_commit_ms{}; /// rename file cost
    UInt64 disk_partition_writer_sync_ms{}; /// last sync op cost
    UInt64 disk_partition_writer_wait_done_ms{}; /// wait for bg task done cost 

    /**
     * receiver metrics
     * multipath receivers will not have query_exchange_log
     */ 
    UInt64 recv_counts{};
    UInt64 recv_time_ms{};
    UInt64 register_time_ms{};
    UInt64 recv_rows{};
    UInt64 recv_bytes{}; /// io buff bytes
    UInt64 recv_uncompressed_bytes{}; /// chunk bytes
    UInt64 dser_time_ms{};


    std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters;

    static std::string name() { return "QueryExchangeLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};


class QueryExchangeLog : public SystemLog<QueryExchangeLogElement>
{
    using SystemLog<QueryExchangeLogElement>::SystemLog;
};

}
