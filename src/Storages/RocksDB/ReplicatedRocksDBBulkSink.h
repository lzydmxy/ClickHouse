#pragma once

#include <condition_variable>
#include <Processors/Sinks/SinkToStorage.h>
#include <rocksdb/db.h>
#include <Common/ThreadStatus.h>
#include <Columns/ColumnString.h>
#include <Processors/Chunk.h>
#include <Storages/Consensus/RaftCommon.h>


namespace DB
{
namespace fs = std::filesystem;

class StorageReplicatedRocksDB;
class ReplicatedRocksDBBulkSink;
struct StorageInMemoryMetadata;
class ChangeDataCapture;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Optimized for bulk importing into StorageEmbeddedRocksDB:
/// 1. No mem-table: an SST file is built from chunk, then import to rocksdb
/// 2. Squash chunks to reduce the number of SST files
class ReplicatedRocksDBBulkSink : public SinkToStorage, public WithContext, public std::enable_shared_from_this<ReplicatedRocksDBBulkSink>
{
public:
    ReplicatedRocksDBBulkSink(
        ContextPtr context_,
        StorageReplicatedRocksDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        Consensus::RaftOpNum op_num_);

    ~ReplicatedRocksDBBulkSink() override;

    /// consume and append to raft
    void consume(Chunk chunk) override;

    /// raft commit and consume data to local rocksdb storage
    void localConsume(Chunk chunk);

    /// raft commit and consume data to local rocksdb storage
    void writeSync(MutableColumnPtr &&, MutableColumnPtr &&, const String &);

    std::future<void> writeAsync(MutableColumnPtr &&, MutableColumnPtr &&, String);

    void onFinish() override;

    String getName() const override { return "ReplicatedRocksDBBulkSink"; }

private:
    /// Get a unique path to write temporary SST file
    String getTemporarySSTFilePath();

    /// Squash chunks to a minimum size
    std::vector<Chunk> squash(Chunk chunk);
    bool isEnoughSize(const std::vector<Chunk> & input_chunks) const;
    bool isEnoughSize(const Chunk & chunk) const;
    void ingestExternalFile(const String & sst_file_path);
    /// Serialize chunks to rocksdb key-value pairs
    std::pair<MutableColumnPtr, MutableColumnPtr> serializeChunks(const std::vector<Chunk> & input_chunks) const;

    const StorageReplicatedRocksDB & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t primary_key_pos = 0;
    Serializations serializations;
    Consensus::RaftOpNum op_num;

    /// For squashing chunks
    std::vector<Chunk> chunks;
    size_t min_block_size_rows = 0;

    /// For writing SST files
    size_t file_counter = 0;
    static constexpr auto TMP_INSERT_PREFIX = "tmp_insert_";
    String insert_directory_queue;
    Poco::Logger * log;
    ThreadPool * rocksdb_bulk_insertions_threadpool;
    std::vector<std::future<void>> futures;

    std::shared_ptr<ChangeDataCapture> cdc;
};

}
