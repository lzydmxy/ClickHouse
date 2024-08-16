#pragma once
#include <Interpreters/Context_fwd.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/Consensus/RaftCommon.h>


namespace DB
{

struct StorageID;
class StorageReplicatedRocksDB;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class ReplicatedRocksDBSink : public SinkToStorage
{
public:
    ReplicatedRocksDBSink(
        ContextPtr context_,
        const StorageReplicatedRocksDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        Consensus::RaftOpNum op_num_);
    /// consume and append to raft
    void consume(Chunk chunk) override;
    /// raft commit and consume data to local rocksdb storage
    void localConsume(Chunk chunk);
    String getName() const override { return "ReplicatedRocksDBSink"; }

private:
    ContextPtr context;
    const StorageReplicatedRocksDB & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t primary_key_pos = 0;
    Consensus::RaftOpNum op_num;
    Poco::Logger * log;
};

}
