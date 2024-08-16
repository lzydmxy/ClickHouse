#pragma once

#include <memory>
#include <Common/SharedMutex.h>
#include <Storages/IStorage.h>
#include <Interpreters/IKeyValueEntity.h>
#include <rocksdb/status.h>


namespace rocksdb
{
    class DB;
    class Statistics;
    class Iterator;
}

namespace DB
{

class Context;

using RocksDBIterator = std::shared_ptr<rocksdb::Iterator>;

/// Wrapper for rocksdb storage.
/// Operates with rocksdb data structures via rocksdb API (holds pointer to rocksdb::DB inside for that).
/// Storage have one primary key.
/// Values are serialized into raw strings to store in rocksdb.
class StorageReplicatedRocksDB final : public IStorage, public IKeyValueEntity, WithContext
{
    friend class ReplicatedRocksDBSink;
    friend class ReadFromReplicatedRocksDB;
public:
    StorageReplicatedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        LoadingStrictnessLevel mode,
        ContextPtr context_,
        const String & primary_key_,
        String second_table_ = "",
        Int32 ttl_ = 0,
        String rocksdb_dir_ = "",
        bool read_only_ = false);

    std::string getName() const override { return "ReplicatedRocksDB"; }
    std::string getSecondTable() const { return second_table; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;
    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands &, ContextPtr) override;
    void localMutate(const MutationCommands &, Chunk chunk);

    bool supportsParallelInsert() const override { return true; }

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {rocksdb_dir}; }

    std::shared_ptr<rocksdb::Statistics> getRocksDBStatistics() const;
    std::vector<rocksdb::Status> multiGet(const std::vector<rocksdb::Slice> & slices_keys, std::vector<String> & values) const;
    Names getPrimaryKey() const override { return {primary_key}; }

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const override;

    Block getSampleBlock(const Names &) const override;

    bool parallelizeOutputAfterReading(ContextPtr) const override { return false; }

    bool supportsDelete() const override { return false; }

    /// Return chunk with data for given serialized keys.
    /// If out_null_map is passed, fill it with 1/0 depending on key was/wasn't found. Result chunk may contain default values.
    /// If out_null_map is not passed. Not found rows excluded from result chunk.
    Chunk getBySerializedKeys(
        const std::vector<std::string> & keys,
        PaddedPODArray<UInt8> * out_null_map) const;

    /// get first interator of this table storage
    RocksDBIterator getFirstIterator();
    /// full scan rows data from the begin iterator
    Chunk getByIterator(RocksDBIterator & iterator, size_t max_block_size);

    const String primary_key;

    /// To turn on the optimization optimize_trivial_approximate_count_query=1 should be set for a query.
    bool supportsTrivialCountOptimization() const override { return true; }

    std::optional<UInt64> totalRows(const Settings & settings) const override;

    std::optional<UInt64> totalBytes(const Settings & settings) const override;
private:
    using RocksDBPtr = std::unique_ptr<rocksdb::DB>;
    RocksDBPtr rocksdb_ptr;
    mutable SharedMutex rocksdb_ptr_mx;
    String second_table;
    Int32 ttl;
    String rocksdb_dir;
    bool read_only;

    Poco::Logger * log;

    void initDB();
    void innerDelete(const MutationCommands &, Block block);
};

}
