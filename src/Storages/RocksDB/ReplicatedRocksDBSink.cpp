#include <Storages/RocksDB/ReplicatedRocksDBSink.h>
#include <Storages/RocksDB/StorageReplicatedRocksDB.h>
#include <Storages/Consensus/ChangeDataCapture.h>
#include <Storages/Consensus/RocksDBChangeData.h>

#include <Storages/StorageInMemoryMetadata.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

#include <rocksdb/utilities/db_ttl.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

ReplicatedRocksDBSink::ReplicatedRocksDBSink(
    ContextPtr context_,
    const StorageReplicatedRocksDB & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    Consensus::RaftOpNum op_num_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , context(context_)
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , op_num(op_num_)
    , log(&Poco::Logger::get("ReplicatedRocksDBSink"))
{
    for (const auto & elem : getHeader())
    {
        if (elem.name == storage.primary_key)
            break;
        ++primary_key_pos;
    }

    if (op_num != RaftOpNum::Insert && op_num != RaftOpNum::Update)
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Replicated rocksDB sink only support insert and update, operator number {}",
            toString(op_num));
}

void ReplicatedRocksDBSink::consume(Chunk chunk)
{
    LOG_DEBUG(log, "Sink consume to cdc");
    auto dispatcher = context->getRaftDispatcher();
    auto storage_id = storage.getStorageID();
    ChangeDataPtr change_data = std::make_shared<RocksDBChangeData>();
    change_data->database = storage_id.getDatabaseName();
    change_data->table = storage_id.getTableName();
    auto storage_type = DB::matchType(storage.getName());
    if (storage_type == StorageType::NotSupport)
    {
        LOG_WARNING(log, "Not support storage {} by raft", storage.getName());
        return;
    }
    change_data->leader_storage = storage.getName();
    change_data->storage_type = storage_type;
    change_data->op_num = op_num;
    change_data->second_table = storage.getSecondTable();

    change_data->block = std::make_shared<Block>(getHeader().cloneWithColumns(chunk.detachColumns()));
    LOG_DEBUG(log, "Sink consume to cdc, header {}", change_data->dumpHeader());
    ChangeDataCapture cdc(dispatcher);
    try
    {
        cdc.sink(change_data);
    }
    catch(Exception ex)
    {
        LOG_ERROR(log, "Replicated rocksdb sink data to CDC, throw exeption {}", ex.displayText());
    }
}

void ReplicatedRocksDBSink::localConsume(Chunk chunk)
{
    auto rows = chunk.getNumRows();
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());

    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;

    rocksdb::WriteBatch batch;
    rocksdb::Status status;
    LOG_DEBUG(log, "Write local rocksdb columns {}, rows {}", block.columns(), rows);
    for (size_t i = 0; i < rows; ++i)
    {
        wb_key.restart();
        wb_value.restart();

        size_t idx = 0;
        for (const auto & elem : block)
        {
            auto column = elem.column;
            elem.type->getDefaultSerialization()->serializeBinary(*column, i, idx == primary_key_pos ? wb_key : wb_value, {});
            ++idx;
        }
        status = batch.Put(wb_key.str(), wb_value.str());
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
    }

    status = storage.rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
}

}
