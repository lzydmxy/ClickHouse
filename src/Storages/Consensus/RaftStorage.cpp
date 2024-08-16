#include <functional>
#include <iomanip>
#include <boost/algorithm/string.hpp>
#include <rocksdb/db.h>
#include <Poco/Base64Encoder.h>
#include <Poco/SHA1Engine.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/queryToString.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Storages/Consensus/RaftStorage.h>
#include <Storages/Consensus/RocksDBChangeData.h>
#include <Storages/Consensus/ChangeDataCapture.h>


namespace DB
{

using namespace Consensus;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int KEEPER_EXCEPTION;
}

NuBufferPtr SnapshotSegment::serialize()
{
    WriteBufferFromNuraftBuffer buf;
    Consensus::write(committed_index, buf);
    Consensus::write(committed_term, buf);

    Consensus::write(snapshot_type, buf);
    Consensus::write(queries.size(), buf);
    for (auto const & [name, query] : queries)
    {
        Consensus::write(name, buf);
        Consensus::write(query.query_type, buf);
        Consensus::write(query.query, buf);
    }

    Consensus::write(total_tables, buf);
    Consensus::write(object_id, buf);
    Consensus::write(is_last, buf);
    Consensus::write(row_count, buf);
    Consensus::write(datas.size(), buf);
    for (auto data : datas)
        data->serialize(buf);
    return buf.getBuffer();
}

void SnapshotSegment::deserialize(NuBuffer & in)
{
    ReadBufferFromNuraftBuffer buf(in);
    Consensus::read(committed_index, buf);
    Consensus::read(committed_term, buf);

    Consensus::read(snapshot_type, buf);
    size_t query_size;
    Consensus::read(query_size, buf);
    for (size_t i = 0; i < query_size; i++)
    {
        std::string name, query;
        uint8_t type;
        Consensus::read(name, buf);
        Consensus::read(type, buf);
        Consensus::read(query, buf);
        queries[name] = CreateQuery(type, query);
    }

    Consensus::read(total_tables, buf);
    Consensus::read(object_id, buf);
    Consensus::read(is_last, buf);
    Consensus::read(row_count, buf);
    size_t changedata_count;
    Consensus::read(changedata_count, buf);
    for (size_t i = 0; i < changedata_count; i++)
        datas.push_back(ChangeData::readFromBuffer(buf));
}

size_t SnapshotTable::totalTables()
{
    return todo_tables.size() + done_tables.size();
}

void SnapshotTable::putStorage(StoragePtr & storage)
{
    auto table = std::make_shared<TableState>();
    table->storage = storage;
    table->row_count = 0;
    table->iterator = nullptr;
    todo_tables.push(table);
}

//return curr not finished table or get next table
TableStatePtr SnapshotTable::getStorage()
{
    if (curr_table != nullptr)
    {
        if (curr_table->iterator->Valid())
            return curr_table;
    }

    if (todo_tables.empty())
        return nullptr;
    auto st = todo_tables.front();
    todo_tables.pop();
    curr_table = st;
    done_tables.push_back(st);
    return st;
}

void SnapshotTable::reset()
{
    committed_index = 0;
    committed_term = 0;
    while (!todo_tables.empty())
        todo_tables.pop();
    done_tables.clear();
    curr_table = nullptr;
}

RaftStorage::RaftStorage(const SettingsPtr & settings_)
    : settings(settings_)
    , log(&(Poco::Logger::get("RaftStorage")))
{
    cdc = std::make_shared<ChangeDataCapture>(settings_);
    cdc->init();
}

void RaftStorage::finalize()
{
    if (finalized)
        throw Exception( ErrorCodes::LOGICAL_ERROR, "Raft store already finalized");
    cdc->exit();
    finalized = false;
}

RaftResponsePtr RaftStorage::processRequest(RaftRequestPtr & request)
{
    LOG_DEBUG(log, "[Process request]Opnum {}", Consensus::toString(request->getOpNum()));

    if (request->getOpNum() == RaftOpNum::Error)
        return nullptr;

    RaftResponsePtr response = request->makeResponse();

    ChangeDataPtr data;

    if (request->getOpNum() == RaftOpNum::Insert)
        data = dynamic_cast<RaftInsertRequest *>(request.get())->data;
    else if (request->getOpNum() == RaftOpNum::Update)
        data = dynamic_cast<RaftUpdateRequest *>(request.get())->data;
    else if (request->getOpNum() == RaftOpNum::Delete)
        data = dynamic_cast<RaftDeleteRequest *>(request.get())->data;
    else
        return nullptr;

    try
    {
        cdc->consume(data);
        response->error = Consensus::Error::ZOK;
    }
    catch(Exception ex)
    {
        response->error = Consensus::Error::STORAGEERROR;
        response->message = ex.displayText();
        LOG_WARNING(log, "CDC comsume exception : {}", ex.displayText());
    }
    return response;
}

void RaftStorage::saveSnapshotMeta(NuSnapshot & snapshot)
{
    snapshot_table.reset();
    snapshot_table.committed_index = snapshot.get_last_log_idx();
    snapshot_table.committed_term = snapshot.get_last_log_term();
}

bool RaftStorage::needCreateSnapshot(std::string & storage_name)
{
    //Only ReplicatedRocksDB storage need be created snapshot
    return storage_name == "ReplicatedRocksDB";
}

void RaftStorage::checkSnapshotMeta(NuSnapshot & snapshot)
{
    if (snapshot.get_last_log_idx() != snapshot_table.committed_index ||
        snapshot.get_last_log_term() != snapshot_table.committed_term)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "nuraft parameters is error, raft term {} index {}, last log term {} index {}",
            snapshot.get_last_log_term(), snapshot.get_last_log_idx(),
            snapshot_table.committed_term, snapshot_table.committed_index);
    }
}

void RaftStorage::loadSnapshot(NuSnapshot & snapshot)
{
    saveSnapshotMeta(snapshot);

    LOG_INFO(log, "Begin create snapshot, last log term {} index {}",
        snapshot.get_last_log_term(), snapshot.get_last_log_idx());

    for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases())
    {
        for (auto it = database->getTablesIterator(nullptr, nullptr); it->isValid(); it->next())
        {
            auto storage = it->table();
            if (!storage) continue;
            if (storage->getName() == "ReplicatedRocksDB")
                snapshot_table.putStorage(storage);
        }
    }
}

SnapshotSegmentPtr RaftStorage::nextSegment(NuSnapshot & snapshot, uint64_t obj_id)
{
    checkSnapshotMeta(snapshot);

    LOG_INFO(log, "Create next snapshot segment, obj id {}, last log term {} index {}",
        obj_id, snapshot.get_last_log_term(), snapshot.get_last_log_idx());

    auto segment = std::make_shared<SnapshotSegment>();
    segment->committed_index = snapshot_table.committed_index;
    segment->committed_term = snapshot_table.committed_term;

    //Create meta snapshot
    if (obj_id == 1)
    {
        segment->snapshot_type = 1;
        auto global_ctx = Context::getGlobalContextInstance();
        for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases())
        {
            for (auto it = database->getTablesIterator(nullptr, nullptr); it->isValid(); it->next())
            {
                auto storage = it->table();
                if (!storage) continue;
                if (storage->getName() == "ReplicatedRocksDB")
                {
                    if (segment->queries.find(database_name) == segment->queries.end())
                    {
                        auto query_ast = database->getCreateDatabaseQuery();
                        auto query = queryToString(query_ast);
                        size_t type = 1;
                        segment->queries[database_name] = CreateQuery(type, query);
                        LOG_DEBUG(log, "Database name {}, query : {}", database_name, query);
                    }
                    auto table_name = storage->getStorageID().getTableName();
                    auto full_table_name = storage->getStorageID().getFullTableName();
                    auto table_ast = database->getCreateTableQuery(table_name, global_ctx);
                    auto query = queryToString(table_ast);
                    size_t type = 2;
                    segment->queries[full_table_name] = CreateQuery(type, query);
                    LOG_DEBUG(log, "Table full name {}, query : {}", full_table_name, query);
                }
            }
        }
    }
    //Create data snapshot
    else
    {
        segment->snapshot_type = 2;
        segment->total_tables = snapshot_table.totalTables();
        segment->object_id = obj_id;
        while(true)
        {
            auto table = snapshot_table.getStorage();
            if (!table)
            {
                segment->is_last = true;
                break;
            }

            LOG_DEBUG(log, "Pack snapshot total_tables {}, todo tables {}", segment->total_tables, snapshot_table.todo_tables.size());

            auto rocksdb_storage = dynamic_cast<StorageReplicatedRocksDB* >(table->storage.get());
            if (!table->iterator)
                table->iterator = rocksdb_storage->getFirstIterator();

            const auto & sample_block = rocksdb_storage->getInMemoryMetadataPtr()->getSampleBlock();
            auto chunk = rocksdb_storage->getByIterator(table->iterator, snapshot_table.max_block_size - segment->row_count);
            if (chunk.getNumRows() > 0)
            {
                auto change_data = std::make_shared<RocksDBChangeData>();
                auto storage_id = rocksdb_storage->getStorageID();
                change_data->database = storage_id.getDatabaseName();
                change_data->table = storage_id.getTableName();
                change_data->leader_storage = rocksdb_storage->getName();
                change_data->storage_type = DB::matchType(rocksdb_storage->getName());
                change_data->op_num = RaftOpNum::Insert;
                change_data->second_table = rocksdb_storage->getSecondTable();
                change_data->block = std::make_shared<Block>(sample_block.cloneWithColumns(chunk.detachColumns()));

                LOG_DEBUG(log, "Pack snapshot header {}", change_data->dumpHeader());

                segment->datas.push_back(change_data);
                segment->row_count += chunk.getNumRows();
                table->row_count += chunk.getNumRows();

                //seek to next
                if (table->iterator->Valid())
                    table->iterator->Next();
                else
                    break;
                if (segment->row_count >= snapshot_table.max_block_size)
                    break;
            }
        }
    }
    return segment;
}

void RaftStorage::beginSaveSnapshot(NuSnapshot & snapshot)
{
    saveSnapshotMeta(snapshot);

    LOG_INFO(log, "Begin save snapshot, last log term {} index {}",
        snapshot.get_last_log_term(), snapshot.get_last_log_idx());
}

void RaftStorage::saveSnapshotSegment(NuSnapshot & snapshot, ulong & obj_id, SnapshotSegmentPtr & segment)
{
    checkSnapshotMeta(snapshot);

    ThreadStatus thread_status;
    try
    {
        //object id == 1: create meta for databases and tables
        if (obj_id == 1)
        {
            LOG_INFO(log, "Save snapshot meta, last log term {} index {}, object id {}",
                snapshot.get_last_log_term(), snapshot.get_last_log_idx(), obj_id);
            cdc->createMeta(segment->queries);
        }
        else
        {
            LOG_INFO(log, "Save snapshot data, last log term {} index {}, object id {}",
                snapshot.get_last_log_term(), snapshot.get_last_log_idx(), obj_id);
            for (auto data : segment->datas)
                cdc->consume(data);
        }
    }
    catch(Exception ex)
    {
        LOG_ERROR(log, "Consum snapshot, throw exeption {}", ex.displayText());
    }
}

void RaftStorage::applySnapshot(NuSnapshot & snapshot)
{
    checkSnapshotMeta(snapshot);

    LOG_INFO(log, "Apply snapshot, last log term {} index {}",
        snapshot.get_last_log_term(), snapshot.get_last_log_idx());
}

}
