#pragma once
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/Consensus/Settings.h>
#include <Storages/Consensus/ChangeData.h>
#include <Storages/RocksDB/StorageReplicatedRocksDB.h>


namespace DB
{

using namespace Consensus;

struct SnapshotSegment;
using SnapshotSegmentPtr = std::shared_ptr<SnapshotSegment>;
class ChangeDataCapture;
using ChangeDataCapturePtr = std::shared_ptr<ChangeDataCapture>;

struct CreateQuery
{
    //1.database; 2.table
    uint8_t query_type;
    std::string query;
    CreateQuery()
    {}

    CreateQuery(uint8_t query_type_, std::string query_) : query_type(query_type_), query(query_)
    {}
};

struct SnapshotSegment
{
    uint64_t committed_index;
    uint64_t committed_term;
    //1. meta; 2. data;
    uint8_t snapshot_type;
    std::map<std::string, CreateQuery> queries;
    size_t total_tables;
    uint64_t object_id;
    bool is_last;
    size_t row_count;
    ChangeDatas datas;

    NuBufferPtr serialize();
    void deserialize(NuBuffer & in);
};

struct TableState
{
    StoragePtr storage;
    uint64_t row_count;
    RocksDBIterator iterator;
};

using TableStatePtr = std::shared_ptr<TableState>;

struct SnapshotTable
{
    uint64_t max_block_size;
    uint64_t committed_index;
    uint64_t committed_term;
    std::queue<TableStatePtr> todo_tables;
    std::vector<TableStatePtr> done_tables;
    TableStatePtr curr_table;

    size_t totalTables();

    void putStorage(StoragePtr & storage);

    //return curr not finished table or get next table
    TableStatePtr getStorage();

    void reset();
};

class RaftStorage
{
public:
    RaftStorage(const SettingsPtr & settings_);
    RaftResponsePtr processRequest(RaftRequestPtr & request);

    void loadSnapshot(NuSnapshot & snapshot);
    SnapshotSegmentPtr nextSegment(NuSnapshot & snapshot, uint64_t obj_id);

    void beginSaveSnapshot(NuSnapshot & snapshot);
    void saveSnapshotSegment(NuSnapshot & snapshot, ulong & obj_id, SnapshotSegmentPtr & data);
    void applySnapshot(NuSnapshot & snapshot);

    void finalize();
private:
    void saveSnapshotMeta(NuSnapshot & snapshot);
    void checkSnapshotMeta(NuSnapshot & snapshot);
    bool needCreateSnapshot(std::string & storage_name);
    SnapshotTable snapshot_table;
    SettingsPtr settings;
    Poco::Logger * log;
    bool finalized{false};
    ChangeDataCapturePtr cdc;
};

}
