#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/RocksDB/StorageReplicatedRocksDB.h>
#include <Storages/RocksDB/ReplicatedRocksDBSink.h>
#include <Storages/MutationCommands.h>
#include <Storages/Consensus/ChangeDataCapture.h>
#include <Storages/Consensus/RocksDBChangeData.h>

#include <DataTypes/DataTypesNumber.h>

#include <Storages/StorageFactory.h>
#include <Storages/KVStorageUtils.h>

#include <Parsers/ASTCreateQuery.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>

#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/MutationsInterpreter.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/NullSource.h>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <base/sort.h>

#include <rocksdb/table.h>
#include <rocksdb/convenience.h>
#include <rocksdb/utilities/db_ttl.h>

#include <cstddef>
#include <filesystem>
#include <utility>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ROCKSDB_ERROR;
}

using FieldVectorPtr = std::shared_ptr<FieldVector>;
using RocksDBOptions = std::unordered_map<std::string, std::string>;

static RocksDBOptions getOptionsFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    RocksDBOptions options;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        options[key] = config.getString(key_path);
    }

    return options;
}

class ReplicatedRocksDBSource : public ISource
{
public:
    ReplicatedRocksDBSource(
        const StorageReplicatedRocksDB & storage_,
        const Block & header,
        FieldVectorPtr keys_,
        FieldVector::const_iterator begin_,
        FieldVector::const_iterator end_,
        const size_t max_block_size_)
        : ISource(header)
        , storage(storage_)
        , primary_key_pos(getPrimaryKeyPos(header, storage.getPrimaryKey()))
        , keys(keys_)
        , begin(begin_)
        , end(end_)
        , it(begin)
        , max_block_size(max_block_size_)
    {
    }

    ReplicatedRocksDBSource(
        const StorageReplicatedRocksDB & storage_,
        const Block & header,
        std::unique_ptr<rocksdb::Iterator> iterator_,
        const size_t max_block_size_)
        : ISource(header)
        , storage(storage_)
        , primary_key_pos(getPrimaryKeyPos(header, storage.getPrimaryKey()))
        , iterator(std::move(iterator_))
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return storage.getName(); }

    Chunk generate() override
    {
        if (keys)
            return generateWithKeys();
        return generateFullScan();
    }

    Chunk generateWithKeys()
    {
        const auto & sample_block = getPort().getHeader();
        if (it >= end)
        {
            it = {};
            return {};
        }

        const auto & key_column_type = sample_block.getByName(storage.getPrimaryKey().at(0)).type;
        auto raw_keys = serializeKeysToRawString(it, end, key_column_type, max_block_size);
        return storage.getBySerializedKeys(raw_keys, nullptr);
    }

    Chunk generateFullScan()
    {
        if (!iterator->Valid())
            return {};

        const auto & sample_block = getPort().getHeader();
        MutableColumns columns = sample_block.cloneEmptyColumns();

        for (size_t rows = 0; iterator->Valid() && rows < max_block_size; ++rows, iterator->Next())
        {
            fillColumns(iterator->key(), iterator->value(), primary_key_pos, getPort().getHeader(), columns);
        }

        if (!iterator->status().ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Engine {} got error while seeking key value data: {}",
                getName(), iterator->status().ToString());
        }
        Block block = sample_block.cloneWithColumns(std::move(columns));
        return Chunk(block.getColumns(), block.rows());
    }

private:
    const StorageReplicatedRocksDB & storage;

    size_t primary_key_pos;

    /// For key scan
    FieldVectorPtr keys = nullptr;
    FieldVector::const_iterator begin;
    FieldVector::const_iterator end;
    FieldVector::const_iterator it;

    /// For full scan
    std::unique_ptr<rocksdb::Iterator> iterator = nullptr;

    const size_t max_block_size;
};


StorageReplicatedRocksDB::StorageReplicatedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        LoadingStrictnessLevel mode,
        ContextPtr context_,
        const String & primary_key_,
        String second_table_,
        Int32 ttl_,
        String rocksdb_dir_,
        bool read_only_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , primary_key{primary_key_}
    , second_table(second_table_)
    , ttl(ttl_)
    , rocksdb_dir(std::move(rocksdb_dir_))
    , read_only(read_only_)
    , log(&Poco::Logger::get("StorageReplicatedRocksDB"))
{
    setInMemoryMetadata(metadata_);
    if (rocksdb_dir.empty())
    {
        rocksdb_dir = context_->getPath() + relative_data_path_;
    }
    if (mode < LoadingStrictnessLevel::ATTACH)
    {
        fs::create_directories(rocksdb_dir);
    }
    initDB();
}

void StorageReplicatedRocksDB::truncate(const ASTPtr &, const StorageMetadataPtr & , ContextPtr, TableExclusiveLockHolder &)
{
    std::lock_guard lock(rocksdb_ptr_mx);
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;

    fs::remove_all(rocksdb_dir);
    fs::create_directories(rocksdb_dir);
    initDB();
}

void StorageReplicatedRocksDB::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    if (commands.empty())
        return;

    if (commands.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations cannot be combined for ReplicatedRocksDB");

    const auto command_type = commands.front().type;
    if (command_type != MutationCommand::Type::UPDATE && command_type != MutationCommand::Type::DELETE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only DELETE and UPDATE mutation supported for ReplicatedRocksDB");
}

void StorageReplicatedRocksDB::mutate(const MutationCommands & commands, ContextPtr context_)
{
    if (commands.empty())
        return;

    assert(commands.size() == 1);

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, context_);

    LOG_DEBUG(log, "Replicated rocksdb mutate {}", commands.toString());
    MutationsInterpreter::Settings settings(true);
    settings.return_all_columns = true;
    settings.return_mutated_rows = true;
    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot,
        commands, context_, settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    if (commands.front().type == MutationCommand::Type::DELETE)
    {
        Block block;
        while (executor.pull(block))
            innerDelete(commands, block);
    }
    else if (commands.front().type == MutationCommand::Type::UPDATE)
    {
        if (commands.front().column_to_update_expression.contains(primary_key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key cannot be updated");

        auto sink = std::make_shared<ReplicatedRocksDBSink>(context_, *this, metadata_snapshot, RaftOpNum::Update);
        Block block;
        while (executor.pull(block))
            sink->consume(Chunk{block.getColumns(), block.rows()});
    }
}

void StorageReplicatedRocksDB::innerDelete(const MutationCommands & commands, Block block)
{
    auto dispatcher = getContext()->getRaftDispatcher();
    auto storage_id = getStorageID();

    ChangeDataPtr change_data = std::make_shared<RocksDBChangeData>();
    change_data->database = storage_id.getDatabaseName();
    change_data->table = storage_id.getTableName();
    change_data->leader_storage = getName();
    change_data->storage_type = DB::matchType(getName());
    change_data->op_num = RaftOpNum::Delete;
    change_data->commands = commands;
    change_data->block = std::make_shared<Block>(block);
    change_data->second_table = getSecondTable();

    LOG_DEBUG(log, "Sink delete cdc header {}", change_data->dumpHeader());

    ChangeDataCapture cdc(dispatcher);
    try
    {
        cdc.sink(change_data);
    }
    catch(Exception ex)
    {
        LOG_ERROR(log, "Replicated rocksdb sink delete data to CDC, throw exeption {}", ex.displayText());
    }
}

void StorageReplicatedRocksDB::localMutate(const MutationCommands & commands, Chunk chunk)
{
    if (commands.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations cannot be combined for ReplicatedRocksDB");
    if (commands.front().type != MutationCommand::Type::DELETE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only DELETE mutation supported for raft ReplicatedRocksDB");

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, getContext());

    //get header from metadata snapshot
    auto header = metadata_snapshot->getSampleBlock();
    auto primary_key_pos = header.getPositionByName(primary_key);

    auto block = header.cloneWithColumns(chunk.detachColumns());

    auto & column_type_name = block.getByPosition(primary_key_pos);

    auto column = column_type_name.column;
    auto size = column->size();

    LOG_DEBUG(log, "Row size {} of primary key column", size);

    rocksdb::WriteBatch batch;
    WriteBufferFromOwnString wb_key;
    for (size_t i = 0; i < size; ++i)
    {
        wb_key.restart();

        column_type_name.type->getDefaultSerialization()->serializeBinary(*column, i, wb_key, {});
        auto status = batch.Delete(wb_key.str());
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
    }

    auto status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
}

void StorageReplicatedRocksDB::initDB()
{
    rocksdb::Status status;
    rocksdb::Options base;

    base.create_if_missing = true;
    base.compression = rocksdb::CompressionType::kZSTD;
    base.statistics = rocksdb::CreateDBStatistics();
    /// It is too verbose by default, and in fact we don't care about rocksdb logs at all.
    base.info_log_level = rocksdb::ERROR_LEVEL;

    rocksdb::Options merged = base;

    const auto & config = getContext()->getConfigRef();
    if (config.has("rocksdb.options"))
    {
        auto config_options = getOptionsFromConfig(config, "rocksdb.options");
        status = rocksdb::GetDBOptionsFromMap(merged, config_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }
    if (config.has("rocksdb.column_family_options"))
    {
        auto column_family_options = getOptionsFromConfig(config, "rocksdb.column_family_options");
        status = rocksdb::GetColumnFamilyOptionsFromMap(merged, column_family_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }

    if (config.has("rocksdb.tables"))
    {
        auto table_name = getStorageID().getTableName();

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("rocksdb.tables", keys);

        for (const auto & key : keys)
        {
            const String key_prefix = "rocksdb.tables." + key;
            if (config.getString(key_prefix + ".name") != table_name)
                continue;

            String config_key = key_prefix + ".options";
            if (config.has(config_key))
            {
                auto table_config_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetDBOptionsFromMap(merged, table_config_options, &merged);
                if (!status.ok())
                {
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key, rocksdb_dir, status.ToString());
                }
            }

            config_key = key_prefix + ".column_family_options";
            if (config.has(config_key))
            {
                auto table_column_family_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetColumnFamilyOptionsFromMap(merged, table_column_family_options, &merged);
                if (!status.ok())
                {
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key, rocksdb_dir, status.ToString());
                }
            }
        }
    }

    if (ttl > 0)
    {
        rocksdb::DBWithTTL * db;
        status = rocksdb::DBWithTTL::Open(merged, rocksdb_dir, &db, ttl, read_only);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}",
                rocksdb_dir, status.ToString());
        }
        rocksdb_ptr = std::unique_ptr<rocksdb::DBWithTTL>(db);
    }
    else
    {
        rocksdb::DB * db;
        if (read_only)
        {
            status = rocksdb::DB::OpenForReadOnly(merged, rocksdb_dir, &db);
        }
        else
        {
            status = rocksdb::DB::Open(merged, rocksdb_dir, &db);
        }
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}",
                rocksdb_dir, status.ToString());
        }
        rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
    }
}

class ReadFromReplicatedRocksDB : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromReplicatedRocksDB"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    ReadFromReplicatedRocksDB(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        const StorageReplicatedRocksDB & storage_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(DataStream{.header = std::move(sample_block)}, column_names_, query_info_, storage_snapshot_, context_)
        , storage(storage_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

private:
    const StorageReplicatedRocksDB & storage;

    size_t max_block_size;
    size_t num_streams;

    FieldVectorPtr keys;
    bool all_scan = false;
};

void StorageReplicatedRocksDB::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t num_streams)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    auto reading = std::make_unique<ReadFromReplicatedRocksDB>(
        column_names, query_info, storage_snapshot, context_, std::move(sample_block), *this, max_block_size, num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromReplicatedRocksDB::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto & sample_block = getOutputStream().header;

    if (all_scan)
    {
        auto iterator = std::unique_ptr<rocksdb::Iterator>(storage.rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        iterator->SeekToFirst();
        auto source = std::make_shared<ReplicatedRocksDBSource>(storage, sample_block, std::move(iterator), max_block_size);
        source->setStorageLimits(query_info.storage_limits);
        pipeline.init(Pipe(std::move(source)));
    }
    else
    {
        if (keys->empty())
        {
            pipeline.init(Pipe(std::make_shared<NullSource>(sample_block)));
            return;
        }

        ::sort(keys->begin(), keys->end());
        keys->erase(std::unique(keys->begin(), keys->end()), keys->end());

        Pipes pipes;

        size_t num_keys = keys->size();
        size_t num_threads = std::min<size_t>(num_streams, keys->size());

        assert(num_keys <= std::numeric_limits<uint32_t>::max());
        assert(num_threads <= std::numeric_limits<uint32_t>::max());

        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
        {
            size_t begin = num_keys * thread_idx / num_threads;
            size_t end = num_keys * (thread_idx + 1) / num_threads;

            auto source = std::make_shared<ReplicatedRocksDBSource>(
                    storage, sample_block, keys, keys->begin() + begin, keys->begin() + end, max_block_size);
            source->setStorageLimits(query_info.storage_limits);
            pipes.emplace_back(std::move(source));
        }
        pipeline.init(Pipe::unitePipes(std::move(pipes)));
    }
}

void ReadFromReplicatedRocksDB::applyFilters(ActionDAGNodes added_filter_nodes)
{
    filter_actions_dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes);
    const auto & sample_block = getOutputStream().header;
    auto primary_key_data_type = sample_block.getByName(storage.primary_key).type;
    std::tie(keys, all_scan) = getFilterKeys(storage.primary_key, primary_key_data_type, filter_actions_dag, context);
}

SinkToStoragePtr StorageReplicatedRocksDB::write(
    const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool /*async_insert*/)
{
    auto storage_id = getStorageID();
    return std::make_shared<ReplicatedRocksDBSink>(context_, *this, metadata_snapshot, RaftOpNum::Insert);
}

std::shared_ptr<rocksdb::Statistics> StorageReplicatedRocksDB::getRocksDBStatistics() const
{
    std::shared_lock lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return nullptr;
    return rocksdb_ptr->GetOptions().statistics;
}

std::vector<rocksdb::Status> StorageReplicatedRocksDB::multiGet(const std::vector<rocksdb::Slice> & slices_keys, std::vector<String> & values) const
{
    std::shared_lock lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    return rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), slices_keys, &values);
}

Chunk StorageReplicatedRocksDB::getByKeys(
    const ColumnsWithTypeAndName & keys,
    PaddedPODArray<UInt8> & null_map,
    const Names &) const
{
    if (keys.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageReplicatedRocksDB supports only one key, got: {}", keys.size());

    auto raw_keys = serializeKeysToRawString(keys[0]);

    if (raw_keys.size() != keys[0].column->size())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Assertion failed: {} != {}", raw_keys.size(), keys[0].column->size());

    return getBySerializedKeys(raw_keys, &null_map);
}

Block StorageReplicatedRocksDB::getSampleBlock(const Names &) const
{
    return getInMemoryMetadataPtr()->getSampleBlock();
}

Chunk StorageReplicatedRocksDB::getBySerializedKeys(
    const std::vector<std::string> & keys,
    PaddedPODArray<UInt8> * null_map) const
{
    std::vector<String> values;
    Block sample_block = getInMemoryMetadataPtr()->getSampleBlock();

    size_t primary_key_pos = getPrimaryKeyPos(sample_block, getPrimaryKey());

    MutableColumns columns = sample_block.cloneEmptyColumns();

    /// Convert from vector of string to vector of string refs (rocksdb::Slice), because multiGet api expects them.
    std::vector<rocksdb::Slice> slices_keys;
    slices_keys.reserve(keys.size());
    for (const auto & key : keys)
        slices_keys.emplace_back(key);

    auto statuses = multiGet(slices_keys, values);
    if (null_map)
    {
        null_map->clear();
        null_map->resize_fill(statuses.size(), 1);
    }

    for (size_t i = 0; i < statuses.size(); ++i)
    {
        if (statuses[i].ok())
        {
            fillColumns(slices_keys[i], values[i], primary_key_pos, sample_block, columns);
        }
        else if (statuses[i].IsNotFound())
        {
            if (null_map)
            {
                (*null_map)[i] = 0;
                for (size_t col_idx = 0; col_idx < sample_block.columns(); ++col_idx)
                {
                    columns[col_idx]->insert(sample_block.getByPosition(col_idx).type->getDefault());
                }
            }
        }
        else
        {
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "rocksdb error {}", statuses[i].ToString());
        }
    }

    size_t num_rows = columns.at(0)->size();
    return Chunk(std::move(columns), num_rows);
}

RocksDBIterator StorageReplicatedRocksDB::getFirstIterator()
{
    auto iterator = std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
    iterator->SeekToFirst();
    return iterator;
}

Chunk StorageReplicatedRocksDB::getByIterator(RocksDBIterator & iterator, size_t max_block_size)
{
    if (!iterator->Valid())
        return {};

    Block sample_block = getInMemoryMetadataPtr()->getSampleBlock();
    size_t primary_key_pos = getPrimaryKeyPos(sample_block, getPrimaryKey());
    MutableColumns columns = sample_block.cloneEmptyColumns();

    for (size_t rows = 0; iterator->Valid() && rows < max_block_size; ++rows, iterator->Next())
    {
        fillColumns(iterator->key(), iterator->value(), primary_key_pos, sample_block, columns);
    }

    if (!iterator->status().ok())
    {
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Engine {} got error while seeking key value data: {}",
            getName(), iterator->status().ToString());
    }
    Block block = sample_block.cloneWithColumns(std::move(columns));
    return Chunk(block.getColumns(), block.rows());
}

std::optional<UInt64> StorageReplicatedRocksDB::totalRows(const Settings & settings) const
{
    if (!settings.optimize_trivial_approximate_count_query)
        return {};
    std::shared_lock lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    UInt64 estimated_rows;
    if (!rocksdb_ptr->GetIntProperty("rocksdb.estimate-num-keys", &estimated_rows))
        return {};
    return estimated_rows;
}

std::optional<UInt64> StorageReplicatedRocksDB::totalBytes(const Settings & /*settings*/) const
{
    std::shared_lock lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    UInt64 estimated_bytes;
    if (!rocksdb_ptr->GetAggregatedIntProperty("rocksdb.estimate-live-data-size", &estimated_bytes))
        return {};
    return estimated_bytes;
}

static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO custom RocksDBSettings, table function
    auto engine_args = args.engine_args;
    if (engine_args.size() > 4)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Engine {} requires at most 3 parameters. "
                        "({} given). Correct usage: ReplicatedRocksDB([ttl, rocksdb_dir, read_only])",
                        args.engine_name, engine_args.size());
    }

    String second_table;
    Int32 ttl{0};
    String rocksdb_dir;
    bool read_only{false};
    if (!engine_args.empty())
        second_table = checkAndGetLiteralArgument<String>(engine_args[0], "second_table");
    if (engine_args.size() > 1)
        ttl = static_cast<Int32>(checkAndGetLiteralArgument<UInt64>(engine_args[1], "ttl"));
    if (engine_args.size() > 2)
        rocksdb_dir = checkAndGetLiteralArgument<String>(engine_args[2], "rocksdb_dir");
    if (engine_args.size() > 3)
        read_only = checkAndGetLiteralArgument<bool>(engine_args[3], "read_only");

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageReplicatedRocksDB must require one column in primary key");

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageReplicatedRocksDB must require one column in primary key");
    }
    return std::make_shared<StorageReplicatedRocksDB>(args.table_id, args.relative_data_path, metadata, args.mode,
        args.getContext(), primary_key_names[0], std::move(second_table), ttl, std::move(rocksdb_dir), read_only);
}

void registerStorageReplicatedRocksDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("ReplicatedRocksDB", create, features);
}

}
