#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <IO/WriteBufferFromString.h>
#include <Storages/RocksDB/ReplicatedRocksDBBulkSink.h>
#include <Storages/RocksDB/StorageReplicatedRocksDB.h>
#include <Storages/Consensus/ChangeDataCapture.h>
#include <Storages/Consensus/RocksDBChangeData.h>

#include <Columns/ColumnString.h>
#include <Core/SortDescription.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/db_ttl.h>
#include <Common/SipHash.h>
#include <Common/getRandomASCIIString.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Common/threadPoolCallbackRunner.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

static const IColumn::Permutation & getAscendingPermutation(const IColumn & column, IColumn::Permutation & perm)
{
    column.getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Stable, 0, 1, perm);
    return perm;
}

/// Build SST file from key-value pairs
static rocksdb::Status buildSSTFile(const String & path, MutableColumnPtr && keys, MutableColumnPtr && values, const std::optional<IColumn::Permutation> & perm_ = {})
{
    /// rocksdb::SstFileWriter requires keys to be sorted in ascending order
    auto logger = &Poco::Logger::get("ReplicatedRocksDBBulkSink::buildSSTFile");
    LOG_DEBUG(logger, "Create sst file {}, size {}", path, values->size());
    IColumn::Permutation calculated_perm;
    const IColumn::Permutation & perm = perm_ ? *perm_ : getAscendingPermutation(*keys, calculated_perm);

    LOG_DEBUG(logger, "Finish sort file {}", path);

    rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions{}, rocksdb::Options{});
    auto status = sst_file_writer.Open(path);
    if (!status.ok())
    {
        LOG_DEBUG(logger, "sst_file_writer {} status {}", path, status.ToString());
        return status;
    }


    auto rows = perm.size();
    for (size_t idx = 0; idx < rows;)
    {
        /// We will write the last row of the same key
        size_t next_idx = idx + 1;
        while (next_idx < rows && keys->compareAt(perm[idx], perm[next_idx], *keys, 1) == 0)
            ++next_idx;

        auto row = perm[next_idx - 1];
        status = sst_file_writer.Put(keys->getDataAt(row).toView(), values->getDataAt(row).toView());
        if (unlikely(!status.ok()))
            return status;

        idx = next_idx;
    }

    LOG_DEBUG(logger, "Finish write file {}", path);

    return sst_file_writer.Finish();
}

ReplicatedRocksDBBulkSink::ReplicatedRocksDBBulkSink(
    ContextPtr context_, StorageReplicatedRocksDB & storage_, const StorageMetadataPtr & metadata_snapshot_, Consensus::RaftOpNum op_num_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
, WithContext(context_)
, storage(storage_)
, metadata_snapshot(metadata_snapshot_)
, op_num(op_num_)
, log(&Poco::Logger::get("ReplicatedRocksDBBulkSink"))
{
    for (const auto & elem : getHeader())
    {
        if (elem.name == storage.primary_key)
            break;
        ++primary_key_pos;
    }

    serializations = getHeader().getSerializations();
    min_block_size_rows = std::max(storage.getSettings().bulk_insert_block_size, getContext()->getSettingsRef().min_insert_block_size_rows);

    rocksdb_bulk_insertions_threadpool = getContext()->getSettingsRef().rocksdb_bulk_insertions_parallel ? & getContext()->getRocksDBBulkInsertionsThreadpool() : nullptr;

    /// If max_insert_threads > 1 we may have multiple ReplicatedRocksDBBulkSink and getContext()->getCurrentQueryId() is not guarantee to
    /// to have a distinct path. Also we cannot use query id as directory name here, because it could be defined by user and not suitable
    /// for directory name
    auto base_directory_name = TMP_INSERT_PREFIX + sipHash128String(getContext()->getCurrentQueryId());
    insert_directory_queue = fs::path(storage.getDataPaths()[0]) / (base_directory_name + "-" + getRandomASCIIString(8));
    LOG_DEBUG(log, "Create directory {} for tmp_file", insert_directory_queue);
    fs::create_directory(insert_directory_queue);
}

ReplicatedRocksDBBulkSink::~ReplicatedRocksDBBulkSink()
{
    LOG_DEBUG(log, "Destruction directory {} for tmp_file", insert_directory_queue);
    try
    {
        if (!futures.empty())
        {
            LOG_DEBUG(log, "There are still {} async write task, it's a bug", futures.size());
            for (auto & future : futures)
                future.wait();
            /// Make sure there is no exception.
            for (auto & future : futures)
                future.get();
        }

        if (fs::exists(insert_directory_queue))
            fs::remove_all(insert_directory_queue);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Error while removing temporary directory {}:", insert_directory_queue));
    }
}

std::vector<Chunk> ReplicatedRocksDBBulkSink::squash(Chunk chunk)
{
    /// End of input stream
    if (chunk.getNumRows() == 0)
    {
        return std::move(chunks);
    }

    /// Just read block is already enough.
    if (isEnoughSize(chunk))
    {
        /// If no accumulated data, return just read block.
        if (chunks.empty())
        {
            chunks.emplace_back(std::move(chunk));
            return {};
        }

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        std::vector<Chunk> to_return;
        std::swap(to_return, chunks);
        chunks.emplace_back(std::move(chunk));
        return to_return;
    }

    /// Accumulated block is already enough.
    if (isEnoughSize(chunks))
    {
        /// Return accumulated data and place new block to accumulated data.
        std::vector<Chunk> to_return;
        std::swap(to_return, chunks);
        chunks.emplace_back(std::move(chunk));
        return to_return;
    }

    chunks.emplace_back(std::move(chunk));
    if (isEnoughSize(chunks))
    {
        std::vector<Chunk> to_return;
        std::swap(to_return, chunks);
        return to_return;
    }

    /// Squashed block is not ready.
    return {};
}

std::pair<MutableColumnPtr, MutableColumnPtr> ReplicatedRocksDBBulkSink::serializeChunks(const std::vector<Chunk> & input_chunks) const
{
    auto serialized_key_column = ColumnString::create();
    auto serialized_value_column = ColumnString::create();

    {
        auto & serialized_key_data = serialized_key_column->getChars();
        auto & serialized_key_offsets = serialized_key_column->getOffsets();
        auto & serialized_value_data = serialized_value_column->getChars();
        auto & serialized_value_offsets = serialized_value_column->getOffsets();
        WriteBufferFromVector<ColumnString::Chars> writer_key(serialized_key_data);
        WriteBufferFromVector<ColumnString::Chars> writer_value(serialized_value_data);

        auto format_settings = FormatSettings{};

        for (const auto & chunk : input_chunks)
        {
            const auto & columns = chunk.getColumns();
            auto rows = chunk.getNumRows();
            for (size_t i = 0; i < rows; ++i)
            {
                serializations[primary_key_pos]->serializeBinary(*columns[primary_key_pos], i,writer_key, format_settings);
                writeChar('\0', writer_key);
                serialized_key_offsets.emplace_back(writer_key.count());
            }

            for (size_t i = 0; i < rows; ++i)
            {
                for (size_t idx = 0; idx < columns.size(); ++idx)
                {
                    if (idx == primary_key_pos)
                    {
                        continue;
                    }
                    serializations[idx]->serializeBinary(*columns[idx], i, writer_value, format_settings);
                }
                /// String in ColumnString must be null-terminated
                writeChar('\0', writer_value);
                serialized_value_offsets.emplace_back(writer_value.count());
            }
        }

        writer_key.finalize();
        writer_value.finalize();
    }

    return {std::move(serialized_key_column), std::move(serialized_value_column)};
}

void ReplicatedRocksDBBulkSink::consume(Chunk chunk)
{
    LOG_DEBUG(log, "Sink consume to cdc");

    Consensus::SettingsPtr settings;
    if (cdc == nullptr)
    {
        auto dispatcher = getContext()->getRaftDispatcher();
        cdc = std::make_shared<ChangeDataCapture>(dispatcher);

        if (dispatcher)
            settings = dispatcher->getSettings();
    }
    else
    {
        settings = cdc->getSettings();
    }

    // Just use default Consensus::settings
    if (!settings)
        settings = std::make_shared<Consensus::Settings>();

    auto storage_id = storage.getStorageID();
    ChangeDataPtr change_data = std::make_shared<RocksDBChangeData>(settings);
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

    try
    {
        cdc->sink(change_data);
    }
    catch(Exception e)
    {
        LOG_ERROR(log, "Replicated rocksdb sink data to CDC, throw exception {}", e.displayText());
        throw;
    }
}

void ReplicatedRocksDBBulkSink::localConsume(Chunk chunk_)
{
    std::vector<Chunk> to_written = squash(std::move(chunk_));

    if (to_written.empty())
        return;

    LOG_DEBUG(log, "Begin serialize");
    auto [serialized_key_column, serialized_value_column] = serializeChunks(to_written);
    LOG_DEBUG(log, "Finish serialize");

    auto sst_file_path = getTemporarySSTFilePath();
    if (rocksdb_bulk_insertions_threadpool)
    {
        LOG_DEBUG(log, "Send task to futures, current size {}", futures.size());
        futures.emplace_back(writeAsync(std::move(serialized_key_column), std::move(serialized_value_column), sst_file_path));
        return;
    }

    writeSync(std::move(serialized_key_column), std::move(serialized_value_column), sst_file_path);
    ingestExternalFile(sst_file_path);
}

void ReplicatedRocksDBBulkSink::writeSync(MutableColumnPtr && serialized_key_column, MutableColumnPtr && serialized_value_column, const String & sst_file_path)
{
    if (auto status = buildSSTFile(sst_file_path, std::move(serialized_key_column), std::move(serialized_value_column)); !status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
}

void ReplicatedRocksDBBulkSink::ingestExternalFile(const String & sst_file_path)
{
    /// Ingest the SST file
    rocksdb::IngestExternalFileOptions ingest_options;
    ingest_options.move_files = true; /// The temporary file is on the same disk, so move (or hardlink) file will be faster than copy
    if (auto status = storage.rocksdb_ptr->IngestExternalFile({sst_file_path}, ingest_options); !status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());

    if (fs::exists(sst_file_path))
        fs::remove(sst_file_path);
}

std::future<void> ReplicatedRocksDBBulkSink::writeAsync(MutableColumnPtr && serialized_key_column, MutableColumnPtr && serialized_value_column, String sst_file_path)
{
    return scheduleFromThreadPool<void>(
        [this, serialized_key_column_ = std::move(serialized_key_column), serialized_value_column_ = std::move(serialized_value_column), sst_file_path]() mutable
        {
            return writeSync(std::move(serialized_key_column_), std::move(serialized_value_column_), sst_file_path);
        },
        *rocksdb_bulk_insertions_threadpool,
        "LoadMarksThread");
}


void ReplicatedRocksDBBulkSink::onFinish()
{
    if (!chunks.empty())
        localConsume({});

    LOG_DEBUG(log, "Finish directory {} for tmp_file, futures size {}", insert_directory_queue, futures.size());

    if (futures.empty())
        return;

    for (auto & future : futures)
        future.wait();
    /// Make sure there is no exception.
    for (auto & future : futures)
        future.get();

    futures.clear();

    LOG_DEBUG(log, "Ingest external file to rocksdb, size {}", file_counter);

    for (size_t i = 0; i < file_counter; ++i)
    {
        ingestExternalFile(fs::path(insert_directory_queue) / (toString(i) + ".sst"));
    }
}

String ReplicatedRocksDBBulkSink::getTemporarySSTFilePath()
{
    return fs::path(insert_directory_queue) / (toString(file_counter++) + ".sst");
}

bool ReplicatedRocksDBBulkSink::isEnoughSize(const std::vector<Chunk> & input_chunks) const
{
    size_t total_rows = 0;
    for (const auto & chunk : input_chunks)
        total_rows += chunk.getNumRows();
    return total_rows >= min_block_size_rows;
}

bool ReplicatedRocksDBBulkSink::isEnoughSize(const Chunk & chunk) const
{
    return chunk.getNumRows() >= min_block_size_rows;
}
}
