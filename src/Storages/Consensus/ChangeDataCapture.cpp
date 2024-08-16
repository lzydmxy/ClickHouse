#include <Poco/Event.h>
#include <Common/CurrentMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/DatabaseCatalog.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Consensus/ChangeDataCapture.h>
#include <Storages/Consensus/RocksDBChangeData.h>
#include <Storages/Consensus/MergeTreeChangeData.h>
#include <Storages/RocksDB/StorageReplicatedRocksDB.h>
#include <Storages/RocksDB/ReplicatedRocksDBSink.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/IStorage.h>
#include <Storages/Consensus/RaftStorage.h>

namespace CurrentMetrics
{
    extern const Metric RaftThreads;
    extern const Metric RaftThreadsActive;
    extern const Metric RaftThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

ChangeDataCapture::ChangeDataCapture(RaftDispatcherPtr dispatcher_)
    : dispatcher(dispatcher_)
    , log(&Poco::Logger::get("ChangeDataCapture"))
    , consume_queue(0)
{
}

ChangeDataCapture::ChangeDataCapture(const SettingsPtr & settings_)
    : dispatcher(nullptr), log(&Poco::Logger::get("ChangeDataCapture")), settings(settings_)
    , consume_queue(settings_->consume_batch_size * settings_->consume_queue_size)
    , batch_queue(settings_->consume_batch_size, settings_->consume_queue_size, settings_->consume_waittime_ms)
    , pop_wait_ms(settings->raft_settings->election_timeout_lower_bound_ms)
    , consume_wait_ms(settings->raft_settings->operation_timeout_ms)
{
}

void ChangeDataCapture::init()
{
    consume_thread = std::make_unique<ThreadFromGlobalPool>([this] { consumeThread(); });
    batch_thread = std::make_shared<ThreadPool>(CurrentMetrics::RaftThreads,
        CurrentMetrics::RaftThreadsActive, CurrentMetrics::RaftThreadsScheduled,
        settings->request_thread_count);
    for (int i = 0; i < settings->request_thread_count; i++)
    {
        batch_thread->trySchedule([this] { batchThread(); });
    }
}

void ChangeDataCapture::exit()
{
    LOG_INFO(log, "Shutting down change data capture");
    if (is_exit)
        return;
    is_exit = true;
    if (consume_thread && consume_thread->joinable())
        consume_thread->join();
    if (batch_thread)
        batch_thread->wait();
    LOG_INFO(log, "Shut down change data capture");
}

void ChangeDataCapture::sink(ChangeDataPtr & data)
{
    if (!dispatcher)
    {
        LOG_WARNING(log, "Dispatcher is null");
        return;
    }

    LOG_TRACE(log, "CDC sink to nuraft header {}", data->dumpHeader());

    auto request = RaftRequestFactory::instance().get(data->op_num);
    request->generateID();
    switch (data->op_num)
    {
        case RaftOpNum::Insert:
        {
            dynamic_cast<RaftInsertRequest*>(request.get())->data = data;
            break;
        }
        case RaftOpNum::Delete:
        {
            dynamic_cast<RaftDeleteRequest*>(request.get())->data = data;
            break;
        }
        case RaftOpNum::Update:
        {
            dynamic_cast<RaftUpdateRequest*>(request.get())->data = data;
            break;
        }
        default: {}
    }

    sink_event.reset();
    sink_res = nullptr;
    request->finish_callback = [this] (const RaftResponsePtr & response)
    {
        sink_res = response;
        sink_event.set();
    };

    dispatcher->putRequest(request);

    //sync response
    Stopwatch time_waiting;
    bool return_reponse = false;
    time_waiting.start();
    while (time_waiting.elapsedMilliseconds() < static_cast<UInt64>(dispatcher->max_wait_ms()))
    {
        try
        {
            if (sink_event.tryWait(std::chrono::milliseconds(1000).count()))
            {
                return_reponse = true;
                if (sink_res->error != Error::ZOK)
                    throw RaftException("CDC Sink data to raft error", sink_res->error);
                else
                {
                    LOG_TRACE(log, "CDC return response {}", sink_res->toString());
                    break;
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            throw;
        }
    }
    if (!return_reponse)
    {
        if (time_waiting.elapsedMilliseconds() >= static_cast<UInt64>(dispatcher->max_wait_ms()))
        {
            LOG_WARNING(log, "CDC timeout, wait {}, max wait milli second {}",
                time_waiting.elapsedMilliseconds(), dispatcher->max_wait_ms());
        }
        else
            throw RaftException("CDC Sink data to raft server error", Error::ZUNIMPLEMENTED);
    }
}

bool ChangeDataCapture::tryExecuteQuery(std::string name, std::string query)
{
    /// Add special comment at the start of query to easily identify DDL-produced queries in query_log
    String query_prefix = "/* raft_entry=" + name + " */ ";
    String query_to_execute = query_prefix + query;

    LOG_DEBUG(log, "Excute query : {}", query_to_execute);

    ReadBufferFromString istr(query_to_execute);
    String dummy_string;
    WriteBufferFromString ostr(dummy_string);
    std::optional<CurrentThread::QueryScope> query_scope;
    try
    {
        auto global_context = Context::getGlobalContextInstance()->getGlobalContext();
        ContextMutablePtr query_context = Context::createCopy(global_context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("");
        executeQuery(query_to_execute, query_context, QueryFlags{ .internal = true });
    }
    catch (const DB::Exception & e)
    {
        tryLogCurrentException(log, "Query " + query + " wasn't finished successfully");

        /// We use return value of tryExecuteQuery(...) in tryExecuteQueryOnLeaderReplica(...) to determine
        /// if replica has stopped being leader and we should retry query.
        /// However, for the majority of exceptions there is no sense to retry, because most likely we will just
        /// get the same exception again. So we return false only for several special exception codes,
        /// and consider query as executed with status "failed" and return true in other cases.
        bool no_sense_to_retry = e.code() != ErrorCodes::TABLE_IS_READ_ONLY &&
                                 e.code() != ErrorCodes::CANNOT_ASSIGN_ALTER &&
                                 e.code() != ErrorCodes::CANNOT_ALLOCATE_MEMORY &&
                                 e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED;
        return no_sense_to_retry;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Query " + query + " wasn't finished successfully");
        return false;
    }

    LOG_DEBUG(log, "Executed query: {}", query);
    return true;
}

void ChangeDataCapture::createMeta(std::map<std::string, CreateQuery> & queries)
{
    for(auto const & [name, query] : queries)
    {
        LOG_DEBUG(log, "Meta name {}, type {}, query : {}", name, query.query_type, query.query);

        //Database
        if (query.query_type == 1)
        {
            if (DatabaseCatalog::instance().tryGetDatabase(name) == nullptr)
                tryExecuteQuery(name, query.query);
        }
        //Table
        else if (query.query_type == 2)
        {
            std::string database_name = name.substr(0, name.find('.'));
            std::string table_name = name.substr(name.find('.')+1);
            auto storage_id = StorageID(database_name, table_name);
            auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, nullptr);
            if (storage == nullptr)
                tryExecuteQuery(table_name, query.query);
        }
    }
}

void ChangeDataCapture::localConsume(StoragePtr target_table, ChangeDataPtr & data)
{
    auto storage_name = target_table->getName();
    auto target_type = matchType(storage_name);
    auto global_ctx = Context::getGlobalContextInstance();
    try
    {
        switch(data->storage_type)
        {
            case StorageType::RocksDB:
            {
                if (target_type == StorageType::RocksDB)
                {
                    LOG_DEBUG(log, "Consum data to storage RocksDB, opnum {}", toString(data->op_num));
                    auto rocksdb_table = dynamic_cast<StorageReplicatedRocksDB*>(target_table.get());
                    if (data->op_num == RaftOpNum::Insert || data->op_num == RaftOpNum::Update)
                    {
                        auto sink = target_table->write(nullptr, target_table->getInMemoryMetadataPtr(), global_ctx, false);
                        auto replicated_sink = dynamic_cast<ReplicatedRocksDBSink*>(sink.get());
                        replicated_sink->localConsume(Chunk{data->block->getColumns(), data->block->rows()});
                    }
                    else if (data->op_num == RaftOpNum::Delete)
                    {
                        rocksdb_table->localMutate(data->commands, Chunk{data->block->getColumns(), data->block->rows()});
                    }
                }
                else if (target_type == StorageType::MergeTree)
                {
                    LOG_DEBUG(log, "Consum data to storage MergeTree, opnum {}, consume mode {}",
                        toString(data->op_num), ConsumeModeNS::toString(settings->consume_mode));

                    switch(settings->consume_mode)
                    {
                        case ConsumeMode::SYNC_SINGLE:
                        {
                            if (data->op_num == RaftOpNum::Insert || data->op_num == RaftOpNum::Update)
                            {
                                auto sink = target_table->write(nullptr, target_table->getInMemoryMetadataPtr(), global_ctx, false);
                                auto mergetree_sink = dynamic_cast<MergeTreeSink*>(sink.get());
                                mergetree_sink->onStart();
                                mergetree_sink->consume(Chunk{data->block->getColumns(), data->block->rows()});
                                mergetree_sink->onFinish();
                            }
                            else if (data->op_num == RaftOpNum::Delete)
                            {
                                target_table->mutate(data->commands, global_ctx);
                            }
                            break;
                        }
                        case ConsumeMode::ASYNC_BATCH:
                        case ConsumeMode::SYNC_BATCH:
                        {
                            data->target_table = target_table;
                            batch_queue.push(data);
                            break;
                        }
                    }
                }
                break;
            }
            case StorageType::MergeTree:
            {
                if (target_type == StorageType::MergeTree)
                {
                    //TODO
                }
                else if (target_type == StorageType::RocksDB)
                {
                    //TODO
                }
                break;
            }
            default:
                break;
        }
    }
    catch(...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        LOG_ERROR(log, "CDC consum data erro : {}", data->dumpHeader());
        throw;
    }
}

void ChangeDataCapture::consumeThread()
{
    while(!is_exit)
    {
        ChangeDataPtr data;
        if (consume_queue.tryPop(data, pop_wait_ms) )
        {
            StoragePtr first_table, second_table;
            //first table
            if (!data->table.empty())
            {
                auto first_storage_id = StorageID(data->database, data->table);
                first_table = DatabaseCatalog::instance().tryGetTable(first_storage_id, nullptr);
            }

            //second table
            if (!data->second_table.empty())
            {
                auto second_storage_id = StorageID(data->database, data->second_table);
                second_table = DatabaseCatalog::instance().tryGetTable(second_storage_id, nullptr);
            }

            //Support only exist one table
            if (!first_table && !second_table)
            {
                LOG_WARNING(log, "Cant find first table {}.{} and second table {}.{}", data->database, data->table,
                    data->database, data->second_table);
                continue;
            }

            if (first_table)
                localConsume(first_table, data);
            else
                LOG_DEBUG(log, "CDC first table is null");

            if (second_table)
                localConsume(second_table, data);
            else
                LOG_DEBUG(log, "CDC second table is null");

            data->callback();
        }
    }
}

void ChangeDataCapture::batchThread()
{
    while (!is_exit)
    {
        ChangeDataVectorPtr datas;
        if (batch_queue.tryPop(datas, pop_wait_ms) )
        {
            std::map<String, ChangeDataVectorPtr> table_datas;
            for (auto & data : datas->change_datas)
            {
                String key = fmt::format("{}_{}", data->database, data->table);
                auto it = table_datas.find(key);
                ChangeDataVectorPtr table_data_vector;
                if (it == table_datas.end())
                {
                    table_data_vector = std::make_shared<ChangeDataVector>();
                    table_data_vector->change_datas.push_back(data);
                    table_datas[key] = table_data_vector;
                }
                else
                    it->second->change_datas.push_back(data);
            }

            auto global_ctx = Context::getGlobalContextInstance();
            for (auto & it : table_datas)
            {
                if (it.second->change_datas.size() > 0)
                {
                    auto first = it.second->change_datas[0];
                    auto sink = first->target_table->write(nullptr, first->target_table->getInMemoryMetadataPtr(), global_ctx, false);

                    auto mergetree_sink = dynamic_cast<MergeTreeSink*>(sink.get());
                    LOG_DEBUG(log, "Consume data from {}.{} batch size {}",
                            first->database, first->table, it.second->change_datas.size());

                    mergetree_sink->onStart();
                    ChangeDataPtr prev_data(nullptr);
                    for (auto & data : it.second->change_datas)
                    {
                        if (data->op_num == RaftOpNum::Delete)
                        {
                            if (prev_data)
                            {
                                LOG_DEBUG(log, "Consume data from {}.{} rows {}", first->database, first->table, prev_data->block->rows());
                                mergetree_sink->consume(Chunk{prev_data->block->getColumns(), prev_data->block->rows()});
                                prev_data = nullptr;
                            }
                            first->target_table->mutate(data->commands, global_ctx);
                            continue;
                        }
                        if (data->op_num == RaftOpNum::Insert || data->op_num == RaftOpNum::Update)
                        {
                            if (prev_data)
                                prev_data->merge(data);
                            else
                                prev_data = data;
                        }
                    }
                    if (prev_data)
                    {
                        LOG_DEBUG(log, "Consume data from {}.{} rows {}", first->database, first->table, prev_data->block->rows());
                        mergetree_sink->consume(Chunk{prev_data->block->getColumns(), prev_data->block->rows()});
                        prev_data = nullptr;
                    }
                    mergetree_sink->onFinish();
                }
            }
        }
    }
}

void ChangeDataCapture::consume(ChangeDataPtr & data)
{
    LOG_TRACE(log, "CDC consum to nuraft header {}", data->dumpHeader());

    if (DatabaseCatalog::instance().tryGetDatabase(data->database) == nullptr)
    {
        LOG_WARNING(log, "Cant find database {}", data->database);
        return;
    }

    consume_event.reset();
    data->callback = [this] ()
    {
        consume_event.set();
    };

    if (consume_queue.push(data))
    {
        if (!consume_event.tryWait(std::chrono::milliseconds(consume_wait_ms).count()))
        {
            LOG_WARNING(log, "Consume data timeout {}", data->dumpHeader());
        }
    }
    else
        LOG_WARNING(log, "Cant push data to queue {}.", data->dumpHeader());
}

}
