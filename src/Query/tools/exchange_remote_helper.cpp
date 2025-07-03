#include "exchange_remote_helper.h"
#include <Poco/ConsoleChannel.h>
#include <Poco/Random.h>
#include <base/sleep.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Common/MultiPathBoundedQueue.h>
#include <Query/Exchange/bRPC/BrpcRemoteBroadcastReceiver.h>
#include <Query/Processors/Exchange/ExchangeSourceExt.h>

namespace Tools
{
using namespace DB;

const ContextHolder & getContext()
{
    return getMutableContext();
}

ContextHolder & getMutableContext()
{
    static ContextHolder holder;
    return holder;
}

static bool is_init = false;
ContextMutablePtr getInitContext()
{
    auto context = getContext().context;
    if (!is_init)
    {
        context->setConfig(Poco::AutoPtr(new Poco::Util::MapConfiguration()));
        context->initializeOptimizerContext();
        context->setTemporaryStoragePath("./tmp/", 1024);
        is_init = true;
    }
    return context;
}

brpc::Server * ExchangeRemoteServer::server = new brpc::Server();
BrpcExchangeReceiverRegistryService * ExchangeRemoteServer::service_impl = new BrpcExchangeReceiverRegistryService(73400320);

void ExchangeRemoteServer::startBrpcServer(int port)
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
    DB::BrpcApplication::getInstance().initialize(*map_config);

    if (server->AddService(service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR) << "Fail to add service";
        return;
    }
    LOG(INFO) << "add service success";
    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    if (server->Start(port, &options) != 0)
    {
        LOG(ERROR) << "Fail to start Server";
        return;
    }
    LOG(INFO) << "start Server";

    auto context = getInitContext();
    DB::SettingsChanges config_settings;
    config_settings.emplace_back("exchange_timeout_ms", 30000);
    config_settings.emplace_back("temporary_files_codec", "NONE");
    context->applySettingsChanges(config_settings);
    service_impl->setContext(context);
}

Block getHeader(size_t column_num)
{
    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < column_num; i++)
    {
        columns.push_back(ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "column" + std::to_string(i)));
    }
    Block header = {columns};
    return header;
}

Chunk createUInt8Chunk(size_t row_num, size_t column_num)
{
    Poco::Random random;
    Columns columns;
    for (size_t i = 0; i < column_num; i++)
    {
        auto col = ColumnUInt8::create(row_num, UInt8(random.nextChar()));
        columns.emplace_back(std::move(col));
    }
    return Chunk(std::move(columns), row_num);
}

void setQueryDuration(ContextMutablePtr context)
{
    auto & client_info = context->getClientInfo();
    const auto current_time = std::chrono::system_clock::now();
    client_info.initial_query_start_time = timeInSeconds(current_time);
    client_info.initial_query_start_time_microseconds = timeInMicroseconds(current_time);
    context->getOptimizerContext()->initQueryExpirationTimeStamp();
}

void PerformanceData::start()
{
    start_time = std::chrono::high_resolution_clock::now();
    end();
}

void PerformanceData::end()
{
    end_time = std::chrono::high_resolution_clock::now();
}

void PerformanceData::addQueryCount()
{
    query_count ++;
}

void PerformanceData::addChunkCount(int & one_chunk_bytes)
{
    chunk_count ++;
    chunk_bytes += one_chunk_bytes;
}

uint32_t PerformanceData::duration_ms()
{
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    return duration.count();
}

void PerformanceData::output()
{
    auto ms = duration_ms();
    LOG(INFO) << "******************************\n"
        << "\tTotal query count : " << query_count << "\n"
        << "\tParallel thread count : " << thread_num << "\n"
        << "\tTotal chunk count : " << chunk_count << "\n"
        << "\tTotal chunk bytes(MB) :" << chunk_bytes / 1024 / 1024 << "\n"
        << "\tTotal duration ms : " << ms << "\n"
        << "\tQuery/second : " <<  query_count * 1000 / ms << "\n"
        << "\tChunk/second : " <<  chunk_count * 1000 / ms << "\n"
        << "\tThroughput(MB/S) : " << chunk_bytes * 1000 / 1024 / 1024 / ms << "\n";
}

void initLogger(const String & level = "trace")
{
    if (!Poco::Logger::root().getChannel())
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel());
        Poco::Logger::root().setChannel(channel);
    }
    Poco::Logger::root().setLevel(level);
}

void ClientParam::output()
{
    LOG(INFO) << "Run params : \n"
        << "thread_num : " << thread_num << "\n"
        << "query_num : " << query_num << "\n"
        << "chunk_num : " << chunk_num << "\n"
        << "chunk_bytes : " << chunk_bytes << "\n"
        << "column_num : " << column_num << "\n"
        << "row_num : " << row_num << "\n";
}

ExchangeRemoteClient::ExchangeRemoteClient(ClientParam & param_, const char* log_level) : param(param_)
{
    data.thread_num = param.thread_num;
    param.row_num = param.chunk_bytes / param.column_num;
    param.output();
    initLogger(log_level);
}

// 100, 100B
// 1000, 1MB
// 10000, 10MB
// 50000, 50MB
// 100000, 100MB
// 500000, 500MB
// 1000000, 1GB
void ExchangeRemoteClient::run(RunMode mode)
{
    auto context = getInitContext();
    setQueryDuration(context);

    auto batch_num = param.query_num / param.thread_num;

    if (startServer(mode))
    {
        ExchangeRemoteServer::startBrpcServer(atoi(param.port.c_str()));
        for (int batch_idx = 0; batch_idx < batch_num; batch_idx ++ )
        {
            std::vector<std::thread> thread_senders;
            LOG(INFO) << "Begin server, batch indx " << batch_idx;
            if (mode == RunMode::ALL || mode == RunMode::SERVER)
            {
                for (int trd_idx = 0; trd_idx < param.thread_num; trd_idx++)
                {
                    send_query_id ++;
                    LOG(INFO) << "Start send, batch index " << batch_idx << ", thread: " << trd_idx << ", query_id:" << send_query_id;
                    std::thread thread_sender(&ExchangeRemoteClient::sender_thread, this, trd_idx, send_query_id);
                    thread_senders.push_back(std::move(thread_sender));
                }
            }
            LOG(INFO) << "Wait join thread, batch index " << batch_idx;
            for (int i = 0; i < param.thread_num; i++)
            {
                thread_senders[i].join();
            }
        }
        LOG(INFO) << "Begin down rpc server";
        ExchangeRemoteServer::tearDown();
        LOG(INFO) << "Finish down rpc server";
    }

    if (startClient(mode))
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
        DB::BrpcApplication::getInstance().initialize(*map_config);
        data.start();
        for (int batch_idx = 0; batch_idx < batch_num; batch_idx ++ )
        {
            std::vector<std::thread> thread_receivers;
            LOG(INFO) << "Begin receive client, batch index " << batch_idx;
            if (mode == RunMode::ALL || mode == RunMode::CLIENT)
            {
                for (int trd_idx = 0; trd_idx < param.thread_num; trd_idx++)
                {
                    receive_query_id ++;
                    LOG(INFO) << "Start receive, batch index " << batch_idx << ", thread: " << trd_idx << ", query id:" << receive_query_id;
                    std::thread thread_receive(&ExchangeRemoteClient::receiver_thread, this, trd_idx, receive_query_id);
                    thread_receivers.push_back(std::move(thread_receive));
                }
            }
            LOG(INFO) << "Wait join thread, batch index " << batch_idx;
            for (int i = 0; i < param.thread_num; i++)
            {
                thread_receivers[i].join();
            }
        }
        data.end();
        data.output();
    }
}


void ExchangeRemoteClient::sender_thread(int trd_idx, uint64_t query_id)
{
    auto context = getInitContext();
    auto header = getHeader(param.column_num);
    Chunk chunk = createUInt8Chunk(param.row_num, param.column_num); // 100 Byte
    auto data_key = std::make_shared<ExchangeDataKey>(query_id, query_id, 1);
    auto sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    sender->accept(context, header);
    setQueryDuration(context);
    for (int i = 0; i < param.chunk_num; i ++)
    {
        auto clone = chunk.clone();
        BroadcastStatus status = sender->send(std::move(clone));
        // LOG(INFO) << "Finish send chunk, thread " << trd_idx << " query id " << query_id << ", chunk " << i << ", size " << chunk.bytes();
    }
    LOG(INFO) << "Finish send data, thread " << trd_idx << ", query id " << query_id;
    sleepForMilliseconds(100); // Wait receiver.
}

void ExchangeRemoteClient::receiver_thread(int trd_idx, uint64_t query_id)
{
    int time_out_ms = 10000; // 10s
    auto context = getInitContext();
    auto tp = getDeltaTimePoint(time_out_ms);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp};
    auto header = getHeader(param.column_num);
    auto data_key = std::make_shared<ExchangeDataKey>(query_id, query_id, 1);
    auto queue = std::make_shared<MultiPathBoundedQueue>(context->getOptimizerContext()->getSettingsRef().exchange_remote_receiver_queue_size, nullptr);
    BrpcRemoteBroadcastReceiverShardPtr receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        data_key,
        param.server + ":" + param.port,
        context,
        header,
        true,
        BrpcRemoteBroadcastReceiver::generateNameForTest(),
        std::move(queue));
    receiver->registerToSenders(time_out_ms);

    auto exchange_source = std::make_shared<ExchangeSourceExt>(std::move(header), receiver, exchange_options);
    Pipe pipe;
    pipe.addSource(exchange_source);
    QueryPipeline pipeline(std::move(pipe));

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    for (int i = 0; i < param.chunk_num; i ++)
    {
        // LOG(INFO) << "Begin pull chunk, thread index " << trd_idx << ", query id " << query_id << ", chunk index " << i;
        executor.pull(pull_chunk);
        // LOG(INFO) << "Finish pull chunk, thread index " << trd_idx << ", query id " << query_id << ", chunk index " << i;
        data.addChunkCount(param.chunk_bytes);
    }
    LOG(INFO) << "Finish query data, thread index " << trd_idx << ", query id " << query_id;
    data.addQueryCount();
    LOG(INFO) << "Client is going to quit";
}

}
