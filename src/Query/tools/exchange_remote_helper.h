#pragma once
#include <chrono>
#include <brpc/server.h>
#include <Poco/Util/MapConfiguration.h>
#include <Query/Exchange/bRPC/BrpcApplication.h>
#include <Query/Exchange/bRPC/BrpcExchangeReceiverRegistryService.h>

namespace Tools
{

using namespace std;
using namespace std::chrono;

// static int SERVER_PORT = 9105;
// static std::string SERVER_HOST = "0.0.0.0:9105";

struct ContextHolder
{
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr context;

    ContextHolder()
        : shared_context(DB::Context::createShared())
        , context(DB::Context::createGlobal(shared_context.get()))
    {
        context->makeGlobalContext();
        context->setPath("./");
    }

    ContextHolder(ContextHolder &&) = default;

    void destroy()
    {
        context->shutdown();
        context.reset();
        shared_context.reset();
    }
};

const ContextHolder & getContext();

ContextHolder & getMutableContext();

class ExchangeRemoteServer
{
public:
    static brpc::Server * server;
    static DB::BrpcExchangeReceiverRegistryService * service_impl;
    static void startBrpcServer(int port);
    static void tearDown()
    {
        server->RunUntilAskedToQuit();
        //server->Stop(1000);
    }
};

// QPS: send_count / (end_time - start_time)
// Throughput: send_bytes / (end_time - start_time)
// Latency: (send_time - start_time) / (send_count / thread_num)
class PerformanceData
{
public:
    int thread_num;
    std::atomic<uint64_t> query_count;
    std::atomic<uint64_t> chunk_count;
    std::atomic<uint64_t> chunk_bytes;
    time_point<high_resolution_clock> start_time;
    time_point<high_resolution_clock> end_time;
    void start();
    void end();
    void addQueryCount();
    void addChunkCount(int & one_chunk_bytes);
    uint32_t duration_ms();
    void output();
};

struct ClientParam
{
    int thread_num;                             // Thread/Query number of client
    int query_num;                              // query number of one thread
    int chunk_num;                              // Chunk number of one thread
    int chunk_bytes{DB::DEFAULT_BLOCK_SIZE};    // Send byte count of one chunk
    int column_num{100};                        // Column number
    int row_num;                                // Row number of one chunk
    std::string server{"0.0.0.0"};              // Server host for brpc service
    std::string port{"9105"};                   // Server port for brpc service
    void output();
};

enum RunMode
{
    ALL,
    SERVER,
    CLIENT
};

class ExchangeRemoteClient
{
public:
    ExchangeRemoteClient(ClientParam & param_, const char* log_level);
    void run(RunMode mode);
    PerformanceData data;
    static bool startServer(RunMode mode)
    {
        return (mode == RunMode::ALL || mode == RunMode::SERVER);
    }
    static bool startClient(RunMode mode)
    {
        return mode == RunMode::ALL || mode == RunMode::CLIENT;
    }
private:
    ClientParam param;
    //std::shared_ptr<DB::ExchangeDataKey> data_key;
    uint64_t send_query_id{0};
    uint64_t receive_query_id{0};
    void sender_thread(int trd_idx, uint64_t query_id);
    void receiver_thread(int trd_idx, uint64_t query_id);
};

}
