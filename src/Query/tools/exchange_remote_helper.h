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
    }
};

class Timer
{
public:
    time_point<high_resolution_clock> start_time;
    Timer()
    {
        start_time = std::chrono::high_resolution_clock::now();
    }
    uint64_t end()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
    }
    uint64_t endMicro()
    {
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
        start_time = std::chrono::high_resolution_clock::now();
        return duration;
    }
};

class PerformanceData
{
public:
    int thread_num;
    int one_query_bytes;
    std::atomic<uint64_t> query_count{0};
    std::atomic<uint64_t> chunk_count{0};
    std::atomic<uint64_t> total_bytes{0};
    std::atomic<uint64_t> min_mic{3600000}; //Max 1 hour
    std::atomic<uint64_t> max_mic{0};
    std::atomic<uint64_t> total_init_mic{0};
    std::atomic<uint64_t> total_register_mic{0};
    std::atomic<uint64_t> total_executor_mic{0};
    std::atomic<uint64_t> total_receive_mic{0};
    std::atomic<uint64_t> total_query_mic{0};
    std::atomic<uint64_t> total_ms{0};
    std::mutex queue_mtx;
    std::priority_queue<uint64_t> queue;
    void setTotalDuration(uint64_t total_duration);
    void addQueryDuration(uint64_t duration, uint64_t init_mic, uint64_t register_mic, uint64_t executor_mic, uint64_t receive_mic);
    void addQueryStepDuration(std::vector<uint64_t> & duration_vec);
    void addChunkCount(uint64_t one_chunk_bytes);
    void output();
};

struct ClientParam
{
    int thread_num;                             // Thread/Query number of client
    int query_num;                              // query number of one thread
    int exchange_bytes;                         // Exchange data bytes of query
    int chunk_bytes;                            // Send byte count of one chunk, default 64KB
    int chunk_num;                              // Chunk number of one thread
    int column_num{100};                        // Column number, 100 bytes at one row
    int row_num;                                // Row number of one chunk
    std::string server{"0.0.0.0"};              // Server host for brpc service
    std::string port{"9105"};                   // Server port for brpc service
    void compute();
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
    uint64_t send_query_id{0};
    uint64_t receive_query_id{0};
    void sender_thread(int trd_idx, uint64_t query_id);
    void receiver_thread(int trd_idx, uint64_t query_id);
};

}
