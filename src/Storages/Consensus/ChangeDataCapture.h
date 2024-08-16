#pragma once

#include <Poco/DateTime.h>
#include <Common/logger_useful.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/Consensus/RaftDispatcher.h>
#include <Storages/Consensus/ChangeData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

class ChangeDataCapture;
using ChangeDataCapturePtr = std::shared_ptr<ChangeDataCapture>;

struct CreateQuery;

struct ChangeDataVector
{
    int64_t start_time {0};
    std::vector<ChangeDataPtr> change_datas;
};

using ChangeDataVectorPtr = std::shared_ptr<ChangeDataVector>;

struct ChangeDataBatchQueue
{
    std::mutex queue_mutex;
    ChangeDataVectorPtr current_vector_ptr;
    ConcurrentBoundedQueue<ChangeDataVectorPtr> changedata_queue;
    int consume_batch_size;
    int consume_queue_size;
    int consume_waittime_ms;

    ChangeDataBatchQueue() : changedata_queue(0) { }

    ChangeDataBatchQueue(int consume_batch_size_, int consume_queue_size_, int consume_waittime_ms_)
        : changedata_queue(consume_queue_size_)
        , consume_batch_size(consume_batch_size_)
        , consume_queue_size(consume_queue_size_)
        , consume_waittime_ms(consume_waittime_ms_)
    {
    }

    //move current vector to queue when push or pop
    void checkMove()
    {
        if (current_vector_ptr != nullptr &&
            (current_vector_ptr->change_datas.size() >= static_cast<size_t>(consume_batch_size)
                || Poco::DateTime().timestamp().epochMicroseconds() / 1000 - current_vector_ptr->start_time >= consume_waittime_ms))
        {
            LOG_DEBUG(&Poco::Logger::get("ChangeDataBatchQueue"),
                "Move current change data to queue, size {} time {}",
                current_vector_ptr->change_datas.size(),
                Poco::DateTime().timestamp().epochMicroseconds() / 1000 - current_vector_ptr->start_time);

            if (changedata_queue.push(std::move(current_vector_ptr)))
                current_vector_ptr = nullptr;
            else
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Push change data to queue error.");
        }
    }

    void push(ChangeDataPtr data)
    {
        {
            std::lock_guard lock(queue_mutex);
            checkMove();
            if (current_vector_ptr == nullptr)
            {
                current_vector_ptr = std::make_shared<ChangeDataVector>();
                current_vector_ptr->start_time = Poco::DateTime().timestamp().epochMicroseconds() / 1000;
            }
        }
        current_vector_ptr->change_datas.push_back(data);
    }

    bool tryPop(ChangeDataVectorPtr & datas, UInt64 milliseconds)
    {
        {
            std::lock_guard lock(queue_mutex);
            checkMove();
        }
        return changedata_queue.tryPop(datas, milliseconds);
    }
};

using ChangeDataQueue = ConcurrentBoundedQueue<ChangeDataPtr>;

class ChangeDataCapture 
{
public:
    ChangeDataCapture(RaftDispatcherPtr dispatcher_);
    ChangeDataCapture(const SettingsPtr & settings_);
    void init();
    void exit();
    //Sink storages change data to raft framework
    void sink(ChangeDataPtr & data);
    void createMeta(std::map<std::string, CreateQuery> & queries);
    //Consum data from raft framework
    void consume(ChangeDataPtr & data);
private:
    void consumeThread();
    void batchThread();
    void localConsume(StoragePtr target_table, ChangeDataPtr & data);
    bool tryExecuteQuery(std::string name, std::string query);
    bool is_exit {false};
    RaftDispatcherPtr dispatcher;
    Poco::Logger* log;
    Poco::Event sink_event;
    RaftResponsePtr sink_res{nullptr};

    SettingsPtr settings;
    Poco::Event consume_event;
    ChangeDataQueue consume_queue;
    std::unique_ptr<ThreadFromGlobalPool> consume_thread;
    ChangeDataBatchQueue batch_queue;
    ThreadPoolPtr batch_thread;
    int32_t pop_wait_ms;
    int32_t consume_wait_ms;
};

}
