#pragma once
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DISTRIBUTE_STAGE_QUERY_EXCEPTION;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

template <typename KeyType, typename ElementType>
class ConcurrentShardElement
{
private:
    std::mutex mutex;
    std::unordered_map<KeyType, ElementType> map_data;
    std::unordered_map<KeyType, std::shared_ptr<std::condition_variable>> cvs;
    LoggerPtr log;

public:
    ConcurrentShardElement() { log = getLogger("ConcurrentShardElement"); }

    bool empty()
    {
        std::unique_lock<std::mutex> lock(mutex);
        return map_data.empty();
    }

    size_t size()
    {
        std::unique_lock<std::mutex> lock(mutex);
        return map_data.size();
    }

    bool exist(const KeyType & key)
    {
        std::unique_lock<std::mutex> lock(mutex);
        return map_data.find(key) != map_data.end();
    }

    void put(const KeyType & key, ElementType value)
    {
        std::unique_lock<std::mutex> lock(mutex);
        map_data.emplace(key, value);
        if (cvs.count(key))
            cvs[key]->notify_all();
    }

    bool putIfNotExists(const KeyType & key, ElementType value)
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (map_data.find(key) == map_data.end())
        {
            map_data.emplace(key, value);
            if (cvs.count(key))
                cvs[key]->notify_all();
            return true;
        }
        return false;
    }

    ElementType & get(const KeyType & key) { get(key, 0); }

    ElementType & get(const KeyType & key, size_t timeout_ms)
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (timeout_ms != 0 )
        {
            size_t try_count = 0;
            size_t max_num = std::max(size_t(3), timeout_ms / 10);
            if (cvs.find(key) == cvs.end())
                cvs.emplace(key, std::make_shared<std::condition_variable>());

            while (map_data.find(key) == map_data.end() && try_count <= max_num)
            {
                // 10 ms
                if (auto cv = cvs[key]; ETIMEDOUT == cv->wait_for(lock, 10000))
                    try_count++;
                else
                    break;
            }

            return map_data[key];
        }
        else
        {
            return map_data[key];
        }
    }

    bool remove(const KeyType & key)
    {
        std::unique_lock<std::mutex> lock(mutex);
        cvs.erase(key);
        auto n = map_data.erase(key);
        return n;
    }

    String keys()
    {
        std::unique_lock<std::mutex> lock(mutex);
        String ret;
        for (const auto & element : map_data)
            ret = ret + element.first + "\n";
        return ret;
    }
};
}
