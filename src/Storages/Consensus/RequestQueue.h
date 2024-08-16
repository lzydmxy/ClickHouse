#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <pcg-random/pcg_random.hpp>
#include <Common/randomSeed.h>
#include <Storages/Consensus/RaftCommon.h>


namespace DB
{

using namespace Consensus;

struct RequestQueue
{
    using RaftQueue = ConcurrentBoundedQueue<RaftRequestPtr>;
    std::vector<std::shared_ptr<RaftQueue>> queues;

    explicit RequestQueue(size_t child_queue_size, size_t capacity = 20000)
    {
        assert(child_queue_size > 0);
        assert(capacity > 0);
        queues.resize(child_queue_size);
        for (size_t i = 0; i < child_queue_size; i++)
            queues[i] = std::make_shared<RaftQueue>(std::max(1ul, capacity / child_queue_size));
    }

    bool push(const RaftRequestPtr & request)
    {
        pcg64 rng(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, queues.size() - 1);
        const auto index = distribution(rng);
        return queues[index]->push(std::forward<const RaftRequestPtr>(request));
    }

    bool tryPush(const RaftRequestPtr & request, UInt64 wait_ms = 0)
    {
        pcg64 rng(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, queues.size() - 1);
        const auto index = distribution(rng);
        return queues[index]->tryPush(std::forward<const RaftRequestPtr>(request), wait_ms);
    }

    bool pop(size_t queue_id, RaftRequestPtr & request)
    {
        assert(queue_id != 0 && queue_id <= queues.size());
        return queues[queue_id]->pop(request);
    }

    bool tryPop(size_t queue_id, RaftRequestPtr & request, UInt64 wait_ms = 0)
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->tryPop(request, wait_ms);
    }

    bool tryPopAny(RaftRequestPtr & request, UInt64 wait_ms = 0)
    {
        for (const auto & queue : queues)
        {
            if (queue->tryPop(request, wait_ms))
                return true;
        }
        return false;
    }

    size_t size() const
    {
        size_t size{};
        for (const auto & queue : queues)
            size += queue->size();
        return size;
    }

    size_t size(size_t queue_id) const
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->size();
    }

    bool empty() const
    {
        return size() == 0;
    }

    bool finish()
    {
        bool rtn = true;
        for(auto & queue : queues)
        {
            rtn = rtn & queue->finish();
        }
        return rtn;
    }
};

using RequestQueuePtr = std::shared_ptr<RequestQueue>;

}
