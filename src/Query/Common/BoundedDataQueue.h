#pragma once
#include <queue>
#include <type_traits>
#include <mutex>
#include <condition_variable>
#include <fmt/core.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/DateLUT.h>
#include <Query/Common/QueryCommon.h>

//#include <Common/time.h>
//#include <common/MoveOrCopyIfThrow.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int STD_EXCEPTION;
}

template <typename T, typename Controller = std::nullptr_t>
class BoundedDataQueue
{
    static constexpr bool use_controller = !std::is_same_v<Controller, std::nullptr_t>;

public:
    explicit BoundedDataQueue(size_t capacity_ = 20) : full_cv(), empty_cv(), capacity(capacity_)
    {
        static_assert(!use_controller);
    }
    BoundedDataQueue(size_t capacity_, Controller memory_controller_)
        : full_cv(), empty_cv(), capacity(capacity_), memory_controller(std::move(memory_controller_))
    {
        static_assert(use_controller);
    }

    inline void push(const T & x)
    {
        pushImpl(x);
    }
    inline void push(T && x)
    {
        pushImpl(std::move(x));
    }

    inline void pop(T & x)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (queue.empty() && !is_closed)
        {
            empty_cv.wait(lock);
        }
        if (is_closed)
            throw Exception(ErrorCodes::STD_EXCEPTION, "Queue is closed");
        x = queue.front();
        queue.pop();
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->decrease(x);
        }
        lock.unlock();
        full_cv.notify_one();
    }

    inline bool tryPush(const T & x, UInt64 timeout_ms = 0)
    {
        return tryPushImpl(x, timeout_ms);
    }
    inline bool tryPush(T && x, UInt64 timeout_ms = 0)
    {
        return tryPushImpl(std::move(x), timeout_ms);
    }

    inline bool tryPop(T & x, UInt64 milliseconds = 0)
    {
        auto timepoint = getDeltaTimePoint(milliseconds);
        return tryPopUntil(x, timepoint);
    }

    bool tryPopUntil(T & x, TimePoint timepoint)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (queue.empty() && !is_closed)
        {
            if (empty_cv.wait_until(lock, timepoint) ==  std::cv_status::timeout)
                return false;
        }

        if (is_closed)
            return false;

        x = std::move(queue.front());
        queue.pop();
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->decrease(x);
        }
        lock.unlock();
        full_cv.notify_one();
        return true;
    }

    template <typename... Args>
    inline bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        auto timepoint = getDeltaTimePoint(milliseconds);
        return tryEmplaceUntil(timepoint, std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool tryEmplaceUntil(TimePoint timepoint, Args &&... args)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (exceedLimit() && !is_closed)
        {
            if (full_cv.wait_until(lock, timepoint) == std::cv_status::timeout)
                return false;
        }
        if (is_closed)
            return false;
        if constexpr (use_controller)
        {
            if (memory_controller)
                (memory_controller->increase(std::forward<Args>(args)), ...);
        }
        queue.emplace(std::forward<Args>(args)...);
        lock.unlock();
        empty_cv.notify_one();
        return true;
    }

    inline size_t size()
    {
        std::unique_lock<std::mutex> lock(mutex);
        return queue.size();
    }

    inline bool empty()
    {
        std::unique_lock<std::mutex> lock(mutex);
        return queue.empty();
    }

    inline void clear()
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (!queue.empty())
        {
            auto x = std::move(queue.front());
            queue.pop();
            if constexpr (use_controller)
            {
                if (memory_controller)
                    memory_controller->decrease(x);
            }
        }
        std::queue<T> empty_queue;
        std::swap(empty_queue, queue);
    }

    inline void setCapacity(size_t queue_capacity)
    {
        std::unique_lock<std::mutex> lock(mutex);
        capacity = queue_capacity;
    }

    inline bool close()
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (is_closed)
            return false;
        is_closed = true;
        lock.unlock();
        empty_cv.notify_all();
        full_cv.notify_all();
        return true;
    }

    inline bool tryWaitUntilEmpty(UInt64 milliseconds = 0)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (queue.size() > 0 && !is_closed)
        {
            if (full_cv.wait_for(lock, std::chrono::milliseconds(milliseconds)) == std::cv_status::timeout)
                return false;
        }
        return queue.size() == 0;
    }

    bool closed()
    {
        std::unique_lock<std::mutex> lock(mutex);
        return is_closed;
    }

private:
    template <typename E>
    inline void pushImpl(E && x)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (exceedLimit() && !is_closed)
        {
            LOG_TRACE(getLogger("BoundedDataQueue"), fmt::format("Queue is full and waiting, current size: {}, max size: {}", queue.size(), capacity));
            full_cv.wait(lock);
        }
        if (is_closed)
            throw Exception(ErrorCodes::STD_EXCEPTION, "Queue is closed");
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->increase(x);
        }
        queue.push(std::forward<E>(x));
        lock.unlock();
        empty_cv.notify_one();
    }

    template <typename E>
    inline bool tryPushImpl(E && x, UInt64 milliseconds = 0)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (exceedLimit() && !is_closed)
        {
            if (full_cv.wait_for(lock, std::chrono::milliseconds(milliseconds)) == std::cv_status::timeout)
                return false;
        }
        if (is_closed)
            return false;
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->increase(x);
        }
        queue.push(std::forward<E>(x));
        lock.unlock();
        empty_cv.notify_one();
        return true;
    }

    ALWAYS_INLINE bool exceedLimit() const
    {
        if constexpr (use_controller)
            return queue.size() >= capacity || (queue.size() != 0 && memory_controller && memory_controller->exceedLimit());
        else
            return queue.size() >= capacity;
    }

    ALWAYS_INLINE void increase(T & x)
    {
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->increase(x);
        }
    }

    ALWAYS_INLINE void decrease(T & x)
    {
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->decrease(x);
        }
    }

    std::queue<T> queue;
    std::mutex mutex;
    std::condition_variable full_cv;
    std::condition_variable empty_cv;
    size_t capacity;
    Controller memory_controller = nullptr;
    bool is_closed = false;
};

}
