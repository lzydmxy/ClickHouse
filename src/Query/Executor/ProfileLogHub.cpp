#include "ProfileLogHub.h"
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric ProfileThreads;
    extern const Metric ProfileThreadsActive;
    extern const Metric ProfileThreadsScheduled;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int EXPLAIN_COLLECT_PROFILE_METRIC_TIMEOUT;
}

static constexpr size_t PROFILE_CONSUME_SIZE = 8;

template <typename ProfileElement>
ProfileLogHub<ProfileElement>::ProfileLogHub():logger(getLogger("ProfileLogHub")) 
{ 
    consume_thread_pool = std::make_unique<ThreadPool>(CurrentMetrics::ProfileThreads,
        CurrentMetrics::ProfileThreadsActive, CurrentMetrics::ProfileThreadsScheduled,
        PROFILE_CONSUME_SIZE
    );
}

template <typename ProfileElement>
void ProfileLogHub<ProfileElement>::initLogChannel(const std::string & query_id, Consumer consumer [[maybe_unused]])
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (!profile_element_queue_map.contains(query_id))
        {
            auto queue = std::make_shared<ProfileElementQueue>(256);
            profile_element_queue_map.emplace(query_id, queue);
        }
    }
    
    try
    {
        registerConsumer(consumer);
    }
    catch (...)
    {
        auto err = DB::getCurrentExceptionMessage(true);
        LOG_ERROR(logger, "Profile element log channel init occur error: {}", err);
    }
}

template <typename ProfileElement>
void ProfileLogHub<ProfileElement>::finalizeLogChannel(const std::string & query_id)
{
    auto consumer = profile_element_consumers.find(query_id)->second;
    consumer->finish();
    profile_element_consumers.erase(query_id);
    profile_element_queue_map.erase(query_id);
    LOG_DEBUG(logger, "Query:{} finish log element consume.", query_id);
}

template <typename ProfileElement>
void ProfileLogHub<ProfileElement>::registerConsumer(const Consumer consumer)
{
    const auto & query_id = consumer->getQueryId();

    if (profile_element_consumers.contains(query_id))
        return;

    profile_element_consumers.emplace(query_id, consumer);

    consume_thread_pool->scheduleOrThrow(
        [consumer, this]() {
            try
            {
                LOG_DEBUG(logger, "Consumer of query:{} start consuming.", consumer->getQueryId());
                auto & queue = profile_element_queue_map.find(consumer->getQueryId())->second;

                while (consumer->stillRunning())
                {
                    ProfileElement element;
                    if (queue->tryPop(element, consumer->getFetchTimeout()))
                        consumer->consume(element);
                }

                while (!queue->empty())
                {
                    ProfileElement element;
                    if (queue->tryPop(element, consumer->getFetchTimeout()))
                        consumer->consume(element);
                }
                finalizeLogChannel(consumer->getQueryId());
            }
            catch (...)
            {
                LOG_ERROR(logger, "Profile element consume occur error.");
                throw;
            }
        },
        0,
        30 * 1000 * 1000);
}

/// the E is used here to ensure perfect forwarding in template class method
template <typename ProfileElement>
template <typename E>
void ProfileLogHub<ProfileElement>::tryPushElementImpl(const std::string & query_id, E && element, const UInt64 & timeout_millseconds)
{
    auto & element_queue = profile_element_queue_map.find(query_id)->second;
    auto time_start = std::chrono::system_clock::now();
    while (!element_queue->tryPush(std::forward<E>(element), timeout_millseconds / 10))
    {
        LOG_WARNING(logger, "Query id:{} push profile element to coordinator log queue fail.Retrying!!!", query_id);
        auto now = std::chrono::system_clock::now();
        UInt64 elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - time_start).count();  
        if (elapsed > timeout_millseconds)
        {
            break;
            LOG_ERROR(logger, "Query id:{} push profile element to coordinator log queue fail after retry.", query_id);
            throw Exception(ErrorCodes::EXPLAIN_COLLECT_PROFILE_METRIC_TIMEOUT, "Push profile element to coordinator fail.");
        }
    }
}

template <typename ProfileElement>
void ProfileLogHub<ProfileElement>::stopConsume(const std::string & query_id)
{
    auto consumer_iterator = profile_element_consumers.find(query_id);
    if (consumer_iterator != profile_element_consumers.end())
        consumer_iterator->second->stop();
}

}
