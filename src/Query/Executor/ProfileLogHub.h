#pragma once
#include <mutex>
#include <ostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Query/Common/BoundedDataQueue.h>
#include <Query/Executor/ProfileElementConsumer.h>

namespace DB
{

template <typename ProfileElement>
class ProfileLogHub
{
public:
    using ProfileElementQueue = BoundedDataQueue<ProfileElement>;
    using ProfileElementQueuePtr = std::shared_ptr<ProfileElementQueue>;
    using Consumer = std::shared_ptr<ProfileElementConsumer<ProfileElement>>;
    using ProfileElementQueues = std::unordered_map<std::string, ProfileElementQueuePtr>;
    using Consumers = std::unordered_map<std::string, Consumer>;
    using ProfileElements = std::vector<ProfileElement>;

    static ProfileLogHub<ProfileElement> & getInstance()
    {
        static ProfileLogHub<ProfileElement> profile_log_hub;
        return profile_log_hub;
    }

    explicit ProfileLogHub();
    ~ProfileLogHub() = default;

    void initLogChannel(const std::string & query_id, Consumer consumer);
    void finalizeLogChannel(const std::string & query_id);
    bool hasConsumer() const { return !profile_element_consumers.empty(); }

    inline void tryPushElement(const std::string & query_id, const ProfileElement & element, const UInt64 & timeout_millseconds = 0)
    {
        tryPushElementImpl(query_id, element, timeout_millseconds);
    }

    inline void tryPushElement(const std::string & query_id, ProfileElement && element, const UInt64 & timeout_millseconds = 0)
    {
        tryPushElementImpl(query_id, std::move(element), timeout_millseconds);
    }

    inline void tryPushElement(const std::string & query_id, const ProfileElements & elements, const UInt64 & timeout_millseconds = 0)
    {
        for (const auto & element : elements)
        {
            tryPushElementImpl(query_id, element, timeout_millseconds);
        }
    }

    inline void tryPushElement(const std::string & query_id, ProfileElements && elements, const UInt64 & timeout_millseconds = 0)
    {
        for (auto & element : elements)
        {
            tryPushElementImpl(query_id, std::move(element), timeout_millseconds);
        }
    }

    void stopConsume(const std::string & query_id);

private:
    void registerConsumer(Consumer consumer);
    template <typename E>
    void tryPushElementImpl(const std::string & query_id, E && element, const UInt64 & timeout_millseconds = 0);

    ProfileElementQueues profile_element_queue_map;
    Consumers profile_element_consumers;
    std::unique_ptr<ThreadPool> consume_thread_pool;
    std::mutex mutex;
    LoggerPtr logger;
};

}
