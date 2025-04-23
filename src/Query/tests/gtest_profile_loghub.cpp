#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include <base/types.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Query/Executor/ProfileLogHub.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB
{

class MockConsumer : public ProfileElementConsumer<ProcessorProfileLogElement>
{
public:
    explicit MockConsumer(std::string query_id):ProfileElementConsumer(query_id) {}
    ~MockConsumer() override;
    void consume(ProcessorProfileLogElement & element) override;
    
    std::vector<ProcessorProfileLogElement> getStoreResult() const {return store_vector;}
    std::vector<ProcessorProfileLogElement> store_vector;
};

MockConsumer::~MockConsumer() = default;;


void MockConsumer::consume(ProcessorProfileLogElement & element)
{
    store_vector.emplace_back(element);
}

TEST(ProfileLogHubTest, ConsumeTest)
{
    auto & profile_log_hub = ProfileLogHub<ProcessorProfileLogElement>::getInstance();
    std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> consumer = std::make_shared<MockConsumer>("test_query_id");
    profile_log_hub.initLogChannel("test_query_id", consumer);

    size_t num_threads = 1;
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_threads);
    pool.scheduleOrThrowOnError([&profile_log_hub]() {
        for (int i = 0; i < 10; i++) 
        {
            ProcessorProfileLogElement element;
            profile_log_hub.tryPushElement("test_query_id", element, 1);
        } 
        
    });
    
    pool.wait();
    profile_log_hub.stopConsume("test_query_id");
    while (!consumer->isFinish())
    {
        sleep(1);
    }
    
    auto result = dynamic_pointer_cast<MockConsumer>(consumer)->getStoreResult();
    ASSERT_EQ(result.size(), 10);
}
}
