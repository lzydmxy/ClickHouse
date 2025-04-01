#include <memory>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Chunk.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Query/tests/gtest_common.h>
#include <Query/Common/QueryCommon.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Processors/Exchange/ExchangeSourceExt.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/DataTrans/LocalBroadcastChannel.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/BroadcastExchangeSink.h>


using namespace DB;
namespace UnitTest
{
TEST(ExchangeSourceTest, LocalNormalTest)
{
    auto log = &Poco::Logger::get("ExchangeSourceTest");
    auto tp = getDeltaTimePoint(200);
    ExchangeOptions exchange_options {.exchange_timeout_ts = tp};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getInitContext();
    auto queue_size = context->getOptimizerContext()->getSettingsRef().exchange_local_receiver_queue_size;
    LocalChannelOptions options{queue_size, exchange_options.exchange_timeout_ts, false};
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    local_sender->accept(context, Block());
    BroadcastReceiverPtr local_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(channel);
    local_receiver->registerToSenders(200);

    Chunk chunk = createUInt8Chunk(10, 1, 8); //row, column, value
    auto total_bytes = chunk.bytes();
    setQueryDuration(context);
    BroadcastStatus status = local_sender->send(std::move(chunk));
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    auto exchange_source = std::make_shared<ExchangeSourceExt>(std::move(header), local_receiver, exchange_options);
    QueryPipeline pipeline(exchange_source);
    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_TRUE(pull_chunk.getNumRows() == 10);
    ASSERT_TRUE(pull_chunk.bytes() == total_bytes);
    LOG_TRACE(log, "Finish pull");
    try
    {
        /// trigger timeout
        executor.pull(pull_chunk);
        /// rethrow exception
        executor.pull(pull_chunk);
        ASSERT_TRUE(false) << "Should have thrown.";
    }
    catch (DB::Exception & e)
    {
        ASSERT_TRUE(e.displayText().find("timeout") != std::string::npos) << "Expected 'timeout after ms', got: " << e.displayText();
    }
    executor.cancel();
}

TEST(ExchangeSourceTest, LocalMultiChunkWithPull)
{
    auto log = &Poco::Logger::get("ExchangeSourceTest");
    auto tp = getDeltaTimePoint(200);
    ExchangeOptions exchange_options {.exchange_timeout_ts = tp};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getInitContext();
    auto queue_size = context->getOptimizerContext()->getSettingsRef().exchange_local_receiver_queue_size;
    LocalChannelOptions options{queue_size, exchange_options.exchange_timeout_ts, false};
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    local_sender->accept(context, Block());
    BroadcastReceiverPtr local_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(channel);
    local_receiver->registerToSenders(200);

    Chunk chunk = createUInt8Chunk(10, 1, 8); //row, column, value
    auto total_bytes = chunk.bytes();
    setQueryDuration(context);
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = local_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    auto exchange_source = std::make_shared<ExchangeSourceExt>(std::move(header), local_receiver, exchange_options);
    QueryPipeline pipeline(exchange_source);
    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    for (int i = 0; i < 5; i++)
    {
        ASSERT_TRUE(executor.pull(pull_chunk));
        ASSERT_TRUE(pull_chunk.getNumRows() == 10);
        ASSERT_TRUE(pull_chunk.bytes() == total_bytes);
    }
    LOG_TRACE(log, "Finish pull");
    executor.cancel();
}

TEST(ExchangeSourceTest, LocalLimitTest)
{
    auto log = &Poco::Logger::get("ExchangeSourceTest");
    auto tp = getDeltaTimePoint(200);
    ExchangeOptions exchange_options {.exchange_timeout_ts = tp};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getInitContext();
    auto queue_size = context->getOptimizerContext()->getSettingsRef().exchange_local_receiver_queue_size;
    LocalChannelOptions options{queue_size, exchange_options.exchange_timeout_ts, false};
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    BroadcastReceiverPtr local_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(channel);
    local_sender->accept(context, Block());
    local_receiver->registerToSenders(200);
    Chunk chunk = createUInt8Chunk(10, 1, 8); //row, column, value

    setQueryDuration(context);
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = local_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    auto exchange_source = std::make_shared<ExchangeSourceExt>(std::move(header), local_receiver, exchange_options);

    Pipe pipe;
    pipe.addSource(exchange_source);

    // Limit 1 row
    pipe.addTransform(std::make_shared<LimitTransform>(exchange_source->getPort().getHeader(), 1, 0));

    QueryPipeline pipeline(std::move(pipe));

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    LOG_TRACE(log, "Finish pull");
    ASSERT_TRUE(pull_chunk.getNumRows() == 1);
    ASSERT_FALSE(executor.pull(pull_chunk) && pull_chunk);
    executor.cancel();
}

}
