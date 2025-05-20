#include <chrono>
#include <memory>
#include <thread>
#include <variant>
#include <ucontext.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <Poco/Util/MapConfiguration.h>
#include <Query/Exchange/bRPC/BrpcApplication.h>
#include <Common/ClickHouseRevision.h>
#include <Common/tests/gtest_global_context.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/LimitTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Query/tests/gtest_common.h>
#include <Query/tests/gtest_exchange_helper.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/bRPC/BrpcRemoteBroadcastReceiver.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/bRPC/ReadBufferFromBrpc.h>
#include <Query/Exchange/bRPC/WriteBufferFromBrpc.h>
#include <Query/Exchange/DataTrans/NativeChunkOutputStream.h>
#include <Query/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Query/Processors/Exchange/ExchangeSourceExt.h>


// #include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
// #include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
// #include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>

// #include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
// #include <Processors/Exchange/DataTrans/IBroadcastSender.h>
// #include <Processors/Exchange/ExchangeOptions.h>
// #include <Processors/tests/gtest_processers_utils.h>

using namespace DB;
using namespace UnitTest;

using Clock = std::chrono::system_clock;

static Block getHeader(size_t column_num)
{
    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < column_num; i++)
    {
        columns.push_back(ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "column" + std::to_string(i)));
    }
    Block header = {columns};
    return header;
}

void receiver1()
{
    auto context = getInitContext();
    auto receiver_data = std::make_shared<ExchangeDataKey>(3, 1, 1);
    Block header = getHeader(1);
    BrpcRemoteBroadcastReceiverShardPtr receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        receiver_data,
        "127.0.0.1:8001",
        context,
        header,
        true,
        BrpcRemoteBroadcastReceiver::generateNameForTest());
    receiver->registerToSenders(1000);
    auto packet = std::dynamic_pointer_cast<IBroadcastReceiver>(receiver)->recv(1000);
    EXPECT_TRUE(std::holds_alternative<Chunk>(packet));
    Chunk & chunk = std::get<Chunk>(packet);
    EXPECT_EQ(chunk.getNumRows(), 1000);
    auto col = chunk.getColumns().at(0);
    EXPECT_EQ(col->getUInt(1), 7);
}

void receiver2()
{
    auto context = getInitContext();
    auto optimizer_context = context->getOptimizerContext();
    auto receiver_data = std::make_shared<ExchangeDataKey>(3, 1, 2);
    Block header = getHeader(1);
    auto queue = std::make_shared<MultiPathBoundedQueue>(optimizer_context->getSettingsRef().exchange_remote_receiver_queue_size, nullptr);
    BrpcRemoteBroadcastReceiverShardPtr receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        receiver_data,
        "127.0.0.1:8001",
        context,
        header,
        true,
        BrpcRemoteBroadcastReceiver::generateNameForTest(),
        std::move(queue));
    receiver->registerToSenders(1000);
    auto packet = std::dynamic_pointer_cast<IBroadcastReceiver>(receiver)->recv(1000);
    EXPECT_TRUE(std::holds_alternative<Chunk>(packet));
    Chunk & chunk = std::get<Chunk>(packet);
    EXPECT_EQ(chunk.getNumRows(), 1000);
    auto col = chunk.getColumns().at(0);
    EXPECT_EQ(col->getUInt(1), 7);
}

void sender_thread(BroadcastSenderProxyPtr sender, Chunk chunk)
{
    auto context = getInitContext();
    setQueryDuration(context);
    BroadcastStatus status = sender->send(std::move(chunk));
}

TEST_F(ExchangeRemoteTest, SerDserChunk)
{
    // ser
    auto origin_chunk = createUInt8Chunk(1000, 1, 7);
    auto header = getHeader(1);
    auto chunk_info = std::make_shared<AggregatedChunkInfo>();
    chunk_info->is_overflows = true;
    chunk_info->bucket_num = 99;
    origin_chunk.setChunkInfo(chunk_info);
    WriteBufferFromBrpc out;
    NativeChunkOutputStream block_out(out, header);
    block_out.write(origin_chunk);
    auto send_buf = out.getFinishedBuf();

    // dser
    ReadBufferFromBrpc read_buffer(send_buf);
    NativeChunkInputStream chunk_in(read_buffer, header);
    Chunk chunk = chunk_in.readImpl();
    EXPECT_EQ(chunk.getNumRows(), 1000);
    const auto dser_chunk_info = std::dynamic_pointer_cast<const AggregatedChunkInfo>(chunk.getChunkInfo());
    EXPECT_TRUE(dser_chunk_info);
    EXPECT_EQ(dser_chunk_info->is_overflows, true);
    EXPECT_EQ(dser_chunk_info->bucket_num, 99);
    auto col = chunk.getColumns().at(0);
    EXPECT_EQ(col->getUInt(1), 7);
    out.finalize();
}

TEST_F(ExchangeRemoteTest, RemoteNormalTest)
{
    const size_t ROW_NUM = 10;
    auto context = getInitContext();
    auto optimizer_context = context->getOptimizerContext();
    
    auto header = getHeader(1);
    auto data_key = std::make_shared<ExchangeDataKey>(3, 1, 1);

    Chunk chunk = createUInt8Chunk(ROW_NUM, 1, 7);
    auto total_bytes = chunk.bytes();

    // Server
    auto sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    sender->accept(context, header);
    setQueryDuration(context);

    std::thread thread_sender(sender_thread, sender, std::move(chunk));

    auto queue = std::make_shared<MultiPathBoundedQueue>(context->getOptimizerContext()->getSettingsRef().exchange_remote_receiver_queue_size, nullptr);
    // Client
    BrpcRemoteBroadcastReceiverShardPtr receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        data_key,
        "127.0.0.1:8001",
        context,
        header,
        true,
        BrpcRemoteBroadcastReceiver::generateNameForTest(),
        std::move(queue));
    receiver->registerToSenders(1000);
    auto tp = getDeltaTimePoint(1000);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp};
    auto exchange_source = std::make_shared<ExchangeSourceExt>(std::move(header), receiver, exchange_options);

    QueryPipeline pipeline(exchange_source);
    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    EXPECT_EQ(pull_chunk.getNumRows(), ROW_NUM);
    EXPECT_EQ(pull_chunk.bytes(), total_bytes);
    try
    {
        /// trigger timeout
        executor.pull(pull_chunk);
        /// rethrow exception
        executor.pull(pull_chunk);
    }
    catch (DB::Exception & e)
    {
        ASSERT_TRUE(e.displayText().find("timeout") != std::string::npos) << "Expected 'timeout after ms', got: " << e.displayText();
    }

    thread_sender.join();
    executor.cancel();
}

TEST_F(ExchangeRemoteTest, SendWithTwoReceivers)
{
    auto context = getInitContext();
    auto receiver_data1 = std::make_shared<ExchangeDataKey>(3, 1, 1);
    auto receiver_data2 = std::make_shared<ExchangeDataKey>(3, 1, 2);

    auto origin_chunk = createUInt8Chunk(1000, 1, 7);
    auto header = getHeader(1);

    std::thread thread_receiver1(receiver1);
    std::thread thread_receiver2(receiver2);

    auto sender_1 = BroadcastSenderProxyRegistry::instance().getOrCreate(receiver_data1);
    auto sender_2 = BroadcastSenderProxyRegistry::instance().getOrCreate(receiver_data2);
    sender_1->accept(context, header);
    sender_2->accept(context, header);
    setQueryDuration(context);
    sender_1->send(origin_chunk.clone());
    sender_2->send(origin_chunk.clone());

    thread_receiver1.join();
    thread_receiver2.join();
}

TEST_F(ExchangeRemoteTest, RemoteSenderLimitTest)
{
    auto context = getInitContext();
    auto tp = getDeltaTimePoint(2000);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp};
    auto header = getHeader(1);
    auto data_key = std::make_shared<ExchangeDataKey>(3, 1, 1);
    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    sender->accept(context, header);
    std::vector<std::thread> thread_senders;
    std::vector<Chunk> chunks;
    setQueryDuration(context);
    for (int i = 0; i < 5; i++)
    {
        Chunk clone = chunk.clone();
        chunks.emplace_back(std::move(clone));
        std::thread thread_sender(sender_thread, sender, std::move(chunks[i]));
        thread_senders.push_back(std::move(thread_sender));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    auto queue = std::make_shared<MultiPathBoundedQueue>(context->getOptimizerContext()->getSettingsRef().exchange_remote_receiver_queue_size, nullptr);
    BrpcRemoteBroadcastReceiverShardPtr receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        data_key,
        "127.0.0.1:8001",
        context,
        header,
        true,
        BrpcRemoteBroadcastReceiver::generateNameForTest(),
        std::move(queue));
    receiver->registerToSenders(1000);

    auto exchange_source = std::make_shared<ExchangeSourceExt>(std::move(header), receiver, exchange_options);
    Pipe pipe;
    pipe.addSource(exchange_source);
    pipe.addTransform(std::make_shared<LimitTransform>(exchange_source->getPort().getHeader(), 1, 0));
    QueryPipeline pipeline(std::move(pipe));

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_TRUE(pull_chunk.getNumRows() == 1);
    ASSERT_FALSE(executor.pull(pull_chunk) && pull_chunk);
    executor.cancel();
    for (auto & th : thread_senders)
    {
        th.join();
    }
}
