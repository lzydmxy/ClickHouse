#include <memory>
#include <vector>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/LimitTransform.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>
#include <Query/Transforms/BufferedCopyTransform.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/BroadcastExchangeSink.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/DataTrans/LocalBroadcastChannel.h>
#include <Query/Exchange/ExchangeBufferedSender.h>
#include <Query/Processors/Exchange/ExchangeSourceExt.h>
#include <Query/Exchange/LoadBalancedExchangeSink.h>
#include <Query/Exchange/MultiPartitionExchangeSink.h>
#include <Query/Exchange/RepartitionTransform.h>
#include <Query/Exchange/SinglePartitionExchangeSink.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Query/tests/gtest_common.h>

using namespace DB;
namespace UnitTest
{

const uint32_t CHUNK_NUM = 5;

//  BroadcastSenderProxy(1) ExchangeSourceExt(output) -> (input)BroadcastExchangeSink BroadcastSenderProxy(2)
//   /               \      /                                                   \     /               \
//source_sender    source_receiver                                             sink_sender     sink_receiver
TEST(ExchangeSinkTest, BroadcastExchangeSinkTest)
{
    //1.Init
    auto context = getInitContext();
    const size_t ROW_NUM = 10;
    const size_t COLUMN_NUM = 1;
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
    auto tp = getDeltaTimePoint(1000);
    ExchangeOptions exchange_options {.exchange_timeout_ts = tp};
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    //2.Source
    auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr source_sender
        = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key, SenderProxyOptions{.wait_timeout_ms = 2000});
    source_sender->accept(context, header);
    source_channel->registerToSenders(1000);
    BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);
    auto exchange_source = std::make_shared<ExchangeSourceExt>(header, source_receiver, exchange_options);

    //3.Sink
    auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(2));
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    sink_sender->accept(context, header);
    sink_channel->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);
    auto exchange_sink = std::make_shared<BroadcastExchangeSink>(header, std::vector<BroadcastSenderPtr>{sink_sender},
        exchange_options, BroadcastExchangeSink::generateNameForTest());

    //4.Connect
    connect(exchange_source->getPort(), exchange_sink->getPort());

    //5.Send data
    Chunk chunk = createUInt8Chunk(ROW_NUM, COLUMN_NUM, 8);
    auto total_bytes = chunk.bytes();
    setQueryDuration(context);
    for (int i = 0; i < CHUNK_NUM; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    //6.Execute
    QueryStatusPtr element;
    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(exchange_source));
    processors->emplace_back(std::move(exchange_sink));
    PipelineExecutor executor(processors, element);
    executor.execute(1, false);

    //7.Receive data
    for (int i = 0; i < CHUNK_NUM; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == ROW_NUM);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST(ExchangeSinkTest, LoadBalancedExchangeSinkTest)
{
    auto context = getInitContext();
    const size_t ROW_NUM = 10;
    const size_t COLUMN_NUM = 1;
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
    auto tp = getDeltaTimePoint(1000);
    ExchangeOptions exchange_options {.exchange_timeout_ts = tp};
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);
    source_channel->registerToSenders(1000);
    BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);
    auto exchange_source = std::make_shared<ExchangeSourceExt>(header, source_receiver, exchange_options);

    auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(2));
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    sink_sender->accept(context, header);
    sink_channel->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);
    auto exchange_sink = std::make_shared<LoadBalancedExchangeSink>(header, std::vector<BroadcastSenderPtr>{sink_sender},
        LoadBalancedExchangeSink::generateNameForTest());

    connect(exchange_source->getPort(), exchange_sink->getPort());

    Chunk chunk = createUInt8Chunk(ROW_NUM, COLUMN_NUM, 8);
    auto total_bytes = chunk.bytes();
    setQueryDuration(context);
    for (int i = 0; i < CHUNK_NUM; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(exchange_source));
    processors->emplace_back(std::move(exchange_sink));
    QueryStatusPtr element;
    PipelineExecutor executor(processors, element);
    executor.execute(1, false);
    for (int i = 0; i < CHUNK_NUM; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == ROW_NUM);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

// TEST(ExchangeSinkTest, MultiPartitionExchangeSinkTest)
// {
//     auto context = getInitContext();
//     const size_t ROW_NUM = 100;
//     Block block = createUInt64Block(ROW_NUM, 10, 88);
//     Block header = block.cloneEmpty();
//     auto tp = getDeltaTimePoint(1000);
//     ExchangeOptions exchange_options {.exchange_timeout_ts = tp};
//     LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

//     auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
//     auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest(1));
//     BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
//     source_sender->accept(context, header);
//     source_channel->registerToSenders(1000);
//     BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);
//     auto exchange_source = std::make_shared<ExchangeSourceExt>(header, source_receiver, exchange_options);

//     auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
//     auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(2));
//     BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
//     sink_sender->accept(context, header);
//     sink_channel->registerToSenders(1000);
//     BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

//     ColumnsWithTypeAndName arguments;
//     arguments.push_back(header.getByPosition(1));
//     arguments.push_back(header.getByPosition(2));
//     auto func = createRepartitionFunction(getContext().context, arguments);

//     auto exchange_sink = std::make_shared<MultiPartitionExchangeSink>(
//         header,
//         std::vector<BroadcastSenderPtr>{sink_sender},
//         func,
//         ColumnNumbers{1, 2},
//         ExchangeOptions{tp, 100000000, ROW_NUM},
//         MultiPartitionExchangeSink::generateNameForTest());

//     connect(exchange_source->getPort(), exchange_sink->getPort());

//     setQueryDuration(context);

//     Chunk chunk(block.mutateColumns(), ROW_NUM);
//     auto total_bytes = chunk.bytes();
//     for (int i = 0; i < CHUNK_NUM; i++)
//     {
//         BroadcastStatus status = source_sender->send(chunk.clone());
//         ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
//     }
//     source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

//     auto processors = std::make_shared<Processors>();
//     processors->emplace_back(std::move(exchange_source));
//     processors->emplace_back(std::move(exchange_sink));
//     QueryStatusPtr element;
//     PipelineExecutor executor(processors, element);
//     executor.execute(2, false);

//     for (int i = 0; i < CHUNK_NUM; i++)
//     {
//         RecvDataPacket recv_res = sink_receiver->recv(2000);
//         ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
//         Chunk & recv_chunk = std::get<Chunk>(recv_res);
//         ASSERT_TRUE(recv_chunk.getNumRows() == ROW_NUM);
//         ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
//     }
// }

// TEST(ExchangeSinkTest, SinglePartitionExchangeSinkNormalTest)
// {
//     auto log = getLogger("ExchangeSinkTest");
//     auto context = getInitContext();
//     const size_t ROW_NUM = 100;
//     Block block = createUInt64Block(ROW_NUM, 10, 88);
//     Block header = block.cloneEmpty();
//     auto tp = getDeltaTimePoint(1000);
//     ExchangeOptions exchange_options {.exchange_timeout_ts = tp};
//     LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

//     auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
//     auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest(1));
//     BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
//     source_sender->accept(context, header);
//     source_channel->registerToSenders(1000);
//     BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);
//     auto exchange_source = std::make_shared<ExchangeSourceExt>(header, source_receiver, exchange_options);

//     ColumnsWithTypeAndName arguments;
//     arguments.push_back(header.getByPosition(1));
//     arguments.push_back(header.getByPosition(2));
//     auto func = createRepartitionFunction(getContext().context, arguments);
//     auto repartition_transform = std::make_shared<RepartitionTransform>(header, 1, ColumnNumbers{1, 2}, func);

//     auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
//     auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(2));
//     BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
//     sink_sender->accept(context, header);
//     sink_channel->registerToSenders(1000);
//     BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);
//     auto exchange_sink = std::make_shared<SinglePartitionExchangeSink>(
//         header, sink_sender, 0, ExchangeOptions{tp, 0, 0}, SinglePartitionExchangeSink::generateNameForTest());

//     connect(exchange_source->getPort(), repartition_transform->getInputPort());
//     connect(repartition_transform->getOutputPort(), exchange_sink->getPort());

//     setQueryDuration(context);

//     LOG_TRACE(log, "Send data");
//     Chunk chunk(block.mutateColumns(), ROW_NUM);
//     auto total_bytes = chunk.bytes();
//     for (int i = 0; i < CHUNK_NUM; i++)
//     {
//         BroadcastStatus status = source_sender->send(chunk.clone());
//         ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
//     }
//     source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

//     auto processors = std::make_shared<Processors>();
//     processors->emplace_back(std::move(exchange_source));
//     processors->emplace_back(std::move(repartition_transform));
//     processors->emplace_back(std::move(exchange_sink));
//     QueryStatusPtr element;
//     PipelineExecutor executor(processors, element);
//     executor.execute(1, false);

//     LOG_TRACE(log, "Recv data");
//     for (int i = 0; i < CHUNK_NUM; i++)
//     {
//         LOG_TRACE(log, "Recv {}", i);
//         RecvDataPacket recv_res = sink_receiver->recv(2000);
//         ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
//         Chunk & recv_chunk = std::get<Chunk>(recv_res);
//         ASSERT_TRUE(recv_chunk.getNumRows() == ROW_NUM);
//         ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
//     }
// }

// TEST(ExchangeSinkTest, SinglePartitionExchangeSinkPipelineTest)
// {
//     auto context = getInitContext();
//     const size_t ROW_NUM = 100;
//     Block block = createUInt64Block(ROW_NUM, 10, 88);
//     Block header = block.cloneEmpty();

//     auto tp = getDeltaTimePoint(1000);
//     ExchangeOptions exchange_options {.exchange_timeout_ts = tp};
//     LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};
//     auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
//     auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest());
//     BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
//     source_sender->accept(context, header);
//     source_channel->registerToSenders(1000);
//     BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);

//     auto sink_key_1 = std::make_shared<ExchangeDataKey>(1, 2, 2);
//     auto sink_channel_1 = std::make_shared<LocalBroadcastChannel>(sink_key_1, options, LocalBroadcastChannel::generateNameForTest());
//     BroadcastSenderProxyPtr sink_sender_1 = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key_1);
//     sink_sender_1->accept(context, header);
//     sink_channel_1->registerToSenders(1000);
//     BroadcastReceiverPtr sink_receiver_1 = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel_1);

//     auto sink_key_2 = std::make_shared<ExchangeDataKey>(1, 3, 3);
//     auto sink_channel_2 = std::make_shared<LocalBroadcastChannel>(sink_key_2, options, LocalBroadcastChannel::generateNameForTest());
//     BroadcastSenderProxyPtr sink_sender_2 = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key_2);
//     sink_sender_2->accept(context, header);
//     sink_channel_2->registerToSenders(1000);
//     BroadcastReceiverPtr sink_receiver_2 = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel_2);

//     Chunk chunk(block.mutateColumns(), ROW_NUM);
//     ColumnsWithTypeAndName arguments;
//     arguments.push_back(header.getByPosition(1));
//     arguments.push_back(header.getByPosition(2));
//     auto func = createRepartitionFunction(getContext().context, arguments);
//     auto chunk_bytes = chunk.bytes();

//     setQueryDuration(context);
//     for (int i = 0; i < CHUNK_NUM; i++)
//     {
//         BroadcastStatus status = source_sender->send(chunk.clone());
//         ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
//     }
//     source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

//     auto exchange_source = std::make_shared<ExchangeSourceExt>(header, source_receiver, exchange_options);
//     auto repartition_transform = std::make_shared<RepartitionTransform>(header, 2, ColumnNumbers{1, 2}, func);
//     auto buffer_copy_transform = std::make_shared<BufferedCopyTransform>(header, 2, 10);

//     auto exchange_sink_1 = std::make_shared<SinglePartitionExchangeSink>(
//         header, sink_sender_1, 0, ExchangeOptions{tp, 0, 0}, SinglePartitionExchangeSink::generateNameForTest());
//     auto exchange_sink_2 = std::make_shared<SinglePartitionExchangeSink>(
//         header, sink_sender_2, 1, ExchangeOptions{tp, 0, 0}, SinglePartitionExchangeSink::generateNameForTest());

//     connect(exchange_source->getPort(), repartition_transform->getInputPort());
//     connect(repartition_transform->getOutputPort(), buffer_copy_transform->getInputPort());
//     connect(buffer_copy_transform->getOutputs().front(), exchange_sink_1->getPort());
//     connect(buffer_copy_transform->getOutputs().back(), exchange_sink_2->getPort());

//     auto processors = std::make_shared<Processors>();
//     processors->emplace_back(std::move(exchange_source));
//     processors->emplace_back(std::move(repartition_transform));
//     processors->emplace_back(std::move(buffer_copy_transform));
//     processors->emplace_back(std::move(exchange_sink_1));
//     processors->emplace_back(std::move(exchange_sink_2));

//     QueryStatusPtr element;
//     PipelineExecutor executor(processors, element);
//     executor.execute(2, false);

//     sink_sender_1->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink1 finish");
//     sink_sender_2->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink2 finish");

//     size_t total_bytes = 0;

//     for (int i = 0; i < CHUNK_NUM; i++)
//     {
//         RecvDataPacket recv_res = sink_receiver_1->recv(2000);
//         if (std::holds_alternative<Chunk>(recv_res))
//         {
//             Chunk & recv_chunk = std::get<Chunk>(recv_res);
//             total_bytes += recv_chunk.bytes();
//         }
//     }

//     ASSERT_TRUE(total_bytes == chunk_bytes * 5);
// }

}
