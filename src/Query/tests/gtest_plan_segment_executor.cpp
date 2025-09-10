#include <memory>
#include <string>
#include <thread>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/ExchangeMode.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentExecutor.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/executePlanSegment.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/DataTrans/LocalBroadcastChannel.h>
#include <Query/Exchange/bRPC/BrpcApplication.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Processors/IQueryPlanStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

#include <gtest/gtest.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Query/tests/gtest_common.h>

using namespace DB;

namespace UnitTest
{

void initLogger(const String & level = "trace")
{
    if (!Poco::Logger::root().getChannel())
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel());
        Poco::Logger::root().setChannel(channel);
    }
    Poco::Logger::root().setLevel(level);
}

inline void setQueryDuration(DB::ContextMutablePtr context = nullptr)
{
    if (!context)
        context = getContext().context;

    auto & client_info = context->getClientInfo();
    const auto current_time = std::chrono::system_clock::now();
    client_info.initial_query_start_time = std::chrono::duration_cast<std::chrono::seconds>(current_time.time_since_epoch()).count();
    //client_info.initial_query_start_time_microseconds = time_in_microseconds(current_time);
    client_info.initial_query_start_time_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(current_time.time_since_epoch()).count();

    context->getOptimizerContext()->initQueryExpirationTimeStamp();
}

class PlanSegmentExecutorTest : public testing::Test
{
protected:
    static brpc::Server server;
    static BrpcExchangeReceiverRegistryService service_impl;
    static void startBrpcServer()
    {
        if (server.AddService(&service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(ERROR) << "Fail to add service";
            return;
        }
        LOG(INFO) << "Add service success";

        // Start the server.
        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        if (server.Start(0, &options) != 0)
        {
            LOG(ERROR) << "Fail to start Server";
            return;
        }
        LOG(INFO) << "Start Server";
    }
    static void SetUpTestCase()
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
        BrpcApplication::getInstance().initialize(*map_config);
        startBrpcServer();
    }
    virtual void SetUp()
    {
        //early initialization for concurrent
        tryRegisterFunctions();
        GlobalThreadPool::instance();
    }

    virtual void TearDown()
    {
    }
};

brpc::Server PlanSegmentExecutorTest::server;
BrpcExchangeReceiverRegistryService PlanSegmentExecutorTest::service_impl(73400320);

TEST_F(PlanSegmentExecutorTest, ExecuteTest)
{
    auto log = getLogger("PlanSegmentExecutorTest");
    const String query_id = "q123";
    const UInt64 query_tx_id = 123;
    std::unordered_map<std::string, Field> settings;
    auto context = createQueryContext(query_id, settings);
    auto optimizer_context = context->getOptimizerContext();
    optimizer_context->setProcessListEntry(nullptr);

    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);

    auto tp = getDeltaTimePoint(2000);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp};

    optimizer_context->setTransactionID(query_tx_id);
    optimizer_context->setPlanSegmentInstanceID({1,0});
    auto rpc_port = server.listen_address().port;
    optimizer_context->setRPCPort(rpc_port);
    auto coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456", optimizer_context->getRPCPort());
    auto local_address = std::make_shared<AddressInfo>("localhost", 0, "test", "123456", optimizer_context->getRPCPort());

    auto coordinator_address_str = extractExchangeHostPort(*coordinator_address);
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto query_unique_id = optimizer_context->getTransactionID(query_id);
    LOG_TRACE(log, "Create source");
    auto source_key = std::make_shared<ExchangeDataKey>(query_unique_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    LOG_TRACE(log, "Create sink");
    auto sink_key = std::make_shared<ExchangeDataKey>(query_unique_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(100));
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 1;
    plan_segment_instance->info.execution_address = local_address;

    LOG_TRACE(log, "Create inputs");
    PlanSegmentInputs inputs;
    auto input = std::make_shared<PlanSegmentInput>(header, RIPlanSegment::EXCHANGE);
    input->setExchangeParallelSize(1);
    input->setExchangeId(1);
    input->setPlanSegmentId(10);
    input->insertSourceAddress(*local_address);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, RIPlanSegment::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setExchangeId(2);
    output->setPlanSegmentId(30);
    output->setExchangeMode(RExchangeMode::REPARTITION);

    LOG_TRACE(log, "Create plan segment");
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId(query_id);
    plan_segment.setPlanSegmentId(20);
    plan_segment.setCoordinatorAddress(*coordinator_address);
    plan_segment.appendPlanSegmentInputs(inputs);
    plan_segment.appendPlanSegmentOutput(output);

    context->getClientInfo().initial_query_id = plan_segment.getQueryId();
    context->getClientInfo().current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    optimizer_context->setCoordinatorAddress(coordinator_address);
    setQueryDuration(context);

    LOG_TRACE(log, "Create plan exchange source step");
    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStepExt>(inputs, datastream, false, false);
    exchange_source_step->setPlanSegment(&plan_segment, context);
    exchange_source_step->setExchangeOptions(exchange_options);

    LOG_TRACE(log, "Source send data aync");
    auto sender_func = [&]() {
        for (int i = 0; i < 5; i++)
        {
            BroadcastStatus status = source_sender->send(chunk.clone());
            ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
        }
        source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");
    };

    ThreadFromGlobalPool thread(std::move(sender_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    LOG_TRACE(log, "Build query plan & pipe line");
    QueryPlanExt query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment.setQueryPlan(std::move(query_plan));
    auto plan_segment_process_entry = optimizer_context->getPlanSegmentProcessList()->insertGroup(context, plan_segment.getPlanSegmentId());
    plan_segment_instance->plan_segment = std::make_unique<PlanSegment>(std::move(plan_segment));
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, std::move(plan_segment_process_entry), exchange_options);
    LOG_TRACE(log, "Execute query");
    executor.execute();
    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == chunk.bytes());
    }
    LOG_TRACE(log, "Finished");

    // Another way to test code logic
    // QueryPipelineBuilder builder;
    // exchange_source_step->initializePipeline(builder, BuildQueryPipelineSettingsExt::fromContext(context));
    // auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    // PlanSegmentExecutor::registerAllExchangeReceivers(getLogger("PlanSegmentExecutor"), pipeline, 1000);
    // PullingAsyncPipelineExecutor executor(pipeline);
    // Chunk pull_chunk;
    // for (int i = 0; i < 5; i++)
    // {
    //     ASSERT_TRUE(executor.pull(pull_chunk));
    //     ASSERT_TRUE(pull_chunk.getNumRows() == rows);
    // }
    // executor.cancel();
}


TEST_F(PlanSegmentExecutorTest, ExecuteAsyncTest)
{
    const String query_id = "q123";
    const UInt64 query_tx_id = 123;

    std::unordered_map<std::string, Field> settings;
    auto context = createQueryContext(query_id, settings);
    auto optimizer_context = context->getOptimizerContext();
    optimizer_context->setProcessListEntry(nullptr);

    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    auto tp = getDeltaTimePoint(2000);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp};

    optimizer_context->setTransactionID(query_tx_id);
    optimizer_context->setPlanSegmentInstanceID({1, 0});
    auto rpc_port = server.listen_address().port;
    optimizer_context->setRPCPort(rpc_port);
    auto coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456", optimizer_context->getRPCPort());
    auto coordinator_address_str = extractExchangeHostPort(*coordinator_address);
    auto local_address = std::make_shared<AddressInfo>("localhost", 0, "test", "123456", optimizer_context->getRPCPort());

    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};
    auto query_unique_id = optimizer_context->getTransactionID(query_id);
    auto source_key = std::make_shared<ExchangeDataKey>(query_unique_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_unique_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(1));
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 0;
    plan_segment_instance->info.execution_address = local_address;

    PlanSegmentInputs inputs;
    auto input = std::make_shared<PlanSegmentInput>(header, RIPlanSegment::EXCHANGE);

    input->setExchangeParallelSize(1);
    input->setExchangeId(1);
    input->setPlanSegmentId(1);
    input->insertSourceAddress(*local_address);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, RIPlanSegment::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setExchangeId(2);
    output->setPlanSegmentId(3);
    output->setExchangeMode(RExchangeMode::REPARTITION);

    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId(query_id);
    plan_segment.setPlanSegmentId(2);
    plan_segment.setCoordinatorAddress(*coordinator_address);
    plan_segment.appendPlanSegmentInputs(inputs);
    plan_segment.appendPlanSegmentOutput(output);

    context->getClientInfo().initial_query_id = plan_segment.getQueryId();
    context->getClientInfo().current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    optimizer_context->setCoordinatorAddress(coordinator_address);
    setQueryDuration(context);

    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStepExt>(inputs, datastream, false, false);
    exchange_source_step->setPlanSegment(&plan_segment, context);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    auto sender_func = [&]() {
        for (int i = 0; i < 5; i++)
        {
            BroadcastStatus status = source_sender->send(chunk.clone());
            ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
        }

        source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");
    };
    ThreadFromGlobalPool thread1(std::move(sender_func));

    QueryPlanExt query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment.setQueryPlan(std::move(query_plan));

    auto plan_segment_process_entry = optimizer_context->getPlanSegmentProcessList()->insertGroup(context, plan_segment.getPlanSegmentId());
    plan_segment_instance->plan_segment = std::make_unique<PlanSegment>(std::move(plan_segment));

    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, std::move(plan_segment_process_entry), exchange_options);
    executor.execute();

    auto receive_func = [&] {
        for (int i = 0; i < 5; i++)
        {
            RecvDataPacket recv_res = sink_receiver->recv(2000);
            ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
            Chunk & recv_chunk = std::get<Chunk>(recv_res);
            ASSERT_TRUE(recv_chunk.getNumRows() == rows);
            ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
        }
    };
    ThreadFromGlobalPool thread2(std::move(receive_func));

    SCOPE_EXIT({
        if (thread1.joinable())
            thread1.join();
        if (thread2.joinable())
            thread2.join();
    });

    // auto execute_func = [&]() { executor.execute(); };
    // ThreadFromGlobalPool thread(std::move(execute_func));
    // SCOPE_EXIT({
    //     if (thread.joinable())
    //         thread.join();
    // });
    // for (int i = 0; i < 5; i++)
    // {
    //     BroadcastStatus status = source_sender->send(chunk.clone());
    //     ASSERT_EQ(status.code, BroadcastStatusCode::RUNNING) << status.message;
    // }
    // source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");
    // for (int i = 0; i < 5; i++)
    // {
    //     RecvDataPacket recv_res = sink_receiver->recv(2000);
    //     ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
    //     Chunk & recv_chunk = std::get<Chunk>(recv_res);
    //     ASSERT_TRUE(recv_chunk.getNumRows() == rows);
    //     ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    // }
}

TEST_F(PlanSegmentExecutorTest, ExecuteCancelTest)
{
    const String query_id = "q1234";
    const UInt64 query_tx_id = 1234;
    auto log = getLogger("PlanSegmentExecutorTest");

    std::unordered_map<std::string, Field> settings;
    auto context = createQueryContext(query_id, settings);
    auto optimizer_context = context->getOptimizerContext();
    optimizer_context->setProcessListEntry(nullptr);

    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    auto tp = getDeltaTimePoint(1000);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp};

    optimizer_context->setTransactionID(query_tx_id);
    optimizer_context->setPlanSegmentInstanceID({1, 0});
    auto rpc_port = server.listen_address().port;
    optimizer_context->setRPCPort(rpc_port);
    auto query_unique_id = optimizer_context->getTransactionID(query_id);
    auto coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456", optimizer_context->getRPCPort());
    auto local_address = std::make_shared<AddressInfo>("localhost", 0, "test", "123456", optimizer_context->getRPCPort());

    auto coordinator_address_str = extractExchangeHostPort(*coordinator_address);
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto source_key = std::make_shared<ExchangeDataKey>(query_unique_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_unique_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(100));
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 0;
    plan_segment_instance->info.execution_address = local_address;

    PlanSegmentInputs inputs;

    auto input = std::make_shared<PlanSegmentInput>(header, RIPlanSegment::EXCHANGE);
    input->setExchangeParallelSize(1);
    input->setExchangeId(1);
    input->setPlanSegmentId(10);
    input->insertSourceAddress(*local_address);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, RIPlanSegment::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setExchangeId(2);
    output->setPlanSegmentId(30);
    output->setExchangeMode(RExchangeMode::REPARTITION);

    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId(query_id);
    plan_segment.setPlanSegmentId(20);
    plan_segment.setCoordinatorAddress(*coordinator_address);
    plan_segment.appendPlanSegmentInputs(inputs);
    plan_segment.appendPlanSegmentOutput(output);

    context->getClientInfo().initial_query_id = plan_segment.getQueryId();
    context->getClientInfo().current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    optimizer_context->setCoordinatorAddress(coordinator_address);
    setQueryDuration(context);

    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStepExt>(inputs, datastream, false, false);
    exchange_source_step->setPlanSegment(&plan_segment, context);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    size_t sleep_ms = 100;
    auto sender_func = [&]() {
        for (int i = 0; i < 100; i++)
        {
            BroadcastStatus status = source_sender->send(chunk.clone());
            LOG_TRACE(log, "*****ExecuteCancelTest send status {}", status.code);
            if (status.code != BroadcastStatusCode::RUNNING)
                break;
            LOG_TRACE(log, "*****ExecuteCancelTest send sleep {} ms", sleep_ms);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        }
        source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");
        LOG_TRACE(log, "*****ExecuteCancelTest finish send");
    };

    auto reveive_func = [&]() {
        for (int i = 0; i < 2; i++)
        {
            LOG_TRACE(log, "*****ExecuteCancelTest receive sleep {} ms", sleep_ms);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            RecvDataPacket recv_res = sink_receiver->recv(5000);
            ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
            Chunk & recv_chunk = std::get<Chunk>(recv_res);
            ASSERT_TRUE(recv_chunk.getNumRows() == rows);
            ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
        }
        LOG_TRACE(log, "*****ExecuteCancelTest finish recevie");

        LOG_TRACE(log, "*****ExecuteCancelTest try cancel plan segment group");
        CancellationCode code = CancellationCode::NotFound;
        int max_time = 100;
        for (; code == CancellationCode::NotFound; code = optimizer_context->getPlanSegmentProcessList()->tryCancelPlanSegmentGroup(query_id))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            max_time--;
            if(max_time < 0)
                break;
        }
        ASSERT_TRUE(code == CancellationCode::CancelSent);
        RecvDataPacket recv_res = sink_receiver->recv(5000);
        ASSERT_TRUE(std::holds_alternative<BroadcastStatus>(recv_res));
        ASSERT_TRUE(std::get<BroadcastStatus>(recv_res).code == BroadcastStatusCode::SEND_CANCELLED);
    };

    LOG_TRACE(log, "*****ExecuteCancelTest start send and receive thread");
    ThreadFromGlobalPool thread1(std::move(sender_func));
    ThreadFromGlobalPool thread2(std::move(reveive_func));
    SCOPE_EXIT({
        if (thread1.joinable())
            thread1.join();
        if (thread2.joinable())
            thread2.join();
    });

    QueryPlanExt query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment.setQueryPlan(std::move(query_plan));
    auto plan_segment_process_entry = optimizer_context->getPlanSegmentProcessList()->insertGroup(context, plan_segment.getPlanSegmentId());
    plan_segment_instance->plan_segment = std::make_unique<PlanSegment>(std::move(plan_segment));
    // buffer will flush when row_num reached to send_threshold_in_row_num
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, std::move(plan_segment_process_entry), exchange_options);
    LOG_TRACE(log, "*****ExecuteCancelTest begin plansegment execute");
    executor.execute(); ;
    LOG_TRACE(log, "*****ExecuteCancelTest finish plansegment execute");
}

void planExecutor(String query_id, size_t query_tx_id, AddressInfoPtr coordinator_address, bool send_data, int rpc_port)
{
    auto log = getLogger("PlanSegmentExecutorTest");
    // query_id = "q123";
    // query_tx_id = 123;
    // coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456");
    LOG_TRACE(log, "*****Plan executor query id {}, query tx id {}, send data {}, coordinator address {}",
        query_id, query_tx_id, send_data, coordinator_address->toShortString());
    auto local_address = std::make_shared<AddressInfo>("localhost", 0, "test", "123456", rpc_port);

    std::unordered_map<std::string, Field> settings;
    auto context = createQueryContext(query_id, settings);
    auto optimizer_context = context->getOptimizerContext();
    optimizer_context->setProcessListEntry(nullptr);
    optimizer_context->setRPCPort(rpc_port);

    const size_t rows = 10;
    Block block = createUInt64Block(rows, 3, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);

    auto tp = getDeltaTimePoint(2000);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp};

    optimizer_context->setTransactionID(query_tx_id);
    optimizer_context->setPlanSegmentInstanceID({1,0});

    auto coordinator_address_str = extractExchangeHostPort(*coordinator_address);
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto query_unique_id = optimizer_context->getTransactionID(query_id);
    auto source_key = std::make_shared<ExchangeDataKey>(query_unique_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_unique_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(100));
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 1;
    plan_segment_instance->info.execution_address = local_address;

    PlanSegmentInputs inputs;

    auto input = std::make_shared<PlanSegmentInput>(header, RIPlanSegment::EXCHANGE);
    input->setExchangeParallelSize(1);
    input->setExchangeId(1);
    input->setPlanSegmentId(10);
    input->insertSourceAddress(*local_address);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, RIPlanSegment::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setExchangeId(2);
    output->setPlanSegmentId(30);
    output->setExchangeMode(RExchangeMode::REPARTITION);

    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId(query_id);
    plan_segment.setPlanSegmentId(20);
    plan_segment.setCoordinatorAddress(*coordinator_address);
    plan_segment.appendPlanSegmentInputs(inputs);
    plan_segment.appendPlanSegmentOutput(output);

    context->getClientInfo().initial_query_id = plan_segment.getQueryId();
    context->getClientInfo().current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    optimizer_context->setCoordinatorAddress(coordinator_address);
    setQueryDuration(context);

    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStepExt>(inputs, datastream, false, false);
    exchange_source_step->setPlanSegment(&plan_segment, context);
    exchange_source_step->setExchangeOptions(exchange_options);

    size_t chunk_num = 3;
    auto sender_func = [&]() {
        for (int i = 0; i < chunk_num; i++)
        {
            BroadcastStatus status = source_sender->send(chunk.clone());
            ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
        }
        source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");
    };

    ThreadFromGlobalPool thread(std::move(sender_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    //QueryPlan root node -> exchange_source_step -> plan_segment -> inputs/output
    QueryPlanExt query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment.setQueryPlan(std::move(query_plan));
    auto plan_segment_process_entry = optimizer_context->getPlanSegmentProcessList()->insertGroup(context, plan_segment.getPlanSegmentId());
    plan_segment_instance->plan_segment = std::make_unique<PlanSegment>(std::move(plan_segment));
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, std::move(plan_segment_process_entry), exchange_options);
    executor.execute();
    for (int i = 0; i < chunk_num; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == chunk.bytes());
    }
}

const int THREAD_COUNT = 2;

void planExecutor1(String query_id, AddressInfo coordinator_address)
{
    try
    {
        const auto context = Context::createCopy(getContext().context);
        context->initializeOptimizerContext();
        context->getOptimizerContext()->setProcessListEntry(nullptr);

        const size_t rows = 100;
        Block block = createUInt64Block(rows, 10, 88);
        Block header = block.cloneEmpty();
        Chunk chunk(block.mutateColumns(), rows);
        ColumnsWithTypeAndName arguments;

        arguments.push_back(header.getByPosition(1));
        arguments.push_back(header.getByPosition(2));
        auto func = createRepartitionFunction(getContext().context, arguments);

        //todo: liyang453, other feat
        /*
        timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 2000 * 1000000;
        ExchangeOptions exchange_options{.exchange_timeout_ts = ts};
        */
        ExchangeOptions exchange_options;

        const UInt64 query_tx_id = 12345;
        //context->setTemporaryTransaction(query_tx_id, query_tx_id, false);
        context->getOptimizerContext()->setPlanSegmentInstanceID({1, 0});

        AddressInfo local_address("localhost", 0, "test", "123456");

        auto coordinator_address_str = extractExchangeHostPort(coordinator_address);
        LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

        auto source_key = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 1);
        BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
        source_sender->accept(context, header);

        auto sink_key = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 1);
        BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
        auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest(31));
        sink_sender->becomeRealSender(sink_channel);
        BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

        auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
        plan_segment_instance->info.parallel_id = 1;
        plan_segment_instance->info.execution_address = std::make_shared<AddressInfo>(local_address);

        PlanSegmentInputs inputs;

        auto input = std::make_shared<PlanSegmentInput>(header, RIPlanSegment::EXCHANGE);
        input->setExchangeParallelSize(1);
        input->setExchangeId(1);
        input->setPlanSegmentId(1);
        input->insertSourceAddress(local_address);
        inputs.push_back(input);

        auto output = std::make_shared<PlanSegmentOutput>(header, RIPlanSegment::EXCHANGE);
        output->setParallelSize(1);
        output->setExchangeParallelSize(1);
        output->setExchangeId(2);
        output->setPlanSegmentId(3);
        output->setExchangeMode(RExchangeMode::REPARTITION);

        PlanSegmentSharedPtr plan_segment = std::make_shared<PlanSegment>();
        plan_segment->setQueryId(query_id);
        plan_segment->setPlanSegmentId(2);
        plan_segment->setCoordinatorAddress(coordinator_address);
        plan_segment->appendPlanSegmentInputs(inputs);
        plan_segment->appendPlanSegmentOutput(output);

        context->getClientInfo().initial_query_id = plan_segment->getQueryId();
        context->getClientInfo().current_query_id = plan_segment->getQueryId() + std::to_string(plan_segment->getPlanSegmentId());
        context->getOptimizerContext()->setCoordinatorAddress(std::make_shared<AddressInfo>(coordinator_address));
        setQueryDuration(context);

        DataStream datastream{.header = header};
        auto exchange_source_step = std::make_unique<RemoteExchangeSourceStepExt>(inputs, datastream, false, false);
        exchange_source_step->setPlanSegment(plan_segment.get(), context);
        exchange_source_step->setExchangeOptions(exchange_options);

        QueryPlanExt query_plan;
        QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
        query_plan.addRoot(std::move(remote_node));
        plan_segment->setQueryPlan(std::move(query_plan));
        auto plan_segment_process_entry = context->getOptimizerContext()->getPlanSegmentProcessList()->insertGroup(context, plan_segment->getPlanSegmentId());
        plan_segment_instance->plan_segment = std::move(plan_segment);
        PlanSegmentExecutor executor(std::move(plan_segment_instance), context, std::move(plan_segment_process_entry), exchange_options);

        executor.execute();
    }
    catch (...)
    {
    }
}

TEST_F(PlanSegmentExecutorTest, ConcurrentWithDiffIdSameAddr)
{
    auto context = getInitContext();
    context->setSetting("max_concurrent_queries_for_user", Field(100));
    auto optimizer_context = context->getOptimizerContext();
    auto rpc_port = server.listen_address().port;
    optimizer_context->setRPCPort(rpc_port);
    std::vector<std::thread> thread_executors;
    for (int i = 0; i < THREAD_COUNT; i++)
    {
        String initial_query_id = "q" + std::to_string(i);
        UInt16 port = 6666;
        auto coordinator_address = std::make_shared<AddressInfo>("localhost", port, "test", "123456", optimizer_context->getRPCPort());
        // planExecutor(initial_query_id, i, coordinator_address, true);
        std::thread thread_executor(planExecutor, initial_query_id, i, coordinator_address, true, rpc_port);
        thread_executors.push_back(std::move(thread_executor));
    }
    for (auto & th : thread_executors)
        th.join();
    ASSERT_EQ(optimizer_context->getPlanSegmentProcessList()->size(), 0);
}

TEST_F(PlanSegmentExecutorTest, ConcurrentWithDiffIdDiffAddr)
{
    auto context = getInitContext();
    context->setSetting("max_concurrent_queries_for_user", Field(100));
    auto optimizer_context = context->getOptimizerContext();
    auto rpc_port = server.listen_address().port;
    optimizer_context->setRPCPort(rpc_port);
    std::vector<std::thread> thread_executors;
    for (int i = 0; i < THREAD_COUNT; i++)
    {
        String initial_query_id = "query" + std::to_string(i);
        UInt16 port = 6666 + i;
        auto coordinator_address = std::make_shared<AddressInfo>("localhost", port, "test", "123456", optimizer_context->getRPCPort());
        std::thread thread_executor(planExecutor, initial_query_id, i, coordinator_address, true, rpc_port);
        thread_executors.push_back(std::move(thread_executor));
    }
    for (auto & th : thread_executors)
        th.join();
    ASSERT_EQ(optimizer_context->getPlanSegmentProcessList()->size(), 0);
}

/*
TEST_F(PlanSegmentExecutorTest, ConcurrentWithSameIdSameAddr)
{
    auto context = getContext().context;
    context->setSetting("max_concurrent_queries_for_user", Field(100));

    std::vector<std::thread> thread_executors;
    for (int i = 0; i < 200; i++)
    {
        String initial_query_id = "PlanSegmentExecutor_test_";
        UInt16 port = 6666;
        AddressInfo coordinator_address("localhost", port, "test", "123456");
        std::thread thread_executor(planExecutor, initial_query_id, i, coordinator_address, true);
        thread_executors.push_back(std::move(thread_executor));
    }

    for (auto & th : thread_executors)
    {
        th.join();
    }

    ASSERT_EQ(context->getOptimizerContext()->getPlanSegmentProcessList()->size(), 0);
}

TEST_F(PlanSegmentExecutorTest, ConcurrentWithReplacingRunningQuery)
{
    auto context = getContext().context;
    context->setSetting("max_concurrent_queries_for_user", Field(100));
    context->setSetting("replace_running_query", Field(1));

    std::vector<std::thread> thread_executors;
    for (int i = 0; i < 200; i++)
    {
        String initial_query_id = "PlanSegmentExecutor_test_";
        UInt16 port = 6666;
        AddressInfo coordinator_address("localhost", port, "test", "123456");
        std::thread thread_executor(planExecutor, initial_query_id, i, coordinator_address, true);
        thread_executors.push_back(std::move(thread_executor));
    }

    for (auto & th : thread_executors)
    {
        th.join();
    }

    ASSERT_EQ(context->getOptimizerContext()->getPlanSegmentProcessList()->size(), 0);
}

TEST_F(PlanSegmentExecutorTest, ConcurrentWithRandomReplacingRunningQuery)
{
    auto context = getContext().context;
    context->setSetting("max_concurrent_queries_for_user", Field(100));
    context->setSetting("replace_running_query", Field(1));

    std::vector<std::thread> thread_executors;
    for (int i = 0; i < 200; i++)
    {
        String initial_query_id = "PlanSegmentExecutor_test_" + std::to_string(i % 10);
        UInt16 port = 6666 + i % 10;
        AddressInfo coordinator_address("localhost", port, "test", "123456");
        std::thread thread_executor(planExecutor, initial_query_id, coordinator_address);
        thread_executors.push_back(std::move(thread_executor));
    }

    for (auto & th : thread_executors)
    {
        th.join();
    }

    ASSERT_EQ(context->getOptimizerContext()->getPlanSegmentProcessList()->size(), 0);
}
*/
}
