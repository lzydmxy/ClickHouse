#include <memory>
#include <string>
#include <thread>

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/ExchangeMode.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentExecutor.h>
#include <Query/Executor/PlanSegmentInstance.h>
#include <Query/Executor/executePlanSegment.h>
#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/DataTrans/LocalBroadcastChannel.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Processors/IQueryPlanStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

#include <gtest/gtest.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

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

ExecutableFunctionPtr createRepartitionFunction(ContextPtr context, const ColumnsWithTypeAndName & arguments)
{
    tryRegisterFunctions();
    //const String repartition_func_name = "cityHash64V2";
    const String repartition_func_name = "cityHash64";
    auto & factory = FunctionFactory::instance();
    auto res = factory.tryGetImpl(repartition_func_name, context);
    FunctionOverloadResolverPtr func_builder = factory.get(repartition_func_name, context);
    FunctionBasePtr function_base = func_builder->build(arguments);
    return function_base->prepare(arguments);
}

Block createUInt64Block(size_t row_num, size_t column_num, UInt8 value)
{
    ColumnsWithTypeAndName cols;
    for (size_t i = 0; i < column_num; i++)
    {
        auto column = ColumnUInt64::create(row_num, value);
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), "column" + std::to_string(i));
    }
    return Block(cols);
}

class PlanSegmentExecutorTest : public testing::Test
{
protected:
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

TEST_F(PlanSegmentExecutorTest, ExecuteTest)
{
    initLogger();
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

    //todo: liyang453
    /*
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 2000 * 1000000;
    std::chrono::nanoseconds total_nanos(ts.tv_sec * 1000000000 + ts.tv_nsec);
    std::chrono::time_point<std::chrono::system_clock> tp(total_nanos);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp, .send_threshold_in_bytes = 0};
    */
    ExchangeOptions exchange_options;

    const String query_id = "PlanSegmentExecutor_test";
    const UInt64 query_tx_id = 12345;
    //context->setTemporaryTransaction(query_tx_id, query_tx_id, false);
    context->getOptimizerContext()->setPlanSegmentInstanceID({1,0});

    AddressInfo coordinator_address("localhost", 8888, "test", "123456");
    AddressInfo local_address("localhost", 0, "test", "123456");

    auto coordinator_address_str = extractExchangeHostPort(coordinator_address);
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto source_key = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 1;
    //plan_segment_instance->info.execution_address = local_address;
    plan_segment_instance->info.execution_address =  std::make_shared<AddressInfo>(local_address);

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
    exchange_source_step->setPlanSegment(plan_segment, context);
    exchange_source_step->setExchangeOptions(exchange_options);

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

    QueryPlanExt query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node), query_plan.getNodeId(&remote_node));
    plan_segment->setQueryPlan(std::move(query_plan));
    auto plan_segment_process_entry = context->getOptimizerContext()->getPlanSegmentProcessList()->insertGroup(context, plan_segment->getPlanSegmentId());
    plan_segment_instance->plan_segment = std::move(plan_segment);
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, std::move(plan_segment_process_entry), exchange_options);

    executor.execute();

    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == chunk.bytes());
    }
}

TEST_F(PlanSegmentExecutorTest, ExecuteAsyncTest)
{
    initLogger();
    const auto context = Context::createCopy(getContext().context);
    context->initializeOptimizerContext();
    context->getOptimizerContext()->setProcessListEntry(nullptr);

    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    //todo: liyang453, other feat
    /*
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 2000 * 1000000;
    ExchangeOptions exchange_options{.exchange_timeout_ts = ts};
    */
    ExchangeOptions exchange_options;

    const String query_id = "PlanSegmentExecutor_test";
    const UInt64 query_tx_id = 11111;
    //context->setTemporaryTransaction(query_tx_id, query_tx_id, false);
    context->getOptimizerContext()->setPlanSegmentInstanceID({1, 0});

    AddressInfo coordinator_address("localhost", 8888, "test", "123456");
    auto coordinator_address_str = extractExchangeHostPort(coordinator_address);
    AddressInfo local_address("localhost", 0, "test", "123456");

    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto source_key = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 0;
    //plan_segment_instance->info.execution_address = local_address;
    plan_segment_instance->info.execution_address =  std::make_shared<AddressInfo>(local_address);


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
    exchange_source_step->setPlanSegment(plan_segment, context);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    QueryPlanExt query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    //query_plan.addRoot(std::move(remote_node));
    query_plan.addRoot(std::move(remote_node), query_plan.getNodeId(&remote_node));
    plan_segment->setQueryPlan(std::move(query_plan));
    auto plan_segment_process_entry = context->getOptimizerContext()->getPlanSegmentProcessList()->insertGroup(context, plan_segment->getPlanSegmentId());
    plan_segment_instance->plan_segment = std::move(plan_segment);
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, std::move(plan_segment_process_entry), exchange_options);

    auto execute_func = [&]() { executor.execute(); };

    ThreadFromGlobalPool thread(std::move(execute_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_EQ(status.code, BroadcastStatusCode::RUNNING) << status.message;
    }

    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST_F(PlanSegmentExecutorTest, ExecuteCancelTest)
{
    initLogger();
    const auto context = Context::createCopy(getContext().context);
    context->initializeOptimizerContext();
    context->getOptimizerContext()->setProcessListEntry(nullptr);

    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    //todo: liyang453, other feat
    /*
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options{.exchange_timeout_ts = ts};
    */
    ExchangeOptions exchange_options;

    const String query_id = "PlanSegmentExecutor_test";
    const UInt64 query_tx_id = 11111;
    //context->setTemporaryTransaction(query_tx_id,query_tx_id,false);
    context->getOptimizerContext()->setPlanSegmentInstanceID({1, 0});

    AddressInfo coordinator_address("localhost", 8888, "test", "123456");
    AddressInfo local_address("localhost", 0, "test", "123456");

    auto coordinator_address_str = extractExchangeHostPort(coordinator_address);
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto source_key = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 0;
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
    exchange_source_step->setPlanSegment(plan_segment, context);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    QueryPlanExt query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node), query_plan.getNodeId(&remote_node));
    plan_segment->setQueryPlan(std::move(query_plan));
    auto plan_segment_process_entry = context->getOptimizerContext()->getPlanSegmentProcessList()->insertGroup(context, plan_segment->getPlanSegmentId());
    plan_segment_instance->plan_segment = std::move(plan_segment);
    // buffer will flush when row_num reached to send_threshold_in_row_num
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, std::move(plan_segment_process_entry), exchange_options);

    auto execute_func = [&]() { executor.execute(); };

    ThreadFromGlobalPool thread(std::move(execute_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    for (int i = 0; i < 2; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(5000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }

    CancellationCode code = CancellationCode::NotFound;
    int max_time = 100;
    for (; code == CancellationCode::NotFound; code = context->getOptimizerContext()->getPlanSegmentProcessList()->tryCancelPlanSegmentGroup(query_id))
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
}

void planExecutor(String query_id, AddressInfo coordinator_address)
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
        auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
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
        exchange_source_step->setPlanSegment(plan_segment, context);
        exchange_source_step->setExchangeOptions(exchange_options);

        QueryPlanExt query_plan;
        QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
        query_plan.addRoot(std::move(remote_node), query_plan.getNodeId(&remote_node));
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
    auto context = getContext().context;
    context->setSetting("max_concurrent_queries_for_user", Field(100));

    std::vector<std::thread> thread_executors;
    for (int i = 0; i < 200; i++)
    {
        String initial_query_id = "PlanSegmentExecutor_test_" + std::to_string(i);
        UInt16 port = 6666;
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

TEST_F(PlanSegmentExecutorTest, ConcurrentWithDiffIdDiffAddr)
{
    auto context = getContext().context;
    context->setSetting("max_concurrent_queries_for_user", Field(100));

    std::vector<std::thread> thread_executors;
    for (int i = 0; i < 200; i++)
    {
        String initial_query_id = "PlanSegmentExecutor_test_" + std::to_string(i);
        UInt16 port = 6666 + i;
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
        std::thread thread_executor(planExecutor, initial_query_id, coordinator_address);
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
        std::thread thread_executor(planExecutor, initial_query_id, coordinator_address);
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
}
