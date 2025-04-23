#include <memory>
#include <string>
#include <thread>
#include <stdlib.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Util/MapConfiguration.h>
#include <base/types.h>
#include <base/scope_guard.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Query/Processors/QueryPlan/RemoteExchangeSourceStepExt.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Executor/PlanSegmentExecutor.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/DataTrans/LocalBroadcastChannel.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Processors/QueryPlan/BuildQueryPipelineSettingsExt.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Query/tests/gtest_common.h>

using namespace DB;

namespace UnitTest
{

TEST(ExchangeSourceStepTest, PipelineOneInput)
{
    // auto log = getLogger("ExchangeSourceStepTest");
    // auto context = getInitContext();
    auto context = Context::createCopy(getInitContext());
    auto optimizer_context = context->getOptimizerContext();
    UInt64 query_tx_id = 666;
    
    AddressInfo local_address("localhost", 0, "test", "123456");
    optimizer_context->setTransactionID(query_tx_id);
    optimizer_context->setPlanSegmentInstanceID({1, 1});

    auto & client_info = context->getClientInfo();
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId("RemoteExchangeSourceStep_test");
    plan_segment.setPlanSegmentId(2);

    client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    client_info.current_user = "test";
    client_info.initial_query_id = plan_segment.getQueryId();

    auto coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456");
    auto coordinator_address_str = extractExchangeHostPort(*(coordinator_address.get()));
    plan_segment.setCoordinatorAddress(*(coordinator_address.get()));
    optimizer_context->setCoordinatorAddress(coordinator_address);

    setQueryDuration(context);
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
    
    PlanSegmentInputs inputs;
    for (int i = 1; i <= 1; ++i)
    {
        auto input = std::make_shared<PlanSegmentInput>(header, RIPlanSegment::EXCHANGE);
        input->setExchangeParallelSize(1);
        input->setPlanSegmentId(1);
        input->setExchangeId(i);
        input->insertSourceAddress(local_address);
        inputs.push_back(input);
    }

    DataStream datastream{.header = header};
    RemoteExchangeSourceStepExt exchange_source_step(inputs, datastream, false, false);
    exchange_source_step.setPlanSegment(&plan_segment, context);

    auto tp = getDeltaTimePoint(1000);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp, .send_threshold_in_bytes = 0};
    exchange_source_step.setExchangeOptions(exchange_options);

    auto data_key_1 = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 1);
    BroadcastSenderProxyPtr local_sender_1 = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key_1);
    local_sender_1->accept(context, header);

    QueryPipelineBuilder builder;
    exchange_source_step.initializePipeline(builder, BuildQueryPipelineSettingsExt::fromContext(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PlanSegmentExecutor::registerAllExchangeReceivers(getLogger("PlanSegmentExecutor"), pipeline, 1000);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    for (int i = 0; i < 3; i++)
    {
        auto status = local_sender_1->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    for (int i = 0; i < 3; i++)
    {
        ASSERT_TRUE(executor.pull(pull_chunk));
        ASSERT_TRUE(pull_chunk.getNumRows() == 10);
        ASSERT_TRUE(pull_chunk.bytes() == total_bytes);
    }
    executor.cancel();
}

TEST(ExchangeSourceStepTest, PipelineMultiInput)
{
    auto log = getLogger("ExchangeSourceStepTest");
    auto context = getInitContext();
    auto optimizer_context = context->getOptimizerContext();
    UInt64 query_tx_id = 666;
    // auto context = Context::createCopy(global_context);
    AddressInfo local_address("localhost", 0, "test", "123456");
    optimizer_context->setTransactionID(query_tx_id);
    optimizer_context->setPlanSegmentInstanceID({1, 1});

    auto & client_info = context->getClientInfo();
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId("RemoteExchangeSourceStep_test");
    plan_segment.setPlanSegmentId(2);

    client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    client_info.current_user = "test";
    client_info.initial_query_id = plan_segment.getQueryId();

    auto coordinator_address = std::make_shared<AddressInfo>("localhost", 8888, "test", "123456");
    auto coordinator_address_str = extractExchangeHostPort(*(coordinator_address.get()));
    plan_segment.setCoordinatorAddress(*(coordinator_address.get()));
    optimizer_context->setCoordinatorAddress(coordinator_address);

    setQueryDuration(context);
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    PlanSegmentInputs inputs;
    for (int i = 1; i <= 2; ++i)
    {
        auto input = std::make_shared<PlanSegmentInput>(header, RIPlanSegment::EXCHANGE);
        input->setExchangeParallelSize(1);
        input->setPlanSegmentId(1);
        input->setExchangeId(i);
        input->insertSourceAddress(local_address);
        inputs.push_back(input);
    }

    DataStream datastream{.header = header};
    RemoteExchangeSourceStepExt exchange_source_step(inputs, datastream, false, false);
    exchange_source_step.setPlanSegment(&plan_segment, context);

    auto tp = getDeltaTimePoint(1000);
    ExchangeOptions exchange_options{.exchange_timeout_ts = tp, .send_threshold_in_bytes = 0};
    exchange_source_step.setExchangeOptions(exchange_options);

    auto data_key_1 = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 1);
    BroadcastSenderProxyPtr local_sender_1 = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key_1);
    local_sender_1->accept(context, header);

    auto data_key_2 = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 1);
    BroadcastSenderProxyPtr local_sender_2 = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key_2);
    local_sender_2->accept(context, header);

    QueryPipelineBuilder builder;
    exchange_source_step.initializePipeline(builder, BuildQueryPipelineSettingsExt::fromContext(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PlanSegmentExecutor::registerAllExchangeReceivers(getLogger("PlanSegmentExecutor"), pipeline, 1000);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    auto sender_func = [&]() {
        auto st = local_sender_1->send(chunk.clone());
        ASSERT_TRUE(st.code == BroadcastStatusCode::RUNNING);
        local_sender_1->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "");
        st = local_sender_2->send(chunk.clone());
        ASSERT_TRUE(st.code == BroadcastStatusCode::RUNNING);
        local_sender_2->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "");
    };

    std::thread thread(std::move(sender_func));

    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    for(int i = 0; i < 2; i++)
    {
        LOG_TRACE(log, "Begin pull {}", i + 1);
        ASSERT_TRUE(executor.pull(pull_chunk));
        ASSERT_TRUE(pull_chunk.getNumRows() == 10);
        ASSERT_TRUE(pull_chunk.bytes() == total_bytes);
    }
    executor.cancel();
}

}
