#include <memory>
#include <Processors/Chunk.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Query/Exchange/DataTrans/DataTrans_fwd.h>
#include <Query/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Query/Exchange/DataTrans/IBroadcastSender.h>
#include <Query/Exchange/DataTrans/LocalBroadcastChannel.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Query/tests/gtest_common.h>

namespace UnitTest
{
using namespace DB;

TEST(ExchangeLocalBroadcastTest, LocalBroadcastRegistryTest)
{
    auto tp = getDeltaTimePoint(1000);
    LocalChannelOptions options{10, tp, false};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getInitContext();
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    local_sender->becomeRealSender(channel);

    ASSERT_TRUE(BroadcastSenderProxyRegistry::instance().countProxies() == 1);

    local_sender.reset();
    ASSERT_TRUE(BroadcastSenderProxyRegistry::instance().countProxies() == 0);
}


TEST(ExchangeLocalBroadcastTest, NormalSendRecvTest)
{
    auto tp = getDeltaTimePoint(1000);
    LocalChannelOptions options{10, tp, false};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getInitContext();
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    local_sender->becomeRealSender(channel);
    BroadcastReceiverPtr local_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(channel);

    Chunk chunk = createUInt8Chunk(10, 10, 8);
    auto total_bytes = chunk.bytes();
    setQueryDuration(context);
    BroadcastStatus status = local_sender->send(std::move(chunk));
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);

    RecvDataPacket recv_res = local_receiver->recv(100);
    ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
    Chunk & recv_chunk = std::get<Chunk>(recv_res);
    ASSERT_TRUE(recv_chunk.getNumRows() == 10);
    ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
}

TEST(ExchangeLocalBroadcastTest, SendTimeoutTest)
{
    auto tp = getDeltaTimePoint(200);
    LocalChannelOptions options{1, tp, false};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getInitContext();
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    local_sender->becomeRealSender(channel);
    BroadcastReceiverPtr local_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(channel);

    Chunk chunk = createUInt8Chunk(10, 10, 8);
    setQueryDuration(context);
    BroadcastStatus status = local_sender->send(chunk.clone());
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    BroadcastStatus timeout_status = local_sender->send(chunk.clone());
    ASSERT_TRUE(timeout_status.code == BroadcastStatusCode::SEND_TIMEOUT);
    ASSERT_TRUE(timeout_status.is_modified_by_operator == true);
}

TEST(ExchangeLocalBroadcastTest, AllSendDoneTest)
{
    auto tp = getDeltaTimePoint(1000);
    LocalChannelOptions options{10, tp, false};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getInitContext();
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest(1));
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    local_sender->becomeRealSender(channel);
    BroadcastReceiverPtr local_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(channel);

    Chunk chunk = createUInt8Chunk(10, 10, 8);
    auto total_bytes = chunk.bytes();

    setQueryDuration(context);
    ASSERT_TRUE(local_sender->send(chunk.clone()).code == BroadcastStatusCode::RUNNING);
    ASSERT_TRUE(local_sender->send(chunk.clone()).code == BroadcastStatusCode::RUNNING);
    local_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "Test graceful close");

    ASSERT_TRUE(std::get<Chunk>(local_receiver->recv(100)).bytes() == total_bytes);
    ASSERT_TRUE(std::get<Chunk>(local_receiver->recv(100)).bytes() == total_bytes);

    /// after consume all data, receiver get the ALL_SENDER_DONE status;
    RecvDataPacket res = local_receiver->recv(100);
    ASSERT_TRUE(std::holds_alternative<BroadcastStatus>(res));

    BroadcastStatus & final_status = std::get<BroadcastStatus>(res);
    ASSERT_TRUE(final_status.code == BroadcastStatusCode::ALL_SENDERS_DONE);
    ASSERT_TRUE(final_status.is_modified_by_operator == false);
}

}
