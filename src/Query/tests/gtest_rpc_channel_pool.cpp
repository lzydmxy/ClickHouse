#include <array>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/tests/gtest_global_context.h>
#include <Query/Exchange/RpcChannelPool.h>
#include <Query/Exchange/bRPC/BrpcExchangeReceiverRegistryService.h>

using namespace DB;

class RPCchannelPoolTest : public ::testing::Test
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
        if (server.Start(8002, &options) != 0)
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

    static void TearDownTestCase()
    {
        server.Stop(1000);
    }
};

brpc::Server RPCchannelPoolTest::server;
BrpcExchangeReceiverRegistryService RPCchannelPoolTest::service_impl(73400320);

const uint16_t THREAD_NUM = 6;
const uint16_t LOOP = 10;

void get_client(
    size_t loop,
    const std::string & address,
    const std::string & client_type,
    bool check_pool_expire_timer,
    bool construct_random_exceptions)
{
    for (int i = 0; i < loop; i++)
    {
        auto client = RpcChannelPool::getInstance().getClient(address, client_type);
        ASSERT_TRUE(client);
        if (check_pool_expire_timer)
        {
            if (i % 9 == 0)
                sleep(3);
        }
        if (construct_random_exceptions)
        {
            if (i % 5 == 0)
                client->setOk(false);
            if (i % 9 == 0)
                client->reportError();
        }
    }
}

TEST_F(RPCchannelPoolTest, single_address_concurrent)
{
    std::vector<std::thread> thread_get_clients;
    for (int i = 0; i < THREAD_NUM; i++)
    {
        std::thread thread_get_client(get_client, LOOP, "127.0.0.1:8001", BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, false, false);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}

TEST_F(RPCchannelPoolTest, multi_address_concurrent)
{
    std::array<std::string, 2> client_types
        = {BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY};
    std::vector<std::thread> thread_get_clients;

    for (int i = 0; i < THREAD_NUM; i++)
    {
        auto address = "127.0.0.1:80" + std::to_string(i % THREAD_NUM);
        std::thread thread_get_client(get_client, LOOP, address, client_types[i % 2], false, false);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}

TEST_F(RPCchannelPoolTest, check_pool_expire_timer_concurrent)
{
    RpcChannelPool::getInstance().initPoolExpireTimer(1, 1);
    std::array<std::string, 2> client_types
        = {BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY};
    std::vector<std::thread> thread_get_clients;

    for (int i = 0; i < THREAD_NUM; i++)
    {
        auto address = "127.0.0.1:80" + std::to_string(i % THREAD_NUM);
        std::thread thread_get_client(get_client, LOOP, address, client_types[i % 2], true, false);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}

TEST_F(RPCchannelPoolTest, construct_random_exceptions)
{
    std::array<std::string, 2> client_types
        = {BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY};
    std::vector<std::thread> thread_get_clients;

    for (int i = 0; i < THREAD_NUM; i++)
    {
        auto address = "127.0.0.1:80" + std::to_string(i % THREAD_NUM);
        std::thread thread_get_client(get_client, LOOP, address, client_types[i % 2], false, true);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}

TEST_F(RPCchannelPoolTest, both_expire_exception)
{
    RpcChannelPool::getInstance().initPoolExpireTimer(1, 1);
    std::array<std::string, 2> client_types
        = {BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY};
    std::vector<std::thread> thread_get_clients;
    for (int i = 0; i < THREAD_NUM; i++)
    {
        auto address = "127.0.0.1:80" + std::to_string(i % THREAD_NUM);
        std::thread thread_get_client(get_client, LOOP, address, client_types[i % 2], true, true);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}
