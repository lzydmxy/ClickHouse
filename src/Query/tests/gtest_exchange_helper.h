#pragma once

// #include <ServiceDiscovery/IServiceDiscovery.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <Poco/Util/MapConfiguration.h>
#include <Query/Exchange/bRPC/BrpcApplication.h>
#include <Common/tests/gtest_global_context.h>
#include <Query/tests/gtest_common.h>
#include <Query/Exchange/bRPC/BrpcExchangeReceiverRegistryService.h>

const int brpc_server_port = 8001;

class ExchangeRemoteTest : public ::testing::Test
{
protected:
    static brpc::Server * server;
    static DB::BrpcExchangeReceiverRegistryService * service_impl;
    static void startBrpcServer()
    {
        if (server->AddService(service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(ERROR) << "Fail to add service";
            return;
        }
        LOG(INFO) << "add service success";

        // Start the server.
        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        if (server->Start(brpc_server_port, &options) != 0)
        {
            LOG(ERROR) << "Fail to start Server";
            return;
        }
        LOG(INFO) << "start Server";
    }

    static void SetUpTestCase()
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
        DB::BrpcApplication::getInstance().initialize(*map_config);
        startBrpcServer();

        auto context = UnitTest::getInitContext();
        DB::SettingsChanges config_settings;
        config_settings.emplace_back("exchange_timeout_ms", 20000);
        config_settings.emplace_back("temporary_files_codec", "NONE");
        context->applySettingsChanges(config_settings);
        ExchangeRemoteTest::service_impl->setContext(context);
        UnitTest::setQueryDuration(context);
    }

    static void TearDownTestCase()
    {
        server->Stop(1000);
    }

    void SetUp() override
    {
    }

    uint64_t query_unique_id_1 = 111;
    uint64_t query_unique_id_2 = 222;
    uint64_t query_unique_id_3 = 333;
    uint64_t query_unique_id_4 = 444;
    String query_id = "query_id";
    uint64_t interval_ms = 10000;
    size_t rows = 7;
    UInt64 exchange_id = 1;
    UInt32 parallel_idx = 0;
    const String host = "localhost:6666";
    const String rpc_host = "127.0.0.1:8001";
    size_t write_segment_id = 0;
    size_t read_segment_id = 1;
};
