#pragma once
#include <bthread/bthread.h>
#include <brpc/server.h>
#include <Interpreters/Context.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterService.h>
#include <Query/Exchange/PlanSegmentRpcService.h>
#include <Query/Exchange/bRPC/BrpcExchangeReceiverRegistryService.h>
// #include <Statistics/OptimizerStatisticsService.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BRPC_EXCEPTION;
}
class BrpcServerHolder
{
public:
    BrpcServerHolder(String & host_port, ContextMutablePtr global_context, bool listen_try)
    {
        rpc_server = std::make_unique<brpc::Server>();
        brpc::ServerOptions options;
        if (global_context->getOptimizerContext()->getComplexQueryActive())
        {
            addService(BrpcExchangeReceiverRegistryService_RegisterService(global_context).service);
            addService(RuntimeFilterService_RegisterService(global_context).service);
            addService(PlanSegmentRpcService_RegisterService(global_context).service);
            // addService(*rpc_server, rpc_services, OptimizerStatisticsService_RegisterService(global_context).service);
        }

        if (rpc_server->Start(host_port.c_str(), &options) != 0)
        {
            start_success = false;
            if (listen_try)
                LOG_ERROR(getLogger("BrpcServerHolder"), "Failed tp start rpc server on {}", host_port);
            else
                throw Exception(ErrorCodes::BRPC_EXCEPTION, "Failed tp start rpc server on {}", host_port);
        }
    }

    void stop()
    {
        rpc_server->Stop(0);
    }

    void join()
    {
        rpc_server->Join();
    }

    bool available()
    {
        return start_success;
    }

private:
    void addService(std::unique_ptr<google::protobuf::Service> service)
    {
        rpc_server->AddService(service.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
        rpc_services.emplace_back(std::move(service));
    }
    bool start_success{true};
    std::vector<std::unique_ptr<::google::protobuf::Service>> rpc_services;
    std::unique_ptr<brpc::Server> rpc_server;
};

}
