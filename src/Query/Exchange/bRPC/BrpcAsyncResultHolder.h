#pragma once
#include <memory>
#include <brpc/stream.h>
#include <Query/Exchange/RpcClient.h>

namespace DB
{
template <typename Request, typename Response>
struct BrpcAsyncResultHolder
{
    std::shared_ptr<RpcClient> channel;
    std::unique_ptr<brpc::Controller> cntl;
    std::shared_ptr<Request> request;
    std::unique_ptr<Response> response;
};
}
