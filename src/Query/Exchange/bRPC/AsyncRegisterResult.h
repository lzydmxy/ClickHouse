#pragma once
#include <memory>
#include <brpc/stream.h>
#include <Query/Protos/registry.pb.h>
#include <Query/Exchange/RpcClient.h>
#include <Query/Exchange/bRPC/BrpcAsyncResultHolder.h>

namespace DB
{
using AsyncRegisterResult = BrpcAsyncResultHolder<Protos::RegistryRequest, Protos::RegistryResponse>;
}

template <>
struct fmt::formatter<DB::Protos::RegistryRequest>
{
    constexpr auto parse(format_parse_context & ctx)
    {
        const auto it = ctx.begin();
        const auto end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("Invalid format for struct Protos::RegistryRequest");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::Protos::RegistryRequest & request, FormatContext & ctx)
    {
        return format_to(
            ctx.out(), "[{}_{}_{}-{}]", request.query_unique_id(), request.exchange_id(), request.parallel_id(), request.query_id());
    }
};
