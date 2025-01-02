#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Protos/runtime_filter.pb.h>
#include <brpc/server.h>
#include <brpc/stream.h>
#include <Common/Brpc/BrpcServiceDefines.h>

namespace DB
{
class RuntimeFilterService : public Protos::RuntimeFilterService
{
public:
    explicit RuntimeFilterService(ContextMutablePtr context_) : context(context_), log(getLogger("RuntimeFilterService")) { }

    /// transfer dynamic filer (segment executor host --> coordinator host)
    void transferRuntimeFilter(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::TransferRuntimeFilterRequest * request,
        ::DB::Protos::TransferRuntimeFilterResponse * response,
        ::google::protobuf::Closure * done) override;

    /// dispatch runtime Filter (coordinator host --> segment executor host)
    void dispatchRuntimeFilter(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::DispatchRuntimeFilterRequest * request,
        ::DB::Protos::DispatchRuntimeFilterResponse * response,
        ::google::protobuf::Closure * done) override;

private:
    ContextMutablePtr context;
    LoggerPtr log;
};

REGISTER_SERVICE_IMPL(RuntimeFilterService);
}
