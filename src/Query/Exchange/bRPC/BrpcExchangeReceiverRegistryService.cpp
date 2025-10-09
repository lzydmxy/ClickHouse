#include "BrpcExchangeReceiverRegistryService.h"
#include <cstdint>
#include <memory>
#include <sstream>
#include <brpc/stream.h>
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/ClientInfo.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/Exchange/ExchangeDataKey.h>
#include <Query/Exchange/ExchangeUtils.h>
#include <Query/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Query/Exchange/bRPC/AsyncRegisterResult.h>
#include <Query/Exchange/bRPC/BrpcRemoteBroadcastSender.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BRPC_EXCEPTION;
}

void BrpcExchangeReceiverRegistryService::registry(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::RegistryRequest * request,
    ::DB::Protos::RegistryResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::StreamId sender_stream_id = brpc::INVALID_STREAM_ID;
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    auto key = std::make_shared<ExchangeDataKey>(request->query_unique_id(), request->exchange_id(), request->parallel_id());
    BroadcastSenderProxyPtr sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(key);
    auto query_id = request->query_id();
    auto coordinator_addr = request->coordinator_address();
    /// SCOPE_EXIT wrap logic which run after done->Run(),
    /// since host socket of the accpeted stream is set in done->Run()
    SCOPE_EXIT({
        /// request is already released in done_guard, so all parameters have to be copied.
        if (sender_proxy && sender_stream_id != brpc::INVALID_STREAM_ID)
            registerSenderToProxy(nullptr, sender_proxy, query_id, sender_stream_id, {}, key, coordinator_addr, false);
    });
    /// this done_guard guarantee to call done->Run() in any situation
    brpc::ClosureGuard done_guard(done);
    auto accept_timeout_ms = request->wait_timeout_ms();
    LOG_TRACE(getLogger("BrpcExchangeReceiverRegistryService"), "registry key {} query {}", key->toString(), query_id);
    acceptStream(cntl, accept_timeout_ms, sender_proxy, request->query_id(), sender_stream_id);
}

// void BrpcExchangeReceiverRegistryService::registerSenderFromDisk(
//     ::google::protobuf::RpcController * controller,
//     const ::DB::Protos::RegistryDiskSenderRequest * request,
//     ::DB::Protos::RegistryResponse * /*response*/,
//     ::google::protobuf::Closure * done)
// {
//     brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
//     ExchangeDataKeyPtr key;
//     try
//     {
//         String trace_log = fmt::format("registerSenderFromDisk for key:{} query:{} ", *key, request->registry().query_id());
//         LOG_TRACE(log, "{}", trace_log);
//     }
//     catch (...)
//     {
//         String error_msg = fmt::format("registerSenderFromDisk failed for key:{} query:{} ", *key, request->registry().query_id());
//         LOG_ERROR(log, "{}", error_msg);
//         cntl->SetFailed(error_msg);
//     }
// }


void BrpcExchangeReceiverRegistryService::registerSenderToProxy(
    const DiskExchangeDataManagerPtr & mgr,
    const BroadcastSenderProxyPtr & sender_proxy,
    const String & query_id,
    const brpc::StreamId & sender_stream_id,
    Processors processors,
    const ExchangeDataKeyPtr & key,
    const String & coordinator_addr,
    bool /*read_from_disk*/) // Not supported BSP mode
{
    try
    {
        auto real_sender = std::dynamic_pointer_cast<IBroadcastSender>(std::make_shared<BrpcRemoteBroadcastSender>(
            sender_proxy->getDataKey(), sender_stream_id, sender_proxy->getContext(), sender_proxy->getHeader()));
        sender_proxy->becomeRealSender(std::move(real_sender));
    }
    catch (...)
    {
        brpc::StreamClose(sender_stream_id);
        LOG_ERROR(log, "registerSenderToProxy failed for query_id:{} key:{} by exception: {}", query_id, *key, getCurrentExceptionMessage(false));
    }
}

void BrpcExchangeReceiverRegistryService::acceptStream(
    brpc::Controller * cntl,
    uint64_t accept_timeout_ms,
    BroadcastSenderProxyPtr sender,
    const String & query_id,
    brpc::StreamId & sender_stream_id)
{
    brpc::StreamOptions stream_options;
    stream_options.max_buf_size = max_buf_size;
    auto key = sender->getDataKey();
    LOG_TRACE(log, "acceptStream, key {}, query {}", key->toString(), query_id);
    try
    {
        sender->waitAccept(accept_timeout_ms);
        if (brpc::StreamAccept(&sender_stream_id, *cntl, &stream_options) != 0)
        {
            sender_stream_id = brpc::INVALID_STREAM_ID;
            String error_msg = "Fail to accept stream " + key->toString() + " for query " + query_id;
            LOG_ERROR(log, "{}", error_msg);
            cntl->SetFailed(error_msg);
        }
    }
    catch (...)
    {
        String error_msg
            = "Create stream " + key->toString() + " for query " + query_id + " failed by exception: " + getCurrentExceptionMessage(false);
        LOG_ERROR(log, "{}", error_msg);
        cntl->SetFailed(error_msg);
    }
}

void BrpcExchangeReceiverRegistryService::cleanupExchangeData(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::CleanupExchangeDataRequest * request,
    ::DB::Protos::CleanupExchangeDataResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        LOG_TRACE(log, "submit cleanup task for query_unique_id:{} successfully", request->query_unique_id());
    }
    catch (...)
    {
        auto error_msg = fmt::format("submit cleanup exchange data failed for query_unique_id:{}", request->query_unique_id());
        tryLogCurrentException(log, error_msg);
        cntl->SetFailed(error_msg);
    }
}

void BrpcExchangeReceiverRegistryService::sendExchangeDataHeartbeat(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ExchangeDataHeartbeatRequest * request,
    ::DB::Protos::ExchangeDataHeartbeatResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        std::stringstream ss;
        for (const auto & info : request->infos())
            ss << "query_unique_id:" << info.query_unique_id() << " query_id:" << info.query_id() << std::endl;
        auto trace_msg = fmt::format("sendExchangeFileHeartbeat exchange data for queries:{}", ss.str());
        LOG_TRACE(log, "{}", trace_msg);
    }
    catch (...)
    {
        std::stringstream ss;
        for (const auto & info : request->infos())
            ss << "query_unique_id:" << info.query_unique_id() << " query_id:" << info.query_id() << std::endl;
        auto error_msg = fmt::format("sendExchangeFileHeartbeat exchange data failed for queries:{}", ss.str());
        tryLogCurrentException(log, error_msg);
        cntl->SetFailed(error_msg);
    }
}
}
