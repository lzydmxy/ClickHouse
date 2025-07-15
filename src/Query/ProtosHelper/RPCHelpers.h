#pragma once
#include <butil/iobuf.h>
#include <Core/UUID.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/Logger.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Common/ExceptionHandler.h>
#include <Query/ProtosHelper/HostWithPorts.h>
#include <Query/ProtosHelper/QueryProto.h>

namespace google::protobuf
{
class RpcController;
}

namespace brpc
{
class Controller;
}

namespace DB
{

using IOBufPtr = std::shared_ptr<butil::IOBuf>;

namespace RPCHelpers
{
    inline UUID createUUID(const RUUID & uuid) { return UUID(UInt128{uuid.low(), uuid.high()}); }
    inline void fillUUID(UUID uuid, RUUID & pb_uuid)
    {
        pb_uuid.set_low(uuid.toUnderType().items[0]);
        pb_uuid.set_high(uuid.toUnderType().items[1]);
    }

    inline StorageID createStorageID(const RStorageID & id)
    {
        return StorageID(id.database(), id.table(), createUUID(id.uuid()));
    }
    inline void fillStorageID(const StorageID & id, RStorageID & pb_id)
    {
        pb_id.set_database(id.database_name);
        pb_id.set_table(id.table_name);
        fillUUID(id.uuid, *pb_id.mutable_uuid());
    }

    void handleException(std::string * exception_str);
    [[noreturn]] void checkException(const std::string & exception_str);

    template <class R>
    inline void checkResponse(const R & r)
    {
        if (r.has_exception())
            checkException(r.exception());
    }

    ContextMutablePtr createSessionContextForRPC(const ContextPtr & context, google::protobuf::RpcController & cntl_base);

    /// throw exception when cntl.Failed
    void assertController(const brpc::Controller & cntl);

    template <typename Resp>
    void onAsyncCallDone(Resp * response, brpc::Controller * cntl, ExceptionHandlerPtr handler);

    template <typename Req, typename Resp>
    void onAsyncCallDoneAssertController(Req * request, Resp * response, brpc::Controller * cntl, LoggerPtr logger,
        std::function<String()> construct_err_msg)
    {
        // request will be use in construct_err_msg
        std::unique_ptr<Req> request_guard(request);
        try
        {
            std::unique_ptr<Resp> response_guard(response);
            std::unique_ptr<brpc::Controller> cntl_guard(cntl);
            RPCHelpers::assertController(*cntl);
        }
        catch (...)
        {
            tryLogCurrentException(logger, construct_err_msg());
        }
    }

    void onAsyncCallDoneAssertControllerProgress(RProgressRequest * request, RProgressResponse * response,
            brpc::Controller * cntl, LoggerPtr logger, std::function<String()> construct_err_msg);

    void onAsyncCallDoneAssertControllerStatus(RPlanSegmentStatusRequest * request, RPlanSegmentStatusResponse * response,
        brpc::Controller * cntl, LoggerPtr logger, std::function<String()> construct_err_msg);

    void onAsyncCallDoneAssertControllerProfile(RPlanSegmentProfileRequest * request, RPlanSegmentProfileResponse * response,
        brpc::Controller * cntl, LoggerPtr logger, std::function<String()> construct_err_msg);

    template <typename Resp>
    void onAsyncCallDoneWithFailedInfo(Resp * response, brpc::Controller * cntl, ExceptionHandlerWithFailedInfoPtr handler, const WorkerID worker_id);

    template <typename Resp, typename Func>
    void serviceHandler(google::protobuf::Closure * done, Resp * resp, Func && f);
}

}
