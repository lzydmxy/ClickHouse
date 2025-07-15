#include "RPCHelpers.h"

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <brpc/controller.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOG_ERROR;
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
    extern const int BRPC_TIMEOUT;
    extern const int BRPC_EXCEPTION;
    extern const int BRPC_HOST_DOWN;
    extern const int BRPC_CONNECT_ERROR;
}

namespace RPCHelpers
{
    constexpr auto unepxected_rare_exception = "rare_exception";

    Exception getSerializableException()
    {
        try
        {
            throw;
        }
        catch (const Exception & e)
        {
            return *e.clone();
        }
        catch (const Poco::Exception & e)
        {
            return Exception(ErrorCodes::POCO_EXCEPTION, "{}", e.displayText());
        }
        catch (const std::exception & e)
        {
            return Exception(ErrorCodes::STD_EXCEPTION, "{}", e.what());
        }
        catch (...)
        {
            return Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception");
        }
    }

    void handleException(std::string * exception_str)
    {
        try
        {
            WriteBufferFromString out(*exception_str);
            writeException(getSerializableException(), out, false);
        }
        catch (...)
        {
            exception_str->assign(unepxected_rare_exception);
        }
    }

    [[noreturn]] void checkException(const std::string & exception_str)
    {
        if (exception_str == unepxected_rare_exception)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Service got a rare exception, but failed to send back");
        ReadBufferFromString in(exception_str);
        throw readException(in);
    }

    ContextMutablePtr createSessionContextForRPC(const ContextPtr & context, google::protobuf::RpcController & cntl_base)
    {
        auto & controller = static_cast<brpc::Controller &>(cntl_base);

        auto rpc_context = Context::createCopy(context);
        rpc_context->makeSessionContext();
        rpc_context->makeQueryContext();

        auto & client_info = rpc_context->getClientInfo();
        client_info.interface = ClientInfo::Interface::GRPC; //TODO: interface need add BRPC enum
        client_info.current_address = Poco::Net::SocketAddress(butil::endpoint2str(controller.remote_side()).c_str());
        client_info.initial_address = client_info.current_address;

        return rpc_context;
    }

    void assertController(const brpc::Controller & cntl)
    {
        if (!cntl.Failed())
            return;

        int err = cntl.ErrorCode();

        if (err == ECONNREFUSED || err == ECONNRESET || err == ENETUNREACH)
        {
            throw Exception(ErrorCodes::BRPC_CONNECT_ERROR, "{}:{}", err, cntl.ErrorText());
        }
        else if (err == EHOSTDOWN)
        {
            /// TODO: handle more error codes, temporarily remove EHOSTDOWN error https://github.com/apache/incubator-brpc/issues/936
            throw Exception(ErrorCodes::BRPC_HOST_DOWN, "{}:{}", err, cntl.ErrorText());
        }
        else if (err == brpc::Errno::ERPCTIMEDOUT)
        {
            throw Exception(ErrorCodes::BRPC_TIMEOUT, "{}:{}", err, cntl.ErrorText());
        }
        else /// Should we throw exception here to cover all other errors?
            throw Exception(ErrorCodes::BRPC_EXCEPTION, "{}:{}", err, cntl.ErrorText());
    }

    template <typename Resp>
    void onAsyncCallDone(Resp * response, brpc::Controller * cntl, ExceptionHandlerPtr handler)
    {
        try
        {
            std::unique_ptr<Resp> response_guard(response);
            std::unique_ptr<brpc::Controller> cntl_guard(cntl);
            RPCHelpers::assertController(*cntl);
            RPCHelpers::checkResponse(*response);
        }
        catch (...)
        {
            handler->setException(std::current_exception());
        }
    }

    void onAsyncCallDoneAssertControllerProgress(RProgressRequest * request, RProgressResponse * response,
        brpc::Controller * cntl, LoggerPtr logger, std::function<String()> construct_err_msg)
    {
        onAsyncCallDoneAssertController<RProgressRequest, RProgressResponse>(request, response, cntl, logger, construct_err_msg);
    }

    void onAsyncCallDoneAssertControllerStatus(RPlanSegmentStatusRequest * request, RPlanSegmentStatusResponse * response,
        brpc::Controller * cntl, LoggerPtr logger, std::function<String()> construct_err_msg)
    {
        onAsyncCallDoneAssertController<RPlanSegmentStatusRequest, RPlanSegmentStatusResponse>(request, response, cntl, logger, construct_err_msg);
    }

    void onAsyncCallDoneAssertControllerProfile(RPlanSegmentProfileRequest * request, RPlanSegmentProfileResponse * response,
        brpc::Controller * cntl, LoggerPtr logger, std::function<String()> construct_err_msg)
    {
        onAsyncCallDoneAssertController<RPlanSegmentProfileRequest, RPlanSegmentProfileResponse>(request, response, cntl, logger, construct_err_msg);
    }

    template <typename Resp>
    void onAsyncCallDoneWithFailedInfo(Resp * response, brpc::Controller * cntl, ExceptionHandlerWithFailedInfoPtr handler, const WorkerID worker_id)
    {
        int32_t error_code = 0;
        try
        {
            std::unique_ptr<Resp> response_guard(response);
            std::unique_ptr<brpc::Controller> cntl_guard(cntl);
            error_code = cntl->ErrorCode();
            RPCHelpers::assertController(*cntl);
            RPCHelpers::checkResponse(*response);
            handler->addHost(worker_id);
        }
        catch (...)
        {
            handler->addFailedRpc(worker_id, error_code);
            handler->setException(std::current_exception());
        }
    }

    template <typename Resp, typename Func>
    void serviceHandler(google::protobuf::Closure * done, Resp * resp, Func && f)
    {
        brpc::ClosureGuard done_guard(done);

        try
        {
            ThreadFromGlobalPool([func = std::forward<Func>(f)] { func(); }).detach();
            done_guard.release();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            RPCHelpers::handleException(resp->mutable_exception());
        }
    }
}

}
