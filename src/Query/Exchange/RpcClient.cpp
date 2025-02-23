#include "RpcClient.h"
#include <errno.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <fmt/core.h>
#include <Common/Exception.h>

namespace DB
{

RpcClient::RpcClient(String host_port_, std::function<void()> report_err_, brpc::ChannelOptions * options)
    : log(getLogger("RpcClient"))
    , host_port(std::move(host_port_))
    , report_err(std::move(report_err_))
    , brpc_channel(std::make_unique<brpc::Channel>())
{
    initChannel(*brpc_channel, host_port, options);
}

void RpcClient::checkAliveWithController(const brpc::Controller & cntl) noexcept
{
    if (cntl.Failed())
    {
        auto err = cntl.ErrorCode();
        if (err == ECONNREFUSED || err == ECONNRESET || err == ENOTCONN)
            setOk(false);
        else if (err == EHOSTDOWN || err == ENETUNREACH)
            reportError();
    }
    else
    {
        setOk(true);
    }
}

void RpcClient::assertController(const brpc::Controller & cntl, int error_code)
{
    if (cntl.Failed())
    {
        auto err = cntl.ErrorCode();
        if (err == ECONNREFUSED || err == ECONNRESET)
            setOk(false);
        else if (err == EHOSTDOWN || err == ENETUNREACH || err == ENOTCONN)
            reportError();
        throw Exception(error_code, "Fail to call {}, error code: {}, msg: {}", cntl.method()->full_name(), err, cntl.ErrorText());
    }
    else
    {
        setOk(true);
    }
}

void RpcClient::initChannel(brpc::Channel & channel_, const String host_port_, brpc::ChannelOptions * options)
{
    if (0 != channel_.Init(host_port_.c_str(), options))
        throw Exception(ErrorCodes::BRPC_CANNOT_INIT_CHANNEL, "Failed to initialize RPC channel to {}", host_port_);

    LOG_TRACE(log, "Create rpc channel listening on : {}", host_port_);
}

}
