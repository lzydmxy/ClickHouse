#pragma once
#include <functional>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BRPC_CANNOT_INIT_CHANNEL;
    extern const int BRPC_EXCEPTION;
}
class RpcClient : private boost::noncopyable
{
public:
    RpcClient(String host_port_, std::function<void()> report_err_, brpc::ChannelOptions * options = nullptr);
    ~RpcClient() = default;

    const auto & getAddress() const { return host_port; }
    bool ok() const { return ok_.load(std::memory_order_relaxed); }
    void setOk(bool ok) { ok_.store(ok, std::memory_order_relaxed); }
    void reportError() { report_err(); }

    void checkAliveWithController(const brpc::Controller & cntl) noexcept;

    auto & getChannel() { return *brpc_channel; }

    void assertController(const brpc::Controller & cntl, int error_code = ErrorCodes::BRPC_EXCEPTION);

protected:
    void initChannel(brpc::Channel & channel_, const String host_port_, brpc::ChannelOptions * options = nullptr);

    LoggerPtr log;
    String host_port;
    std::function<void()> report_err;

    std::unique_ptr<brpc::Channel> brpc_channel;
    std::atomic_bool ok_{true};
};

}
