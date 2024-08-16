#pragma once

#include <Storages/Consensus/RaftConnectionHandler.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Net/NetException.h>
#include <Common/logger_useful.h>
#include <Server/IServer.h>
#include <string>

namespace DB
{

using ConfigGetter = std::function<const Poco::Util::AbstractConfiguration & ()>;

class RaftConnectionHandlerFactory : public TCPServerConnectionFactory
{
private:
    ConfigGetter config_getter;
    ContextMutablePtr & global_context;
    Poco::Logger * log;
    Poco::Timespan receive_timeout;
    Poco::Timespan send_timeout;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    RaftConnectionHandlerFactory(
        ConfigGetter config_getter_,
        ContextMutablePtr & context_,
        uint64_t receive_timeout_seconds,
        uint64_t send_timeout_seconds,
        bool secure)
        : config_getter(config_getter_)
        , global_context(context_)
        , log(&Poco::Logger::get(std::string{"RaftTCP"} + (secure ? "S" : "") + "HandlerFactory"))
        , receive_timeout(/* seconds = */ receive_timeout_seconds, /* microseconds = */ 0)
        , send_timeout(/* seconds = */ send_timeout_seconds, /* microseconds = */ 0)
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer &) override
    {
        try
        {
            LOG_TRACE(log, "Raft request. Address: {}", socket.peerAddress().toString());
            return new RaftConnectionHandler(config_getter(), global_context, receive_timeout, send_timeout, socket);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
