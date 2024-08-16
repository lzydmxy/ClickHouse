#include <Poco/Net/NetException.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Storages/Consensus/RaftConnectionHandler.h>
#include <Storages/Consensus/RaftConnection.h>
#include <Storages/Consensus/RaftDispatcher.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
    extern const int READONLY;
}

RaftConnectionHandler::RaftConnectionHandler(
    const Poco::Util::AbstractConfiguration & config_ref,
    ContextMutablePtr & context_,
    Poco::Timespan receive_timeout_,
    Poco::Timespan send_timeout_,
    const Poco::Net::StreamSocket & socket_)
    : Poco::Net::TCPServerConnection(socket_)
    , log(&Poco::Logger::get("RaftConnectionHandler"))
    , global_context(context_)
    , operation_timeout(
          0,
          config_ref.getUInt(
              "raft_server.raft_configuration.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS) * 1000)
    , send_timeout(send_timeout_)
    , receive_timeout(receive_timeout_)
{
    RaftConnectionHandler::registerConnection(this);
}

RaftConnectionHandler::~RaftConnectionHandler()
{
    RaftConnectionHandler::unregisterConnection(this);
    try
    {
        cancel = true;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

std::mutex RaftConnectionHandler::conns_mutex;
std::unordered_set<RaftConnectionHandler *> RaftConnectionHandler::connections;

void RaftConnectionHandler::registerConnection(RaftConnectionHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.insert(conn);
}

void RaftConnectionHandler::unregisterConnection(RaftConnectionHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.erase(conn);
}

void RaftConnectionHandler::dumpConnections(WriteBufferFromOwnString & buf)
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->dumpStats(buf);
    }
}

void RaftConnectionHandler::resetConnsStats()
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->resetStats();
    }
}

bool RaftConnectionHandler::sendHandshake(ServerRequestPtr & request)
{
    ServerResponse response;
    response.protocol = PkgType::Handshake;
    response.accepted = true;
    if (request->version != RAFT_PROTOCOL_VERSION)
    {
        LOG_WARNING(log, "Ignoring user request, because the version is different");
        response.accepted = false;
    }

    if (!global_context->getRaftDispatcher()->isServerActive())
    {
        LOG_WARNING(log, "Ignoring user request, because the server is not active yet");
        response.accepted = false;
    }

    sendResponse(response);
    return response.accepted;
}

void RaftConnectionHandler::sendResponse(const ServerResponse & server_response)
{
    server_response.write(*out);
    out->next();
}

void RaftConnectionHandler::receiveRequest(ServerRequestPtr & request)
{
    event.reset();
    request->raft_request->finish_callback = [this] (const RaftResponsePtr & response)
    {
        ServerResponse server_response;
        server_response.accepted = true;
        server_response.protocol = PkgType::Forward;
        server_response.raft_response = response;
        sendResponse(server_response);
        event.set();
    };

    global_context->getRaftDispatcher()->putRequest(request->raft_request);

   //sync response
    while (true)
    {
        try
        {
            if (event.tryWait(operation_timeout.totalMicroseconds()))
                break;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            break;
        }
    }
}

void RaftConnectionHandler::receiveData(ServerRequestPtr & /*request*/)
{
}

bool RaftConnectionHandler::poll(size_t timeout_microseconds)
{
    return static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_microseconds);
}


void RaftConnectionHandler::run()
{
    runImpl();
}

void RaftConnectionHandler::runImpl()
{
    setThreadName("RaftHandler");
    ThreadStatus thread_status;

    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    while(!cancel)
    {
        while (!poll(receive_timeout.totalMicroseconds()))
            continue;

        if (in->eof())
        {
            LOG_WARNING(log, "Client has not sent any data.");
            break;
        }

        auto request = std::make_shared<ServerRequest>();
        request->read(*in);

        LOG_TRACE(log, "Read protocol {} from client", toString(request->protocol));

        if (request->protocol == PkgType::Handshake)
        {
            if (!sendHandshake(request))
            {
                LOG_WARNING(log, "Send hand shake failed");
                break;
            }
            request = std::make_shared<ServerRequest>();
            request->read(*in);
        }

        if (request->protocol == PkgType::Forward)
        {
            receiveRequest(request);
        }
        else if (request->protocol == PkgType::Data)
        {
            receiveData(request);
        }
    }
}

RaftConnectionStats & RaftConnectionHandler::getConnectionStats()
{
    return conn_stats;
}

void RaftConnectionHandler::dumpStats(WriteBufferFromOwnString & buf)
{
    if (!connected.load())
        return;

    auto & stats = getConnectionStats();
    writeText('[', buf);
    writeText(socket().peerAddress().toString(), buf);
    writeText(']', buf);
    stats.dump(buf);
    writeText('\n', buf);
}

void RaftConnectionHandler::resetStats()
{
    conn_stats.reset();
}

}
