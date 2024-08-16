#pragma once

//#include <unordered_set>
#include <Poco/Exception.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Storages/Consensus/RaftConnection.h>
#include <Storages/Consensus/RaftConnectionStats.h>

namespace DB
{

using Poco::Net::StreamSocket;
using Poco::AutoPtr;
using Poco::Thread;
using Poco::FIFOBuffer;
using Poco::Logger;

using namespace Poco::Net;

class RaftDispatcher;
using RaftDispatcherPtr = std::shared_ptr<RaftDispatcher>;

using ReadBufferSocketPtr = std::shared_ptr<ReadBufferFromPocoSocket>;
using WriteBufferSocketPtr = std::shared_ptr<WriteBufferFromPocoSocket>;

class RaftConnectionHandler : public Poco::Net::TCPServerConnection
{
public:
    static void registerConnection(RaftConnectionHandler * conn);
    static void unregisterConnection(RaftConnectionHandler * conn);
    static void dumpConnections(WriteBufferFromOwnString & buf);
    static void resetConnsStats();

private:
    static std::mutex conns_mutex;
    /// all connections
    static std::unordered_set<RaftConnectionHandler *> connections;

public:
    RaftConnectionHandler(
        const Poco::Util::AbstractConfiguration & config_ref,
        ContextMutablePtr & context_,
        Poco::Timespan receive_timeout_,
        Poco::Timespan send_timeout_,
        const Poco::Net::StreamSocket & socket_);
    void run() override;

    RaftConnectionStats & getConnectionStats();
    void dumpStats(WriteBufferFromOwnString & buf);
    void resetStats();

    ~RaftConnectionHandler() override;

private:
    void runImpl();
    bool poll(size_t timeout_microseconds);
    bool sendHandshake(ServerRequestPtr & request);
    void sendResponse(const ServerResponse & response);
    void receiveRequest(ServerRequestPtr & request);
    void receiveData(ServerRequestPtr & request);

private:
    Poco::Logger * log;
    ContextMutablePtr & global_context;

    /// Streams for reading/writing from/to client connection socket.
    ReadBufferSocketPtr in;
    WriteBufferSocketPtr out;

    Poco::Event event;

    Poco::Timespan operation_timeout;
    Poco::Timespan send_timeout;
    Poco::Timespan receive_timeout;

    std::atomic<bool> connected{false};
    std::atomic<bool> cancel{false};

    RaftConnectionStats conn_stats;
};

}
