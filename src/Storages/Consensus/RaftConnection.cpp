
#include <Poco/Net/NetException.h>
#include <IO/WriteHelpers.h>
#include <Storages/Consensus/RaftConnection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int SOCKET_TIMEOUT;
    extern const int NETWORK_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

std::shared_ptr<ServerResponse> ServerRequest::makeResponse()
{
    auto res =  std::make_shared<ServerResponse>();
    res->protocol = protocol;
    if (raft_request != nullptr)
        res->raft_response = raft_request->makeResponse();
    return res;
}

void RaftConnection::sendHandshake()
{
    ServerRequest request;
    request.protocol = PkgType::Handshake;
    request.write(*out);
    out->next();
}

void RaftConnection::receiveHandshake()
{
    ServerResponse response;
    receive(response);
    if (!response.accepted || response.protocol != PkgType::Handshake)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Receive hand shake error, accepted {}, protocol {}", response.accepted, toString(response.protocol));
}

void RaftConnection::connect(Poco::Timespan connection_timeout)
{
    if (connected)
        return;

    Poco::Net::SocketAddress address{endpoint};

    static constexpr size_t max_retries = 1;
    size_t retry_count = 0;
    while (retry_count <= max_retries)
    {
        try
        {
            LOG_TRACE(log, "Try connect {}, connection timeout {} ms, operation timeout {} ms",
                endpoint, connection_timeout.totalMilliseconds(), operation_timeout.totalMilliseconds());

            socket = Poco::Net::StreamSocket();
            socket.connect(address, connection_timeout);
            socket.setReceiveTimeout(operation_timeout);
            socket.setSendTimeout(operation_timeout);
            socket.setNoDelay(true);
            socket.setKeepAlive(true);

            in.emplace(socket);
            out.emplace(socket);

            connected = true;

            sendHandshake();
            receiveHandshake();

            break;
        }
        catch (Poco::Net::NetException & e)
        {
            disconnect();
            LOG_WARNING(log, "Got net exception connection {}, {}, retry count {}, {}",
                endpoint, address.toString(), retry_count, e.displayText());
            retry_count ++;
            if (retry_count > max_retries) throw e;
        }
        catch (Poco::TimeoutException & e)
        {
            disconnect();
            LOG_WARNING(log, "Got timeout exception connection {}, {}, retry count {}, {}",
                endpoint, address.toString(), retry_count, e.displayText());
            retry_count ++;
            if (retry_count > max_retries) throw e;
        }
        catch(Exception e)
        {
            disconnect();
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            retry_count ++;
            if (retry_count > max_retries) throw e;
        }
    }
}

void RaftConnection::disconnect()
{
    if (connected)
    {
        socket.close();
        connected = false;
        errno = 0;
    }
}

void RaftConnection::send(ServerRequest & request)
{
    if (!connected)
    {
        connect(operation_timeout.totalMicroseconds() / 3);
    }

    if (!connected)
    {
        throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "RaftConnection connect failed");
    }

    request.leader_id = leader_id;
    request.thread_id = thread_id;

    try
    {
        request.write(*out);
        out->next();
    }
    catch(...)
    {
        disconnect();
        throw Exception(ErrorCodes::NETWORK_ERROR, "RaftConnection send failed");
    }
}

bool RaftConnection::poll(size_t timeout_microseconds)
{
    return static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_microseconds);
}

bool RaftConnection::hasReadPendingData() const
{
    return static_cast<const ReadBufferFromPocoSocket &>(*in).hasPendingData();
}

void RaftConnection::receive(ServerResponse & response)
{
    if (!connected)
    {
        throw Exception(ErrorCodes::NETWORK_ERROR, "Sockect is not connected!");
    }

    int num_tries = 0;
    while(num_tries < 3)
    {
        if (poll(operation_timeout.totalMicroseconds() / 3))
            break;
        num_tries ++;
    }

    if (num_tries == 3)
    {
        disconnect();
        throw Exception(ErrorCodes::NETWORK_ERROR,  "Cant poll data, reach max retry times {}", num_tries);
    }

    try
    {
        response.read(*in);
    }
    catch (Exception & e)
    {
        disconnect();
        LOG_ERROR(log, "Got exception while receiving forward result {}, {}, {}",
            endpoint, e.displayText(), getCurrentExceptionMessage(true));
        throw e;
    }
}

}
