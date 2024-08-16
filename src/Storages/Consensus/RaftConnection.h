#pragma once

#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/Consensus/WriteBufferFromFiFoBuffer.h>


namespace DB
{

using namespace Consensus;

static constexpr int32_t RAFT_PROTOCOL_VERSION = 0;

enum class PkgType : int8_t
{
    Unknown = -1,
    Handshake = 1,
    Forward = 2,
    Data = 3
};

inline void write(const PkgType & x, WriteBuffer & out)
{
    Consensus::write(static_cast<int8_t>(x), out);
}

inline PkgType getPkgType(int8_t raw_type)
{
    return static_cast<PkgType>(raw_type);
}

inline void read(PkgType & x, ReadBuffer & in)
{
    int8_t raw_type;
    Consensus::read(raw_type, in);
    x = getPkgType(raw_type);
}

inline std::string toString(PkgType & type)
{
    switch (type)
    {
        case PkgType::Handshake:
            return "Handshake";
        case PkgType::Forward:
            return "Forward";
        case PkgType::Data:
            return "Data";
        default:
            return "Unknown";
    }
}

struct ServerResponse;

struct ServerRequest
{
    int32_t version{RAFT_PROTOCOL_VERSION};
    PkgType protocol{-1};
    int32_t leader_id;
    int32_t thread_id;
    RaftRequestPtr raft_request{nullptr};

    void write(WriteBuffer & out) const
    {
        Consensus::write(version, out);
        DB::write(protocol, out);
        if (protocol == PkgType::Forward)
        {
            Consensus::write(leader_id, out);
            Consensus::write(thread_id, out);
            raft_request->write(out);
        }
    }

    void read(ReadBuffer & in)
    {
        Consensus::read(version, in);
        DB::read(protocol, in);
        if (protocol == PkgType::Forward)
        {
            Consensus::read(leader_id, in);
            Consensus::read(thread_id, in);
            raft_request = RaftRequest::read(in);
        }
    }

    std::string toString()
    {
        if (protocol == PkgType::Handshake)
            return fmt::format("version {}, protocol {}", version, DB::toString(protocol));
        else
            return fmt::format("version {}, protocol {}, leader {}, thread {}, raft request {}",
                version, DB::toString(protocol), leader_id, thread_id,
                raft_request ? raft_request->toString() : "null");
    }

    std::shared_ptr<ServerResponse> makeResponse();
};

using ServerResponseCallback = std::function<void(const ServerResponse & response)>;

struct ServerResponse
{
    PkgType protocol{-1};
    bool accepted{true};
    RaftResponsePtr raft_response{nullptr};
    ServerResponseCallback callback{nullptr};

    void write(WriteBuffer & out) const
    {
        DB::write(protocol, out);
        Consensus::write(accepted, out);
        if (protocol == PkgType::Forward)
            raft_response->write(out);
    }

    void read(ReadBuffer & in)
    {
        DB::read(protocol, in);
        Consensus::read(accepted, in);
        if (protocol == PkgType::Forward)
            raft_response = RaftResponse::read(in);
    }

    String toString() const
    {
        return fmt::format(
            "protocol {}, accepted {}, raft response {}",
            DB::toString(protocol), accepted, raft_response ? raft_response->toString() : "null");
    }
};

using ServerRequestPtr = std::shared_ptr<ServerRequest>;
using ServerResponsePtr = std::shared_ptr<ServerResponse>;

/**
 * 1. Forward insert/update request to leader
 * 2. Pull snapshot data
 */
class RaftConnection
{
public:
    RaftConnection(int32_t leader_id_, int32_t thread_id_, String endpoint_, Poco::Timespan operation_timeout_ms)
        : leader_id(leader_id_)
        , thread_id(thread_id_)
        , endpoint(endpoint_)
        , operation_timeout(operation_timeout_ms)
        , log(&Poco::Logger::get("RaftConnection"))
    {
    }

    void send(ServerRequest& request);
    void receive(ServerResponse & response);

    bool isConnected() const { return connected; }

    ~RaftConnection()
    {
        try
        {
            disconnect();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks, because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    void connect(Poco::Timespan connection_timeout);
    void disconnect();
    bool poll(size_t timeout_microseconds);
    bool hasReadPendingData() const;

    void sendHandshake();
    void receiveHandshake();

    int32_t leader_id;
    int32_t thread_id;
    bool connected{false};
    String endpoint;
    Poco::Timespan operation_timeout;
    Poco::Net::StreamSocket socket;
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    Poco::Logger * log;
};

using RaftConnectionPtr = std::shared_ptr<RaftConnection>;

}
