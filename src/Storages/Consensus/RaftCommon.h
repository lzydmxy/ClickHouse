#pragma once

#include <map>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <vector>
#include <memory>
#include <thread>
#include <atomic>
#include <cstdint>
#include <optional>
#include <functional>
#include <Poco/FIFOBuffer.h>
#include <boost/noncopyable.hpp>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/Consensus/NuRaft.h>
#include <Storages/Consensus/IRaftCommon.h>
#include <Storages/Consensus/ThreadSafeQueue.h>


namespace DB
{
    using ThreadPoolPtr = std::shared_ptr<ThreadPool>;
    using GlobalThreadPtr = std::unique_ptr<ThreadFromGlobalPool>;
}

namespace Consensus
{

//Only support insert/delete/update SQL operator
enum class RaftOpNum : int8_t
{
    Close = -11,
    Error = -1,
    Insert = 1,
    Delete = 2,
    Update = 3,
    Fetch = 4,
    Multi = 10,
    MultiRead = 11
};

void write(int8_t x, WriteBuffer & out);

std::string toString(RaftOpNum op_num);

std::int8_t toInt8(RaftOpNum op_num);

RaftOpNum getOpNum(int8_t raw_op_num);

void write(RaftOpNum x, WriteBuffer & out);

void read(RaftOpNum & x, ReadBuffer & in);

struct RaftRequest;
struct RaftResponse;

using RaftRequestPtr = std::shared_ptr<RaftRequest>;
using RaftRequests = std::vector<RaftRequestPtr>;
using RaftRequestsQueue = ConcurrentBoundedQueue<RaftRequestPtr>;
using RaftRequestsQueuePtr = std::shared_ptr<RaftRequestsQueue>;

using RaftResponsePtr = std::shared_ptr<RaftResponse>;
using RaftResponses = std::vector<RaftResponsePtr>;
using RaftResponseQueue = ConcurrentBoundedQueue<RaftResponsePtr>;
using RaftResponseQueuePtr = std::shared_ptr<RaftResponseQueue>;

using RaftFinishCallback = std::function<void(const RaftResponsePtr & response)>;
using RaftRetryCallback = std::function<void(RaftRequestPtr & request)>;

using IOResponseQueue = ThreadSafeQueue<std::shared_ptr<Poco::FIFOBuffer>>;
using IOResponseQueuePtr = std::unique_ptr<IOResponseQueue>;

/// Exposed in header file for some external code.
struct RaftRequest
{
    UUID id;
    /// If the request was not send and the error happens, we definitely sure, that it has not been processed by the server.
    /// If the request was sent and we didn't get the response and the error happens, then we cannot be sure was it processed or not.
    bool probably_sent = false;

    UInt64 request_created_time_ns = 0;
    UInt64 thread_id = 0;
    String query_id;

    /// millisecond
    int64_t create_time = 0;
    /// for forward request
    int32_t server_id = -1;
    int32_t client_id = -1;

    uint32_t retry_count = 0;

    bool needRetry() { return retry_count < 2; }

    RaftRetryCallback retry_callback{nullptr};
    RaftFinishCallback finish_callback{nullptr};

    RaftRequest() = default;
    RaftRequest(const RaftRequest &) = default;
    virtual ~RaftRequest();

    void generateID();
    virtual RaftOpNum getOpNum() const = 0;

    NuBufferPtr write() const;
    void write(WriteBuffer & out) const;
    virtual void writeImpl(WriteBuffer &) const = 0;

    static RaftRequestPtr read(NuBuffer & in);
    static RaftRequestPtr read(ReadBuffer & in);
    virtual void readImpl(ReadBuffer &) = 0;

    std::string toString() const;
    virtual std::string toStringImpl() const = 0;

    virtual RaftResponsePtr makeResponse() const = 0;

    RaftResponsePtr setInformation(RaftResponsePtr response) const;

    bool isForwardRequest() const { return server_id > -1 && client_id > -1; }
};

struct RaftResponse
{
    UUID id;
    Error error = Error::ZOK;
    std::string message;
    UInt64 response_created_time_ns = 0;

    RaftFinishCallback finish_callback{nullptr};

    RaftResponse() = default;
    RaftResponse(const RaftResponse &) = default;
    virtual ~RaftResponse();
    static RaftResponsePtr read(NuBuffer & in);
    static RaftResponsePtr read(ReadBuffer & in);
    virtual void readImpl(ReadBuffer &) = 0;
    virtual NuBufferPtr write() const;
    virtual void write(WriteBuffer & out) const;
    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual RaftOpNum getOpNum() const = 0;
    std::string toString() const;
    virtual std::string toStringImpl() const = 0;
    virtual int32_t tryGetOpNum() const { return static_cast<int32_t>(getOpNum()); }
};

struct RaftInsertRequest final : public RaftRequest
{
    ChangeDataPtr data;
    RaftInsertRequest() = default;
    RaftOpNum getOpNum() const override { return RaftOpNum::Insert; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;
    RaftResponsePtr makeResponse() const override;
};

using RaftInsertRequestPtr = std::shared_ptr<RaftInsertRequest>;

struct RaftInsertResponse final : public RaftResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    std::string toStringImpl() const override;
    RaftOpNum getOpNum() const override { return RaftOpNum::Insert; }
};

struct RaftDeleteRequest final : public RaftRequest
{
    ChangeDataPtr data;
    RaftDeleteRequest() = default;
    RaftOpNum getOpNum() const override { return RaftOpNum::Delete; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    RaftResponsePtr makeResponse() const override;
};

using RaftDeleteRequestPtr = std::shared_ptr<RaftDeleteRequest>;


struct RaftDeleteResponse final : public RaftResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    std::string toStringImpl() const override { return std::string(); }
    RaftOpNum getOpNum() const override { return RaftOpNum::Delete; }
};

struct RaftUpdateRequest final : public RaftRequest
{
    ChangeDataPtr data;
    RaftUpdateRequest() = default;
    RaftOpNum getOpNum() const override { return RaftOpNum::Update; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;
    RaftResponsePtr makeResponse() const override;
};

using RaftUpdateRequestPtr = std::shared_ptr<RaftUpdateRequest>;

struct RaftUpdateResponse final : public RaftResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    std::string toStringImpl() const override;
    RaftOpNum getOpNum() const override { return RaftOpNum::Update; }
};

/// This response may be received only as an element of responses in MultiResponse.
struct RaftErrorResponse final : public RaftResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    std::string toStringImpl() const override { return std::string(); }
    RaftOpNum getOpNum() const override { return RaftOpNum::Error; }
};

struct RaftMultiRequest final : public RaftRequest
{
    RaftRequests requests;
    RaftMultiRequest() = default;
    explicit RaftMultiRequest(const RaftRequests & generic_requests);
    RaftOpNum getOpNum() const override;
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    RaftResponsePtr makeResponse() const override;

    enum class OperationType : UInt8
    {
        Read,
        Write
    };

    std::optional<OperationType> operation_type;
private:
    void checkOperationType(OperationType type);
};

struct RaftMultiResponse : public RaftResponse
{
    RaftResponses responses;
    explicit RaftMultiResponse(const RaftRequests & requests)
    {
        responses.reserve(requests.size());

        for (const auto & request : requests)
            responses.emplace_back(dynamic_cast<const RaftRequest &>(*request).makeResponse());
    }

    explicit RaftMultiResponse(const RaftResponses & responses_)
    {
        responses = responses_;
    }

    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    std::string toStringImpl() const override;
};

struct RaftMultiWriteResponse final : public RaftMultiResponse
{
    RaftOpNum getOpNum() const override { return RaftOpNum::Multi; }
    using RaftMultiResponse::RaftMultiResponse;
};

struct RaftMultiReadResponse final : public RaftMultiResponse
{
    RaftOpNum getOpNum() const override { return RaftOpNum::MultiRead; }
    using RaftMultiResponse::RaftMultiResponse;
};

class RaftRequestFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<RaftRequestPtr()>;
    using OpNumToRequest = std::unordered_map<RaftOpNum, Creator>;
    static RaftRequestFactory & instance();
    RaftRequestPtr get(RaftOpNum op_num) const;
    void registerRequest(RaftOpNum op_num, Creator creator);
private:
    OpNumToRequest op_num_to_request;
    RaftRequestFactory();
};

}
