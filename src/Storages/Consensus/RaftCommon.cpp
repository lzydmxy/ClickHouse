#include <array>
#include <fmt/format.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <Core/UUID.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/Stopwatch.h>
#include <Storages/Consensus/IRaftCommon.h>
#include <Storages/Consensus/RaftCommon.h>
#include <Storages/Consensus/ChangeData.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>


namespace Consensus
{

static const std::unordered_set<int8_t> VALID_OPERATIONS =
{
    static_cast<int8_t>(RaftOpNum::Error),
    static_cast<int8_t>(RaftOpNum::Insert),
    static_cast<int8_t>(RaftOpNum::Delete),
    static_cast<int8_t>(RaftOpNum::Update),
    static_cast<int8_t>(RaftOpNum::Multi),
    static_cast<int8_t>(RaftOpNum::MultiRead),
};

std::string toString(RaftOpNum op_num)
{
    switch (op_num)
    {
        case RaftOpNum::Close:
            return "Close";
        case RaftOpNum::Error:
            return "Error";
        case RaftOpNum::Insert:
            return "Insert";
        case RaftOpNum::Delete:
            return "Delete";
        case RaftOpNum::Update:
            return "Update";
        case RaftOpNum::Fetch:
            return "Fetch";
        case RaftOpNum::Multi:
            return "Multi";
        case RaftOpNum::MultiRead:
            return "MultiRead";
    }
    int8_t raw_op = static_cast<int8_t>(op_num);
    throw RaftException(Error::ZUNIMPLEMENTED, "Operation number" + std::to_string(raw_op) + " is unknown");
}

std::int8_t toInt8(RaftOpNum op_num)
{
    return static_cast<int8_t>(op_num);
}

RaftOpNum getOpNum(int8_t raw_op_num)
{
    if (!VALID_OPERATIONS.contains(raw_op_num))
        throw RaftException(Error::ZUNIMPLEMENTED, "Operation number " + std::to_string(raw_op_num) + " is unknown");
    return static_cast<RaftOpNum>(raw_op_num);
}

void write(RaftOpNum x, WriteBuffer & out)
{
    Consensus::write(static_cast<int8_t>(x), out);
}

void read(RaftOpNum & x, ReadBuffer & in)
{
    int8_t raw_op_num;
    Consensus::read(raw_op_num, in);
    x = getOpNum(raw_op_num);
}

void RaftRequest::generateID()
{
    id = UUIDHelpers::generateV4();
}

RaftRequest::~RaftRequest()
{
    if (!request_created_time_ns)
        return;
    UInt64 elapsed_ns = clock_gettime_ns() - request_created_time_ns;
    constexpr UInt64 max_request_time_ns = 1000000000ULL; /// 1 sec
    if (max_request_time_ns < elapsed_ns)
    {
        LOG_TEST(&Poco::Logger::get(__PRETTY_FUNCTION__), "Processing of request took {} ms", elapsed_ns / 1000000UL);
    }
}

NuBufferPtr RaftRequest::write() const
{
    WriteBufferFromNuraftBuffer buf;
    write(buf);
    return buf.getBuffer();
}

void RaftRequest::write(WriteBuffer & out) const
{
    /*
    WriteBufferFromOwnString buf;
    Consensus::write(getOpNum(), buf);
    Consensus::write(id, buf);
    writeImpl(buf);
    //write size + data
    Consensus::write(static_cast<int32_t>(buf.str().size()), out);
    out.write(buf.str().data(), buf.str().size());
    out.next();
    */

    WriteBufferFromNuraftBuffer buf;
    Consensus::write(getOpNum(), buf);
    Consensus::write(id, buf);
    writeImpl(buf);
    //write size + data
    auto rbuf = buf.getBuffer();
    Consensus::write(static_cast<int32_t>(rbuf->size()), out);
    out.write(reinterpret_cast<char *>(rbuf->data()), rbuf->size());
    out.next();
}

RaftRequestPtr RaftRequest::read(NuBuffer & in)
{
    ReadBufferFromNuraftBuffer buffer(in);
    return read(buffer);
}

RaftRequestPtr RaftRequest::read(ReadBuffer & in)
{
    //read size + data
    int32_t size;
    Consensus::read(size, in);
    if (size <= 0)
        return nullptr;

    RaftOpNum op_num;
    Consensus::read(op_num, in);

    auto request = RaftRequestFactory::instance().get(op_num);
    Consensus::read(request->id, in);
    request->readImpl(in);
    return request;
}

std::string RaftRequest::toString() const
{
    return fmt::format(
        "ID {}, OpNum : {}, Info : {}",
        DB::toString(id),
        Consensus::toString(getOpNum()),
        toStringImpl());
}

NuBufferPtr RaftResponse::write() const
{
    WriteBufferFromNuraftBuffer buffer;
    write(buffer);
    return buffer.getBuffer();
}

void RaftResponse::write(WriteBuffer & out) const
{
    WriteBufferFromOwnString buf;
    Consensus::write(getOpNum(), buf);
    Consensus::write(id, buf);
    Consensus::write(error, buf);
    Consensus::write(message, buf);
    writeImpl(buf);
    //write size + data
    Consensus::write(static_cast<int32_t>(buf.str().size()), out);
    out.write(buf.str().data(), buf.str().size());
    out.next();
}

RaftResponsePtr RaftResponse::read(NuBuffer & in)
{
    ReadBufferFromNuraftBuffer buffer(in);
    return read(buffer);
}

RaftResponsePtr RaftResponse::read(ReadBuffer & in)
{
    //read size + data
    int32_t length;
    Consensus::read(length, in);
    if (length <= 0)
        return nullptr;
    RaftResponsePtr response;
    RaftOpNum op_num;
    Consensus::read(op_num, in);
    switch (op_num)
    {
        case RaftOpNum::Insert:
        {
            response = std::make_shared<RaftInsertResponse>();
            break;
        }
        case RaftOpNum::Update:
        {
            response = std::make_shared<RaftUpdateResponse>();
            break;
        }
        case RaftOpNum::Delete:
        {
            response = std::make_shared<RaftInsertResponse>();
            break;
        }
        default:
            break;
    }
    if (response != nullptr)
    {
        Consensus::read(response->id, in);
        Consensus::read(response->error, in);
        Consensus::read(response->message, in);
        response->readImpl(in);
    }
    return response;
}

std::string RaftResponse::toString() const
{
    return fmt::format(
        "ID {}, OpNum : {}, Error : {}, Message : {}, Info : {}",
        DB::toString(id),
        Consensus::toString(getOpNum()),
        errorMessage(error),
        message,
        toStringImpl());
}

void RaftInsertRequest::writeImpl(WriteBuffer & out) const
{
    if (data != nullptr)
        data->serialize(out);
}

void RaftInsertRequest::readImpl(ReadBuffer & in)
{
    data = ChangeData::readFromBuffer(in);
}

std::string RaftInsertRequest::toStringImpl() const
{
    if (data != nullptr)
        return data->dumpHeader();
    else
        return std::string();
}

void RaftInsertResponse::readImpl(ReadBuffer & /*in*/)
{
}

void RaftInsertResponse::writeImpl(WriteBuffer & /*out*/) const
{
}

std::string RaftInsertResponse::toStringImpl() const
{
    return std::string();
}

void RaftDeleteRequest::writeImpl(WriteBuffer & out) const
{
    if (data != nullptr)
        data->serialize(out);
}

std::string RaftDeleteRequest::toStringImpl() const
{
    if (data != nullptr)
        return data->dumpHeader();
    else
        return std::string();
}

void RaftDeleteRequest::readImpl(ReadBuffer & in)
{
    data = ChangeData::readFromBuffer(in);
}

void RaftUpdateRequest::writeImpl(WriteBuffer & out) const
{
    if (data != nullptr)
        data->serialize(out);
}

void RaftUpdateRequest::readImpl(ReadBuffer & in)
{
    data = ChangeData::readFromBuffer(in);
}

std::string RaftUpdateRequest::toStringImpl() const
{
    if (data != nullptr)
        return data->dumpHeader();
    else
        return std::string();
}

void RaftUpdateResponse::readImpl(ReadBuffer & /*in*/)
{
}

void RaftUpdateResponse::writeImpl(WriteBuffer & /*out*/) const
{
}

std::string RaftUpdateResponse::toStringImpl() const
{
    return std::string();
}

void RaftErrorResponse::readImpl(ReadBuffer & in)
{
    Error read_error;
    Consensus::read(read_error, in);
    if (read_error != error)
        throw RaftException(
            Error::ZMARSHALLINGERROR, 
            "Error code in ErrorResponse ({}) doesn't match error code in header ({})", read_error, error);
}

void RaftErrorResponse::writeImpl(WriteBuffer & out) const
{
    Consensus::write(error, out);
}

void RaftMultiRequest::checkOperationType(OperationType type)
{
    chassert(!operation_type.has_value() || *operation_type == type);
    operation_type = type;
}

RaftOpNum RaftMultiRequest::getOpNum() const
{
    return !operation_type.has_value() || *operation_type == OperationType::Write ? RaftOpNum::Multi : RaftOpNum::MultiRead;
}

RaftMultiRequest::RaftMultiRequest(const RaftRequests & generic_requests)
{
    /// Convert nested Requests to RaftRequests.
    /// Note that deep copy is required to avoid modifying path in presence of chroot prefix.
    requests.reserve(generic_requests.size());

    using enum OperationType;
    for (const auto & generic_request : generic_requests)
    {
        if (const auto * concrete_request_insert = dynamic_cast<const RaftInsertRequest *>(generic_request.get()))
        {
            checkOperationType(Write);
            auto create = std::make_shared<RaftInsertRequest>(*concrete_request_insert);
            requests.push_back(create);
        }
        else if (const auto * concrete_request_delete = dynamic_cast<const RaftDeleteRequest *>(generic_request.get()))
        {
            checkOperationType(Write);
            requests.push_back(std::make_shared<RaftDeleteRequest>(*concrete_request_delete));
        }
        else if (const auto * concrete_request_update = dynamic_cast<const RaftUpdateRequest *>(generic_request.get()))
        {
            checkOperationType(Write);
            requests.push_back(std::make_shared<RaftUpdateRequest>(*concrete_request_update));
        }
        else
            throw RaftException(Error::ZBADARGUMENTS, "Illegal command as part of multi Raft request");
    }
}

void RaftMultiRequest::writeImpl(WriteBuffer & out) const
{
    for (const auto & request : requests)
    {
        const auto & rf_request = dynamic_cast<const RaftRequest &>(*request);

        bool done = false;
        int32_t error = -1;

        Consensus::write(rf_request.getOpNum(), out);
        Consensus::write(done, out);
        Consensus::write(error, out);
        rf_request.writeImpl(out);
    }

    RaftOpNum op_num = RaftOpNum::Error;
    bool done = true;
    int32_t error = -1;

    Consensus::write(op_num, out);
    Consensus::write(done, out);
    Consensus::write(error, out);
}

void RaftMultiRequest::readImpl(ReadBuffer & in)
{
    while (true)
    {
        RaftOpNum op_num;
        bool done;
        int32_t error;
        Consensus::read(op_num, in);
        Consensus::read(done, in);
        Consensus::read(error, in);

        if (done)
        {
            if (op_num != RaftOpNum::Error)
                throw RaftException(Error::ZMARSHALLINGERROR, "Unexpected op_num received at the end of results for multi transaction");
            if (error != -1)
                throw RaftException(Error::ZMARSHALLINGERROR, "Unexpected error value received at the end of results for multi transaction");
            break;
        }

        RaftRequestPtr request = RaftRequestFactory::instance().get(op_num);
        request->readImpl(in);
        requests.push_back(request);

        if (in.eof())
            throw RaftException(Error::ZMARSHALLINGERROR, "Not enough results received for multi transaction");
    }
}

std::string RaftMultiRequest::toStringImpl() const
{
    auto out = fmt::memory_buffer();
    for (const auto & request : requests)
    {
        const auto & zk_request = dynamic_cast<const RaftRequest &>(*request);
        format_to(std::back_inserter(out), "SubRequest\n{}\n", zk_request.toString());
    }
    return {out.data(), out.size()};
}

void RaftMultiResponse::readImpl(ReadBuffer & in)
{
    for (auto & response : responses)
    {
        RaftOpNum op_num;
        bool done;
        Error op_error;

        Consensus::read(op_num, in);
        Consensus::read(done, in);
        Consensus::read(op_error, in);

        if (done)
            throw RaftException(Error::ZMARSHALLINGERROR, "Not enough results received for multi transaction");

        /// op_num == -1 is special for multi transaction.
        /// For unknown reason, error code is duplicated in header and in response body.

        if (op_num == RaftOpNum::Error)
            response = std::make_shared<RaftErrorResponse>();

        if (op_error != Error::ZOK)
        {
            response->error = op_error;

            /// Set error for whole transaction.
            /// If some operations fail, ZK send global error as zero and then send details about each operation.
            /// It will set error code for first failed operation and it will set special "runtime inconsistency" code for other operations.
            if (error == Error::ZOK && op_error != Error::ZRUNTIMEINCONSISTENCY)
                error = op_error;
        }

        if (op_error == Error::ZOK || op_num == RaftOpNum::Error)
            dynamic_cast<RaftResponse &>(*response).readImpl(in);
    }

    /// Footer.
    {
        RaftOpNum op_num;
        bool done;
        int32_t error_read;

        Consensus::read(op_num, in);
        Consensus::read(done, in);
        Consensus::read(error_read, in);

        if (!done)
            throw RaftException(Error::ZMARSHALLINGERROR, "Too many results received for multi transaction");
        if (op_num != RaftOpNum::Error)
            throw RaftException(Error::ZMARSHALLINGERROR, "Unexpected op_num received at the end of results for multi transaction");
        if (error_read != -1)
            throw RaftException(Error::ZMARSHALLINGERROR, "Unexpected error value received at the end of results for multi transaction");
    }
}

void RaftMultiResponse::writeImpl(WriteBuffer & out) const
{
    for (const auto & response : responses)
    {
        const RaftResponse & rf_response = dynamic_cast<const RaftResponse &>(*response);
        RaftOpNum op_num = rf_response.getOpNum();
        bool done = false;
        Error op_error = rf_response.error;

        Consensus::write(op_num, out);
        Consensus::write(done, out);
        Consensus::write(op_error, out);
        if (op_error == Error::ZOK || op_num == RaftOpNum::Error)
            rf_response.writeImpl(out);
    }

    /// Footer.
    {
        RaftOpNum op_num = RaftOpNum::Error;
        bool done = true;
        int32_t error_read = - 1;

        Consensus::write(op_num, out);
        Consensus::write(done, out);
        Consensus::write(error_read, out);
    }
}

std::string RaftMultiResponse::toStringImpl() const
{
    return std::string();
}

RaftResponsePtr RaftInsertRequest::makeResponse() const { return setInformation(std::make_shared<RaftInsertResponse>()); }
RaftResponsePtr RaftDeleteRequest::makeResponse() const { return setInformation(std::make_shared<RaftDeleteResponse>()); }
RaftResponsePtr RaftUpdateRequest::makeResponse() const { return setInformation(std::make_shared<RaftUpdateResponse>()); }

RaftResponsePtr RaftMultiRequest::makeResponse() const
{
    std::shared_ptr<RaftMultiResponse> response;
    if (getOpNum() == RaftOpNum::Multi)
       response = std::make_shared<RaftMultiWriteResponse>(requests);
    else
       response = std::make_shared<RaftMultiReadResponse>(requests);

    return setInformation(std::move(response));
}

void RaftRequestFactory::registerRequest(RaftOpNum op_num, Creator creator)
{
    if (!op_num_to_request.try_emplace(op_num, creator).second)
        throw RaftException(Error::ZRUNTIMEINCONSISTENCY, "Request type {} already registered", op_num);
}

RaftResponsePtr RaftRequest::setInformation(RaftResponsePtr response) const
{
    if (request_created_time_ns)
    {
        response->response_created_time_ns = clock_gettime_ns();
    }

    response->id = id;
    response->finish_callback = std::move(finish_callback);
    return response;
}

RaftResponse::~RaftResponse()
{
    if (!response_created_time_ns)
        return;
    UInt64 elapsed_ns = clock_gettime_ns() - response_created_time_ns;
    constexpr UInt64 max_request_time_ns = 1000000000ULL; /// 1 sec
    if (max_request_time_ns < elapsed_ns)
    {
        LOG_TEST(&Poco::Logger::get(__PRETTY_FUNCTION__), "Processing of response took {} ms", elapsed_ns / 1000000UL);
    }
}


RaftRequestPtr RaftRequestFactory::get(RaftOpNum op_num) const
{
    auto it = op_num_to_request.find(op_num);
    if (it == op_num_to_request.end())
        throw RaftException(Error::ZBADARGUMENTS, "Unknown operation type {}", toString(op_num));

    return it->second();
}

RaftRequestFactory & RaftRequestFactory::instance()
{
    static RaftRequestFactory factory;
    return factory;
}

template<RaftOpNum num, typename RequestT>
void registerRaftRequest(RaftRequestFactory & factory)
{
    factory.registerRequest(num, []
    {
        auto res = std::make_shared<RequestT>();
        res->request_created_time_ns = clock_gettime_ns();

        if constexpr (num == RaftOpNum::MultiRead)
            res->operation_type = RaftMultiRequest::OperationType::Read;
        else if constexpr (num == RaftOpNum::Multi)
            res->operation_type = RaftMultiRequest::OperationType::Write;

        return res;
    });
}

RaftRequestFactory::RaftRequestFactory()
{
    registerRaftRequest<RaftOpNum::Insert, RaftInsertRequest>(*this);
    registerRaftRequest<RaftOpNum::Delete, RaftDeleteRequest>(*this);
    registerRaftRequest<RaftOpNum::Update, RaftUpdateRequest>(*this);
    registerRaftRequest<RaftOpNum::Multi, RaftMultiRequest>(*this);
}

}
