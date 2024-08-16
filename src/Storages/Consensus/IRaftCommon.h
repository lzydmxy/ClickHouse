#pragma once

#include <vector>
#include <memory>
#include <cstdint>
#include <functional>
#include <base/types.h>
#include <Common/Exception.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <libnuraft/async.hxx>

namespace DB 
{
    class ChangeData;
    using ChangeDataPtr = std::shared_ptr<ChangeData>;
}

namespace Consensus
{

using namespace DB;

enum class Error : int32_t
{
    ZOK = 0,

    /** System and server-side errors.
        * This is never thrown by the server, it shouldn't be used other than
        * to indicate a range. Specifically error codes greater than this
        * value, but lesser than ZAPIERROR, are system errors.
        */
    ZSYSTEMERROR = -1,

    ZRUNTIMEINCONSISTENCY = -2, /// A runtime inconsistency was found
    ZDATAINCONSISTENCY = -3,    /// A data inconsistency was found
    ZCONNECTIONLOSS = -4,       /// Connection to the server has been lost
    ZMARSHALLINGERROR = -5,     /// Error while marshalling or unmarshalling data
    ZUNIMPLEMENTED = -6,        /// Operation is unimplemented
    ZOPERATIONTIMEOUT = -7,     /// Operation timeout
    ZBADARGUMENTS = -8,         /// Invalid arguments
    ZINVALIDSTATE = -9,         /// Invalid zhandle state

    STORAGEERROR = -10,         /// Storage internal error
    NURAFTERROR = -11,          /// Nuraft internal error

    /** API errors.
        * This is never thrown by the server, it shouldn't be used other than
        * to indicate a range. Specifically error codes greater than this
        * value are API errors.
        */
    ZAPIERROR = -100,

    ZNONODE = -101,                     /// Node does not exist
    ZNOAUTH = -102,                     /// Not authenticated
    ZBADVERSION = -103,                 /// Version conflict
    ZNOCHILDRENFOREPHEMERALS = -108,    /// Ephemeral nodes may not have children
    ZNODEEXISTS = -110,                 /// The node already exists
    ZNOTEMPTY = -111,                   /// The node has children
    ZSESSIONEXPIRED = -112,             /// The session has been expired by the server
    ZINVALIDCALLBACK = -113,            /// Invalid callback specified
    ZINVALIDACL = -114,                 /// Invalid ACL specified
    ZAUTHFAILED = -115,                 /// Client authentication failed
    ZCLOSING = -116,                    /// ZooKeeper is closing
    ZNOTHING = -117,                    /// (not error) no server responses to process
    ZSESSIONMOVED = -118                /// Session moved to another server, so operation is ignored
};

/// Network errors and similar. You should reinitialize ZooKeeper session in case of these errors
bool isHardwareError(Error code);

/// Valid errors sent from the server about database state (like "no node"). Logical and authentication errors (like "bad arguments") are not here.
bool isUserError(Error code);

const char * errorMessage(Error code);

Error convertToError(const nuraft::cmd_result_code & nuraft_code);

class RaftException : public DB::Exception
{
private:
    /// Delegate constructor, used to minimize repetition; last parameter used for overload resolution.
    RaftException(const std::string & msg, const Error code_, int); /// NOLINT

public:
    explicit RaftException(const Error code_); /// NOLINT
    RaftException(const std::string & msg, const Error code_); /// NOLINT
    RaftException(const Error code_, const std::string & key); /// NOLINT
    RaftException(const Exception & exc);

    template <typename... Args>
    RaftException(const Error code_, fmt::format_string<Args...> fmt, Args &&... args)
        : RaftException(fmt::format(fmt, std::forward<Args>(args)...), code_)
    {
    }

    const char * name() const noexcept override { return "Consensus::RaftException"; }
    const char * className() const noexcept override { return "Consensus::RaftException"; }
    RaftException * clone() const override { return new RaftException(*this); }

    const Error code;
};

/// ZooKeeper has 1 MB node size and serialization limit by default,
/// but it can be raised up, so we have a slightly larger limit on our side.
static constexpr int32_t MAX_STRING_OR_ARRAY_SIZE = 1 << 28;  /// 256 MiB

void write(UUID x, WriteBuffer & out);
void write(size_t x, WriteBuffer & out);
/// uint64_t != size_t on darwin
#ifdef OS_DARWIN
void write(uint64_t x, WriteBuffer & out);
#endif
void write(uint64_t x, WriteBuffer & out);
void write(int64_t x, WriteBuffer & out);
void write(int32_t x, WriteBuffer & out);
void write(uint8_t x, WriteBuffer & out);
void write(int8_t x, WriteBuffer & out);
void write(bool x, WriteBuffer & out);
void write(const std::string & s, WriteBuffer & out);
void write(const Error & x, WriteBuffer & out);

template <size_t N>
void write(const std::array<char, N> s, WriteBuffer & out)
{
    write(int32_t(N), out);
    out.write(s.data(), N);
}

template <typename T>
void write(const std::vector<T> & arr, WriteBuffer & out)
{
    write(int32_t(arr.size()), out);
    for (const auto & elem : arr)
        write(elem, out);
}

void read(UUID & x, ReadBuffer & in);
void read(size_t & x, ReadBuffer & in);
#ifdef OS_DARWIN
void read(uint64_t & x, ReadBuffer & in);
#endif
void read(int64_t & x, ReadBuffer & in);
void read(int32_t & x, ReadBuffer & in);
void read(uint8_t & x, ReadBuffer & in);
void read(int8_t & x, ReadBuffer & in);
void read(bool & x, ReadBuffer & in);
void read(std::string & s, ReadBuffer & in);
void read(Error & x, ReadBuffer & in);

template <size_t N>
void read(std::array<char, N> & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size != N)
        throw RaftException("Unexpected array size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);
    in.readStrict(s.data(), N);
}

template <typename T>
void read(std::vector<T> & arr, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw RaftException("Negative size while reading array from ZooKeeper", Error::ZMARSHALLINGERROR);
    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw RaftException("Too large array size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);
    arr.resize(size);
    for (auto & elem : arr)
        read(elem, in);
}

}
