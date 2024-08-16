#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Storages/Consensus/IRaftCommon.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int KEEPER_EXCEPTION;
    }
}

namespace ProfileEvents
{
    extern const Event ZooKeeperUserExceptions;
    extern const Event ZooKeeperHardwareExceptions;
    extern const Event ZooKeeperOtherExceptions;
}

using namespace DB;
using namespace nuraft;

namespace Consensus
{

void write(UUID x, WriteBuffer & out)
{
    write(x.toUnderType().items[0], out);
    write(x.toUnderType().items[1], out);
}

void write(size_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}

#ifdef OS_DARWIN
void write(uint64_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}
#endif

void write(int64_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}
void write(int32_t x, WriteBuffer & out)
{
    x = __builtin_bswap32(x);
    writeBinary(x, out);
}

void write(uint8_t x, WriteBuffer & out)
{
    writeBinary(x, out);
}

void write(int8_t x, WriteBuffer & out)
{
    writeBinary(x, out);
}

void write(bool x, WriteBuffer & out)
{
    writeBinary(x, out);
}

void write(const std::string & s, WriteBuffer & out)
{
    write(static_cast<int32_t>(s.size()), out);
    out.write(s.data(), s.size());
}

void write(const Error & x, WriteBuffer & out)
{
    write(static_cast<int32_t>(x), out);
}

void read(UUID & x, ReadBuffer & in)
{
    read(x.toUnderType().items[0], in);
    read(x.toUnderType().items[1], in);
}

void read(size_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}

#ifdef OS_DARWIN
void read(uint64_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}
#endif

void read(int64_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}

void read(uint8_t & x, ReadBuffer & in)
{
    readBinary(x, in);
}

void read(int8_t & x, ReadBuffer & in)
{
    readBinary(x, in);
}

void read(int32_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap32(x);
}

void read(bool & x, ReadBuffer & in)
{
    readBinary(x, in);
}

void read(std::string & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);

    if (size == -1)
    {
        /// It means that zookeeper node has NULL value. We will treat it like empty string.
        s.clear();
        return;
    }

    if (size < 0)
        throw RaftException("Negative size while reading string from ZooKeeper", Error::ZMARSHALLINGERROR);

    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw RaftException("Too large string size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);

    s.resize(size);
    size_t read_bytes = in.read(s.data(), size);
    if (read_bytes != static_cast<size_t>(size))
        throw RaftException(
            Error::ZMARSHALLINGERROR, "Buffer size read from Zookeeper is not big enough. Expected {}. Got {}", size, read_bytes);
}

void read(Error & x, ReadBuffer & in)
{
    int32_t code;
    read(code, in);
    x = Error(code);
}

RaftException::RaftException(const std::string & msg, const Error code_, int)
    : DB::Exception(msg, DB::ErrorCodes::KEEPER_EXCEPTION), code(code_)
{
    if (isUserError(code))
        ProfileEvents::increment(ProfileEvents::ZooKeeperUserExceptions);
    else if (isHardwareError(code))
        ProfileEvents::increment(ProfileEvents::ZooKeeperHardwareExceptions);
    else
        ProfileEvents::increment(ProfileEvents::ZooKeeperOtherExceptions);
}

RaftException::RaftException(const std::string & msg, const Error code_)
    : RaftException(msg + " (" + errorMessage(code_) + ")", code_, 0)
{
}

RaftException::RaftException(const Error code_)
    : RaftException(errorMessage(code_), code_, 0)
{
}

RaftException::RaftException(const Error code_, const std::string & key)
    : RaftException(std::string{errorMessage(code_)} + ", key: " + key, code_, 0)
{
}

const char * errorMessage(Error code)
{
    switch (code)
    {
        case Error::ZOK:                      return "Ok";
        case Error::ZSYSTEMERROR:             return "System error";
        case Error::ZRUNTIMEINCONSISTENCY:    return "Run time inconsistency";
        case Error::ZDATAINCONSISTENCY:       return "Data inconsistency";
        case Error::ZCONNECTIONLOSS:          return "Connection loss";
        case Error::ZMARSHALLINGERROR:        return "Marshalling error";
        case Error::ZUNIMPLEMENTED:           return "Unimplemented";
        case Error::ZOPERATIONTIMEOUT:        return "Operation timeout";
        case Error::ZBADARGUMENTS:            return "Bad arguments";
        case Error::ZINVALIDSTATE:            return "Invalid zhandle state";
        case Error::STORAGEERROR:             return "Storage internal error";
        case Error::NURAFTERROR:              return "Nuraft internal error";
        case Error::ZAPIERROR:                return "API error";
        case Error::ZNONODE:                  return "No node";
        case Error::ZNOAUTH:                  return "Not authenticated";
        case Error::ZBADVERSION:              return "Bad version";
        case Error::ZNOCHILDRENFOREPHEMERALS: return "No children for ephemerals";
        case Error::ZNODEEXISTS:              return "Node exists";
        case Error::ZNOTEMPTY:                return "Not empty";
        case Error::ZSESSIONEXPIRED:          return "Session expired";
        case Error::ZINVALIDCALLBACK:         return "Invalid callback";
        case Error::ZINVALIDACL:              return "Invalid ACL";
        case Error::ZAUTHFAILED:              return "Authentication failed";
        case Error::ZCLOSING:                 return "ZooKeeper is closing";
        case Error::ZNOTHING:                 return "(not error) no server responses to process";
        case Error::ZSESSIONMOVED:            return "Session moved to another server, so operation is ignored";
    }

    UNREACHABLE();
}

bool isHardwareError(Error zk_return_code)
{
    return zk_return_code == Error::ZINVALIDSTATE
        || zk_return_code == Error::ZSESSIONEXPIRED
        || zk_return_code == Error::ZSESSIONMOVED
        || zk_return_code == Error::ZCONNECTIONLOSS
        || zk_return_code == Error::ZMARSHALLINGERROR
        || zk_return_code == Error::ZOPERATIONTIMEOUT;
}

bool isUserError(Error zk_return_code)
{
    return zk_return_code == Error::ZNONODE
        || zk_return_code == Error::ZBADVERSION
        || zk_return_code == Error::ZNOCHILDRENFOREPHEMERALS
        || zk_return_code == Error::ZNODEEXISTS
        || zk_return_code == Error::ZNOTEMPTY;
}

Error convertToError(const nuraft::cmd_result_code & nuraft_code)
{
    switch (nuraft_code)
    {
        case cmd_result_code::OK:
            return Error::ZOK;
        case cmd_result_code::CANCELLED:
            return Error::ZCLOSING;
        case cmd_result_code::TIMEOUT:
            return Error::ZOPERATIONTIMEOUT;
        case cmd_result_code::BAD_REQUEST:
            return Error::ZBADARGUMENTS;
        case cmd_result_code::FAILED:
            return Error::ZAPIERROR;
        case cmd_result_code::NOT_LEADER:
        case cmd_result_code::SERVER_ALREADY_EXISTS:
        case cmd_result_code::CONFIG_CHANGING:
        case cmd_result_code::SERVER_IS_JOINING:
        case cmd_result_code::SERVER_NOT_FOUND:
        case cmd_result_code::CANNOT_REMOVE_LEADER:
        case cmd_result_code::SERVER_IS_LEAVING:
        case cmd_result_code::TERM_MISMATCH:
        case cmd_result_code::RESULT_NOT_EXIST_YET:
            return Error::NURAFTERROR;
    }
};

}
