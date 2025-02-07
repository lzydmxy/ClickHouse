#pragma once
#include <Common/Exception.h>
#include <Query/Common/HostID.h>

namespace DB
{

/// Allows to save first catched exception in jobs and postpone its rethrow.
class ExceptionHandler
{
public:
    bool setException(std::exception_ptr && exception);
    void throwIfException();
    bool hasException() const;

protected:
    std::exception_ptr first_exception;
    mutable std::mutex mutex;
};

class ExceptionHandlerWithFailedInfo : public ExceptionHandler
{
    using ErrorCode = int32_t;
    using HostErrorCodeMap = std::unordered_map<HostID, ErrorCode, HostIDHash>;
public:
    void addFailedRpc(const HostID & host_id, int32_t error_code)
    {
        std::unique_lock lock(mutex);
        failed_rpc_info.emplace(host_id, error_code);
    }
    void setNeedRecord() { record_all_workers = true; }
    void addHost(const HostID & host_id)
    {
        if (record_all_workers)
        {
            std::unique_lock lock(mutex);
            hosts.emplace(host_id);
        }
    }
    const HostErrorCodeMap & getFailedRpcInfo() { return failed_rpc_info; }
    const HostIDSet & getWorkers() { return hosts; }

private:
    HostErrorCodeMap failed_rpc_info;
    HostIDSet hosts;
    bool record_all_workers{false};
};

using ExceptionHandlerWithFailedInfoPtr = std::shared_ptr<ExceptionHandlerWithFailedInfo>;
using ExceptionHandlerPtr = std::shared_ptr<ExceptionHandler>;

}
