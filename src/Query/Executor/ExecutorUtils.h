#pragma once
#include <list>
#include <memory>
#include <fmt/core.h>
#include <fmt/format.h>
#include <Query/Common/OptimizerContext.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED_INTERNAL;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
}

enum class QueryMPPStatusCode
{
    INIT = 0,
    // REGISTER,
    // DISPACHED,
    // RUNNING,
    // RESULT_RECEIVED,
    CANCEL = 1,
    WAIT_ROOT_ERROR = 2,
    FINISH = 3
};

struct QueryError
{
    Int32 code{0};
    String message;
    String host_name{""};
    UInt16 tcp_port{0};
    UInt16 exchange_status_port{0};
    Int32 segment_id{0};
};

constexpr bool isAmbiguosError(int error_code)
{
    return error_code == ErrorCodes::QUERY_WAS_CANCELLED_INTERNAL || error_code == ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION;
}

struct QueryMPPStatus
{
    std::atomic<QueryMPPStatusCode> status_code {QueryMPPStatusCode::INIT};
    bool success {false};
    bool cancelled {false};
    QueryError root_cause_error;
    std::list<QueryError> additional_errors;
};

struct SummarizedQueryStatus
{
    bool success {false};
    bool cancelled {false};
    Int32 error_code {0};
    String summarized_error_msg{""};
};

}

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <>
struct fmt::formatter<DB::QueryError>
{
    constexpr auto parse(format_parse_context & ctx)
    {
        auto it = ctx.begin();
        auto end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("Invalid format for struct QueryError");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::QueryError & e, FormatContext & ctx)
    {
        return format_to(ctx.out(), "SegmentId: {}, ErrorCode:{}, Message: {}", e.segment_id, e.code, e.message);
    }
};


