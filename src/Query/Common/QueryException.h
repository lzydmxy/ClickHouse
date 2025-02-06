#pragma once

#include <Common/Exception.h>


namespace DB
{

class QueryException : public Exception
{
public:
    template <typename T>
    requires std::is_convertible_v<T, String>
    QueryException(T && message, int code) : Exception(std::forward<T>(message), code)
    {
        message_format_string = tryGetStaticFormatString(message);
    }

    template<> QueryException(const String & message, int code) : Exception(message, code) {}
    template<> QueryException(String & message, int code) : Exception(message, code) {}
    template<> QueryException(String && message, int code) : Exception(std::move(message), code) {}

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    QueryException(int code, FormatStringHelper<Args...> fmt, Args &&... args)
        : Exception(fmt::format(fmt.fmt_str, std::forward<Args>(args)...), code)
    {
        message_format_string = fmt.message_format_string;
    }

    QueryException * clone() const override { return new QueryException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(cert-err60-cpp)

private:
    const char * name() const noexcept override { return "DB::QueryException"; }
    const char * className() const noexcept override { return "DB::QueryException"; }
};

}
