#include "ExceptionHandler.h"

namespace DB
{

bool ExceptionHandler::setException(std::exception_ptr && exception)
{
    std::unique_lock lock(mutex);
    if (!first_exception)
    {
        first_exception = std::move(exception);
        return true;
    }
    return false;
}

void ExceptionHandler::throwIfException()
{
    std::unique_lock lock(mutex);
    if (first_exception)
        std::rethrow_exception(first_exception);
}

bool ExceptionHandler::hasException() const
{
    std::unique_lock lock(mutex);
    return first_exception != nullptr;
}

}
