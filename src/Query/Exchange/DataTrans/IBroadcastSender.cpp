#include "IBroadcastSender.h"
#include <string>
#include <Poco/Exception.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
BroadcastStatus IBroadcastSender::send(Chunk chunk) noexcept
{
    try
    {
        if (needMetrics() && enable_sender_metrics)
        {
            sender_metrics.num_send_times << 1;
            sender_metrics.send_rows << chunk.getNumRows();
            sender_metrics.send_uncompressed_bytes << chunk.bytes();
            sender_timer.restart();
            auto status = sendImpl(std::move(chunk));
            sender_metrics.send_time_ms << sender_timer.elapsedMilliseconds();
            return status;
        }
        else
        {
            return sendImpl(std::move(chunk));
        }
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("IBroadcastSender"), __PRETTY_FUNCTION__);
        String exception_str = getCurrentExceptionMessage(true);
        BroadcastStatus current_status = finish(BroadcastStatusCode::SEND_UNKNOWN_ERROR, exception_str);
        return current_status;
    }
}
}
