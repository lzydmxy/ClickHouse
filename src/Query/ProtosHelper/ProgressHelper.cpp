#include "ProgressHelper.h"
#include <fmt/format.h>

namespace DB
{
/*
    UInt64 read_rows = 0;
    UInt64 read_bytes = 0;
    UInt64 total_rows_to_read = 0;
    UInt64 total_bytes_to_read = 0;
    UInt64 written_rows = 0;
    UInt64 written_bytes = 0;
    UInt64 result_rows = 0;
    UInt64 result_bytes = 0;
    UInt64 elapsed_ns = 0;
*/
RProgress ProgressHelper::toProto(const ProgressValues & vals)
{
    RProgress proto;
    proto.set_read_rows(vals.read_rows);
    proto.set_read_bytes(vals.read_bytes);
    proto.set_total_rows_to_read(vals.total_rows_to_read);
    proto.set_total_bytes_to_read(vals.total_bytes_to_read);
    proto.set_written_rows(vals.written_rows);
    proto.set_written_bytes(vals.written_bytes);
    proto.set_result_rows(vals.result_rows);
    proto.set_result_bytes(vals.result_bytes);
    proto.set_elapsed_ns(vals.elapsed_ns);
    return proto;
}

ProgressValues ProgressHelper::fromProto(const RProgress & progress)
{
    ProgressValues vals;
    vals.read_rows = progress.read_rows();
    vals.read_bytes = progress.read_bytes();
    vals.total_rows_to_read = progress.total_rows_to_read();
    vals.total_bytes_to_read = progress.total_bytes_to_read();
    vals.written_rows = progress.written_rows();
    vals.written_bytes = progress.written_bytes();
    vals.result_rows = progress.result_rows();
    vals.result_bytes = progress.result_bytes();
    vals.elapsed_ns = progress.elapsed_ns();
    return vals;
}
bool ProgressHelper::empty(const ProgressValues & vals)
{
    return vals.read_rows == 0 && vals.read_bytes == 0
        && vals.total_rows_to_read == 0 && vals.total_bytes_to_read == 0 
        && vals.written_rows == 0 && vals.written_bytes == 0 
        && vals.result_rows == 0 && vals.result_bytes == 0
        && vals.elapsed_ns == 0;
}

ProgressValues ProgressHelper::reduce(const ProgressValues & v1, const ProgressValues & v2)
{
    ProgressValues v;
    v.read_rows = v1.read_rows >= v2.read_rows ? v1.read_rows - v2.read_rows : 0;
    v.read_bytes = v1.read_bytes >= v2.read_bytes ? v1.read_bytes - v2.read_bytes : 0;
    v.total_rows_to_read
        = v1.total_rows_to_read >= v2.total_rows_to_read ? v1.total_rows_to_read - v2.total_rows_to_read : 0;
    v.total_bytes_to_read
        = v1.total_bytes_to_read >= v2.total_bytes_to_read ? v1.total_bytes_to_read - v2.total_bytes_to_read : 0;
    v.written_rows = v1.written_rows >= v2.written_rows ? v1.written_rows - v2.written_rows : 0;
    v.written_bytes = v1.written_bytes >= v2.written_bytes ? v1.written_bytes - v2.written_bytes : 0;
    v.result_rows = v1.result_rows >= v2.result_rows ? v1.result_rows - v2.result_rows : 0;
    v.result_bytes = v1.result_bytes >= v2.result_bytes ? v1.result_bytes - v2.result_bytes : 0;
    v.elapsed_ns = v1.elapsed_ns >= v2.elapsed_ns ? v1.elapsed_ns - v2.elapsed_ns : 0;
    return v;
}

ProgressValues ProgressHelper::add(const ProgressValues & v1, const ProgressValues & v2)
{
    ProgressValues v;
    v.read_rows = v1.read_rows + v2.read_rows;
    v.read_bytes = v1.read_bytes + v2.read_bytes;
    v.total_rows_to_read = v1.total_rows_to_read + v2.total_rows_to_read;
    v.total_bytes_to_read = v1.total_bytes_to_read + v2.total_bytes_to_read;
    v.written_rows = v1.written_rows + v2.written_rows;
    v.written_bytes = v1.written_bytes + v2.written_bytes;
    v.result_rows = v1.result_rows + v2.result_rows;
    v.result_bytes = v1.result_bytes + v2.result_bytes;
    v.elapsed_ns = v1.elapsed_ns + v2.elapsed_ns;
    return v;
}

bool ProgressHelper::equals(const ProgressValues & v1, const ProgressValues & v2)
{
    return v1.read_rows == v2.read_rows && v1.read_bytes == v2.read_bytes
        && v1.total_rows_to_read == v2.total_rows_to_read && v1.total_bytes_to_read == v2.total_bytes_to_read
        && v1.written_rows == v2.written_rows && v1.written_bytes == v2.written_bytes
        && v1.result_rows == v2.result_rows && v1.result_bytes == v2.result_bytes
        && v1.elapsed_ns == v2.elapsed_ns;
}

String ProgressHelper::toString(const ProgressValues & vals)
{    
    return fmt::format(
        "Progress[read_rows={}, read_bytes={}, total_rows_to_read={}, total_bytes_to_read={},"
        " written_rows={}, written_bytes={}, result_rows={}, result_bytes={},"
        " elapsed_ns={}]",
        vals.read_rows, vals.read_bytes,
        vals.total_rows_to_read, vals.total_bytes_to_read,
        vals.written_rows, vals.written_bytes,
        vals.result_rows, vals.result_bytes,
        vals.elapsed_ns);
}

}
