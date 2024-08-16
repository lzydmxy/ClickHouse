#pragma once
#include <Storages/Consensus/NuRaft.h>


namespace Consensus
{

struct LogEntryHeader
{
    UInt64 term;
    UInt64 index;
    // The length of the batch data (uncompressed)
    UInt32 data_length;
    // The CRC32C of the batch data.
    // If compression is enabled, this is the checksum of the compressed data.
    UInt32 data_crc;
    void reset()
    {
        term = 0;
        index = 0;
        data_length = 0;
        data_crc = 0;
    }
    static constexpr size_t HEADER_SIZE = 24;
};

struct NuLogBuilder
{
    static char * serializeEntry(NuLogEntryPtr & entry, NuBufferPtr & entry_buf, size_t & buf_size);
    static NuLogEntryPtr parseEntry(const char * entry_str, const UInt64 & term, size_t buf_size);
};

}
