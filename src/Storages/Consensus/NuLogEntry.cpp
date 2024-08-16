#include <Storages/Consensus/NuLogEntry.h>


namespace Consensus
{

using nuraft::byte;

char * NuLogBuilder::serializeEntry(NuLogEntryPtr & entry, NuBufferPtr & entry_buf, size_t & buf_size)
{
    NuBufferPtr data_buf = entry->get_buf_ptr();
    data_buf->pos(0);
    entry_buf = NuBuffer::alloc(sizeof(char) + data_buf->size());
    entry_buf->put((static_cast<byte>(entry->get_val_type())));
    entry_buf->put(*data_buf);
    entry_buf->pos(0);
    buf_size = entry_buf->size();
    return reinterpret_cast<char *>(entry_buf->data_begin());
}

NuLogEntryPtr NuLogBuilder::parseEntry(const char * entry_str, const UInt64 & term, size_t buf_size)
{
    auto entry_buf = NuBuffer::alloc(buf_size);
    entry_buf->put_raw(reinterpret_cast<const byte *>(entry_str), buf_size);
    entry_buf->pos(0);
    nuraft::log_val_type tp = static_cast<nuraft::log_val_type>(entry_buf->get_byte());
    NuBufferPtr data = NuBuffer::copy(*entry_buf);
    return RaftNew<NuLogEntry>(term, data, tp);
}

}
