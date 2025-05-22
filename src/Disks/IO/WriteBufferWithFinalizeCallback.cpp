#include "WriteBufferWithFinalizeCallback.h"

namespace DB
{

WriteBufferWithFinalizeCallback::WriteBufferWithFinalizeCallback(
    std::unique_ptr<WriteBuffer> impl_,
    FinalizeCallback && create_callback_,
    Undo && undo_,
    const String & remote_path_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , create_metadata_callback(std::move(create_callback_))
    , undo(undo_)
    , remote_path(remote_path_)
{
}

WriteBufferWithFinalizeCallback::~WriteBufferWithFinalizeCallback()
{
    if (!finalized)
    {
        undo();
    }
}

void WriteBufferWithFinalizeCallback::finalizeImpl()
{
    WriteBufferFromFileDecorator::finalizeImpl();
    if (create_metadata_callback)
        create_metadata_callback(count());
}


}
