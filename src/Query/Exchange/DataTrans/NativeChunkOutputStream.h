#pragma once
#include <memory>
#include <base/types.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>

namespace DB
{

class WriteBuffer;
class CompressedWriteBuffer;

/// Serializes the stream of chunk in their native binary format.
class NativeChunkOutputStream
{
public:
    NativeChunkOutputStream(WriteBuffer & ostr_, const Block & header_);

    void write(const Chunk & chunk);

private:
    WriteBuffer & ostr;
    Block header;
};

using NativeChunkOutputStreamHolder = std::unique_ptr<NativeChunkOutputStream>;

}
