#pragma once
#include <memory>
#include <IO/ReadBuffer.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Common/PODArray.h>

namespace DB
{
class CompressedReadBufferFromFile;

/** Deserializes the stream of chunks from the native binary format (with names and column types).
  */
class NativeChunkInputStream
{
public:
    /// For cases when data structure (header) is known in advance.
    NativeChunkInputStream(ReadBuffer & istr_, const Block & header_);

    static void readData(const IDataType & type, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint);

    Chunk readImpl();

private:
    ReadBuffer & istr;
    Block header;
    PODArray<double> avg_value_size_hints;
};

using NativeChunkInputStreamHolder = std::unique_ptr<NativeChunkInputStream>;
}
