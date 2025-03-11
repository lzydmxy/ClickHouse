#pragma once

#include <Query/Exchange/ChunkInfo.h>

namespace DB
{

ChunkType getChunkType(const Chunk & chunk)
{
    const auto & chunk_info = chunk.getChunkInfo();
    if (auto total_chunk =  dynamic_pointer_cast<const ChunkInfoTotals>(chunk_info))
    {
        return total_chunk->getType();
    }
    if (auto extreme_chunk =  dynamic_pointer_cast<const ChunkInfoExtremes>(chunk_info))
    {
        return extreme_chunk->getType();
    }
    return ChunkType::Any;
}

}
