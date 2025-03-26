#pragma once
#include <Processors/Chunk.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{
enum class ChunkType
{
    Any = 0,
    AggregatedArenasChunkInfo = 1,
    AggregatedChunkInfo = 2,
    ChunkMissingValues = 3,
    ChunksToMerge = 4,
    RepartitionChunkInfo = 5,
    SelectorInfo = 6,
    Totals = 7,
    Extremes = 8,
    FilterChunkInfo = 9
};

class ChunkInfoTotals: public ChunkInfo
{
public:
    ChunkType getType() const { return ChunkType::Totals; }
};

class ChunkInfoExtremes: public ChunkInfo
{
public:
    ChunkType getType() const { return ChunkType::Extremes; }
};

ChunkType getChunkType(const ChunkInfoPtr & chunk_info);
void readAggregatedChunkInfo(ReadBuffer & in, std::shared_ptr<AggregatedChunkInfo> agg_info);
void writeAggregatedChunkInfo(WriteBuffer & out, const AggregatedChunkInfo * agg_info);

}
