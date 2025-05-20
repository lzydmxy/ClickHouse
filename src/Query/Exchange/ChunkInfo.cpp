#include <Query/Exchange/ChunkInfo.h>
#include <Query/Exchange/RepartitionTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

namespace DB
{
    
ChunkType getChunkType(const ChunkInfoPtr & chunk_info)
{
    if (typeid_cast<const AggregatedChunkInfo *>(chunk_info.get()))
        return ChunkType::AggregatedChunkInfo;
    else if (typeid_cast<const ChunkMissingValues *>(chunk_info.get()))
        return ChunkType::ChunkMissingValues;
    else if (typeid_cast<const ChunksToMerge *>(chunk_info.get()))
        return ChunkType::ChunksToMerge;
    else if (typeid_cast<const RepartitionTransform::RepartitionChunkInfo *>(chunk_info.get()))
        return ChunkType::RepartitionChunkInfo;
    else if (typeid_cast<const ChunkInfoTotals *>(chunk_info.get()))
        return ChunkType::Totals;
    else if (typeid_cast<const ChunkInfoExtremes *>(chunk_info.get()))
        return ChunkType::Extremes;

    else
        return ChunkType::Any;
}

void readAggregatedChunkInfo(ReadBuffer & in, std::shared_ptr<AggregatedChunkInfo> agg_info)
{
    readVarUInt(agg_info->is_overflows, in);
    readVarUInt(agg_info->bucket_num, in);
    // todo: need to check
    readVarUInt(agg_info->chunk_num, in);
}

void writeAggregatedChunkInfo(WriteBuffer & out, const AggregatedChunkInfo * agg_info)
{
    writeVarUInt(agg_info->is_overflows, out);
    writeVarUInt(agg_info->bucket_num, out);
    // todo: need to check
    writeVarUInt(agg_info->chunk_num, out);
}

}

