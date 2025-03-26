#include "ChunkInfo.h"

namespace DB
{

ChunkType getChunkType(const ChunkInfoPtr & chunk_info)
{
    if (typeid_cast<const AggregatedChunkInfo *>(chunk_info.get()))
        return ChunkType::AggregatedChunkInfo;
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
}

void writeAggregatedChunkInfo(WriteBuffer & out, const AggregatedChunkInfo * agg_info)
{
    writeVarUInt(agg_info->is_overflows, out);
    writeVarUInt(agg_info->bucket_num, out);
}

}
