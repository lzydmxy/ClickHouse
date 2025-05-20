#include <gtest/gtest.h>
#include <Query/Exchange/ChunkInfo.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Query/Exchange/RepartitionTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

using namespace DB;


TEST(ChunkInfoTest, getChunkTypeTest)
{
    auto aggregated_chunk_info = std::make_shared<AggregatedChunkInfo>();
    EXPECT_EQ(getChunkType(aggregated_chunk_info), ChunkType::AggregatedChunkInfo);

    auto missing_values_chunk_info = std::make_shared<ChunkMissingValues>();
    EXPECT_EQ(getChunkType(missing_values_chunk_info), ChunkType::ChunkMissingValues);

    auto chunks_to_merge_info = std::make_shared<ChunksToMerge>();
    EXPECT_EQ(getChunkType(chunks_to_merge_info), ChunkType::ChunksToMerge);

    auto totals_chunk_info = std::make_shared<ChunkInfoTotals>();
    EXPECT_EQ(getChunkType(totals_chunk_info), ChunkType::Totals);

    auto extremes_chunk_info = std::make_shared<ChunkInfoExtremes>();
    EXPECT_EQ(getChunkType(extremes_chunk_info), ChunkType::Extremes);
}

TEST(ChunkInfoTest, readAndWriteAggregatedChunInfoTest)
{
    auto aggregated_chunk_info = std::make_shared<AggregatedChunkInfo>();
    aggregated_chunk_info->bucket_num = 1;
    aggregated_chunk_info->chunk_num = 1;
    aggregated_chunk_info->is_overflows = true;

    WriteBufferFromOwnString out;
    writeAggregatedChunkInfo(out, aggregated_chunk_info.get());

    auto serialized_chunk_info = std::make_shared<AggregatedChunkInfo>();
    ReadBufferFromString in(out.str());
    readAggregatedChunkInfo(in, serialized_chunk_info);

    EXPECT_EQ(aggregated_chunk_info->bucket_num, serialized_chunk_info->bucket_num);
    EXPECT_EQ(aggregated_chunk_info->chunk_num, serialized_chunk_info->chunk_num);
    EXPECT_EQ(aggregated_chunk_info->is_overflows, serialized_chunk_info->is_overflows);
}
