#pragma once

#include <Core/ColumnNumbers.h>
#include <IO/ReadHelpers.h>
#include <Processors/IProcessor.h>
#include <Query/Processors/QueryPlan/TopNModel.h>
#include <Common/WeakHash.h>

#include <queue>

namespace DB
{

struct PartitionTopNTransformExt : IProcessor
{
    PartitionTopNTransformExt(
        Block header_,
        size_t topN_,
        ColumnNumbers partition_by_column_,
        ColumnNumbers order_by_columns_,
        TopNModel model_,
        bool reverse_ = false);

    String getName() const override { return "PartitionTopNTransformExt"; }
    Status prepare() override;
    void work() override;

private:
    struct RowNumber
    {
        uint64_t block = 0;
        uint64_t row = 0;

        bool operator<(const RowNumber & other) const { return block < other.block || (block == other.block && row < other.row); }
        bool operator==(const RowNumber & other) const { return block == other.block && row == other.row; }
        bool operator<=(const RowNumber & other) const { return *this < other || *this == other; }
    };

    TopNModel model;

    size_t topN;
    ColumnNumbers partition_by_columns;
    ColumnNumbers order_by_columns;
    bool reverse;

    bool receive_all_data = false;
    bool start_output_chunk = false;
    bool handle_all_data = false;
    Chunk chunk;
    std::vector<Chunk> chunk_list;

    std::unordered_map<size_t, std::priority_queue<Field>> partition_to_heap;
    std::unordered_map<size_t, std::priority_queue<Field, std::vector<Field>, std::greater<>>> partition_to_heap_reverse;
    std::unordered_map<size_t, std::map<Field, std::vector<RowNumber>>> partition_to_map;

    std::list<Chunk> output_chunk_list;

    WeakHash32 hash;
    // For tests
public:
    void setChunk(Chunk chunk_) { std::swap(chunk, chunk_); }
    void setReceiveAllData(bool receive_all_data_) { receive_all_data = receive_all_data_; }
    void setStartOutputChunk(bool start_output_chunk_) { start_output_chunk = start_output_chunk_; }
    void printOutputChunk()
    {
        while (!output_chunk_list.empty())
        {
            Chunk & output_first_chunk = output_chunk_list.front();
            output_first_chunk.dumpStructure();
            for (size_t i = 0; i < output_first_chunk.getNumRows(); i++)
            {
                for (size_t j = 0; j < output_first_chunk.getNumColumns(); j++)
                    std::cerr << output_first_chunk.getColumns()[j]->get64(i) << "\t";
                std::cerr << std::endl;
            }
            output_chunk_list.pop_front();
        }
    }
};

}
