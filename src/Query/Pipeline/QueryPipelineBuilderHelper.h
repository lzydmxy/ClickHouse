#pragma once

#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

class QueryPipelineBuilderHelper
{

public:
    static std::unique_ptr<QueryPipelineBuilder> joinPipelinesWithRuntimeFilter(
        std::unique_ptr<QueryPipelineBuilder> left,
        std::unique_ptr<QueryPipelineBuilder> right,
        JoinPtr join,
        const Block & output_header,
        size_t max_block_size,
        size_t max_streams,
        bool keep_left_read_in_order,
        bool join_parallel_left_right,
        Processors * collected_processors = nullptr,
        bool need_build_runtime_filter = false);
};

}
