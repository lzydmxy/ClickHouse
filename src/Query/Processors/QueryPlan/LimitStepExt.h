#pragma once

#include <Processors/QueryPlan/LimitStep.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

/// Executes LIMIT. See LimitTransform.
class LimitStepExt : public LimitStep
{
public:
    LimitStepExt(
        const DataStream & input_stream_,
        size_t limit_,
        size_t offset_,
        bool always_read_till_end_ = false, /// Read all data even if limit is reached. Needed for totals.
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {},
        bool partial_ = false);

    String getName() const override
    {
        return "LimitStepExt";
    }

    size_t getLimitForSorting() const
    {
        if (getLimit() > std::numeric_limits<UInt64>::max() - getOffset())
            return 0;

        return getLimit() + getOffset();
    }

    const size_t & getLimit() const
    {
        return limit;
    }

    const size_t & getOffset() const
    {
        return offset;
    }

    bool hasPreparedParam() const
    {
        return limit || offset;
        // todo: need to implement check the logic
        //return std::holds_alternative<String>(limit) || std::holds_alternative<String>(offset);
    }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    bool isAlwaysReadTillEnd() const { return always_read_till_end; }
    const SortDescription & getSortDescription() const { return description; }
    bool isPartial() const { return partial; }
    
    //todo: need to implement prepare
    //void prepare(const PreparedStatementContext & prepared_context) override;

private:
    bool partial;
};

}
