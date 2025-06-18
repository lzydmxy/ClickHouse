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
        return "LimitExt";
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
        // todo: zhangwanyun1, other feat, support prepared param
        return false;
    }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    bool isAlwaysReadTillEnd() const { return always_read_till_end; }
    bool isWithTies() const { return with_ties; }
    const SortDescription & getSortDescription() const { return description; }
    bool isPartial() const { return partial; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

    void toProto(Protos::LimitStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<LimitStepExt> fromProto(const Protos::LimitStepExt & proto, ContextPtr);

private:
    bool partial;
};

}
