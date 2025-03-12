#pragma once

#include <Interpreters/TableJoin.h>
#include <Query/Common/OptimizerContext.h>

namespace DB
{

class RuntimeFilterConsumer;

class TableJoinExt: public TableJoin
{

public:
    TableJoinExt()
        : TableJoin()
        , runtime_filter_bloom_build_threshold(RUNTIME_FILTER_BLOOM_BUILD_THRESHOLD)
        , runtime_filter_in_build_threshold(RUNTIME_FILTER_IN_BUILD_THRESHOLD)
    {
    }

    TableJoinExt(const Settings & settings, const OptimizerSettingsPtr & optimizer_settings, VolumePtr tmp_volume)
        : TableJoin(settings, tmp_volume), runtime_filter_bloom_build_threshold(optimizer_settings->runtime_filter_bloom_build_threshold)
        , runtime_filter_in_build_threshold(optimizer_settings->runtime_filter_in_build_threshold)
    {
    }

    TableJoinExt(SizeLimits limits, bool use_nulls, JoinKind kind, JoinStrictness strictness, const Names & key_names_right_)
        : TableJoin(limits, use_nulls, kind, strictness, key_names_right_)
        , runtime_filter_bloom_build_threshold(RUNTIME_FILTER_BLOOM_BUILD_THRESHOLD)
        , runtime_filter_in_build_threshold(RUNTIME_FILTER_IN_BUILD_THRESHOLD)
    {
    }

    void setRuntimeFilterConsumer(const std::shared_ptr<RuntimeFilterConsumer> &filter_consumer) { runtime_filter_consumer = filter_consumer; }
    std::shared_ptr<RuntimeFilterConsumer> getRuntimeFilterConsumer() const { return runtime_filter_consumer; }

    void setInequalCondition(ExpressionActionsPtr inequal_condition_actions_, String inequal_column_name_);

    bool allowMergeJoin() const;
    bool forceHashJoin() const
    {
    /// HashJoin always used for DictJoin
    return join_algorithm.size() == 1 && (join_algorithm[0] == JoinAlgorithm::HASH || join_algorithm[0] == JoinAlgorithm::PARALLEL_HASH || join_algorithm[0] == JoinAlgorithm::DIRECT);
    }
    bool preferMergeJoin() const { return join_algorithm.size() == 1 && join_algorithm[0] == JoinAlgorithm::PREFER_PARTIAL_MERGE; }
    bool forceMergeJoin() const { return join_algorithm.size() == 1 && join_algorithm[0] == JoinAlgorithm::PARTIAL_MERGE; }
    bool forceGraceHashJoin() const { return join_algorithm.size() == 1 && join_algorithm[0] == JoinAlgorithm::GRACE_HASH; }

    size_t getBloomBuildThreshold() const { return runtime_filter_bloom_build_threshold;}
    size_t getInBuildThreshold() const { return runtime_filter_in_build_threshold;}

private:
    friend class JoinStepExt;

    /// Original name -> name. Only renamed columns.
    std::unordered_map<String, String> renames;

    std::shared_ptr<RuntimeFilterConsumer> runtime_filter_consumer = nullptr;
    ExpressionActionsPtr inequal_condition_actions;
    String inequal_column_name;

    const size_t runtime_filter_bloom_build_threshold;
    const size_t runtime_filter_in_build_threshold;
};

}
