#pragma once

#include <Core/Joins.h>
#include <Processors/QueryPlan/JoinStep.h>

#include <Interpreters/ActionsVisitor.h>

#include <Query/Common/LinkedHashMap.h>
// #include <Query/Optimizer/PredicateConst.h>
#include <Query/Core/JoinsExt.h>
#include <Query/Parsers/ASTHelper.h>

#include <Query/Executor/RuntimeFilter/RuntimeFilterBuilder.h>
#include "Query/Optimizer/PredicateConst.h"

namespace DB
{

ENUM_WITH_PROTO_CONVERTER(
    DistributionType, // enum name
    Protos::DistributionType, // proto enum message
    (UNKNOWN, 0),
    (REPARTITION),
    (BROADCAST));

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;
class RuntimeFilterConsumer;

/// Join two data streams.
class JoinStepExt : public JoinStep
{
public:
    JoinStepExt(
        const DataStream & left_stream_,
        const DataStream & right_stream_,
        JoinPtr join_,
        size_t max_block_size_,
        size_t max_streams_,
        bool keep_left_read_in_order_,
        bool is_ordered_ = false,
        bool simple_reordered_ = false);

    JoinStepExt(
        DataStreams input_streams_,
        DataStream output_stream_,
        JoinKind kind_,
        JoinStrictness strictness_,
        size_t max_streams_ = 1,
        bool keep_left_read_in_order_ = false,
        Names left_keys_ = {},
        Names right_keys_ = {},
        std::vector<bool> key_ids_null_safe_ = {},
        ConstASTPtr filter_ = PredicateConst::TRUE_VALUE,
        bool has_using_ = false,
        std::optional<std::vector<bool>> require_right_keys_ = std::nullopt,
        ASOFJoinInequality asof_inequality_ = ASOFJoinInequality::GreaterOrEquals,
        DistributionType distribution_type_ = DistributionType::UNKNOWN,
        JoinAlgorithm join_algorithm = JoinAlgorithm::DEFAULT,
        bool is_magic_ = false,
        bool is_ordered_ = false,
        bool simple_reordered_ = false,
        LinkedHashMap<String, RuntimeFilter> runtime_filter_builders = {});


    String getName() const override { return "JoinExt"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    JoinKind getKind() const { return kind; }
    void setKind(JoinKind kind_) { kind = kind_; }
    JoinStrictness getStrictness() const { return strictness; }

    size_t getMaxStreams() const { return max_streams; }
    bool getKeepLeftReadInOrder() const { return keep_left_read_in_order; }

    const Names & getLeftKeys() const { return left_keys; }
    const Names & getRightKeys() const { return right_keys; }
    const std::vector<bool> & getKeyIdsNullSafe() const { return key_ids_null_safe; }
    bool hasKeyIdNullSafe() const;
    bool getKeyIdNullSafe(size_t key_index) const;
    const ConstASTPtr & getFilter() const { return filter; }
    bool isHasUsing() const { return has_using; }
    void resetUsing()
    {
        has_using = false;
        require_right_keys = std::nullopt;
    }
    std::optional<std::vector<bool>> getRequireRightKeys() const { return require_right_keys; }
    ASOFJoinInequality getAsofInequality() const { return asof_inequality; }
    DistributionType getDistributionType() const { return distribution_type; }
    void setDistributionType(DistributionType distribution_type_) { distribution_type = distribution_type_; }

    bool isCrossJoin() const { return kind == JoinKind::Cross || (kind == JoinKind::Inner && left_keys.empty()); }

    bool isInnerJoin() const { return kind == JoinKind::Inner; }

    bool isOuterJoin() const
    {
        return (kind == JoinKind::Left || kind == JoinKind::Right || kind == JoinKind::Full)
            && (strictness == JoinStrictness::All || strictness == JoinStrictness::Any);
    }

    bool isLeftOrRightOuterJoin() const
    {
        return (kind == JoinKind::Left || kind == JoinKind::Right)
            && (strictness == JoinStrictness::All || strictness == JoinStrictness::Any);
    }

    bool isLeftOuterJoin() const
    {
        return kind == JoinKind::Left && (strictness == JoinStrictness::All || strictness == JoinStrictness::Any);
    }

    bool isRightOuterJoin() const
    {
        return kind == JoinKind::Right && (strictness == JoinStrictness::All || strictness == JoinStrictness::Any);
    }

    bool isMagic() const { return is_magic; }
    void setMagic(bool is_magic_) { is_magic = is_magic_; }

    bool isOrdered() const { return is_ordered; }
    void setOrdered(bool is_ordered_) { is_ordered = is_ordered_; }

    bool isSimpleReordered() const { return simple_reordered; }
    void setSimpleReordered(bool simple_reordered_) { simple_reordered = simple_reordered_; }

    bool mustReplicate() const;
    bool mustRepartition() const;


    bool supportReorder(bool support_filter, bool support_cross = false) const;

    bool supportSwap() const
    {
        if (getStrictness() != JoinStrictness::Unspecified && getStrictness() != JoinStrictness::All
            && getStrictness() != JoinStrictness::Any && getStrictness() != JoinStrictness::Semi && getStrictness() != JoinStrictness::Anti)
            return false;

        if (require_right_keys || has_using)
            return false;

        return true;
    }

    void setJoinAlgorithm(JoinAlgorithm join_algorithm_) { join_algorithm = join_algorithm_; }
    JoinAlgorithm getJoinAlgorithm() const { return join_algorithm; }

    /**
     * Hash Join don't support non-equivalent filter yet, so we must use nest loop join.
     */
    bool enforceNestLoopJoin() const;

    bool needStreamWithNonJoinedRows() const
    {
        if (strictness == JoinStrictness::Asof || strictness == JoinStrictness::Semi)
            return false;
        return isRightOrFull(kind);
    }

    JoinPtr makeJoin(
        ContextPtr context,
        std::shared_ptr<RuntimeFilterConsumer> && consumer,
        size_t num_streams,
        ExpressionActionsPtr filter_action,
        String filter_column_name);

    bool enforceGraceHashJoin() const;

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;

    void setOutputStream(DataStream output_stream_);
    // TODO(gouguilin): protobuf serde

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void updateOutputStream() override;

    const LinkedHashMap<String, RuntimeFilter> & getRuntimeFilterBuilders() const { return runtime_filter_builders; }
    RuntimeFilterBuilderPtr createRuntimeFilterBuilder(ContextPtr context) const;

    void toProto(Protos::JoinStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<JoinStepExt> fromProto(const Protos::JoinStepExt & proto, ContextPtr context);

protected:
    JoinKind kind;
    JoinStrictness strictness;
    Names left_keys;
    Names right_keys;
    std::vector<bool> key_ids_null_safe;

    /**
     * Non-equals predicate
     *
     * For example:
     *
     * LEFT JOIN orders ON (c_custkey = o_custkey) AND (o_comment NOT LIKE '%special%requests%')
     */
    ConstASTPtr filter;

    bool has_using;

    // A right join key which has its require_right_key = FALSE has below effects:
    // 1. It will be excluded of the output columns.
    // 2. For RIGHT/FULL JOIN, the counterpart left keys will carry the data of the right key
    // NB: If the require_right_keys is nullopt, it's {TRUE, TRUE...} equivalently.
    // NB: It's only be used in Clickhouse semantics currently.
    //
    // Examples:
    // For query "SELECT k FROM (SELECT 1 AS k) x RIGHT JOIN (SELECT 2 AS k) y USING k",
    //   if require_right_keys = FALSE, it outputs: [2]
    //   if require_right_keys = TRUE, it outputs: [NULL] (currently QueryPlanner does not generate this case)
    //
    // For query "SELECT k FROM (SELECT 1 AS k) x FULL JOIN (SELECT 2 AS k) y USING k",
    //   if require_right_keys = FALSE, it outputs: [1], [2]
    //   if require_right_keys = TRUE, it outputs: [1], [NULL] (currently QueryPlanner does not generate this case)
    std::optional<std::vector<bool>> require_right_keys;

    ASOFJoinInequality asof_inequality;

    DistributionType distribution_type = DistributionType::UNKNOWN;
    JoinAlgorithm join_algorithm = JoinAlgorithm::DEFAULT;
    bool is_magic;
    bool is_ordered;
    bool simple_reordered;

    LinkedHashMap<String, RuntimeFilter> runtime_filter_builders;
};

}
