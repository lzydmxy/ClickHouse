#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Common/PredicateUtils.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <memory>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/JoinSwitcher.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinStepExt::JoinStepExt(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_,
    bool is_ordered_,
    bool simple_reordered_)
    : JoinStep(left_stream_, right_stream_, join_, max_block_size_, max_streams_, keep_left_read_in_order_)
    , is_ordered(is_ordered_)
    , simple_reordered(simple_reordered_)
{
}

JoinStepExt::JoinStepExt(
    DataStreams input_streams_,
    DataStream output_stream_,
    JoinKind kind_,
    JoinStrictness strictness_,
    size_t max_streams_,
    bool keep_left_read_in_order_,
    Names left_keys_,
    Names right_keys_,
    std::vector<bool> key_ids_null_safe_,
    ConstASTPtr filter_,
    bool has_using_,
    std::optional<std::vector<bool>> require_right_keys_,
    ASOF::Inequality asof_inequality_,
    DistributionType distribution_type_,
    JoinAlgorithm join_algorithm_,
    bool is_magic_,
    bool is_ordered_,
    bool simple_reordered_
    /*LinkedHashMap<String, RuntimeFilterBuildInfos> runtime_filter_builders_*/)
    : JoinStep({}, {}, nullptr, 0, max_streams_, keep_left_read_in_order_)
    , kind(kind_)
    , strictness(strictness_)
    , left_keys(std::move(left_keys_))
    , right_keys(std::move(right_keys_))
    , key_ids_null_safe(std::move(key_ids_null_safe_))
    , filter(std::move(filter_))
    , has_using(has_using_)
    , require_right_keys(std::move(require_right_keys_))
    , asof_inequality(asof_inequality_)
    , distribution_type(distribution_type_)
    , join_algorithm(join_algorithm_)
    , is_magic(is_magic_)
    , is_ordered(is_ordered_)
    , simple_reordered(simple_reordered_)
    /*, runtime_filter_builders(std::move(runtime_filter_builders_))*/
{
    assert(kind != JoinKind::Comma);
    assert(left_keys.size() == right_keys.size());
    // fixme@kaixi
    // assert(!isCross(kind) || isUnspecified(strictness)); // CROSS JOIN must use Unspecified strictness

    input_streams = std::move(input_streams_);
    output_stream = std::move(output_stream_);
}

bool JoinStepExt::hasKeyIdNullSafe() const
{
    return std::any_of(key_ids_null_safe.begin(), key_ids_null_safe.end(), [](auto x) { return x; });
}

bool JoinStepExt::getKeyIdNullSafe(size_t key_index) const
{
    if (key_index >= key_ids_null_safe.size())
        return false;
    return key_ids_null_safe.at(key_index);
}

void JoinStepExt::setOutputStream(DataStream output_stream_)
{
    output_stream = std::move(output_stream_);
}

QueryPipelineBuilderPtr JoinStepExt::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");
    // TODO Impl updatePipeline for JoinStepExt
    return JoinStep::updatePipeline(std::move(pipelines), settings);
}

bool JoinStepExt::enforceNestLoopJoin() const
{
    if (filter && !PredicateUtils::isTruePredicate(filter))
    {
        bool strictness_join = strictness == JoinStrictness::Any || strictness == JoinStrictness::Asof;
        return strictness_join || (left_keys.empty() && isLeftOrRightOuterJoin());
    }
    return false;
}

bool JoinStepExt::enforceGraceHashJoin() const
{
    return false;
}

bool JoinStepExt::supportReorder(bool support_filter, bool support_cross) const
{
    if (!support_filter && !PredicateUtils::isTruePredicate(filter))
        return false;

    if (require_right_keys || has_using)
        return false;

    if (hasKeyIdNullSafe())
        return false;

    if (strictness != JoinStrictness::Unspecified && strictness != JoinStrictness::All)
        return false;

    bool cross_join = isCrossJoin();
    if (!support_cross && cross_join)
        return false;

    if (support_cross && cross_join)
        return true;

    return kind == JoinKind::Inner && !left_keys.empty();
}

std::shared_ptr<IQueryPlanStep> JoinStepExt::copy(ContextPtr) const
{
    return std::make_shared<JoinStepExt>(
        input_streams,
        output_stream.value(),
        kind,
        strictness,
        max_streams,
        keep_left_read_in_order,
        left_keys,
        right_keys,
        std::move(key_ids_null_safe),
        filter,
        has_using,
        require_right_keys,
        asof_inequality,
        distribution_type,
        join_algorithm,
        is_magic,
        is_ordered,
        simple_reordered
        /*runtime_filter_builders*/);
}

void JoinStepExt::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

// RuntimeFilterBuilderPtr JoinStepExt::createRuntimeFilterBuilder(ContextPtr context) const
// {
//     return std::make_shared<RuntimeFilterBuilder>(context->getSettingsRef(), runtime_filter_builders);
// }

bool JoinStepExt::mustReplicate() const
{
    if (left_keys.empty() && (kind == JoinKind::Inner || kind == JoinKind::Left || kind == JoinKind::Cross))
    {
        // There is nothing to partition on
        return true;
    }
    return false;
}

bool JoinStepExt::mustRepartition() const
{
    return kind == JoinKind::Right || kind == JoinKind::Full;
}

}
