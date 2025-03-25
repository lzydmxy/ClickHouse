#include <Query/Processors/QueryPlan/JoinStepExt.h>

#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Processors/Transforms/FilterTransformExt.h>
#include <Query/Common/PredicateUtils.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterConsumer.h>
#include <Query/Executor/PlanSegmentInstance.h>

#include <QueryPipeline/QueryPipelineBuilder.h>


#include <memory>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/JoinSwitcher.h>
#include <Query/Interpreters/TableJoinExt.h>
#include <Query/Common/SymbolsExtractor.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinPtr JoinStepExt::makeJoin(
    ContextPtr context,
    std::shared_ptr<RuntimeFilterConsumer> && consumer,
    size_t num_streams,
    ExpressionActionsPtr filter_action,
    String filter_column_name)
{
    const auto & settings = context->getSettingsRef();
    const auto & optimizer_settings = context->getOptimizerContext()->getSettingsRef();
    auto table_join = std::make_shared<TableJoinExt>(settings, optimizer_settings, context->getGlobalTemporaryVolume());
    if (consumer)
        table_join->setRuntimeFilterConsumer(consumer);

    if (kind != JoinKind::Inner && kind != JoinKind::Cross)
        table_join->setInequalCondition(filter_action, filter_column_name);

    // TODO support storage join
    //    if (table_to_join.database_and_table_name)
    //    {
    //        auto joined_table_id = context->resolveStorageID(table_to_join.database_and_table_name);
    //        StoragePtr table = DatabaseCatalog::instance().tryGetTable(joined_table_id, context);
    //        if (table)
    //        {
    //            if (dynamic_cast<StorageJoin *>(table.get()) ||
    //                dynamic_cast<StorageDictionary *>(table.get()))
    //                table_join->joined_storage = table;
    //        }
    //    }

    auto left_table_names = input_streams[0].header.getNames();
    NameSet left_table_names_set(left_table_names.begin(), left_table_names.end());

    table_join->setColumnsFromJoinedTable(input_streams[1].header.getNamesAndTypesList(), left_table_names_set, "");

    auto using_ast = std::make_shared<ASTExpressionList>();
    ASTs on_ast_terms;
    for (size_t index = 0; index < left_keys.size(); ++index)
    {
        ASTPtr left = std::make_shared<ASTIdentifier>(left_keys[index]);
        ASTPtr right = std::make_shared<ASTIdentifier>(right_keys[index]);
        if (has_using)
        {
            table_join->renames[left_keys[index]] = right_keys[index];
            table_join->addUsingKey(left);
            using_ast->children.emplace_back(left);
        }
        else
        {
            bool null_safe = getKeyIdNullSafe(index);
            table_join->addOnKeys(left, right, null_safe);
            const String fn = null_safe ? "bitEquals" : "equals";
            on_ast_terms.emplace_back(makeASTFunction(fn, left, right));
        }
    }

    if (has_using)
    {
        table_join->table_join.using_expression_list = using_ast;
    }
    else
    {
        if (on_ast_terms.size() == 1)
            table_join->table_join.on_expression = on_ast_terms.back();
        else if (on_ast_terms.size() > 1)
            table_join->table_join.on_expression = makeASTFunction("and", on_ast_terms);
    }

    for (const auto & item : output_stream->header)
    {
        if (!input_streams[0].header.has(item.name))
        {
            NameAndTypePair joined_column{item.name, item.type};
            table_join->addJoinedColumn(joined_column);
        }
    }

    // add the symbol needed in the join filter but not existed in join output stream to the output,
    // because FilterTransform built after the join need these symbols.
    if (filter && !PredicateUtils::isTruePredicate(filter))
    {
        for (const auto & symbol : SymbolsExtractor::extract(filter))
        {
            if (!output_stream->header.has(symbol) && input_streams[1].header.has(symbol))
            {
                NameAndTypePair joined_column{symbol, input_streams[1].header.getByName(symbol).type};
                table_join->addJoinedColumn(joined_column);
            }
        }
    }

    table_join->setAsofInequality(asof_inequality);
    if (context->getOptimizerContext()->getSettingsRef().enforce_all_join_to_any_join)
    {
        strictness = JoinStrictness::RightAny;
    }

    table_join->table_join.strictness = isCrossJoin() ? JoinStrictness::Unspecified : strictness;
    table_join->table_join.kind = isCrossJoin() ? JoinKind::Cross : kind;

    if (enforceNestLoopJoin())
    {
        if (context->getOptimizerContext()->getSettingsRef().enable_nested_loop_join)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Set enable_nested_loop_join=1 to enable outer join with filter");
        // TODO support NESTED_LOOP_JOIN join Algorithm, we may not need
        // table_join->join_algorithm = JoinAlgorithm::NESTED_LOOP_JOIN;
        // table_join->table_join.on_expression = filter->clone();
        // table_join->table_join.kind = isCrossJoin() ? JoinKind::Inner : kind;
    }

    bool allow_merge_join = table_join->allowMergeJoin();
    bool allow_grace_hash_join = true;
    if (context->getOptimizerContext()->getSettingsRef().use_grace_hash_only_repartition && distribution_type != DistributionType::REPARTITION)
        allow_grace_hash_join = false;
    /// HashJoin with Dictionary optimisation
    auto l_sample_block = input_streams[0].header;
    auto r_sample_block = input_streams[1].header;
    String dict_name;
    String key_name;

    // TODO support NESTED_LOOP_JOIN join Algorithm, we may not need
    // if (table_join->forceNestedLoopJoin())
    //     return std::make_shared<NestedLoopJoin>(table_join, r_sample_block, context);

    if (table_join->forceHashJoin() || (table_join->preferMergeJoin() && !allow_merge_join))
    {
        if (table_join->allowParallelHashJoin() && join_algorithm == JoinAlgorithm::PARALLEL_HASH)
        {
            // TODO: Yuanning RuntimeFilter, compare with CE code when fix
            // if (enable_parallel_hash_join)
            // {
            //     LOG_TRACE(getLogger("JoinStep::makeJoin"), "will use parallel Hash Join");
            //     std::vector<JoinPtr> res;
            //     res.reserve(num_streams);
            //     for (size_t i = 0; i < num_streams; ++i)
            //         res.emplace_back(std::make_shared<HashJoin>(table_join, r_sample_block));

            //     if (consumer)
            //         consumer->fixParallel(num_streams);
            //     return res;
            // }

            auto toPowerOfTwo = [](UInt32 x) -> UInt32
            {
                if (x <= 1)
                    return 1;
                return static_cast<UInt32>(1) << (32 - std::countl_zero(x - 1));
            };

            LOG_TRACE(getLogger("JoinStep::makeJoin"), "will use ConcurrentHashJoin");
            if (consumer)
                consumer->fixParallel(toPowerOfTwo(std::min<size_t>(num_streams, 256)));

            return std::make_shared<ConcurrentHashJoin>(context, table_join, settings.max_threads, r_sample_block);
        }
        else if (join_algorithm == JoinAlgorithm::GRACE_HASH && GraceHashJoin::isSupported(table_join) && allow_grace_hash_join)
        {
            if (GraceHashJoin::isSupported(table_join) ) {
                table_join->join_algorithm = {JoinAlgorithm::GRACE_HASH};
                // TODO support join left side parallel for GraceHashJoin
                // auto parallel = (context->getOptimizerContext()->getSettingsRef()->grace_hash_join_left_side_parallel != 0 ? context->getOptimizerContext()->getSettingsRef()->grace_hash_join_left_side_parallel: num_streams);
                return std::make_shared<GraceHashJoin>(context, table_join, l_sample_block, r_sample_block, context->getTempDataOnDisk(), false);
            } else if (allow_merge_join) { // fallback into merge join
                LOG_WARNING(getLogger("JoinStep::makeJoin"), "Grace hash join is not support, fallback into merge join.");
                return std::make_shared<JoinSwitcher>(table_join, r_sample_block);
            } else { // fallback into hash join when grace hash and merge join not supported
                LOG_WARNING(getLogger("JoinStep::makeJoin"), "Grace hash join and merge join is not support, fallback into hash join.");
                return std::make_shared<HashJoin>(table_join, r_sample_block);
            }
        }
        return std::make_shared<HashJoin>(table_join, r_sample_block);
    }
    else if (table_join->forceMergeJoin() || (table_join->preferMergeJoin() && allow_merge_join))
        return {std::make_shared<MergeJoin>(table_join, r_sample_block)};
    else if ((table_join->forceGraceHashJoin() || join_algorithm == JoinAlgorithm::GRACE_HASH) && allow_grace_hash_join)
    {
        if (GraceHashJoin::isSupported(table_join) ) {
            // TODO support join left side parallel for GraceHashJoin
            // auto parallel = (context->getOptimizerContext()->getSettingsRef()->grace_hash_join_left_side_parallel != 0 ? context->getOptimizerContext()->getSettingsRef()->grace_hash_join_left_side_parallel: num_streams);
            // return std::make_shared<GraceHashJoin>(context, table_join, l_sample_block, r_sample_block, context->getTempDataOnDisk(), parallel, context->getSettingsRef().spill_mode == SpillMode::AUTO, false, num_streams);
            return std::make_shared<GraceHashJoin>(context, table_join, l_sample_block, r_sample_block, context->getTempDataOnDisk(), false);
        } else if (allow_merge_join) { // fallback into merge join
            LOG_WARNING(getLogger("JoinStep::makeJoin"), "Grace hash join is not support, fallback into merge join.");
            return std::make_shared<JoinSwitcher>(table_join, r_sample_block);
        } else { // fallback into hash join when grace hash and merge join not supported
            LOG_WARNING(getLogger("JoinStep::makeJoin"), "Grace hash join and merge join is not support, fallback into hash join.");
            return std::make_shared<HashJoin>(table_join, r_sample_block);
        }
    }
    return std::make_shared<JoinSwitcher>(table_join, r_sample_block);
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
    ASOFJoinInequality asof_inequality_,
    DistributionType distribution_type_,
    JoinAlgorithm join_algorithm_,
    bool is_magic_,
    bool is_ordered_,
    bool simple_reordered_,
    LinkedHashMap<String, RuntimeFilter> runtime_filter_builders_)
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
    , runtime_filter_builders(std::move(runtime_filter_builders_))
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

    bool need_build_runtime_filter = false;

    ExpressionActionsPtr filter_action;

    const auto & settings_ext = settings.getBuildQueryPipelineSettingsExt();

    if (!join)
    {
        if (filter && !PredicateUtils::isTruePredicate(filter))
        {
            Names output;

            bool has_outer_join_semantic = settings_ext.context->getSettingsRef().join_use_nulls &&
                (getStrictness() == JoinStrictness::Any || getStrictness() == JoinStrictness::All || getStrictness() == JoinStrictness::RightAny || getStrictness() == JoinStrictness::Asof);
            bool make_nullable_for_left = has_outer_join_semantic && isRightOrFull(getKind());
            bool make_nullable_for_right = has_outer_join_semantic && isLeftOrFull(getKind());

            Block header;
            for (const auto & col : input_streams[0].header)
            {
                if (make_nullable_for_left && JoinCommon::canBecomeNullable(col.type))
                {
                    header.insert(ColumnWithTypeAndName{col.column, JoinCommon::convertTypeToNullable(col.type), col.name});
                }
                else
                {
                    header.insert(col);
                }
            }
            for (const auto & col : input_streams[1].header)
            {
                if (make_nullable_for_right && JoinCommon::canBecomeNullable(col.type))
                {
                    header.insert(ColumnWithTypeAndName{col.column, JoinCommon::convertTypeToNullable(col.type), col.name});
                }
                else
                {
                    header.insert(col);
                }
            }
            for (const auto & item : header)
                output.emplace_back(item.name);
            output.emplace_back(filter->getColumnName());

            auto actions_dag = QueryPlanStepHelper::createExpressionActions(settings_ext.context, header.getNamesAndTypesList(), output, filter->clone());
            filter_action = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());
        }

        if (!runtime_filter_builders.empty() && settings_ext.distributed_settings.is_distributed)
        {
            auto builder = createRuntimeFilterBuilder(settings_ext.context);
            std::shared_ptr<RuntimeFilterConsumer> consumer = std::make_shared<RuntimeFilterConsumer>(
                builder,
                settings_ext.context->getInitialQueryId(),
                1, /// for normal HashJoin only one right table, parallel or concurrent hash join will change it to num_streams
                settings_ext.distributed_settings.parallel_size,
                settings_ext.distributed_settings.coordinator_address,
                settings_ext.context->getOptimizerContext()->getPlanSegmentInstanceID().parallel_index); // TODO: Yuanning RuntimeFilter, parallel_id

            join = makeJoin(settings_ext.context, std::move(consumer), pipelines[0]->getNumStreams(), filter_action, filter->getColumnName());
            need_build_runtime_filter = true;
        }
        else
            join = makeJoin(settings_ext.context, nullptr, pipelines[0]->getNumStreams(), filter_action, filter->getColumnName());
        max_block_size = settings_ext.context->getSettingsRef().max_block_size;
    }

    if (need_build_runtime_filter)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryPipelineBuilder should support runtime filter.");
    }

    auto pipeline = QueryPipelineBuilder::joinPipelinesRightLeft(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        output_stream->header,
        max_block_size,
        max_streams,
        keep_left_read_in_order,
        /* need_build_runtime_filter, TODO QueryPipelineBuilder::joinPipelinesRightLeft support runtime filter.*/
        &processors);

    // if NestLoopJoin is choose, no need to add filter stream.
    if (filter && !PredicateUtils::isTruePredicate(filter) /*&& join->getType() != JoinType::NestedLoop*/
        && (kind == JoinKind::Inner || kind == JoinKind::Cross))
    {
        Names output;
        auto header = pipeline->getHeader();
        for (const auto & item : header)
            output.emplace_back(item.name);
        output.emplace_back(filter->getColumnName());

        auto actions_dag = QueryPlanStepHelper::createExpressionActions(settings_ext.context, header.getNamesAndTypesList(), output, filter->clone());
        auto expression = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

        pipeline->addSimpleTransform([&](const Block & input_header, QueryPipelineBuilder::StreamType stream_type) {
            bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
            return std::make_shared<FilterTransformExt>(input_header, expression, filter->getColumnName(), true, on_totals);
        });
    }

    QueryPlanStepHelper::projection(*pipeline, output_stream->header, settings);
    return pipeline;
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

RuntimeFilterBuilderPtr JoinStepExt::createRuntimeFilterBuilder(ContextPtr context) const
{
    return std::make_shared<RuntimeFilterBuilder>(context->getOptimizerContext()->getSettings(), runtime_filter_builders);
}

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
