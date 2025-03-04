#include <Query/Processors/QueryPlan/ReadFromMergeTreeExt.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INDEX_NOT_USED;
    extern const int LOGICAL_ERROR;
}

ReadFromMergeTreeExt::ReadFromMergeTreeExt(
    MergeTreeData::DataPartsVector parts_,
    std::vector<AlterConversionsPtr> alter_conversions_,
    Names all_column_names_,
    const MergeTreeData & data_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    size_t max_block_size_,
    size_t num_streams_,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
    LoggerPtr log_,
    AnalysisResultPtr analyzed_result_ptr_,
    bool enable_parallel_reading_,
    DeleteBitmapGetter delete_bitmap_getter_,
    const size_t min_block_size_,
    const bool size_predictor_estimate_lc_size_by_fullstate_,
    const bool map_column_keys_column_queried_)
    : ReadFromMergeTree(parts_, alter_conversions_, all_column_names_, data_, query_info_, storage_snapshot, context_, max_block_size_, num_streams_, max_block_numbers_to_read_, log_, analyzed_result_ptr_, enable_parallel_reading_)
    , delete_bitmap_getter(delete_bitmap_getter_)
    , min_block_size(min_block_size_)
    , size_predictor_estimate_lc_size_by_fullstate(size_predictor_estimate_lc_size_by_fullstate_)
    , map_column_keys_column_queried(map_column_keys_column_queried_)
{
    if (map_column_keys_column_queried)
    {
        auto type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
        output_stream->header.insert({type->createColumn(), type, "_map_column_keys"});
    }
}

void ReadFromMergeTreeExt::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto result = getAnalysisResult();
    LOG_DEBUG(
        log,
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        result.parts_before_pk,
        result.total_parts,
        result.selected_parts,
        result.selected_marks_pk,
        result.total_marks_pk,
        result.selected_marks,
        result.selected_ranges);

    //todo: need to implement the relevant processors
    /*
    if (context->getSettingsRef().report_segment_profiles)
        fillRuntimeAttributeDescriptions(result);

    */
    /*
    ProfileEvents::increment(ProfileEvents::SelectedParts, result.selected_parts);
    ProfileEvents::increment(ProfileEvents::SelectedRanges, result.selected_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, result.selected_marks);

    auto query_id_holder = MergeTreeDataSelectExecutor::checkLimits(data, result.parts_with_ranges, context);

    if (result.part_cache_holder)
        pipeline.addCacheHolder(std::move(result.part_cache_holder));

    if (result.parts_with_ranges.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    auto selected_parts_vector = getSelectedPartsVector(result);

    ActionsDAGPtr result_projection;

    Names column_names_to_read = std::move(result.column_names_to_read);
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.final() && result.sampling.use_sampling && !context->getSettingsRef().enable_sample_by_range
        && !context->getSettingsRef().enable_deterministic_sample_by_range)
    {
        std::vector<String> add_columns = result.sampling.filter_expression->getRequiredColumns().getNames();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
        std::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()),
                                   column_names_to_read.end());
    }

    const auto & input_order_info = query_info.input_order_info
        ? query_info.input_order_info
        : (query_info.projection ? query_info.projection->input_order_info : nullptr);

    Pipe pipe;

    const auto & settings = context->getSettingsRef();
    bool can_read_in_partition_order = false;

    if (select.final())
    {
        std::vector<String> add_columns = metadata_for_reading->getColumnsRequiredForSortingKey();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

        if (!data.merging_params.sign_column.empty())
            column_names_to_read.push_back(data.merging_params.sign_column);
        if (!data.merging_params.version_column.empty())
            column_names_to_read.push_back(data.merging_params.version_column);

        std::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());

        pipe = spreadMarkRangesAmongStreamsFinal(
            std::move(result.parts_with_ranges),
            column_names_to_read,
            result_projection);
    }
    else if ((settings.optimize_read_in_order || settings.optimize_aggregation_in_order) && input_order_info)
    {
        size_t prefix_size = input_order_info->order_key_prefix_descr.size();
        auto order_key_prefix_ast = metadata_for_reading->getSortingKey().expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);

        auto syntax_result = TreeRewriter(context).analyze(order_key_prefix_ast, metadata_for_reading->getColumns().getAllPhysical());
        auto sorting_key_prefix_expr = ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActionsDAG(false);

        can_read_in_partition_order = (settings.optimize_read_in_partition_order || settings.force_read_in_partition_order)
            && canReadInPartitionOrder(
                *metadata_for_reading, *input_order_info, query_info.query->as<ASTSelectQuery &>(),
                data.getSettings()->partition_by_monotonicity_hint);

        if (can_read_in_partition_order && result.selected_partitions > 1)
        {
            pipe = spreadMarkRangesAmongStreamsWithPartitionOrder(
                std::move(result.parts_with_ranges),
                column_names_to_read,
                sorting_key_prefix_expr,
                result_projection,
                input_order_info,
                result.delayed_indices);
        }
        else
        {
            bool need_preliminary_merge = (result.parts_with_ranges.size() > settings.read_in_order_two_level_merge_threshold);
            pipe = spreadMarkRangesAmongStreamsWithOrder(
                std::move(result.parts_with_ranges),
                column_names_to_read,
                sorting_key_prefix_expr,
                result_projection,
                input_order_info,
                requested_num_streams,
                need_preliminary_merge,
                result.delayed_indices);
        }
    }
    else
    {
        pipe = spreadMarkRangesAmongStreams(
            std::move(result.parts_with_ranges),
            column_names_to_read);
    }

    if (settings.force_read_in_partition_order && !can_read_in_partition_order)
        throw Exception(ErrorCodes::INDEX_NOT_USED, "Cannot read in partition order but 'force_read_in_partition_order' is set");

    if (pipe.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    if (result.sampling.use_sampling && !settings.enable_sample_by_range && !settings.enable_deterministic_sample_by_range)
    {
        auto sampling_actions = std::make_shared<ExpressionActions>(result.sampling.filter_expression);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header,
                sampling_actions,
                result.sampling.filter_function->getColumnName(),
                false);
        });
    }

    Block cur_header = pipe.getHeader();

    auto append_actions = [&result_projection](ActionsDAGPtr actions)
    {
        if (!result_projection)
            result_projection = std::move(actions);
        else
            result_projection = ActionsDAG::merge(std::move(*result_projection), std::move(*actions));
    };

    if (sample_factor_column_queried)
    {
        ColumnWithTypeAndName column;
        column.name = "_sample_factor";
        column.type = std::make_shared<DataTypeFloat64>();
        column.column = column.type->createColumnConst(0, Field(result.sampling.used_sample_factor));

        auto adding_column = ActionsDAG::makeAddingColumnActions(std::move(column));
        append_actions(std::move(adding_column));
    }

    if (map_column_keys_column_queried)
    {
        if (settings.early_limit_for_map_virtual_columns > 0)
        {
            pipe.addSimpleTransform([&](const Block & header) {
                return std::make_shared<LimitTransform>(header, settings.early_limit_for_map_virtual_columns, 0);
            });
        }

        ColumnWithTypeAndName column;
        column.name = "_map_column_keys";
        column.type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));

        column.column = column.type->createColumnConst(0, Field(extractMapColumnKeys(data, selected_parts_vector)));

        auto adding_column = ActionsDAG::makeAddingColumnActions(std::move(column));
        append_actions(std::move(adding_column));
    }

    if (result_projection)
        cur_header = result_projection->updateHeader(cur_header);

    if (!isCompatibleHeader(cur_header, getOutputStream().header))
    {
        auto converting = ActionsDAG::makeConvertingActions(
            cur_header.getColumnsWithTypeAndName(),
            getOutputStream().header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        append_actions(std::move(converting));
    }

    if (result_projection)
    {
        auto projection_actions = std::make_shared<ExpressionActions>(result_projection);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, projection_actions);
        });
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    if (query_id_holder)
        pipe.addQueryIdHolder(std::move(query_id_holder));

    pipeline.init(std::move(pipe));
    */
}

std::shared_ptr<IQueryPlanStep> ReadFromMergeTreeExt::copy() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromReadFromMergeTreeExtMergeTree can not copy");
}

}
