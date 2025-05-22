#include <Query/Processors/QueryPlan/LineageInfo.h>

#include <memory>
#include <unordered_map>
#include <vector>
#include <Core/Names.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Storages/StorageDistributed.h>

namespace DB
{
void LineageInfoVisitor::visitPlanNode(PlanNodeBase & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitPartitionTopNStepExtNode(PartitionTopNStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitRemoteExchangeSourceStepExtNode(RemoteExchangeSourceStepExtNode &, LineageInfoContext &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not impl LineageInfo");
}

void LineageInfoVisitor::visitOffsetStepNode(OffsetStepNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitBufferStepExtNode(BufferStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitFinishSortingStepExtNode(FinishSortingStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitISourceNodeWithoutStorage(
    PlanNodeBase & node, LineageInfoContext & lineage_info_context, DatabaseAndTableName source_table)
{
    const auto * step = node.getStep().get();
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    SourceInfo new_expression_or_value_source_info;
    if (!source_table.first.empty())
        new_expression_or_value_source_info.source_tables.emplace_back(source_table);
    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
        outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitReadNothingStepNode(ReadNothingStepNode & node, LineageInfoContext & lineage_info_context)
{
    visitISourceNodeWithoutStorage(node, lineage_info_context);
}

void LineageInfoVisitor::visitValuesStepExtNode(ValuesStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    visitISourceNodeWithoutStorage(node, lineage_info_context);
}

void LineageInfoVisitor::visitReadStorageRowCountStepExtNode(ReadStorageRowCountStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    visitISourceNodeWithoutStorage(node, lineage_info_context, {step->getStorageID().getDatabaseName(), step->getStorageID().getTableName()});
}

void LineageInfoVisitor::visitExtremesStepNode(ExtremesStepNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitLimitStepExtNode(LimitStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitFinalSampleStepExtNode(FinalSampleStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitMultiJoinStepExtNode(MultiJoinStepExtNode &, LineageInfoContext &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not impl LineageInfo");
}

void LineageInfoVisitor::visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitLocalExchangeStepExtNode(LocalExchangeStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitLimitByStepNode(LimitByStepNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitWindowStepNode(WindowStepNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    auto input_stream_lineages = lineage_info_context.output_stream_lineages;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    std::unordered_map<String, const WindowFunctionDescription &> column_name_to_window_desc;
    for (const auto & function : QueryPlanStepHelper::getWindowStepFunctions(*step))
    {
        column_name_to_window_desc.emplace(function.column_name, function);
    }

    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        if (input_stream_lineages.contains(output_name))
        {
            new_output_stream_lineages.emplace(output_name, input_stream_lineages.at(output_name));
            continue;
        }
        else
        {
            auto outputstream_info = std::make_shared<OutputStreamInfo>();
            outputstream_info->output_name = output_name;
            String new_expression_name = output_name;
            if (column_name_to_window_desc.contains(output_name))
            {
                const auto & window_desc = column_name_to_window_desc.at(output_name);
                for (const auto & arg_name : window_desc.argument_names)
                {
                    if (input_stream_lineages.contains(arg_name))
                        outputstream_info->add(input_stream_lineages.at(arg_name));
                }
                if (outputstream_info->isEmpty())
                    new_expression_name = window_desc.function_node->getColumnName();
            }

            if (outputstream_info->isEmpty())
            {
                new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
                outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
            }
            new_output_stream_lineages.emplace(output_name, outputstream_info);
        }
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitArrayJoinStepNode(ArrayJoinStepNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::addNewExpressionSource(PlanNodeBase & node, LineageInfoContext & lineage_info_context, Names new_header_names)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    for (auto & new_header_name : new_header_names)
    {
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = new_header_name;
        new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_header_name);
        outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_header_name));
        lineage_info_context.output_stream_lineages.emplace(outputstream_info->output_name, std::move(outputstream_info));
    }
    expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
}

void LineageInfoVisitor::visitExpandStepExtNode(ExpandStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    addNewExpressionSource(node, lineage_info_context, {step->getGroupIdSymbol()});
}

void LineageInfoVisitor::visitMarkDistinctStepExtNode(MarkDistinctStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    addNewExpressionSource(node, lineage_info_context, {step->getMarkerSymbol()});
}

void LineageInfoVisitor::visitSortingStepExtNode(SortingStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitMergeSortingStepExtNode(MergeSortingStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitMergingSortedStepExtNode(MergingSortedStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitPartialSortingStepExtNode(PartialSortingStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitDistinctStepExtNode(DistinctStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    addNewExpressionSource(node, lineage_info_context, {step->getUniqueId()});
}

void LineageInfoVisitor::visitExchangeStepExtNode(ExchangeStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    std::vector<LineageInfoContext> children_lineage_info_context{node.getChildren().size()};
    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        VisitorUtil::accept(node.getChildren()[i], *this, children_lineage_info_context[i]);
        lineage_info_context.tables.insert(
            lineage_info_context.tables.end(),
            children_lineage_info_context[i].tables.begin(),
            children_lineage_info_context[i].tables.end());
    }

    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    const auto & output_to_inputs = step->getOutToInputs();
    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;

        if (output_to_inputs.contains(output_name))
        {
            const auto & input_stream_list = output_to_inputs.at(output_name);
            for (size_t i = 0; i < input_stream_list.size(); ++i)
                outputstream_info->add(children_lineage_info_context[i].output_stream_lineages.at(input_stream_list[i]));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitCTERefStepExtNode(CTERefStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    auto cte_id = step->getId();
    if (!cte_visit_results.contains(cte_id))
    {
        LineageInfoContext cte_lineage_info_context;
        cte_helper.accept(cte_id, *this, cte_lineage_info_context);
        cte_visit_results.emplace(cte_id, cte_lineage_info_context);
    }

    if (cte_visit_results.contains(cte_id))
    {
        lineage_info_context = cte_visit_results.at(cte_id);
        const auto & output_to_input = step->getOutputColumns();
        auto input_stream_lineages = lineage_info_context.output_stream_lineages;
        std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
        for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
        {
            auto output_name = output_column_and_type.name;
            auto outputstream_info = std::make_shared<OutputStreamInfo>();
            outputstream_info->output_name = output_name;
            if (output_to_input.contains(output_name) && input_stream_lineages.contains(output_to_input.at(output_name)))
                outputstream_info->add(input_stream_lineages.at(output_to_input.at(output_name)));
            new_output_stream_lineages.emplace(output_name, outputstream_info);
        }
        lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
    }
}
void LineageInfoVisitor::visitExplainAnalyzeStepExtNode(ExplainAnalyzeStepExtNode &, LineageInfoContext &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not impl LineageInfo");
}

void LineageInfoVisitor::visitIntermediateResultCacheStepExtNode(IntermediateResultCacheStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitTopNFilteringStepExtNode(TopNFilteringStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitFillingStepNode(FillingStepNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitTotalsHavingStepExtNode(TotalsHavingStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitMergingAggregatedStepExtNode(MergingAggregatedStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    NameSet keys{step->getKeys().begin(), step->getKeys().end()};
    auto input_stream_lineages = lineage_info_context.output_stream_lineages;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        String new_expression_name = output_name;
        if (input_stream_lineages.contains(output_name))
        {
            outputstream_info->add(input_stream_lineages.at(output_name));
        }
        else
        {
            for (const auto & agg : step->getAggregates())
            {
                if (agg.column_name == output_name)
                {
                    for (const auto & argument_name : agg.argument_names)
                    {
                        if (input_stream_lineages.contains(argument_name))
                            outputstream_info->add(input_stream_lineages.at(argument_name));
                    }
                }
                if (outputstream_info->isEmpty())
                    new_expression_name = agg.function->getDescription();
            }
        }

        if (outputstream_info->isEmpty())
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitSetOperationNode(PlanNodeBase & node, LineageInfoContext & lineage_info_context)
{
    const auto & step = dynamic_cast<const SetOperationStepExt &>(*node.getStep());
    std::vector<LineageInfoContext> children_lineage_info_context{node.getChildren().size()};
    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        VisitorUtil::accept(node.getChildren()[i], *this, children_lineage_info_context[i]);
        lineage_info_context.tables.insert(
            lineage_info_context.tables.end(),
            children_lineage_info_context[i].tables.begin(),
            children_lineage_info_context[i].tables.end());
    }

    SourceInfo new_expression_or_value_source_info;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    const auto & output_to_inputs = step.getOutToInputs();
    for (const auto & output_column_and_type : step.getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        if (output_to_inputs.contains(output_name))
        {
            const auto & input_stream_list = output_to_inputs.at(output_name);
            for (size_t i = 0; i < input_stream_list.size(); ++i)
                outputstream_info->add(children_lineage_info_context[i].output_stream_lineages.at(input_stream_list[i]));
        }
        else
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitUnionStepExtNode(UnionStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto & step = dynamic_cast<const UnionStepExt &>(*node.getStep());
    std::vector<LineageInfoContext> children_lineage_info_context{node.getChildren().size()};
    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        VisitorUtil::accept(node.getChildren()[i], *this, children_lineage_info_context[i]);
        lineage_info_context.tables.insert(
            lineage_info_context.tables.end(),
            children_lineage_info_context[i].tables.begin(),
            children_lineage_info_context[i].tables.end());
    }

    SourceInfo new_expression_or_value_source_info;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    const auto & output_to_inputs = step.getOutToInputs();
    for (const auto & output_column_and_type : step.getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        if (output_to_inputs.contains(output_name))
        {
            const auto & input_stream_list = output_to_inputs.at(output_name);
            for (size_t i = 0; i < input_stream_list.size(); ++i)
                outputstream_info->add(children_lineage_info_context[i].output_stream_lineages.at(input_stream_list[i]));
        }
        else
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitExceptStepExtNode(ExceptStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    visitSetOperationNode(node, lineage_info_context);
}

void LineageInfoVisitor::visitIntersectStepExtNode(IntersectStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    visitSetOperationNode(node, lineage_info_context);
}

void LineageInfoVisitor::visitIntersectOrExceptStepNode(IntersectOrExceptStepNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    std::vector<LineageInfoContext> children_lineage_info_context{node.getChildren().size()};
    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        VisitorUtil::accept(node.getChildren()[i], *this, children_lineage_info_context[i]);
        lineage_info_context.tables.insert(
            lineage_info_context.tables.end(),
            children_lineage_info_context[i].tables.begin(),
            children_lineage_info_context[i].tables.end());
    }

    SourceInfo new_expression_or_value_source_info;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        for (auto & child_lineage_info_context : children_lineage_info_context)
        {
            if (child_lineage_info_context.output_stream_lineages.contains(output_name))
                outputstream_info->add(child_lineage_info_context.output_stream_lineages.at(output_name));
        }

        if (outputstream_info->isEmpty())
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        }

        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }

    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitApplyStepExtNode(ApplyStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    LineageInfoContext left_lineage_info_context;
    VisitorUtil::accept(node.getChildren()[0], *this, left_lineage_info_context);

    LineageInfoContext right_lineage_info_context;
    VisitorUtil::accept(node.getChildren()[1], *this, right_lineage_info_context);

    lineage_info_context.tables.insert(
        lineage_info_context.tables.end(), left_lineage_info_context.tables.begin(), left_lineage_info_context.tables.end());
    lineage_info_context.tables.insert(
        lineage_info_context.tables.end(), right_lineage_info_context.tables.begin(), right_lineage_info_context.tables.end());

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    auto & left_output_stream_lineages = left_lineage_info_context.output_stream_lineages;
    auto & right_output_stream_lineages = right_lineage_info_context.output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        if (left_output_stream_lineages.contains(output_name))
        {
            lineage_info_context.output_stream_lineages.emplace(output_name, left_output_stream_lineages.at(output_name));
        }
        else if (right_output_stream_lineages.contains(output_name))
        {
            lineage_info_context.output_stream_lineages.emplace(output_name, right_output_stream_lineages.at(output_name));
        }
        else
        {
            auto outputstream_info = std::make_shared<OutputStreamInfo>();
            outputstream_info->output_name = output_name;
            String new_expression_name = output_name;
            if (step->getAssignment().first == output_name)
            {
                RequiredSourceColumnsVisitor::Data col_context;
                RequiredSourceColumnsVisitor(col_context).visit(step->getAssignment().second->clone());
                NameSet required_symbols = col_context.requiredColumns();
                for (const auto & symbol : required_symbols)
                {
                    if (left_output_stream_lineages.contains(symbol))
                        outputstream_info->add(left_output_stream_lineages.at(symbol));
                    else if (right_output_stream_lineages.contains(symbol))
                        outputstream_info->add(right_output_stream_lineages.at(symbol));
                    else
                        new_expression_name = step->getAssignment().second->getColumnName();
                }
            }
            if (outputstream_info->isEmpty())
            {
                new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
                outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
            }
            lineage_info_context.output_stream_lineages.emplace(output_name, std::move(outputstream_info));
        }
    }

    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
}

void LineageInfoVisitor::visitAggregatingStepExtNode(AggregatingStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    NameSet keys{step->getKeys().begin(), step->getKeys().end()};
    auto input_stream_lineages = lineage_info_context.output_stream_lineages;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        String new_expression_name = output_name;
        if (keys.contains(output_name) && input_stream_lineages.contains(output_name))
        {
            outputstream_info->add(input_stream_lineages.at(output_name));
        }
        else
        {
            for (const auto & agg : step->getAggregates())
            {
                if (agg.column_name == output_name)
                {
                    for (const auto & argument_name : agg.argument_names)
                    {
                        if (input_stream_lineages.contains(argument_name))
                            outputstream_info->add(input_stream_lineages.at(argument_name));
                    }

                    if (outputstream_info->isEmpty())
                        new_expression_name = agg.function->getDescription();
                }
            }
        }

        if (outputstream_info->isEmpty())
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}

void LineageInfoVisitor::visitJoinStepExtNode(JoinStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    LineageInfoContext left_lineage_info_context;
    VisitorUtil::accept(node.getChildren()[0], *this, left_lineage_info_context);

    LineageInfoContext right_lineage_info_context;
    VisitorUtil::accept(node.getChildren()[1], *this, right_lineage_info_context);

    lineage_info_context.tables.insert(
        lineage_info_context.tables.end(), left_lineage_info_context.tables.begin(), left_lineage_info_context.tables.end());
    lineage_info_context.tables.insert(
        lineage_info_context.tables.end(), right_lineage_info_context.tables.begin(), right_lineage_info_context.tables.end());

    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    auto & left_output_stream_lineages = left_lineage_info_context.output_stream_lineages;
    auto & right_output_stream_lineages = right_lineage_info_context.output_stream_lineages;
    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        if (left_output_stream_lineages.contains(output_name))
        {
            lineage_info_context.output_stream_lineages.emplace(output_name, left_output_stream_lineages.at(output_name));
        }
        else if (right_output_stream_lineages.contains(output_name))
        {
            lineage_info_context.output_stream_lineages.emplace(output_name, right_output_stream_lineages.at(output_name));
        }
        else
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            auto outputstream_info = std::make_shared<OutputStreamInfo>();
            outputstream_info->output_name = output_name;
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
            lineage_info_context.output_stream_lineages.emplace(output_name, std::move(outputstream_info));
        }
    }

    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
}

void LineageInfoVisitor::visitFilterStepExtNode(FilterStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
}

void LineageInfoVisitor::visitProjectionStepExtNode(ProjectionStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();
    VisitorUtil::accept(node.getChildren()[0], *this, lineage_info_context);
    const auto & assignments = step->getAssignments();
    if (assignments.empty())
        return;

    auto input_stream_lineages = lineage_info_context.output_stream_lineages;
    std::unordered_map<String, OutputStreamInfoPtr> new_output_stream_lineages;
    SourceInfo new_expression_or_value_source_info;
    new_expression_or_value_source_info.source_tables = lineage_info_context.tables;

    for (const auto & output_column_and_type : step->getOutputStream().header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        String new_expression_name = output_name;
        if (assignments.contains(output_name))
        {
            RequiredSourceColumnsVisitor::Data col_context;
            RequiredSourceColumnsVisitor(col_context).visit(assignments.at(output_name)->clone());
            NameSet required_symbols = col_context.requiredColumns();
            for (const auto & symbol : required_symbols)
            {
                if (input_stream_lineages.contains(symbol))
                    outputstream_info->add(input_stream_lineages.at(symbol));
            }
            if (outputstream_info->isEmpty())
                new_expression_name = assignments.at(output_name)->getColumnName();
        }

        if (outputstream_info->isEmpty())
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), new_expression_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(new_expression_name));
        }
        new_output_stream_lineages.emplace(output_name, outputstream_info);
    }
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
    lineage_info_context.output_stream_lineages = std::move(new_output_stream_lineages);
}


void LineageInfoVisitor::visitTableScanStepExtNode(TableScanStepExtNode & node, LineageInfoContext & lineage_info_context)
{
    const auto * step = node.getStep().get();

    String database_name;
    String table_name;
    const auto & table_expression = getTableExpression(*step->getQueryInfo().query->as<ASTSelectQuery>(), 0);
    if (table_expression && table_expression->table_function)
    {
        auto storage = context->getQueryContext()->executeTableFunction(table_expression->table_function);
        if (const auto * distributed = dynamic_cast<StorageDistributed *>(storage.get()))
        {
            database_name = distributed->getRemoteDatabaseName();
            table_name = distributed->getRemoteTableName();
        }
    }

    if (database_name.empty() || table_name.empty())
    {
        database_name = step->getStorageID().database_name;
        table_name = step->getStorageID().table_name;
    }

    // get source column name
    String full_table_name = database_name + "." + table_name;
    SourceInfo column_source_info;
    SourceInfo new_expression_or_value_source_info;
    if (!table_sources.contains(full_table_name))
        table_sources.emplace(full_table_name, column_source_info);

    column_source_info = table_sources.at(full_table_name);
    column_source_info.source_tables.emplace_back(database_name, table_name);
    new_expression_or_value_source_info.source_tables.emplace_back(database_name, table_name);
    lineage_info_context.tables.emplace_back(database_name, table_name);

    const auto & columns_and_alias = step->getColumnToAliasMap();
    for (const auto & column_and_alias : columns_and_alias)
    {
        if (column_source_info.contains(column_and_alias.first))
            continue;
        column_source_info.add(column_id_allocator->nextId(), column_and_alias.first);
    }

    // analyze the mapping relationship between column name and outputStream
    const auto & alias_to_columns = step->getAliasToColumnMap();
    const DataStream & table_output_stream = step->getTableOutputStream();
    for (const auto & output_column_and_type : table_output_stream.header.getNamesAndTypes())
    {
        auto output_name = output_column_and_type.name;
        auto outputstream_info = std::make_shared<OutputStreamInfo>();
        outputstream_info->output_name = output_name;
        if (alias_to_columns.contains(output_name) && column_source_info.contains(alias_to_columns.at(output_name)))
            outputstream_info->column_ids.emplace(column_source_info.getIdByName(alias_to_columns.at(output_name)));
        else if (!step->hasInlineExpressions() && step->getInlineExpressions().contains(output_name))
        {
            const auto & inline_expression = step->getInlineExpressions().at(output_name);
            RequiredSourceColumnsVisitor::Data col_context;
            RequiredSourceColumnsVisitor(col_context).visit(inline_expression->clone());
            NameSet required_cols = col_context.requiredColumns();
            for (const auto & col : required_cols)
            {
                if (column_source_info.contains(col))
                    outputstream_info->column_ids.emplace(column_source_info.getIdByName(col));
            }
        }
        else
        {
            new_expression_or_value_source_info.add(column_id_allocator->nextId(), output_name);
            outputstream_info->column_ids.emplace(new_expression_or_value_source_info.getIdByName(output_name));
        }
        lineage_info_context.output_stream_lineages.emplace(output_name, std::move(outputstream_info));
    }
    table_sources[full_table_name] = std::move(column_source_info);
    if (!new_expression_or_value_source_info.isEmpty())
        expression_or_value_sources.emplace_back(std::move(new_expression_or_value_source_info));
}

}
