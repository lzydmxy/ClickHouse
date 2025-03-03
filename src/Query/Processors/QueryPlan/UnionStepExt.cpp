#include <Query/Processors/QueryPlan/UnionStepExt.h>

#include <Interpreters/ExpressionActions.h>

#include <Query/Common/Utils.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Parsers/ASTExpressionList.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnConst.h>

#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>

namespace DB
{

ColumnPtr getCommonColumnForUnion(const std::vector<const ColumnWithTypeAndName *> & columns)
{
    ColumnWithTypeAndName result = *columns[0];
    size_t num_const = 0;
    DataTypes types(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        types[i] = columns[i]->type;
        if (isColumnConst(*columns[i]->column))
            ++num_const;
    }

    static auto same_constants = [](const IColumn & a, const IColumn & b) {
        return assert_cast<const ColumnConst &>(a).getField() == assert_cast<const ColumnConst &>(b).getField();
    };

    /// Create supertype column saving constness if possible.
    bool save_constness = false;
    if (columns.size() == num_const)
    {
        save_constness = true;
        for (size_t i = 1; i < columns.size(); ++i)
        {
            const ColumnWithTypeAndName & first = *columns[0];
            const ColumnWithTypeAndName & other = *columns[i];

            if (!same_constants(*first.column, *other.column))
            {
                save_constness = false;
                break;
            }
        }
    }

    ColumnPtr column = result.type->createColumn();
    if (save_constness)
        column = result.type->createColumnConst(0, assert_cast<const ColumnConst &>(*columns[0]->column).getField());

    return column;
}

UnionStepExt::UnionStepExt(
    DataStreams input_streams_, DataStream output_stream_, OutputToInputs output_to_inputs_, size_t max_threads_, bool local_)
    : UnionStep(input_streams_, max_threads_), local(local_)
{
    if (output_stream_.header.getNamesAndTypes().empty())
        output_stream = input_streams.front();
    else
    {
        output_stream = output_stream_;
    }

        size_t num_selects = input_streams.size();
    std::vector<const ColumnWithTypeAndName *> columns(num_selects);
    for (size_t column_num = 0; column_num < output_stream->header.columns(); ++column_num)
    {
        ColumnWithTypeAndName & result_elem = output_stream->header.getByPosition(column_num);
        for (size_t i = 0; i < num_selects; ++i)
        {
            if (output_to_inputs.contains(result_elem.name))
            {
                for (auto & input_name : output_to_inputs[result_elem.name])
                {
                    if (input_streams[i].header.findByName(input_name))
                        columns[i] = input_streams[i].header.findByName(input_name);
                }
            }
            else
                columns[i] = &input_streams[i].header.getByPosition(column_num);
        }
        result_elem.column = getCommonColumnForUnion(columns);
    }

    if (output_to_inputs.empty())
    {
        for (size_t i = 0; i < output_stream->header.columns(); ++i)
        {
            String output_symbol = output_stream->header.getByPosition(i).name;
            std::vector<String> inputs;
            for (auto & input_stream : input_streams)
            {
                String input_symbol = input_stream.header.getByPosition(i).name;
                inputs.emplace_back(input_symbol);
            }
            output_to_inputs[output_symbol] = inputs;
        }
    }

    for (const auto & value : output_to_inputs)
    {
        Utils::checkArgument(
            value.second.size() == input_streams.size(), "Every source needs to map its symbols to an output operation symbol");
    }

    // Make sure each source positionally corresponds to their Symbol values in the Multimap
    for (size_t i = 0; i < input_streams.size(); i++)
    {
        for (auto value : output_to_inputs)
        {
            const Names & input_symbols = input_streams[i].header.getNames();
            String symbol = value.second[i];
            Utils::checkArgument(
                std::find(input_symbols.begin(), input_symbols.end(), symbol) != input_symbols.end(),
                "Every source needs to map its symbols to an output operation symbol");
        }
    }

//    header = Block();
//    for (auto & item : output_stream->header)
//        header.insert(ColumnWithTypeAndName(item.type, item.name));
//
//    if (header.columns() > 1 && header.has("_dummy"))
//        header.erase("_dummy");
}

const OutputToInputs & UnionStepExt::getOutToInputs() const
{
    return output_to_inputs;
}

NameToNameMap UnionStepExt::getOutToInput(size_t source_idx) const
{
    NameToNameMap res;
    for (const auto & [out, inputs] : output_to_inputs)
        res.emplace(out, inputs.at(source_idx));
    return res;
}

QueryPipelineBuilderPtr UnionStepExt::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    if (pipelines.empty())
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    // TODO Impl updatePipeline for UnionStepExt
    // size_t index = 0;
    for (auto & cur_pipeline : pipelines)
    {
        ASTPtr expr_list = std::make_shared<ASTExpressionList>();
        NamesWithAliases output_names;

        /// Headers for union must be equal.
        /// But, just in case, convert it to the same header if not.
        if (!isCompatibleHeader(cur_pipeline->getHeader(), getOutputStream().header))
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputStream().header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            cur_pipeline->addSimpleTransform(
                [&](const Block & cur_header) { return std::make_shared<ExpressionTransform>(cur_header, converting_actions); });
        }
        // index++;
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), getMaxThreads());
    return pipeline;
}

std::shared_ptr<IQueryPlanStep> UnionStepExt::copy(ContextPtr) const
{
    return std::make_shared<UnionStepExt>(input_streams, output_stream.value(), output_to_inputs, getMaxThreads(), local);
}

}
