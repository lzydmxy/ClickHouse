#include <Query/Processors/QueryPlan/ReadStorageRowCountStepExt.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Columns/ColumnAggregateFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}

ReadStorageRowCountStepExt::ReadStorageRowCountStepExt(Block output_header, ASTPtr query_, AggregateDescription agg_desc_, bool is_final_agg_, StorageID storage_id_, ContextPtr context_)
    : ISourceStep(DataStream{.header = output_header})
    , query(query_)
    , agg_desc(agg_desc_)
    , is_final_agg(is_final_agg_)
    , storage_id(storage_id_)
    , context(context_)
{
}

void ReadStorageRowCountStepExt::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (storage_id)
    {
        std::optional<UInt64> rows_cnt{};
        // get storage
        StoragePtr storage = DatabaseCatalog::instance().getTable(storage_id, context);

        // get row number
        auto & select_query = query->as<ASTSelectQuery &>();
        if (!select_query.where() && !select_query.prewhere())
        {
            rows_cnt = storage->totalRows(context->getSettingsRef());
            rows_cnt = storage->totalRows(context->getSettingsRef());
        }
        else // It's possible to optimize count() given only partition predicates
        {
            auto interpreter = std::make_shared<InterpreterSelectQuery>(query->clone(), context, SelectQueryOptions());
            SelectQueryInfo temp_query_info;
            temp_query_info.query = interpreter->getQuery();
            // todo: getSyntaxAnalyzerResult need to be implemented by interpreter
            //temp_query_info.syntax_analyzer_result = interpreter->getSyntaxAnalyzerResult();
            temp_query_info.prepared_sets = interpreter->getQueryAnalyzer()->getPreparedSets();
            // todo: need to implement get ActionsDAGPtr from SelectQueryInfo
            //rows_cnt = storage->totalRowsByPartitionPredicate(temp_query_info, context);
        }

        if (!rows_cnt)
        {
            try
            {
                auto select_list = std::make_shared<ASTExpressionList>();
                auto count_func = makeASTFunction("count");
                select_query.refSelect() = std::make_shared<ASTExpressionList>();
                select_query.refSelect()->children.emplace_back(count_func);
                DataTypes types;
                // todo: need to implement InterpreterSelectQueryUseOptimizer
                // auto pre_execute = [&types](InterpreterSelectQueryUseOptimizer & interpreter) { types = interpreter.getSampleBlock().getDataTypes(); };

                // todo: need to implement createContextForSubQuery
                //auto query_context = createContextForSubQuery(context);
                SettingsChanges changes;
                changes.emplace_back("max_result_rows", 1);
                changes.emplace_back("result_overflow_mode", "throw");
                changes.emplace_back("extremes", false);
                changes.emplace_back("optimize_trivial_count_query", false);
                //query_context->applySettingsChanges(changes);
                //auto block = executeSubPipelineWithOneRow(query, query_context, pre_execute);
                Block  block;

                if (block.rows() != 1 || block.columns() != 1)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Trivial count query returned error data");

                block = materializeBlock(block);
                auto columns = block.getColumns();
                num_rows = columns[0]->getUInt(0);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Trivial count query execution failed. Please set optimize_trivial_count_query = 0 , and try again.");
            }
        }
        else
            num_rows = rows_cnt.value();
    }

    const auto & func = agg_desc.function;
    const AggregateFunctionCount & agg_count = static_cast<const AggregateFunctionCount &>(*func);
    Block output_header;
    if (is_final_agg)
    {
        auto count_column = ColumnVector<UInt64>::create();
        count_column->insertValue(num_rows);
        // todo: need to implement getReturnType in AggregateFunction
        // output_header.insert({count_column->getPtr(), agg_count.getReturnType(), agg_desc.column_name});
    }
    else
    {
        std::vector<char> state(agg_count.sizeOfData());
        AggregateDataPtr place = state.data();

        agg_count.create(place);
        SCOPE_EXIT_MEMORY_SAFE(agg_count.destroy(place));

        agg_count.set(place, num_rows);
        auto column = ColumnAggregateFunction::create(func);
        column->insertFrom(place);

        // AggregateFunction's argument type must keep same. 
        // todo: need to implement getArgumentTypes in AggregateFunction
        output_header.insert({std::move(column), std::make_shared<DataTypeAggregateFunction>(func, func->getArgumentTypes(), agg_desc.parameters), agg_desc.column_name});
    }

    // todo: need to implement OneBlockInputStream and SourceFromInputStream
    //auto istream = std::make_shared<OneBlockInputStream>(output_header);
    //auto pipe = Pipe(std::make_shared<SourceFromInputStream>(istream));

    //for (const auto & processor : pipe.getProcessors())
    //    processors.emplace_back(processor);

    //pipeline.init(std::move(pipe));

    //if (context)
    //    pipeline.addInterpreterContext(context);
}

}
