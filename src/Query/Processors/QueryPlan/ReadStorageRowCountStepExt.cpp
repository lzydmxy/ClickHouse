#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Query/Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Query/Interpreters/executeSubQuery.h>
#include <Query/Processors/QueryPlan/ReadStorageRowCountStepExt.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <arrow/type_fwd.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}

namespace
{
Block getOutPutHeader(const Block & output_header, bool is_final_agg, AggregateDescription agg_desc)
{
    auto func = agg_desc.function;
    if (is_final_agg)
        return output_header;

    /// TODO wujianchao alias in output_header
    Block header;
    auto column = ColumnAggregateFunction::create(func);
    header.insert({std::move(column), std::make_shared<DataTypeAggregateFunction>(func, func->getArgumentTypes(), agg_desc.parameters), agg_desc.column_name});
    return header;
}

[[maybe_unused]] ActionsDAGPtr getActionsDagByPredicate(const ASTPtr & predicate, const ContextPtr & context, const StoragePtr & storage)
{
    auto actions = std::make_shared<ActionsDAG>(storage->getInMemoryMetadataPtr()->getSampleBlock().getColumnsWithTypeAndName());
    PreparedSetsPtr prepared_sets;
    const NamesAndTypesList source_columns;
    const NamesAndTypesList aggregation_keys;
    const ColumnNumbersList grouping_set_keys;

    ActionsVisitor::Data visitor_data(
        context,
        SizeLimits{},
        1,
        source_columns,
        std::move(actions),
        prepared_sets,
        true,
        true,
        false,
        { aggregation_keys, grouping_set_keys, GroupByKind::NONE });
    ActionsVisitor(visitor_data).visit(predicate);
    return visitor_data.getActions();
}

}

ReadStorageRowCountStepExt::ReadStorageRowCountStepExt(Block output_header, ASTPtr query_, AggregateDescription agg_desc_, bool is_final_agg_, StorageID storage_id_, ContextPtr context_)
    : ISourceStep(DataStream{.header = getOutPutHeader(output_header, is_final_agg_, agg_desc_)})
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
        }
        else // It's possible to optimize count() given only partition predicates
        {
            // convert where and prewhere to ActionsDag
            // ActionsDAG::NodeRawConstPtrs action_nodes;
            // ActionsDAGPtr prewhere_actions_dag;
            // ActionsDAGPtr where_actions_dag;
            // if (select_query.where())
            // {
            //     where_actions_dag = getActionsDagByPredicate(select_query.where(), context, storage);
            //     for (const auto & node :where_actions_dag->getNodes())
            //         action_nodes.push_back(&node); // TODO wujianchao only add where_column node
            // }
            // if (select_query.prewhere())
            // {
            //     prewhere_actions_dag = getActionsDagByPredicate(select_query.where(), context, storage);
            //     for (const auto & node :prewhere_actions_dag->getNodes())
            //         action_nodes.push_back(&node); // TODO wujianchao only add prewhere_column node
            // }
            // auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(action_nodes);

            auto interpreter = std::make_shared<InterpreterSelectQuery>(select_query.clone(), context, SelectQueryOptions());
            rows_cnt = interpreter->getTrivialCount(context->getSettingsRef().max_parallel_replicas);
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
                auto pre_execute = [&types](InterpreterSelectQueryUseOptimizer & interpreter) { types = interpreter.getSampleBlock().getDataTypes(); };
                auto query_context = createContextForSubQuery(context);
                SettingsChanges changes;
                changes.emplace_back("max_result_rows", 1);
                changes.emplace_back("result_overflow_mode", "throw");
                changes.emplace_back("extremes", false);
                changes.emplace_back("optimize_trivial_count_query", false);
                query_context->applySettingsChanges(changes);
                auto block = executeSubPipelineWithOneRow(query, query_context, pre_execute);

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

    Block output_header = output_stream->header.cloneWithoutColumns();
    chassert(output_header.columns() == 1);
    chassert(output_header.getByPosition(0).name == agg_desc.column_name);

    if (is_final_agg)
    {
        auto count_column = ColumnVector<UInt64>::create();
        count_column->insertValue(num_rows);
        output_header.getByPosition(0).column = count_column->getPtr();
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

        output_header.getByPosition(0).column = column->getPtr();
    }

    auto pipe = Pipe(std::make_shared<SourceFromSingleChunk>(output_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));

    if (context)
        pipeline.addContext(context);
}

std::shared_ptr<IQueryPlanStep> ReadStorageRowCountStepExt::copy(ContextPtr context) const
{
    auto step = std::make_shared<ReadStorageRowCountStepExt>(output_stream->header, query, agg_desc, is_final_agg, storage_id, context);
    step->setNumRows(num_rows);
    return step;
}

void ReadStorageRowCountStepExt::toProto(Protos::ReadStorageRowCountStepExt & proto, bool for_hash_equals) const
{
    ProtosSerDerHelper::serializeToProtoBase(*this, *proto.mutable_query_plan_base());
    serializeASTToProto(query, *proto.mutable_query());
    ProtosSerDerHelper::toProto(agg_desc, *proto.mutable_agg_desc());
    proto.set_num_rows(num_rows);
    proto.set_is_final_agg(is_final_agg);
    if (storage_id)
    {
        auto storage_id_without_uuid = storage_id;
        // we should clear uuid to avoid table does not exist exception in another node.
        storage_id_without_uuid.uuid = UUIDHelpers::Nil;
        ProtosSerDerHelper::toProto(storage_id_without_uuid, *proto.mutable_storage_id());
    }
}

std::shared_ptr<ReadStorageRowCountStepExt> ReadStorageRowCountStepExt::fromProto(const Protos::ReadStorageRowCountStepExt & proto, ContextPtr context)
{
    auto base_output_header = ProtosSerDerHelper::deserializeFromProtoBase(proto.query_plan_base());
    auto query = deserializeASTFromProto(proto.query());
    AggregateDescription agg_desc;
    ProtosSerDerHelper::fillFromProto(agg_desc, proto.agg_desc());
    auto num_rows = proto.num_rows();
    bool is_final = proto.is_final_agg();
    StorageID storage_id = StorageID::createEmpty();
    if (proto.has_storage_id())
    {
        auto storage_id_tmp = ProtosSerDerHelper::fromProto(proto.storage_id(), context);
        storage_id = *storage_id_tmp;
    }

    auto step = std::make_shared<ReadStorageRowCountStepExt>(base_output_header, query, agg_desc, is_final, storage_id, context);
    step->setNumRows(num_rows);
    return step;
}

}
