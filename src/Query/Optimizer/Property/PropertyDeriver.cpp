#include <Query/Optimizer/Property/PropertyDeriver.h>

#include <Core/Names.h>
#include <Interpreters/StorageID.h>
#include <Query/Optimizer/ExpressionRewriter.h>
#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Utils.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/FilterStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>
#include <Query/Processors/QueryPlan/UnionStepExt.h>
#include <Poco/StringTokenizer.h>
#include <Parsers/ASTIdentifier.h>

#include <algorithm>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int OPTIMIZER_NONSUPPORT;
}

Property PropertyDeriver::deriveProperty(QueryPlanStepPtr step, ContextMutablePtr & context, const Property & require, int worker_size)
{
    PropertySet property_set;
    return deriveProperty(step, property_set, require, context, worker_size);
}

Property PropertyDeriver::deriveProperty(PlanNodePtr node, ContextMutablePtr & context, CTEInfo & cte_info, bool ignore_null, int worker_size)
{
    PlanDeriverVisitor visitor{cte_info, ignore_null, worker_size};
    return VisitorUtil::accept(node, visitor, context);
}

Property
PropertyDeriver::deriveProperty(QueryPlanStepPtr step, Property & input_property, const Property & require, ContextMutablePtr & context, int worker_size)
{
    PropertySet input_properties = std::vector<Property>();
    input_properties.emplace_back(input_property);
    auto result = deriveProperty(step, input_properties, require, context, worker_size);
    if (getQueryPlanStepType(step) != QueryPlanStepType::ExchangeStepExt)
    {
        if (result.getNodePartitioning().getComponent() == Partitioning::Component::ANY)
        {
            result.getNodePartitioningRef().setComponent(input_property.getNodePartitioning().getComponent());
        }
    }

    return result;
}

Property PropertyDeriver::deriveProperty(
    QueryPlanStepPtr step, PropertySet & input_properties, const Property & require, ContextMutablePtr & context, int worker_size)
{
    DeriverContext deriver_context{input_properties, require, context, false, worker_size};
    DeriverVisitor visitor{};
    auto result = VisitorUtil::accept(step, visitor, deriver_context);
    if (getQueryPlanStepType(step) != QueryPlanStepType::ExchangeStepExt)
    {
        if (result.getNodePartitioning().getComponent() == Partitioning::Component::ANY && !input_properties.empty())
        {
            result.getNodePartitioningRef().setComponent(input_properties[0].getNodePartitioning().getComponent());
        }
    }

    return result;
}

Property PropertyDeriver::deriveStorageProperty(const StoragePtr & storage, const Property & required, ContextMutablePtr & context, int worker_size)
{
    if (storage->getStorageID().getDatabaseName() == "system" || storage->getStorageID().getDatabaseName() == "_table_function")
    {
        auto node = Partitioning(Partitioning::Handle::SINGLE);
        node.setComponent(Partitioning::Component::COORDINATOR);
        return Property{node, Partitioning(Partitioning::Handle::ARBITRARY)};
    }
    Sorting sorting;
    const auto & descs = storage->getInMemoryMetadataPtr()->sorting_key;

    for (size_t i = 0; i < descs.column_names.size(); i++)
    {
        if (Utils::canIgnoreNullsDirection(descs.data_types[i]))
            sorting.emplace_back(SortColumn(descs.column_names[i], SortOrder::ASC_ANY));
        else
            sorting.emplace_back(SortColumn(descs.column_names[i], SortOrder::ASC_NULLS_FIRST));
    }

    bool use_reverse_sorting = !required.getSorting().empty()
        && (required.getSorting()[0].getOrder() == SortOrder::DESC_ANY || required.getSorting()[0].getOrder() == SortOrder::DESC_NULLS_FIRST
            || required.getSorting()[0].getOrder() == SortOrder::DESC_NULLS_LAST);
    if (use_reverse_sorting)
        sorting = sorting.toReverseOrder();

    if (worker_size == 1 && !context->getOptimizerContext()->getSettingsRef().disable_single_server_optimization)
        return Property{Partitioning(Partitioning::Handle::SINGLE), Partitioning(Partitioning::Handle::ARBITRARY), sorting};
    return Property{Partitioning(Partitioning::Handle::UNKNOWN), Partitioning(Partitioning::Handle::UNKNOWN), sorting};
}

Property PropertyDeriver::deriveStoragePropertyWhatIfMode(
    const StoragePtr & storage, ContextMutablePtr & context, const Property & required_property, int worker_size)
{
    Property actual_storage_property = deriveStorageProperty(storage, required_property, context, worker_size);

    const auto & table_layout = required_property.getTableLayout();

    if (!table_layout.contains(storage->getStorageID().getQualifiedName()))
        return actual_storage_property;

    auto what_if_table_partitioning = table_layout.at(storage->getStorageID().getQualifiedName());

    if (what_if_table_partitioning.isStarPartitioned()) // use required property to calculate lower bound
        return required_property;

    Names cluster_by{what_if_table_partitioning.getPartitionKey().column};
    // the bucket number is only used for matching, can be set to anything
    UInt64 buckets =  actual_storage_property.getNodePartitioning().getBuckets();

    Partitioning new_partitioning{
        Partitioning::Handle::BUCKET_TABLE, cluster_by, true, buckets, nullptr, true, Partitioning::Component::ANY};
    actual_storage_property.setNodePartitioning(new_partitioning);

    return actual_storage_property;
}

Property DeriverVisitor::visitStep(const IQueryPlanStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitOffsetStep(const OffsetStep &, DeriverContext & context)
{
    return Property{
        context.getInput()[0].getNodePartitioning(), Partitioning(Partitioning::Handle::SINGLE), context.getInput()[0].getSorting()};
}

Property DeriverVisitor::visitTotalsHavingStepExt(const TotalsHavingStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitFinishSortingStepExt(const FinishSortingStepExt & step, DeriverContext & context)
{
    auto prop = context.getInput()[0];
    Sorting sorting;
    for (auto item : step.getResultDescription())
    {
        sorting.emplace_back(item);
    }

    prop.setSorting(sorting);
    return prop;
}

Property DeriverVisitor::visitPartitionTopNStepExt(const PartitionTopNStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitBufferStepExt(const BufferStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitFinalSampleStepExt(const FinalSampleStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitLocalExchangeStepExt(const LocalExchangeStepExt & step, DeriverContext & context)
{
    Property output = context.getInput()[0];
    output.setStreamPartitioning(step.getSchema());
    return output.clearSorting();
}

Property DeriverVisitor::visitIntermediateResultCacheStepExt(const IntermediateResultCacheStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitProjectionStepExt(const ProjectionStepExt & step, DeriverContext & context)
{
    const auto & assignments = step.getAssignments();

    if (!context.getInput()[0].getNodePartitioning().getColumns().empty()
        && context.getContext()->getOptimizerContext()->getSettingsRef().enable_injective_in_property)
    {
        for (const auto & item : assignments)
        {
            if (item.second->as<ASTFunction>())
            {
                try
                {
                    auto partition_col = context.getInput()[0].getNodePartitioning().getColumns();
                    NameSet partition_col_set{partition_col.begin(), partition_col.end()};
                    if (FunctionIsInjective::isInjective(
                            item.second, context.getContext(), step.getInputStreams()[0].header.getNamesAndTypes(), partition_col_set))
                    {
                        auto prop = context.getInput()[0];
                        prop.getNodePartitioningRef().setColumns({item.first});
                        return prop;
                    }
                }
                catch (...)
                {
                }
            }
        }
    }

    if (context.isIgnoreNull() && !context.getInput()[0].getNodePartitioning().getColumns().empty()
        && context.getContext()->getOptimizerContext()->getSettingsRef().enable_case_when_prop)
    {
        for (const auto & item : assignments)
        {
            auto extract_arg = [&](ASTPtr arg, String & col, bool & same_col) {
                if (const auto * id = arg->as<ASTIdentifier>())
                {
                    if (col.empty())
                    {
                        col = id->name();
                        same_col = true;
                    }
                    else
                    {
                        same_col &= (col == id->name());
                    }
                }
                else if (const auto * field = arg->as<ASTLiteral>())
                {
                    same_col &= field->value.isNull();
                }
                else
                {
                    same_col = false;
                }
            };
            if (const auto func = item.second->as<ASTFunction>())
            {
                String col;
                bool same_col = false;
                if (func->name == "if")
                {
                    if (func->arguments->children.size() == 3)
                    {
                        extract_arg(func->arguments->children[1], col, same_col);
                        extract_arg(func->arguments->children[2], col, same_col);
                    }
                }
                else
                {
                    if (func->name == "multiIf")
                    {
                        for (size_t i = 1; i < func->arguments->children.size(); i += 2)
                        {
                            extract_arg(func->arguments->children[i], col, same_col);
                        }
                        extract_arg(func->arguments->children.back(), col, same_col);
                    }
                }
                const auto & node_partition = context.getInput()[0].getNodePartitioning();
                if (same_col && node_partition.getColumns().size() == 1 && node_partition.getColumns()[0] == col)
                {
                    auto prop = context.getInput()[0];
                    prop.getNodePartitioningRef().setColumns({item.first});
                    return prop;
                }
            }
        }
    }

    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    std::unordered_map<String, String> revert_identifies;

    // TODO(gouguilin): check isBitEngineEncodeDecodeFunction in functions
    // TODO(gouguilin):     when bitengine is ready
    bool has_bitmap_func = false;

    for (auto & item : identities)
    {
        revert_identifies[item.second] = item.first;
    }
    Property translated;
    if (!context.getInput().empty())
    {
        translated = context.getInput()[0].translate(revert_identifies);
    }

    // if partition columns are pruned, the output data has no property.
    if (translated.getNodePartitioning().getColumns().size() != context.getInput()[0].getNodePartitioning().getColumns().size())
    {
        return Property{};
    }

    if (translated.getStreamPartitioning().getColumns().size() != context.getInput()[0].getStreamPartitioning().getColumns().size())
    {
        // TODO stream partition
    }
    if (has_bitmap_func)
    {
        translated.getNodePartitioningRef().setComponent(Partitioning::Component::WORKER);
    }
    return translated;
}

Property DeriverVisitor::visitFilterStepExt(const FilterStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitJoinStepExt(const JoinStepExt & step, DeriverContext & context)
{
    std::unordered_map<String, String> identities;
    for (const auto & item : step.getOutputStream().header)
    {
        identities[item.name] = item.name;
    }

    Property translated;

    if (step.getKind() == JoinKind::Inner || step.getKind() == JoinKind::Cross)
    {
        Property left_translated = context.getInput()[0].translate(identities);
        Property right_translated = context.getInput()[1].translate(identities);

        translated = left_translated;

        // if partition columns are pruned, the output data has no property.
        if (translated.getNodePartitioning().getColumns().size() != context.getInput()[0].getNodePartitioning().getColumns().size())
        {
            translated.setNodePartitioning({});
        }
        if (translated.getStreamPartitioning().getColumns().size() != context.getInput()[0].getStreamPartitioning().getColumns().size())
        {
            translated.setStreamPartitioning({});
        }
    }

    if (step.getKind() == JoinKind::Left || step.getKind() == JoinKind::Right)
    {
        Property left_translated = context.getInput()[0].translate(identities);
        translated = left_translated;

        // if partition columns are pruned, the output data has no property.
        if (translated.getNodePartitioning().getColumns().size() != context.getInput()[0].getNodePartitioning().getColumns().size())
        {
            translated.setNodePartitioning({});
        }
        if (translated.getStreamPartitioning().getColumns().size() != context.getInput()[0].getStreamPartitioning().getColumns().size())
        {
            // TODO stream partition
        }
    }

    if (step.getKind() == JoinKind::Full)
    {
        return Property{};
    }

    translated = translated.clearSorting();
    return translated;
}

Property DeriverVisitor::visitArrayJoinStep(const ArrayJoinStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitAggregatingStepExt(const AggregatingStepExt &, DeriverContext & context)
{
    auto prop = context.getInput()[0].clearSorting();
    return prop;
}

Property DeriverVisitor::visitMarkDistinctStepExt(const MarkDistinctStepExt &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitMergingAggregatedStepExt(const MergingAggregatedStepExt &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitUnionStepExt(const UnionStepExt & step, DeriverContext & context)
{
    Property first_child_property = context.getInput()[0];
    if (first_child_property.getNodePartitioning().getHandle() == Partitioning::Handle::SINGLE)
    {
        bool all_single = true;
        for (const auto & input : context.getInput())
        {
            all_single &= input.getNodePartitioning().getHandle() == Partitioning::Handle::SINGLE;
        }

        if (all_single)
        {
            if (step.isLocal())
            {
                return Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}};
            }
            else
            {
                return Property{Partitioning{Partitioning::Handle::SINGLE}};
            }
        }
    }

    std::vector<Property> transformed_children_prop;
    const auto & output_to_inputs = step.getOutToInputs();
    size_t index = 0;
    for (const auto & child_prop : context.getInput())
    {
        NameToNameMap mapping;
        for (const auto & output_to_input : output_to_inputs)
        {
            mapping[output_to_input.second[index]] = output_to_input.first;
        }
        index++;
        transformed_children_prop.emplace_back(child_prop.translate(mapping));
    }

    if (first_child_property.getNodePartitioning().getHandle() == Partitioning::Handle::FIXED_HASH
        || first_child_property.getNodePartitioning().getHandle() == Partitioning::Handle::BUCKET_TABLE)
    {
        const Names & keys = first_child_property.getNodePartitioning().getColumns();
        Names output_keys;
        bool match = true;
        bool satisfy_worker = true;
        bool bucket_size_match = true;
        for (auto & transformed : transformed_children_prop)
        {
            if (transformed.getNodePartitioning().getBuckets() != first_child_property.getNodePartitioning().getBuckets())
            {
                bucket_size_match = false;
            }
            transformed.getNodePartitioningRef().setBuckets(0);
            if (!(transformed.getNodePartitioning() == transformed_children_prop[0].getNodePartitioning()))
            {
                match = false;
            }
            satisfy_worker &= transformed.getNodePartitioning().isSatisfyWorker();
        }

        if (!satisfy_worker)
        {
            match &= bucket_size_match;
        }

        if (match && keys.size() == transformed_children_prop[0].getNodePartitioning().getColumns().size())
        {
            output_keys = transformed_children_prop[0].getNodePartitioning().getColumns();
        }
        if (step.isLocal())
        {
            return Property{
                Partitioning{
                    first_child_property.getNodePartitioning().getHandle(),
                    output_keys,
                    true,
                    first_child_property.getNodePartitioning().getBuckets(),
                    first_child_property.getNodePartitioning().getBucketExpr(),
                    first_child_property.getNodePartitioning().isEnforceRoundRobin(),
                    first_child_property.getNodePartitioning().getComponent(),
                    false,
                    satisfy_worker},
                Partitioning{Partitioning::Handle::SINGLE}};
        }
        else
        {
            return Property{Partitioning{
                first_child_property.getNodePartitioning().getHandle(),
                output_keys,
                true,
                first_child_property.getNodePartitioning().getBuckets(),
                first_child_property.getNodePartitioning().getBucketExpr(),
                first_child_property.getNodePartitioning().isEnforceRoundRobin(),
                first_child_property.getNodePartitioning().getComponent(),
                false,
                satisfy_worker}};
        }
    }
    return Property{};
}

Property DeriverVisitor::visitExceptStepExt(const ExceptStepExt &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitIntersectStepExt(const IntersectStepExt &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitIntersectOrExceptStep(const IntersectOrExceptStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitExchangeStepExt(const ExchangeStepExt & step, DeriverContext & context)
{
    const RExchangeMode::Enum & mode = step.getExchangeMode();
    if (mode == RExchangeMode::GATHER)
    {
        Property output = context.getInput()[0];
        output.setNodePartitioning(Partitioning{Partitioning::Handle::SINGLE});
        return output.clearSorting();
    }

    if (mode == RExchangeMode::REPARTITION)
    {
        Property output = context.getInput()[0];
        output.setNodePartitioning(step.getSchema());
        output.setStreamPartitioning(step.getSchema());
        return output.clearSorting();
    }

    if (mode == RExchangeMode::BROADCAST)
    {
        Property output = context.getInput()[0];
        output.setNodePartitioning(Partitioning{Partitioning::Handle::FIXED_BROADCAST});
        return output.clearSorting();
    }

    if (mode == RExchangeMode::LOCAL_NO_NEED_REPARTITION)
    {
        Property output = context.getInput()[0];
        return output.clearSorting();
    }

    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitRemoteExchangeSourceStepExt(const RemoteExchangeSourceStepExt &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitTableScanStepExt(const TableScanStepExt & step, DeriverContext & context)
{
    Property prop;

    if (!context.getRequire().getTableLayout().empty())
    {
        prop = PropertyDeriver::deriveStoragePropertyWhatIfMode(step.getStorage(), context.getContext(), context.getRequire(), context.workerSize());
    }
    else
    {
        prop = PropertyDeriver::deriveStorageProperty(step.getStorage(), context.getRequire(), context.getContext(), context.workerSize());
    }

    auto result = prop.translate(step.getColumnToAliasMap(), true);
    if (prop.getNodePartitioning().getColumns().size() != result.getNodePartitioning().getColumns().size())
    {
        result.setNodePartitioning({});
    }
    return result;
}

Property DeriverVisitor::visitReadNothingStep(const ReadNothingStep &, DeriverContext &)
{
    return Property{Partitioning(Partitioning::Handle::SINGLE), Partitioning(Partitioning::Handle::ARBITRARY)};
}

Property DeriverVisitor::visitReadStorageRowCountStepExt(const ReadStorageRowCountStepExt &, DeriverContext &)
{
    auto prop = Partitioning(Partitioning::Handle::FIXED_ARBITRARY);
    return Property{prop, Partitioning(Partitioning::Handle::ARBITRARY)};
}

Property DeriverVisitor::visitValuesStepExt(const ValuesStepExt &, DeriverContext &)
{
    return Property{Partitioning(Partitioning::Handle::SINGLE), Partitioning(Partitioning::Handle::ARBITRARY)};
}

Property DeriverVisitor::visitLimitStepExt(const LimitStepExt &, DeriverContext & context)
{
    return Property{
        context.getInput()[0].getNodePartitioning(), Partitioning(Partitioning::Handle::SINGLE), context.getInput()[0].getSorting()};
}

Property DeriverVisitor::visitLimitByStep(const LimitByStep &, DeriverContext & context)
{
    return context.getInput()[0].withStreamPartitioning(Partitioning{Partitioning::Handle::SINGLE});
}

Property DeriverVisitor::visitSortingStepExt(const SortingStepExt & step, DeriverContext & context)
{
    auto prop = context.getInput()[0];
    Sorting sorting;
    for (auto item : step.getSortDescription())
    {
        sorting.emplace_back(item);
    }
    prop.setSorting(sorting);
    return prop;
}


Property DeriverVisitor::visitMergeSortingStepExt(const MergeSortingStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitPartialSortingStepExt(const PartialSortingStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitMergingSortedStepExt(const MergingSortedStepExt &, DeriverContext & context)
{
    return Property{context.getInput()[0].getNodePartitioning(), Partitioning(Partitioning::Handle::SINGLE)};
}

Property DeriverVisitor::visitDistinctStepExt(const DistinctStepExt & step, DeriverContext & context)
{
    auto result = context.getInput()[0];
    result.clearSorting();
    if (!step.preDistinct())
    {
        result.setStreamPartitioning(Partitioning{Partitioning::Handle::SINGLE});
    }
    return result;
}

Property DeriverVisitor::visitExtremesStep(const ExtremesStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitWindowStep(const WindowStep &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting().withStreamPartitioning({});
}

Property DeriverVisitor::visitApplyStepExt(const ApplyStepExt &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitEnforceSingleRowStepExt(const EnforceSingleRowStepExt &, DeriverContext & context)
{
    return context.getInput()[0].withStreamPartitioning(Partitioning{Partitioning::Handle::SINGLE});
}

Property DeriverVisitor::visitAssignUniqueIdStepExt(const AssignUniqueIdStepExt &, DeriverContext & context)
{
    return context.getInput()[0].clearSorting();
}

Property DeriverVisitor::visitCTERefStepExt(const CTERefStepExt & cte_step, DeriverContext & context)
{
    if (context.getInput().size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input porporties should be set for cte property derive");
    auto prop = context.getInput()[0];
    return prop.translate(cte_step.getReverseOutputColumns());
}

Property DeriverVisitor::visitExplainAnalyzeStepExt(const ExplainAnalyzeStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitTopNFilteringStepExt(const TopNFilteringStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitFillingStep(const FillingStep &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitMultiJoinStepExt(const MultiJoinStepExt &, DeriverContext & context)
{
    return context.getInput()[0];
}

Property DeriverVisitor::visitExpandStepExt(const ExpandStepExt &, DeriverContext & context)
{
    auto prop = context.getInput()[0].clearSorting();
    prop.getNodePartitioningRef().resetIfPartitionHandle();
    prop.getStreamPartitioningRef().resetIfPartitionHandle();
    return prop;
}

Property PlanDeriverVisitor::visitPlanNode(PlanNodeBase & node, ContextMutablePtr & context)
{
    PropertySet input_properties;
    Property require;

    for (auto & child : node.getChildren())
    {
        input_properties.emplace_back(VisitorUtil::accept(child, *this, context));
    }

    DeriverContext deriver_context{input_properties, require, context, ignore_null, worker_size};
    DeriverVisitor visitor{};
    auto result = VisitorUtil::accept(node.getStep(), visitor, deriver_context);
    if (getQueryPlanStepType(node.getStep()) != QueryPlanStepType::ExchangeStepExt)
    {
        if (result.getNodePartitioning().getComponent() == Partitioning::Component::ANY && !input_properties.empty())
        {
            result.getNodePartitioningRef().setComponent(input_properties[0].getNodePartitioning().getComponent());
        }
    }

    return result;
}

Property PlanDeriverVisitor::visitCTERefStepExtNode(CTERefStepExtNode & node, ContextMutablePtr & c)
{
    const auto * cte_step = dynamic_cast<const CTERefStepExt *>(node.getStep().get());
    auto cte_id = cte_step->getId();
    return cte_helper.accept(cte_id, *this, c);
}

}
