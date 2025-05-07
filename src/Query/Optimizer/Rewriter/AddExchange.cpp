#include <Query/Optimizer/Rewriter/AddExchange.h>

#include <Query/Optimizer/Property/PropertyDeriver.h>
#include <Query/Optimizer/Property/PropertyDeterminer.h>
#include <Query/Optimizer/Property/PropertyEnforcer.h>
#include <Query/Optimizer/Property/PropertyMatcher.h>
#include <Query/Optimizer/Utils.h>
#include <Query/Processors/QueryPlan/AggregatingStepExt.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Query/Processors/QueryPlan/ProjectionStepExt.h>

namespace DB
{
bool AddExchange::rewrite(QueryPlanExt & plan, ContextMutablePtr context) const
{
    ExchangeVisitor visitor{};
    Property required{Partitioning{Partitioning::Handle::SINGLE}};

    required.getNodePartitioningRef().setComponent(Partitioning::Component::COORDINATOR);
    ExchangeContext cxt{context, required};
    ExchangeResult result = VisitorUtil::accept(plan.getPlanNode(), visitor, cxt);

    PlanNodePtr node = result.getNodePtr();
    Property output = result.getOutputProperty();
    if (!PropertyMatcher::matchNodePartitioning(*context, required.getNodePartitioningRef(), output.getNodePartitioning()))
    {
        Utils::checkArgument(node->getChildren().size() == 1, "Output node has more than 1 child");

        PlanNodePtr child = node->getChildren()[0];
        PlanNodePtr enforced_node = PropertyEnforcer::enforceNodePartitioning(child, required, output, *cxt.getContext());
        PlanNodes enforced_children{enforced_node};
        node->replaceChildren(enforced_children);
    }

    // enforce a gather with keep_order if offloading_with_query_plan enabled
    if (context->getOptimizerContext()->getSettingsRef().offloading_with_query_plan)
        node = PropertyEnforcer::enforceOffloadingGatherNode(node, *context);
    plan.update(node);
    return true;
}

ExchangeResult ExchangeVisitor::visitPlanNode(PlanNodeBase & node, ExchangeContext & cxt)
{
    PlanNodePtr ptr = node.shared_from_this();
    PropertySet required_set = PropertyDeterminer::determineRequiredProperty(node.getStep(), cxt.getRequired(), *cxt.getContext())[0];
    PlanNodePtr child = ptr->getChildren()[0];
    Property preferred = required_set[0];
    ExchangeContext child_context{cxt.getContext(), preferred};
    ExchangeResult result = visitChild(child, child_context);
    return rebaseAndDeriveProperties(ptr, result, cxt.getRequired(), cxt.getContext());
}

ExchangeResult ExchangeVisitor::visitProjectionStepExtNode(ProjectionStepExtNode & node, ExchangeContext & cxt)
{
    const auto & step = *node.getStep();
    const auto & assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    Property translated = cxt.getRequired().translate(identities);
    ExchangeContext child_cxt{cxt.getContext(), translated};
    return visitPlanNode(node, child_cxt);
}

ExchangeResult ExchangeVisitor::visitFilterStepExtNode(FilterStepExtNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitJoinStepExtNode(JoinStepExtNode & node, ExchangeContext & cxt)
{
    PlanNodePtr ptr = node.shared_from_this();
    PropertySet required_set = PropertyDeterminer::determineRequiredProperty(node.getStep(), cxt.getRequired(), *cxt.getContext())[0];

    PlanNodePtr left = ptr->getChildren()[0];
    PlanNodePtr right = ptr->getChildren()[1];

    Property left_property = required_set[0];
    Property right_property = required_set[1];

    ExchangeContext left_child_property{cxt.getContext(), left_property};
    ExchangeContext right_child_property{cxt.getContext(), right_property};
    ExchangeResult left_result = visitChild(left, left_child_property);
    ExchangeResult right_result = visitChild(right, right_child_property);

    if (!PropertyMatcher::matchNodePartitioning(
            *cxt.getContext(), left_property.getNodePartitioningRef(), left_result.getOutputProperty().getNodePartitioning()))
    {
        PlanNodePtr enforced_node = PropertyEnforcer::enforceNodePartitioning(
            left_result.getNodePtr(), left_property, left_result.getOutputProperty(), *cxt.getContext());
        Property enforced_prop = PropertyDeriver::deriveProperty(
            enforced_node->getStep(), left_result.getOutputProperty(), cxt.getRequired(), cxt.getContext());
        ExchangeResult enforced_result{enforced_node, enforced_prop};
        left_result = enforced_result;
    }

    if (!PropertyMatcher::matchNodePartitioning(
            *cxt.getContext(), right_property.getNodePartitioningRef(), right_result.getOutputProperty().getNodePartitioning()))
    {
        PlanNodePtr enforced_node = PropertyEnforcer::enforceNodePartitioning(
            right_result.getNodePtr(), right_property, right_result.getOutputProperty(), *cxt.getContext());
        Property enforced_prop = PropertyDeriver::deriveProperty(
            enforced_node->getStep(), right_result.getOutputProperty(), cxt.getRequired(), cxt.getContext());
        ExchangeResult enforced_result{enforced_node, enforced_prop};
        right_result = enforced_result;
    }

    std::vector<ExchangeResult> results;
    results.emplace_back(left_result);
    results.emplace_back(right_result);
    return rebaseAndDeriveProperties(ptr, results, cxt.getRequired(), cxt.getContext());
}

ExchangeResult ExchangeVisitor::visitAggregatingStepExtNode(AggregatingStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitMarkDistinctStepExtNode(MarkDistinctStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitMergingAggregatedStepExtNode(MergingAggregatedStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitUnionStepExtNode(UnionStepExtNode & node, ExchangeContext & cxt)
{
    PlanNodePtr ptr = node.shared_from_this();
    PropertySet required_set = PropertyDeterminer::determineRequiredProperty(node.getStep(), cxt.getRequired(), *cxt.getContext())[0];

    std::vector<ExchangeResult> results;
    PlanNodes children;
    PropertySet children_property;

    for (size_t index = 0; index < ptr->getChildren().size(); ++index)
    {
        PlanNodePtr child = ptr->getChildren()[index];
        Property preferred = required_set[index];
        ExchangeContext child_cxt(cxt.getContext(), preferred);
        auto result = visitChild(child, child_cxt);
        children_property.emplace_back(result.getOutputProperty());
        children.push_back(result.getNodePtr());
        results.emplace_back(result);
    }
return rebaseAndDeriveProperties(ptr, results, cxt.getRequired(), cxt.getContext());
}

ExchangeResult ExchangeVisitor::visitExchangeStepExtNode(ExchangeStepExtNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitRemoteExchangeSourceStepExtNode(RemoteExchangeSourceStepExtNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitTableScanStepExtNode(TableScanStepExtNode & node, ExchangeContext & cxt)
{
    PlanNodePtr ptr = node.shared_from_this();
    Property output = PropertyDeriver::deriveProperty(ptr->getStep(), cxt.getContext(), cxt.getRequired());
    return ExchangeResult{ptr, output};
}

ExchangeResult ExchangeVisitor::visitReadNothingStepNode(ReadNothingStepNode & node, ExchangeContext & cxt)
{
    PlanNodePtr ptr = node.shared_from_this();
    Property output = PropertyDeriver::deriveProperty(ptr->getStep(), cxt.getContext(), cxt.getRequired());
    return ExchangeResult{ptr, output};
}

ExchangeResult ExchangeVisitor::visitValuesStepExtNode(ValuesStepExtNode & node, ExchangeContext & cxt)
{
    PlanNodePtr ptr = node.shared_from_this();
    Property output = PropertyDeriver::deriveProperty(ptr->getStep(), cxt.getContext(), cxt.getRequired());
    return ExchangeResult{ptr, output};
}

ExchangeResult ExchangeVisitor::visitLimitStepExtNode(LimitStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitOffsetStepNode(OffsetStepNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitLimitByStepNode(LimitByStepNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitTotalsHavingStepExtNode(TotalsHavingStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitSortingStepExtNode(SortingStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitMergeSortingStepExtNode(MergeSortingStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitPartialSortingStepExtNode(PartialSortingStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitMergingSortedStepExtNode(MergingSortedStepExtNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitIntersectOrExceptStepNode(IntersectOrExceptStepNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitFillingStepNode(FillingStepNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitDistinctStepExtNode(DistinctStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitExtremesStepNode(ExtremesStepNode & node, ExchangeContext & cxt)
{
    return enforceNode(node, cxt);
}

//ExchangeResult ExchangeVisitor::visitFinalSamplingNode(FinalSamplingNode & node, ExchangeContext & cxt)
//{
//    return enforceNodeAndStream(node, cxt);
//}

ExchangeResult ExchangeVisitor::visitWindowStepNode(WindowStepNode & node, ExchangeContext & cxt)
{
    return enforceNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitApplyStepExtNode(ApplyStepExtNode & node, ExchangeContext &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported node {}", node.getStep()->getName());
}

ExchangeResult ExchangeVisitor::visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitExplainAnalyzeStepExtNode(ExplainAnalyzeStepExtNode & node, ExchangeContext & cxt)
{
    return enforceNodeAndStream(node, cxt);
}

ExchangeResult ExchangeVisitor::visitCTERefStepExtNode(CTERefStepExtNode & node, ExchangeContext &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported node {}", node.getStep()->getName());
}

ExchangeResult ExchangeVisitor::visitTopNFilteringStepExtNode(TopNFilteringStepExtNode & node, ExchangeContext & cxt)
{
    return visitPlanNode(node, cxt);
}

ExchangeResult ExchangeVisitor::visitChild(PlanNodePtr node, ExchangeContext & cxt)
{
    return VisitorUtil::accept(node, *this, cxt);
}

ExchangeResult
ExchangeVisitor::rebaseAndDeriveProperties(const PlanNodePtr & node, ExchangeResult & result, Property & require, ContextMutablePtr & cxt)
{
    // replace the children of current node
    PlanNodes child;
    child.emplace_back(result.getNodePtr());
    node->replaceChildren(child);

    // derive property base on the replaced node
    return deriveProperties(node, result.getOutputProperty(), require, cxt);
}

ExchangeResult ExchangeVisitor::rebaseAndDeriveProperties(
    const PlanNodePtr & node, std::vector<ExchangeResult> & results, Property & require, ContextMutablePtr & cxt)
{
    PlanNodes children;
    PropertySet input_properties = std::vector<Property>();
    for (auto result : results)
    {
        children.emplace_back(result.getNodePtr());
        input_properties.emplace_back(result.getOutputProperty());
    }
    node->replaceChildren(children);
    return deriveProperties(node, input_properties, require, cxt);
}

ExchangeResult
ExchangeVisitor::deriveProperties(const PlanNodePtr & node, Property & input_property, Property & require, ContextMutablePtr & cxt)
{
    PropertySet input_properties = std::vector<Property>();
    input_properties.emplace_back(input_property);
    return deriveProperties(node, input_properties, require, cxt);
}

ExchangeResult
ExchangeVisitor::deriveProperties(const PlanNodePtr & node, PropertySet & input_properties, Property & require, ContextMutablePtr & cxt)
{
    Property property = PropertyDeriver::deriveProperty(node->getStep(), input_properties, require, cxt);
    return ExchangeResult{node, property};
}

ExchangeResult ExchangeVisitor::enforceNodeAndStream(PlanNodeBase & node, ExchangeContext & cxt)
{
    PlanNodePtr ptr = node.shared_from_this();
    PropertySet required_set = PropertyDeterminer::determineRequiredProperty(node.getStep(), cxt.getRequired(), *cxt.getContext())[0];
    PlanNodePtr child = ptr->getChildren()[0];
    Property preferred = required_set[0];
    ExchangeContext child_context{cxt.getContext(), preferred};
    ExchangeResult result = visitChild(child, child_context);

    ExchangeResult enforced_node_result;
    if (!PropertyMatcher::matchNodePartitioning(
            *cxt.getContext(), preferred.getNodePartitioningRef(), result.getOutputProperty().getNodePartitioning()))
    {
        PlanNodePtr enforced_node
            = PropertyEnforcer::enforceNodePartitioning(result.getNodePtr(), preferred, result.getOutputProperty(), *cxt.getContext());
        Property enforced_node_prop
            = PropertyDeriver::deriveProperty(enforced_node->getStep(), result.getOutputProperty(), cxt.getRequired(), cxt.getContext());
        enforced_node_result = ExchangeResult{enforced_node, enforced_node_prop};
    }

    ExchangeResult enforced_stream_result;
    if (!PropertyMatcher::matchStreamPartitioning(
            *cxt.getContext(), preferred.getStreamPartitioning(), result.getOutputProperty().getStreamPartitioning(),
            {}, {}, cxt.getContext()->getOptimizerContext()->getSettingsRef().enable_add_local_exchange))
    {
        if (enforced_node_result.getNodePtr() != nullptr)
        {
            PlanNodePtr enforced_stream_node = PropertyEnforcer::enforceStreamPartitioning(
                enforced_node_result.getNodePtr(), preferred, enforced_node_result.getOutputProperty(), *cxt.getContext());
            Property enforced_stream_prop = PropertyDeriver::deriveProperty(
                enforced_stream_node->getStep(), enforced_node_result.getOutputProperty(), cxt.getRequired(), cxt.getContext());
            enforced_stream_result = ExchangeResult{enforced_stream_node, enforced_stream_prop};
        }
        else
        {
            PlanNodePtr enforced_stream_node = PropertyEnforcer::enforceStreamPartitioning(
                result.getNodePtr(), preferred, result.getOutputProperty(), *cxt.getContext());
            Property enforced_stream_prop = PropertyDeriver::deriveProperty(
                enforced_stream_node->getStep(), result.getOutputProperty(), cxt.getRequired(), cxt.getContext());
            enforced_stream_result = ExchangeResult{enforced_stream_node, enforced_stream_prop};
        }
    }

    if (enforced_stream_result.getNodePtr() != nullptr)
    {
        return rebaseAndDeriveProperties(ptr, enforced_stream_result, cxt.getRequired(), cxt.getContext());
    }
    if (enforced_node_result.getNodePtr() != nullptr)
    {
        return rebaseAndDeriveProperties(ptr, enforced_node_result, cxt.getRequired(), cxt.getContext());
    }
    return rebaseAndDeriveProperties(ptr, result, cxt.getRequired(), cxt.getContext());
}

ExchangeResult ExchangeVisitor::enforceNode(PlanNodeBase & node, ExchangeContext & cxt)
{
    PlanNodePtr ptr = node.shared_from_this();
    PropertySet required_set = PropertyDeterminer::determineRequiredProperty(node.getStep(), cxt.getRequired(), *cxt.getContext())[0];
    PlanNodePtr child = ptr->getChildren()[0];
    Property preferred = required_set[0];
    ExchangeContext child_context{cxt.getContext(), preferred};
    ExchangeResult result = visitChild(child, child_context);

    if (PropertyMatcher::matchNodePartitioning(
            *cxt.getContext(), preferred.getNodePartitioningRef(), result.getOutputProperty().getNodePartitioning()))
    {
        return rebaseAndDeriveProperties(ptr, result, cxt.getRequired(), cxt.getContext());
    }
    PlanNodePtr enforced_node
        = PropertyEnforcer::enforceNodePartitioning(result.getNodePtr(), preferred, result.getOutputProperty(), *cxt.getContext());
    Property enforced_node_prop
        = PropertyDeriver::deriveProperty(enforced_node->getStep(), result.getOutputProperty(), cxt.getRequired(), cxt.getContext());
    ExchangeResult enforced_node_result{enforced_node, enforced_node_prop};

    return rebaseAndDeriveProperties(ptr, enforced_node_result, cxt.getRequired(), cxt.getContext());
}

}
