#pragma once

#include <Interpreters/Context.h>
#include <Query/Optimizer/Property/Property.h>
#include <Query/Optimizer/Rewriter/Rewriter.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

#include <utility>

namespace DB
{
/**
 * Reference paper : Incorporating Partitioning and Parallel Plans into the SCOPE Optimizer.
 */
class AddExchange : public Rewriter
{
public:
    String name() const override { return "AddExchange"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getOptimizerContext()->getSettingsRef().enable_add_exchange; }
    bool rewrite(QueryPlanExt & plan, ContextMutablePtr context) const override;
};

class ExchangeResult
{
public:
    explicit ExchangeResult(PlanNodePtr node_ = {}, Property property = Property{})
        : node(std::move(node_)), output_property(std::move(property))
    {
    }
    PlanNodePtr getNodePtr() { return node; }
    Property & getOutputProperty() { return output_property; }

private:
    PlanNodePtr node;
    Property output_property;
};

class ExchangeContext
{
public:
    ExchangeContext(ContextMutablePtr context_, Property & required_) : context(context_), required(required_) { }
    ContextMutablePtr & getContext() { return context; }
    Property & getRequired() { return required; }

private:
    ContextMutablePtr context;
    Property & required;
};

class ExchangeVisitor : public PlanNodeVisitor<ExchangeResult, ExchangeContext>
{
public:
    ExchangeResult visitPlanNode(PlanNodeBase &, ExchangeContext &) override;
    ExchangeResult visitProjectionStepExtNode(ProjectionStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitFilterStepExtNode(FilterStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitJoinStepExtNode(JoinStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitAggregatingStepExtNode(AggregatingStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitMarkDistinctStepExtNode(MarkDistinctStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitMergingAggregatedStepExtNode(MergingAggregatedStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitUnionStepExtNode(UnionStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitExchangeStepExtNode(ExchangeStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitRemoteExchangeSourceStepExtNode(RemoteExchangeSourceStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitExplainAnalyzeStepExtNode(ExplainAnalyzeStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitFillingStepNode(FillingStepNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitTableScanStepExtNode(TableScanStepExtNode & node, ExchangeContext &) override;
    ExchangeResult visitReadNothingStepNode(ReadNothingStepNode & node, ExchangeContext &) override;
    ExchangeResult visitValuesStepExtNode(ValuesStepExtNode & node, ExchangeContext &) override;
    ExchangeResult visitLimitStepExtNode(LimitStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitTotalsHavingStepExtNode(TotalsHavingStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitOffsetStepNode(OffsetStepNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitLimitByStepNode(LimitByStepNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitSortingStepExtNode(SortingStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitMergeSortingStepExtNode(MergeSortingStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitPartialSortingStepExtNode(PartialSortingStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitMergingSortedStepExtNode(MergingSortedStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitIntersectOrExceptStepNode(IntersectOrExceptStepNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitDistinctStepExtNode(DistinctStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitExtremesStepNode(ExtremesStepNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitWindowStepExtNode(WindowStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitApplyStepExtNode(ApplyStepExtNode & node, ExchangeContext &) override;
    ExchangeResult visitEnforceSingleRowStepExtNode(EnforceSingleRowStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitAssignUniqueIdStepExtNode(AssignUniqueIdStepExtNode & node, ExchangeContext & cxt) override;
    ExchangeResult visitCTERefStepExtNode(CTERefStepExtNode & node, ExchangeContext &) override;
    ExchangeResult visitTopNFilteringStepExtNode(TopNFilteringStepExtNode & node, ExchangeContext & cxt) override;

private:
    /**
     * Execute the AddExchange rewrite rule for child node.
     *
     * @param node child node
     * @param cxt the preferred property for child node and context
     * @return child node with it's out property (which already have satisfied the requirement)
     */
    ExchangeResult visitChild(PlanNodePtr node, ExchangeContext & cxt);

    /**
     * Replace child first, then derive the output property.
     *
     * @param node current node.
     * @param result child with it's output property.
     * @return node with it's output property.
     */
    static ExchangeResult
    rebaseAndDeriveProperties(const PlanNodePtr & node, ExchangeResult & result, Property & require, ContextMutablePtr & cxt);

    /**
     * Replace children first, then derive the output property.
     *
     * @param node current node.
     * @param results children with it's output property.
     * @return node with it's output property.
     */
    static ExchangeResult
    rebaseAndDeriveProperties(const PlanNodePtr & node, std::vector<ExchangeResult> & results, Property & require, ContextMutablePtr & cxt);

    /**
     * Derive the actual property of node.
     *
     * @param node current node, which has single child. e.g filter, projection, aggregation.
     * @param inputProperty the actual property of child node.
     * @return node with it's actual property.
     */
    static ExchangeResult deriveProperties(const PlanNodePtr & node, Property & inputProperty, Property & require, ContextMutablePtr & cxt);

    /**
     * Derive the actual property of node.
     *
     * @param node current node, which has multiple children. e.g Join, Union.
     * @param inputProperties the actual property of child node.
     * @return node with it's actual property.
     */
    static ExchangeResult
    deriveProperties(const PlanNodePtr & node, PropertySet & inputProperties, Property & require, ContextMutablePtr & cxt);

    ExchangeResult enforceNodeAndStream(PlanNodeBase & node, ExchangeContext & cxt);
    ExchangeResult enforceNode(PlanNodeBase & node, ExchangeContext & cxt);
};

}
