#pragma once

#include <Query/Processors/IQueryPlanStepExt.h>
#include <Processors/QueryPlan/QueryPlan.h>
//todo: delete , will gen many build error
//#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>

namespace DB
{
    template <typename R, typename C>
    class NodeVisitor
    {
    public:
        virtual ~NodeVisitor() = default;
    
        virtual R visitNode(QueryPlan::Node *, C &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Visitor does not supported this step."); }
    
        //virtual R visit##TYPE##Node(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        //virtual R visitNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitValuesNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitCTERefNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitExchangeNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitReadNothingNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitTableScanNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitTotalsHavingNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitReadStorageRowCountNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitExtremesNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
        virtual R visitRemoteExchangeSourceNode(QueryPlan::Node * node, C & context) { return visitNode(node, context); }

        //std::vector<size_t> visitNode(QueryPlan::Node * node, const Context & context) override;
        //std::vector<size_t> visitValuesNode(QueryPlan::Node * node, const Context & context) override;
        //std::vector<size_t> visitReadNothingNode(QueryPlan::Node * node, const Context & context) override;
        //std::vector<size_t> visitTableScanNode(QueryPlan::Node * node, const Context & context) override;
        //std::vector<size_t> visitRemoteExchangeSourceNode(QueryPlan::Node * node, const Context & context) override;
        //std::vector<size_t> visitReadStorageRowCountNode(QueryPlan::Node * node, const Context & context) override;
    };


class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;

template <typename R, typename C>
class PlanNodeVisitor
{
public:
    virtual ~PlanNodeVisitor() = default;

    
    virtual R visitPlanNode(PlanNodeBase & node, C &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Visitor does not supported this plan node");
    }

    virtual R visitNode (PlanNodeBase & node, C & context)
    {
        return visitPlanNode(static_cast<PlanNodeBase &>(node), context);
    }
    
    //virtual R visitTableScanNode(TableScanNode & node, C & context) { return visitPlanNode(static_cast<PlanNodeBase &>(node), context); }
};

template <typename R, typename C>
class StepVisitor
{
public:
    virtual ~StepVisitor() = default;

    virtual R visitStep(const IQueryPlanStep & step, C &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Visitor does not supported this step: ");
    }
};

class VisitorUtil
{
public:
    template <typename R, typename C>
    static R accept(PlanNodeBase & node, PlanNodeVisitor<R, C> & visitor, C & context){ return visitor.visitNode(node, context); }
    template <typename R, typename C>
    static R accept(QueryPlan::Node * node, NodeVisitor<R, C> & visitor, C & context){ return visitor.visitNode(node, context); }
    template <typename R, typename C>
    static R accept(const IQueryPlanStep & step, StepVisitor<R, C> & visitor, C & context){ return visitor.visitNode(step, context); }
    template <typename R, typename C>
    static R accept(const PlanNodePtr & node, PlanNodeVisitor<R, C> & visitor, C & context){ return visitor.visitNode(*node, context); }
    template <typename R, typename C>
    static R accept(const QueryPlanStepPtr & step, StepVisitor<R, C> & visitor, C & context){ return visitor.visitNode(*step, context); }
};

}

