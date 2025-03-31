#pragma once

#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{

/// PlanNode visitor, for optimizer only.
template <typename R, typename C>
class PlanNodeVisitor
{
public:
    virtual ~PlanNodeVisitor() = default;

    virtual R visitPlanNode(PlanNodeBase & node, C &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Visitor does not supported this plan node: {}", node.getStep()->getName());
    }

#define VISITOR_DEF(TYPE) \
    virtual R visit##TYPE##Node(TYPE##Node & node, C & context) { return visitPlanNode(static_cast<PlanNodeBase &>(node), context); }
    APPLY_QUERY_PLAN_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

/// IQueryPlanStep visitor
template <typename R, typename C>
class StepVisitor
{
public:
    virtual ~StepVisitor() = default;

    virtual R visitStep(const IQueryPlanStep & step, C &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Visitor does not supported this step: {}", step.getName());
    }

#define VISITOR_DEF(TYPE) \
    virtual R visit##TYPE(const TYPE & step, C & context) \
    { \
        return visitStep(static_cast<const IQueryPlanStep &>(step), context); \
    }
    APPLY_QUERY_PLAN_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

/// QueryPlan::Node visitor
template <typename R, typename C>
class NodeVisitor
{
public:
    virtual ~NodeVisitor() = default;

    virtual R visitNode(QueryPlan::Node *, C &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Visitor does not supported this step."); }

#define VISITOR_DEF(TYPE) \
    virtual R visit##TYPE##Node(QueryPlan::Node * node, C & context) { return visitNode(node, context); }
    APPLY_QUERY_PLAN_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

class VisitorUtil
{
public:
    template <typename R, typename C>
    static R accept(PlanNodeBase & node, PlanNodeVisitor<R, C> & visitor, C & context)
    {
        switch (node.getType())
        {
#define VISITOR_DEF(TYPE) \
    case QueryPlanStepType::TYPE: { \
        return visitor.visit##TYPE##Node(static_cast<TYPE##Node &>(node), context); \
    }
            APPLY_QUERY_PLAN_STEP_TYPES(VISITOR_DEF)

#undef VISITOR_DEF
            default:
                return visitor.visitPlanNode(node, context);
        }
    }

    template <typename R, typename C>
    static R accept(const PlanNodePtr & node, PlanNodeVisitor<R, C> & visitor, C & context)
    {
        switch (node->getType())
        {
#define VISITOR_DEF(TYPE) \
    case QueryPlanStepType::TYPE: { \
        return visitor.visit##TYPE##Node(static_cast<TYPE##Node &>(*node), context); \
    }
            APPLY_QUERY_PLAN_STEP_TYPES(VISITOR_DEF)

#undef VISITOR_DEF
            default:
                return visitor.visitPlanNode(*node, context);
        }
    }

    template <typename R, typename C>
    static R accept(const IQueryPlanStep & step, StepVisitor<R, C> & visitor, C & context)
    {
        switch (getQueryPlanStepType(std::make_shared<IQueryPlanStep>(step)))
        {
#define VISITOR_DEF(TYPE) \
    case QueryPlanStepType::TYPE: { \
        return visitor.visit##TYPE(static_cast<const TYPE &>(step), context); \
    }
            APPLY_QUERY_PLAN_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
            default:
                return visitor.visitStep(step, context);
        }
    }

    template <typename R, typename C>
    static R accept(const QueryPlanStepPtr & step, StepVisitor<R, C> & visitor, C & context)
    {
        switch (getQueryPlanStepType(step))
        {
#define VISITOR_DEF(TYPE) \
    case QueryPlanStepType::TYPE: { \
        return visitor.visit##TYPE(static_cast<const TYPE &>(*step), context); \
    }
            APPLY_QUERY_PLAN_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
            default:
                return visitor.visitStep(*step, context);
        }
    }

    template <typename R, typename C>
    static R accept(QueryPlan::Node * node, NodeVisitor<R, C> & visitor, C & context)
    {
        switch (node ? getQueryPlanStepType(node->step) : QueryPlanStepType::Any)
        {
#define VISITOR_DEF(TYPE) \
    case QueryPlanStepType::TYPE: { \
        return visitor.visit##TYPE##Node(node, context); \
    }
            APPLY_QUERY_PLAN_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
            default:
                return visitor.visitNode(node, context);
        }
    }
};

}

