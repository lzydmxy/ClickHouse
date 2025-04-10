#pragma once

#include <Planner/PlannerContext.h>
#include <Query/Planner/SymbolAllocator.h>

namespace DB
{

class PlannerContextExt
{
public:
    /// Create planner context with query context and global planner context
    PlannerContextExt(ContextMutablePtr query_context_, GlobalPlannerContextPtr global_planner_context_, const SelectQueryOptions & select_query_options_);

    /// Create planner with modified query_context
    PlannerContextExt(ContextMutablePtr query_context_, PlannerContextPtr planner_context_);

    const PlannerContextPtr & getPlannerContextPtr() const { return planner_context; }

    PlannerContextPtr & getPlannerContextPtr() { return planner_context; }

    ContextPtr getQueryContext() const
    {
        return planner_context->getQueryContext();
    }

    /// Get planner context mutable query context
    const ContextMutablePtr & getMutableQueryContext() const
    {
        return planner_context->getMutableQueryContext();
    }

    /// Get planner context mutable query context
    ContextMutablePtr & getMutableQueryContext()
    {
        return planner_context->getMutableQueryContext();
    }

    /// Get global planner context
    const GlobalPlannerContextPtr & getGlobalPlannerContext() const
    {
        return planner_context->getGlobalPlannerContext();
    }

    /// Get global planner context
    GlobalPlannerContextPtr & getGlobalPlannerContext()
    {
        return planner_context->getGlobalPlannerContext();
    }

private:
    std::shared_ptr<SymbolAllocator> symbol_allocator = std::make_shared<SymbolAllocator>();
    PlannerContextPtr planner_context;
};

}
