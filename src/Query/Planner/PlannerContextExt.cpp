#include <Query/Planner/PlannerContextExt.h>

namespace DB
{

PlannerContextExt::PlannerContextExt(ContextMutablePtr query_context_, GlobalPlannerContextPtr global_planner_context_, const SelectQueryOptions & select_query_options_)
{
    planner_context = std::make_shared<PlannerContext>(query_context_, global_planner_context_, select_query_options_);
}

PlannerContextExt::PlannerContextExt(ContextMutablePtr query_context_, PlannerContextPtr planner_context_)
{
    planner_context = std::make_shared<PlannerContext>(query_context_, planner_context_);
}

}
