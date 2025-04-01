#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{

PlanNodePtr PlanNodeBase::getNodeById(PlanNodeId node_id) const
{
    PlanNodes stack;
    stack.push_back(std::const_pointer_cast<PlanNodeBase>(this->shared_from_this()));

    while (!stack.empty())
    {
        auto node = stack.back();
        stack.pop_back();
        if (node->getId() == node_id)
            return node;

        for (auto & child : node->getChildren())
            stack.push_back(child);
    }

    return nullptr;
}

#define PLAN_NODE_DEF(TYPE) template class PlanNode<TYPE>;
APPLY_QUERY_PLAN_STEP_TYPES(PLAN_NODE_DEF)
// PLAN_NODE_DEF(Any)
#undef PLAN_NODE_DEF

}
