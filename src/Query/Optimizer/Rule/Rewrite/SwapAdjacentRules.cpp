#include <Query/Optimizer/Rule/Rewrite/SwapAdjacentRules.h>

#include <Query/Optimizer/PlanNodeCardinality.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Processors/QueryPlan/PlanNodeIdAllocator.h>
#include <Processors/QueryPlan/WindowStep.h>

namespace DB
{
TransformResult SwapAdjacentWindows::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto & step = dynamic_cast<const WindowStep &>(*node->getStep().get());
    PlanNodePtr child_ptr = node->getChildren()[0];

    if (getQueryPlanStepType(child_ptr->getStep()) != QueryPlanStepType::WindowStep)
        return {};

    auto & partition_scheme = QueryPlanStepHelper::getWindowStepWindow(step).partition_by;
    std::vector<std::string> partition_keys;

    for (const auto & sort_column_description : partition_scheme)
    {
        partition_keys.emplace_back(sort_column_description.column_name);
    }

    std::vector<std::string> child_partition_keys;
    auto & child_step = dynamic_cast<const WindowStep &>(*child_ptr->getStep());
    auto & child_partition_scheme = QueryPlanStepHelper::getWindowStepWindow(child_step).partition_by;

    for (const auto & sort_column_description : child_partition_scheme)
    {
        child_partition_keys.emplace_back(sort_column_description.column_name);
    }

    auto partition_len = partition_keys.size();
    size_t iterator_index = 0;
    auto child_partition_len = child_partition_keys.size();
    bool re_order = false;

    while (iterator_index < partition_len && iterator_index < child_partition_len)
    {
        if (partition_keys[iterator_index] < child_partition_keys[iterator_index] && !re_order)
        {
            re_order = true;
        }
        else if (partition_keys[iterator_index] > child_partition_keys[iterator_index] && !re_order)
        {
            return {};
        }

        iterator_index++;
    }

    if (!re_order && partition_len >= child_partition_len)
    {
        return {};
    }

    auto & sort_scheme = QueryPlanStepHelper::getWindowStepWindow(step).order_by;
    std::vector<std::string> order_keys;

    for (auto & sort_column_description : sort_scheme)
    {
        order_keys.emplace_back(sort_column_description.column_name);
    }

    std::vector<std::string> window_function_arguments;

    for (auto & window_function_description : QueryPlanStepHelper::getWindowStepWindow(step).window_functions)
    {
        for (auto & window_function_argument : window_function_description.argument_names)
        {
            window_function_arguments.emplace_back(window_function_argument);
        }
    }

    std::unordered_set<std::string> child_get_created_symbol_set;

    for (auto & window_function_description : QueryPlanStepHelper::getWindowStepWindow(child_step).window_functions)
    {
        child_get_created_symbol_set.insert(window_function_description.column_name);
    }

    for (auto & partition_key : partition_keys)
    {
        if (child_get_created_symbol_set.count(partition_key))
        {
            return {};
        }
    }

    for (auto & order_key : order_keys)
    {
        if (child_get_created_symbol_set.count(order_key))
        {
            return {};
        }
    }

    for (auto & argument : window_function_arguments)
    {
        if (child_get_created_symbol_set.count(argument))
        {
            return {};
        }
    }

    QueryPlanStepPtr new_child_step = std::make_shared<WindowStep>(child_step.getInputStreams()[0],
                                                                   QueryPlanStepHelper::getWindowStepWindow(step),
                                                                   QueryPlanStepHelper::getWindowStepFunctions(step),
                                                                   QueryPlanStepHelper::getWindowStepStreamsFanOut(step));
    QueryPlanStepPtr new_step = std::make_shared<WindowStep>(new_child_step->getOutputStream(),
                                                             QueryPlanStepHelper::getWindowStepWindow(child_step),
                                                             QueryPlanStepHelper::getWindowStepFunctions(child_step),
                                                             QueryPlanStepHelper::getWindowStepStreamsFanOut(child_step));

    auto new_child_node = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(new_child_step), child_ptr->getChildren());
    auto new_node = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(new_step), PlanNodes{new_child_node});

    return new_node;
}

}
