#include <Core/Names.h>
#include <Query/Optimizer/ExpressionInterpreter.h>
#include <Query/Optimizer/PlanNodeCardinality.h>
#include <Query/Optimizer/Rule/Rewrite/FilterWindowToPartitionTopN.h>
#include <Query/Optimizer/Rule/Rule.h>
#include <Parsers/ASTFunction.h>
//#include <Processors/Transforms/PartitionTopNTransform.h>
#include <Query/Processors/QueryPlan/ExchangeStepExt.h>
#include <Query/Processors/QueryPlan/PartitionTopNStepExt.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <base/types.h>
#include <Query/Core/BlockHelper.h>

namespace DB
{
TransformResult FilterWindowToPartitionTopN::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * filter_node = dynamic_cast<FilterStepExtNode *>(node.get());
    const auto & step = *filter_node->getStep();
    const auto & predicate = step.getFilter();

    auto * window_node = dynamic_cast<WindowStepNode *>(node->getChildren()[0].get());
    const auto & window_step = *window_node->getStep();

    auto * exchange_node = dynamic_cast<ExchangeStepExtNode *>(window_node->getChildren()[0].get());
    const auto & exchange_step = *exchange_node->getStep();

    if (dynamic_cast<PartitionTopNStepExtNode *>(exchange_node->getChildren()[0].get()))
    {
        return {};
    }

    if (const auto * func = predicate->as<ASTFunction>())
    {
        if (func->name == "less" || func->name == "lessOrEquals")
        {
            auto symbol = func->arguments->children[0];
            auto field_with_type = ExpressionInterpreter::evaluateConstantExpression(func->arguments->children[1], BlockHelper::getNamesToTypes(step.getInputStreams()[0].header), context.context);

            if (symbol->as<ASTIdentifier>() && field_with_type.has_value())
            {
                UInt64 limit;
                if (field_with_type->second.tryGet(limit))
                {
                    const auto & window_desc = window_step.getWindowDescription();
                    if (window_desc.window_functions.size() == 1)
                    {
                        auto window_func = window_desc.window_functions[0];

                        static std::map<String, TopNModel> funcs{
                            {"row_number", TopNModel::ROW_NUMBER}, {"rank", TopNModel::RANKER}, {"dense_rank", TopNModel::DENSE_RANK}};
                        auto func_name = window_func.function_node->name;
                        if (window_func.column_name == symbol->getColumnName() && funcs.contains(func_name) && !window_desc.order_by.empty()
                            && !window_desc.partition_by.empty())
                        {
                            Names partition;
                            for (auto part : window_desc.partition_by)
                            {
                                partition.emplace_back(part.column_name);
                            }

                            Names order_by;
                            for (auto part : window_desc.order_by)
                            {
                                order_by.emplace_back(part.column_name);
                                if (part.direction == 1)
                                {
                                    return {};
                                }
                            }

                            auto before_exchange_sort = std::make_unique<PartitionTopNStepExt>(
                                exchange_node->getChildren()[0]->getCurrentDataStream(), partition, order_by, limit, funcs[func_name]);
                            auto before_exchange_sort_node = PlanNodeBase::createPlanNode(
                                context.context->getOptimizerContext()->nextNodeId(), std::move(before_exchange_sort), {exchange_node->getChildren()[0]});

                            auto new_exchange_step = exchange_step.copy(context.context);
                            auto new_exchange_node = PlanNodeBase::createPlanNode(
                                context.context->getOptimizerContext()->nextNodeId(), std::move(new_exchange_step), {before_exchange_sort_node});

                            QueryPlanStepPtr new_window = QueryPlanStepHelper::copyQueryPlanStep(window_node->getStep(), context.context);
                            auto new_window_node
                                = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(new_window), {new_exchange_node});

                            QueryPlanStepPtr new_filter = step.copy(context.context);
                            auto new_filter_node
                                = PlanNodeBase::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(new_filter), {new_window_node});

                            return new_filter_node;
                        }
                    }
                }
            }
        }
    }


    return {};
}

}
