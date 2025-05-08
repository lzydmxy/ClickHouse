#include <Query/Optimizer/Rule/Rewrite/AddRepartitionColumn.h>

#include <Query/Optimizer/ProjectionPlanner.h>
#include <Query/Parsers/ASTClusterByElementExt.h>

namespace DB
{
    TransformResult AddRepartitionColumn::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
    {
        const auto * step = dynamic_cast<const ExchangeStepExt *>(node->getStep().get());
        if (!step)
            return {};


        ProjectionPlanner proj(node->getChildren()[0], context.context);
        auto col = proj.addColumn(step->getSchema().getShuffleExpr());
        auto projection = proj.build();

        auto new_partition = step->getSchema();
        auto bucket_expr = new_partition.getBucketExpr();
        if (bucket_expr)
        {
            if (auto * cluster_by_ast_element = bucket_expr->as<ASTClusterByElementExt>())
            {
                cluster_by_ast_element->children[0] = std::make_shared<ASTIdentifier>("$0");
                new_partition.setBucketExpr(bucket_expr);
                new_partition.setColumns({col.first});
                return PlanNodeBase::createPlanNode(
                    context.context->getOptimizerContext()->nextNodeId(),
                    std::make_shared<ExchangeStepExt>(
                        DataStreams{projection->getCurrentDataStream()}, step->getExchangeMode(), new_partition, step->needKeepOrder()),
                    {projection});
            }
        }

        return {};
    }

}
