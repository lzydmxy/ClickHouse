#include <Query/Optimizer/Rule/Rewrite/PushDownApplyRules.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Query/Optimizer/PlanNodeCardinality.h>
#include <Query/Optimizer/Rule/Pattern.h>
#include <Query/Optimizer/Rule/Patterns.h>
#include <Query/Processors/QueryPlan/ApplyStepExt.h>
#include <Query/Processors/IQueryPlanStepExt.h>
#include <Query/Processors/QueryPlan/JoinStepExt.h>
#include <Query/Processors/QueryPlan/LimitStepExt.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>
//#include <Query/Processors/QueryPlan/WindowStepExt.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{
ConstRefPatternPtr PushDownApplyThroughJoin::getPattern() const
{
    static auto pattern = Patterns::apply()
        .matchingStep<ApplyStepExt>([](const ApplyStepExt & step) { return !step.getOuterColumns().empty() && step.getCorrelation().empty(); })
        .with(Patterns::join(), Patterns::any())
        .result();
    return pattern;
}

TransformResult PushDownApplyThroughJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * apply = dynamic_cast<ApplyStepExt *>((node->getStep().get()));
    const auto & outer_columns = apply->getOuterColumns();
    auto join = node->getChildren()[0];
    size_t index = 0;
    for (const auto & child : join->getChildren())
    {
        auto out_names = child->getOutputNames();
        if (std::includes(out_names.begin(), out_names.end(), outer_columns.begin(), outer_columns.end()))
        {
            DataStreams input{child->getStep()->getOutputStream(), node->getChildren()[1]->getStep()->getOutputStream()};

            auto apply_step = std::make_shared<ApplyStepExt>(
                input,
                apply->getCorrelation(),
                apply->getApplyType(),
                apply->getSubqueryType(),
                apply->getAssignment(),
                apply->getOuterColumns(),
                apply->supportSemiAnti());
            PlanNodes children{child, node->getChildren()[1]};
            auto apply_node = ApplyStepExtNode::createPlanNode(context.context->getOptimizerContext()->nextNodeId(), std::move(apply_step), children, node->getStatistics());
            PlanNodes join_children = join->getChildren();
            join_children[index] = apply_node;

            DataStreams inputs;
            ColumnsWithTypeAndName output;
            for (const auto & item : join_children)
            {
                auto stream = item->getCurrentDataStream();
                inputs.emplace_back(stream);
                output.insert(output.end(), stream.header.begin(), stream.header.end());
            }

            auto step = dynamic_cast<JoinStepExt *>(join->getStep().get());
            auto new_step = std::make_shared<JoinStepExt>(
                inputs,
                DataStream{Block{output}},
                step->getKind(),
                step->getStrictness(),
                step->getMaxStreams(),
                step->getKeepLeftReadInOrder(),
                step->getLeftKeys(),
                step->getRightKeys(),
                step->getKeyIdsNullSafe(),
                step->getFilter(),
                step->isHasUsing(),
                step->getRequireRightKeys(),
                step->getAsofInequality(),
                step->getDistributionType(),
                step->getJoinAlgorithm(),
                step->isMagic(),
                step->isOrdered(),
                step->isSimpleReordered(),
                step->getRuntimeFilterBuilders());
                //todo: liyang453, other feat: need hints
                //step->getHints());

            return PlanNodeBase::createPlanNode(join->getId(), new_step, join_children);
        }
        ++index;
    }
    return {};
}

}
