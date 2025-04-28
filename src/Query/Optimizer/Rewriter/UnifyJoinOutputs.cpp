#include <Query/Optimizer/Rewriter/UnifyJoinOutputs.h>

#include <Query/Optimizer/SymbolsExtractor.h>
#include <Query/Processors/QueryPlan/PlanNode.h>
#include <Query/Planner/SymbolMapper.h>
#include <Query/Processors/QueryPlan/CTERefStepExt.h>

namespace DB
{

bool UnifyJoinOutputs::rewrite(QueryPlanExt & plan, ContextMutablePtr context) const
{
    auto union_find_map = UnionFindExtractor::extract(plan);
    UnifyJoinOutputs::Rewriter rewriter{context, plan.getCTEInfo(), union_find_map};
    std::set<String> require;
    auto result = VisitorUtil::accept(plan.getPlanNode(), rewriter, require);
    plan.update(result);
    return true;
}

std::unordered_map<PlanNodeId, UnionFind<String>> UnifyJoinOutputs::UnionFindExtractor::extract(QueryPlanExt & plan)
{
    UnionFindExtractor extractor {plan.getCTEInfo()};
    std::unordered_map<PlanNodeId, UnionFind<String>> union_find_map;
    VisitorUtil::accept(plan.getPlanNode(), extractor, union_find_map);
    return union_find_map;
}

Void UnifyJoinOutputs::UnionFindExtractor::visitJoinStepExtNode(JoinStepExtNode & node, std::unordered_map<PlanNodeId, UnionFind<String>> & union_find_map)
{
    auto step = dynamic_cast<const JoinStepExt *>(node.getStep().get());
    if (!step->supportReorder(true))
        return visitPlanNode(node, union_find_map);

    VisitorUtil::accept(node.getChildren()[0], *this, union_find_map);
    VisitorUtil::accept(node.getChildren()[1], *this, union_find_map);

    UnionFind<String> union_find{union_find_map[node.getChildren()[0]->getId()], union_find_map[node.getChildren()[1]->getId()]};
    for (size_t i = 0; i < step->getLeftKeys().size(); i++)
        union_find.add(step->getLeftKeys()[i], step->getRightKeys()[i]);

    union_find_map.emplace(node.getId(), std::move(union_find));
    return Void{};
}

Void UnifyJoinOutputs::UnionFindExtractor::visitCTERefStepExtNode(CTERefStepExtNode & node, std::unordered_map<PlanNodeId, UnionFind<String>> & context)
{
    const auto * step = dynamic_cast<const CTERefStepExt *>(node.getStep().get());
    cte_helper.accept(step->getId(), *this, context);
    return Void{};
}

PlanNodePtr UnifyJoinOutputs::Rewriter::visitPlanNode(PlanNodeBase & node, std::set<String> &)
{
    if (node.getChildren().empty())
        return node.shared_from_this();

    PlanNodes children;
    for (const auto & child : node.getChildren())
    {
        std::set<String> require;
        for (const auto & item : child->getStep()->getOutputStream().header)
            require.insert(item.name);

        auto result = VisitorUtil::accept(child, *this, require);
        children.emplace_back(result);
    }

    node.replaceChildren(children);
    return node.shared_from_this();
}

PlanNodePtr UnifyJoinOutputs::Rewriter::visitJoinStepExtNode(JoinStepExtNode & node, std::set<String> & require)
{
    auto step = dynamic_cast<const JoinStepExt *>(node.getStep().get());
    if (!step->supportReorder(true))
        return visitPlanNode(node, require);

    if (step->getFilter())
    {
        auto filter_symbols = SymbolsExtractor::extract(step->getFilter());
        require.insert(filter_symbols.begin(), filter_symbols.end());
    }

    auto & union_find = union_find_map[node.getId()];
    auto & left_union_find = union_find_map[node.getChildren()[0]->getId()];
    auto & right_union_find = union_find_map[node.getChildren()[1]->getId()];

    std::vector<std::pair<String, String>> criteria;
    auto left_sets = left_union_find.getSets();
    auto right_sets = right_union_find.getSets();
    NameSet represent_set;
    for (size_t i = 0; i < step->getLeftKeys().size(); i++)
    {
        String left_key = step->getLeftKeys()[i];
        for (const auto & set : left_sets)
            if (set.count(left_key))
                left_key = *std::min_element(set.begin(), set.end());

        String right_key = step->getRightKeys()[i];
        for (const auto & set : right_sets)
            if (set.count(right_key))
                right_key = *std::min_element(set.begin(), set.end());

        if (!represent_set.count(union_find.find(left_key)))
        {
            criteria.emplace_back(left_key, right_key);
            represent_set.insert(union_find.find(left_key));
        }
    }

    std::sort(criteria.begin(), criteria.end(), [](auto & a, auto & b) { return a.first < b.first; });
    Names left_keys;
    Names right_keys;
    std::set<String> left_require = require;
    std::set<String> right_require = require;
    for (const auto & item : criteria)
    {
        left_keys.emplace_back(item.first);
        left_require.insert(item.first);
        right_keys.emplace_back(item.second);
        right_require.insert(item.second);
    }

    auto left = VisitorUtil::accept(node.getChildren()[0], *this, left_require);
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, right_require);

    std::unordered_map<String, DataTypePtr> name_to_types;
    for (auto & item : left->getStep()->getOutputStream().header)
        name_to_types.emplace(item.name, item.type);
    for (auto & item : right->getStep()->getOutputStream().header)
        name_to_types.emplace(item.name, item.type);

    ColumnsWithTypeAndName new_outputs;
    for (const auto & name : require)
        if (name_to_types.count(name))
            new_outputs.emplace_back(name_to_types[name], name);

    auto new_step = std::make_shared<JoinStepExt>(
        DataStreams{left->getStep()->getOutputStream(), right->getStep()->getOutputStream()},
        DataStream{Block(new_outputs)},
        step->getKind(),
        step->getStrictness(),
        step->getMaxStreams(),
        step->getKeepLeftReadInOrder(),
        std::move(left_keys),
        std::move(right_keys),
        std::vector<bool>{},
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
    return PlanNodeBase::createPlanNode(node.getId(), new_step, PlanNodes{left, right});
}

PlanNodePtr UnifyJoinOutputs::Rewriter::visitCTERefStepExtNode(CTERefStepExtNode & node, std::set<String> &)
{
    const auto * step = dynamic_cast<const CTERefStepExt *>(node.getStep().get());
    auto outputs = cte_helper.getCTEInfo().getCTEDef(step->getId())->getOutputNames();
    std::set<String> require{outputs.begin(), outputs.end()};
    auto cte_plan = cte_helper.acceptAndUpdate(step->getId(), *this, require);
    return node.shared_from_this();
}

}
