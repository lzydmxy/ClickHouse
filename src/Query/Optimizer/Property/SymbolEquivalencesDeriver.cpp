#include <Query/Optimizer/Property/SymbolEquivalencesDeriver.h>

#include <Query/Optimizer/Utils.h>

namespace DB
{

SymbolEquivalencesPtr
SymbolEquivalencesDeriver::deriveEquivalences(QueryPlanStepPtr step, std::vector<SymbolEquivalencesPtr> children_equivalences)
{
    static SymbolEquivalencesDeriverVisitor derive;
    auto output = step->getOutputStream().header.getNames();

    NameSet output_set(output.begin(), output.end());
    auto result = VisitorUtil::accept(step, derive, children_equivalences);
    result->createRepresentMap(output_set);
    return result;
}

SymbolEquivalencesPtr SymbolEquivalencesDeriverVisitor::visitStep(const IQueryPlanStep &, std::vector<SymbolEquivalencesPtr> &)
{
    return std::make_shared<SymbolEquivalences>();
}

SymbolEquivalencesPtr SymbolEquivalencesDeriverVisitor::visitJoinStepExt(const JoinStepExt & step, std::vector<SymbolEquivalencesPtr> & context)
{
    auto result = std::make_shared<SymbolEquivalences>(*context[0], *context[1]);

    if (step.getKind() == JoinKind::Inner)
    {
        for (size_t index = 0; index < step.getLeftKeys().size(); index++)
        {
            result->add(step.getLeftKeys().at(index), step.getRightKeys().at(index));
        }
    }
    return result;
}

SymbolEquivalencesPtr SymbolEquivalencesDeriverVisitor::visitFilterStepExt(const FilterStepExt &, std::vector<SymbolEquivalencesPtr> & context)
{
    return context[0];
}

SymbolEquivalencesPtr
SymbolEquivalencesDeriverVisitor::visitProjectionStepExt(const ProjectionStepExt & step, std::vector<SymbolEquivalencesPtr> & context)
{
    const auto & assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    for (auto & item : identities)
        context[0]->add(item.second, item.first);
    return context[0];
}

SymbolEquivalencesPtr
SymbolEquivalencesDeriverVisitor::visitAggregatingStepExt(const AggregatingStepExt &, std::vector<SymbolEquivalencesPtr> & context)
{
    return context[0];
}

SymbolEquivalencesPtr
SymbolEquivalencesDeriverVisitor::visitExchangeStepExt(const ExchangeStepExt &, std::vector<SymbolEquivalencesPtr> & context)
{
    return context[0];
}

SymbolEquivalencesPtr
SymbolEquivalencesDeriverVisitor::visitCTERefStepExt(const CTERefStepExt & step, std::vector<SymbolEquivalencesPtr> & context)
{
    if (!context.empty() && context[0])
    {
        auto mappings = step.getOutputColumns();
        for (const auto & mapping : mappings)
            context[0]->add(mapping.first, mapping.second);
        return context[0];
    }
    return std::make_shared<SymbolEquivalences>();
}

}
