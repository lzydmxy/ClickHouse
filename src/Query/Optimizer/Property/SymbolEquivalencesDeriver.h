#pragma once

#include <Query/Optimizer/Property/Equivalences.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>

#include <utility>

namespace DB
{
using SymbolEquivalences = Equivalences<String>;
using SymbolEquivalencesPtr = std::shared_ptr<SymbolEquivalences>;

class SymbolEquivalencesDeriver
{
public:
    static SymbolEquivalencesPtr deriveEquivalences(QueryPlanStepPtr step, std::vector<SymbolEquivalencesPtr> children_equivalences);
};

class SymbolEquivalencesDeriverVisitor : public StepVisitor<SymbolEquivalencesPtr, std::vector<SymbolEquivalencesPtr>>
{
public:
    SymbolEquivalencesPtr visitStep(const IQueryPlanStep & step, std::vector<SymbolEquivalencesPtr> & c) override;
    SymbolEquivalencesPtr visitJoinStepExt(const JoinStepExt & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitFilterStepExt(const FilterStepExt & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitProjectionStepExt(const ProjectionStepExt & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitAggregatingStepExt(const AggregatingStepExt & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitExchangeStepExt(const ExchangeStepExt & step, std::vector<SymbolEquivalencesPtr> & context) override;
SymbolEquivalencesPtr visitCTERefStepExt(const CTERefStepExt & step, std::vector<SymbolEquivalencesPtr> & context) override;
};
}
