#pragma once

#include <Query/Optimizer/Property/Property.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/TotalsHavingStepExt.h>

#include <utility>

namespace DB
{
class PropertyDeterminer
{
public:
    static PropertySets determineRequiredProperty(QueryPlanStepPtr step, const Property & property, Context & context, int worker_size = -1 /*-1 means not known*/);
};

class DeterminerContext
{
public:
    DeterminerContext(Property required_, Context & context_) : required(required_), context(context_) { }
    Property getRequired() const { return required; }
    Context & getContext() const { return context; }

private:
    Property required;
    Context & context;
};

class DeterminerVisitor : public StepVisitor<PropertySets, DeterminerContext>
{
public:
    PropertySets visitStep(const IQueryPlanStep &, DeterminerContext &) override;

#define VISITOR_DEF(TYPE) PropertySets visit##TYPE(const TYPE &, DeterminerContext &) override;
    APPLY_PROTOBUF_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

private:
    static PropertySet single()
    {
        return {Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}};
    }
};

}
