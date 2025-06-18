#pragma once

#include <Query/Optimizer/Property/Property.h>
#include <Query/Processors/QueryPlan/CTEVisitHelper.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>


namespace DB
{
class PropertyDeriver
{
public:
    static Property deriveProperty(QueryPlanStepPtr step, ContextMutablePtr & context, const Property & require, int worker_size = -1 /* -1 means not known*/);
    static Property deriveProperty(PlanNodePtr node, ContextMutablePtr & context, CTEInfo & cte_info, bool ignore_null = true, int worker_size = -1 /* -1 means not known*/);
    static Property deriveProperty(QueryPlanStepPtr step, Property & input_property, const Property & require, ContextMutablePtr & context, int worker_size = -1 /* -1 means not known*/);
    static Property
    deriveProperty(QueryPlanStepPtr step, PropertySet & input_properties, const Property & require, ContextMutablePtr & context, int worker_size = -1 /* -1 means not known*/);
    static Property deriveStorageProperty(const StoragePtr & storage, const Property & require, ContextMutablePtr & context, int worker_size = -1 /* -1 means not known*/);

    static Property
    deriveStoragePropertyWhatIfMode(const StoragePtr & storage, ContextMutablePtr & context, const Property & required_property, int worker_size = -1 /* -1 means not known*/);
};

class DeriverContext
{
public:
    DeriverContext(PropertySet input_properties_, const Property & require_, ContextMutablePtr & context_, bool ignore_null_ = false, int worker_size_ = -1 /* -1 means not known*/)
        : input_properties(std::move(input_properties_)), require(require_), context(context_), ignore_null(ignore_null_), worker_size(worker_size_)
    {
    }
    const PropertySet & getInput() { return input_properties; }
    ContextMutablePtr & getContext() { return context; }
    const Property & getRequire() const { return require; }
    bool isIgnoreNull() const { return ignore_null; }
    int workerSize() const { return worker_size; }

private:
    PropertySet input_properties;
    const Property & require;
    ContextMutablePtr & context;
    bool ignore_null;
    int worker_size;
};

class DeriverVisitor : public StepVisitor<Property, DeriverContext>
{
public:
    Property visitStep(const IQueryPlanStep &, DeriverContext &) override;

#define VISITOR_DEF(TYPE) Property visit##TYPE(const TYPE & step, DeriverContext & context) override;
    APPLY_PROTOBUF_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

class PlanDeriverVisitor : public PlanNodeVisitor<Property, ContextMutablePtr>
{
public:
    PlanDeriverVisitor(CTEInfo & cte_info, bool ignore_null_, int worker_size_) : cte_helper(cte_info), ignore_null(ignore_null_), worker_size(worker_size_) { }

    Property visitPlanNode(PlanNodeBase &, ContextMutablePtr &) override;
    Property visitCTERefStepExtNode(CTERefStepExtNode & node, ContextMutablePtr & context) override;

private:
    SimpleCTEVisitHelper<Property> cte_helper;
    bool ignore_null;
    int worker_size;
};

}
