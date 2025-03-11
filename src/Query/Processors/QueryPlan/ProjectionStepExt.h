#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Query/Processors/QueryPlan/Assignment.h>

namespace DB
{
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class RuntimeFilterBuilder;
using RuntimeFilterBuilderPtr = std::shared_ptr<RuntimeFilterBuilder>;
using NameToType = std::map<String, DataTypePtr>;

class ProjectionStepExt : public ITransformingStep
{
public:
    explicit ProjectionStepExt(
        const DataStream & input_stream_,
        Assignments assignments_,
        NameToType name_to_type_,
        bool final_project_ = false,
        bool index_project_ = false);

    String getName() const override { return "ProjectionStepExt"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    const Assignments & getAssignments() const { return assignments; }
    const NameToType & getNameToType() const { return name_to_type; }
    bool isFinalProject() const { return final_project; }
    bool isIndexProject() const { return index_project; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;
    ActionsDAGPtr createActions(ContextPtr context) const;

    static ActionsDAGPtr createActions(const Assignments & assignments, const NamesAndTypesList & source, ContextPtr context);

private:
    Assignments assignments;
    NameToType name_to_type;
    // final output step
    bool final_project;
    bool index_project;

    void updateOutputStream() override {};
};

}
