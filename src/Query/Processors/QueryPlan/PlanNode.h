#pragma once

#include <Core/Types.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
//#include <Query/Optimizer/CardinalityEstimate/PlanNodeStatisticsEstimate.h>
#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Processors/IQueryPlanStepExt.h>

namespace DB
{

template <class Step>
class PlanNode;

class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;
using PlanNodes = std::vector<PlanNodePtr>;

using QueryPlanStepPtr = std::shared_ptr<IQueryPlanStep>;
using PlanNodeId = UInt32;

class PlanNodeBase : public std::enable_shared_from_this<PlanNodeBase>
{
public:
    PlanNodeBase(PlanNodeId id_, PlanNodes children_) : id(id_), children(std::move(children_)) { }
    virtual ~PlanNodeBase() = default;
    PlanNodeId getId() const { return id; }

    PlanNodes & getChildren() { return children; }
    const PlanNodes & getChildren() const { return children; }
    void replaceChildren(const PlanNodes & children_) { replaceChildrenImpl(children_); }
    // void setStatistics(const PlanNodeStatisticsEstimate & statistics_) { statistics = statistics_; }
    // const PlanNodeStatisticsEstimate & getStatistics() const { return statistics; }
    QueryPlanStepPtr getStep() const { return getStepImpl(); }
    void setStep(QueryPlanStepPtr & step_) { setStepImpl(step_); }


    virtual PlanNodePtr addStep(PlanNodeId new_id, QueryPlanStepPtr new_step, PlanNodes new_children) = 0;
    virtual PlanNodePtr copy(PlanNodeId new_id, ContextPtr context) = 0;
    virtual QueryPlanStepType getType() const = 0;
    virtual const DataStream & getCurrentDataStream() const = 0;

    NamesAndTypes getOutputNamesAndTypes() const { return getCurrentDataStream().header.getNamesAndTypes(); }
    // TODO: implement
    // NameToType getOutputNamesToTypes() const { return getCurrentDataStream().header.getNamesToTypes(); }
    Names getOutputNames() const { return getCurrentDataStream().header.getNames(); }
    PlanNodePtr getNodeById(PlanNodeId node_id) const;

    static PlanNodePtr createPlanNode(
        [[maybe_unused]] PlanNodeId id_, [[maybe_unused]] QueryPlanStepPtr step_, [[maybe_unused]] const PlanNodes & children_ = {}
        // [[maybe_unused]] const PlanNodeStatisticsEstimate & statistics_ = {}
    )
    {

        //if (step_->getType() == IQueryPlanStep::Type::TYPE)

        PlanNodePtr plan_node;
#define CREATE_PLAN_NODE(TYPE) \
    if (getQueryPlanStepType(step_) == QueryPlanStepType::TYPE) \
    { \
        auto spec_step = std::dynamic_pointer_cast<TYPE>(step_); \
        if (!spec_step) \
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type cast failed for {}", #TYPE); \
        plan_node = std::dynamic_pointer_cast<PlanNodeBase>(std::make_shared<PlanNode<TYPE>>(id_, std::move(spec_step), children_)); \
    }

        APPLY_QUERY_PLAN_STEP_TYPES(CREATE_PLAN_NODE)
        // CREATE_PLAN_NODE(Any)
        // CREATE_PLAN_NODE(MultiJoin)
#undef CREATE_PLAN_NODE
        //todo: need optimizer Statistics
        //plan_node->setStatistics(statistics_);
        return plan_node;
    }

protected:
    PlanNodeId id;
    PlanNodes children;
    //todo: need optimizer Statistics
    //PlanNodeStatisticsEstimate statistics;

private:
    virtual QueryPlanStepPtr getStepImpl() const = 0;
    virtual void setStepImpl(QueryPlanStepPtr & step_) = 0;
    virtual void replaceChildrenImpl(const PlanNodes & children_) = 0;
};

template <class Step>
class PlanNode : public PlanNodeBase
{
public:
    using StepPtr = std::shared_ptr<Step>;
    PlanNode(const PlanNode &) = delete;
    PlanNode(const PlanNode &&) = delete;
    PlanNode(PlanNode &&) = delete;
    PlanNode & operator=(const PlanNode &) = delete;
    PlanNode & operator=(PlanNode &&) = delete;

    QueryPlanStepType getType() const override { return step->getType(); }
    StepPtr & getStep() { return step; }

    void setStep(StepPtr & step_) { step = step_; }
    const DataStream & getCurrentDataStream() const override { return step->getOutputStream(); }

    //todo: need optimizer Statistics
    //static PlanNodePtr createPlanNode(PlanNodeId id_, StepPtr step_, const PlanNodes & children_ = {}, const PlanNodeStatisticsEstimate & statistics_ = {})
    static PlanNodePtr createPlanNode(PlanNodeId id_, StepPtr step_, const PlanNodes & children_ = {})
    {
        PlanNodePtr plan_node = std::make_shared<PlanNode<Step>>(id_, std::move(step_), children_);
        ////todo: need optimizer Statistics
        //plan_node->setStatistics(statistics_);
        return plan_node;
    }


    PlanNodePtr copy(PlanNodeId new_id, ContextPtr context) override
    {
        auto new_step = dynamic_pointer_cast<Step>(step->copy(context));
        if (!new_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to copy step with type mismatch");
        //return createPlanNode(new_id, std::move(new_step), children, statistics);
        return createPlanNode(new_id, std::move(new_step), children);
    }

    PlanNodePtr addStep(PlanNodeId new_id, QueryPlanStepPtr new_step, PlanNodes new_children) override
    {
        if (new_children.empty() && new_step->getInputStreams().size() == 1)
        {
            new_children.emplace_back(this->shared_from_this());
        }
        else if (children.size() != step->getInputStreams().size())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected {} children, but input arguments have {}",
                std::to_string(step->getInputStreams().size()),
                std::to_string(children.size()));
        }
        return PlanNodeBase::createPlanNode(new_id, std::move(new_step), new_children);
    }

    PlanNode(PlanNodeId id_, StepPtr step_, PlanNodes children_ = {}) : PlanNodeBase(id_, children_), step(std::move(step_)) { }

private:
    QueryPlanStepPtr getStepImpl() const override { return step; }

    void replaceChildrenImpl(const PlanNodes & children_) override
    {
        children = children_;

        DataStreams inputs;
        for (const auto & child : children)
            inputs.emplace_back(child->getCurrentDataStream());

        getStep()->setInputStreams(inputs);
    }

    void setStepImpl(QueryPlanStepPtr & step_) override
    {
        auto new_step = std::dynamic_pointer_cast<Step>(step_);
        if (new_step)
            step = new_step;
    }

    StepPtr step;
};

/*
class TableScan;
class TableWrite;
class CTERef
{

};
extern template class PlanNode<TableScan>;
using TableScanNode = PlanNode<TableScan>;

extern template class PlanNode<TableWrite>;
using TableWriteNode = PlanNode<TableWrite>;

extern template class PlanNode<CTERef>;
using CTERefNode = PlanNode<CTERef>;
*/

#define PLAN_NODE_DEF(TYPE) \
    extern template class PlanNode<TYPE>; \
    using TYPE##Node = PlanNode<TYPE>;

APPLY_QUERY_PLAN_STEP_TYPES(PLAN_NODE_DEF)
// PLAN_NODE_DEF(Any)
// PLAN_NODE_DEF(MultiJoin)
#undef PLAN_NODE_DEF

}
