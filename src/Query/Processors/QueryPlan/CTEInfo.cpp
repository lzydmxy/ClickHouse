#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <Query/Processors/QueryPlan/CTEVisitHelper.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Common/Void.h>
#include <Common/Exception.h>
#include <Query/Processors/QueryPlan/PlanNode.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

class CTEInfo::ReferenceCountsVisitor : public PlanNodeVisitor<Void, std::unordered_map<CTEId, UInt64>>
{
public:
    explicit ReferenceCountsVisitor(CTEInfo & cte_info_) : cte_helper(cte_info_) { }

    Void visitPlanNode(PlanNodeBase & node, std::unordered_map<CTEId, UInt64> & c) override
    {
        for (auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, c);
        return Void{};
    }

    Void visitCTERefStepExtNode(CTERefStepExtNode & node, std::unordered_map<CTEId, UInt64> & reference_counts) override
    {
        const auto * cte_step = dynamic_cast<const CTERefStepExt *>(node.getStep().get());
        auto cte_id = cte_step->getId();
        ++reference_counts[cte_id];
        cte_helper.accept(cte_id, *this, reference_counts);
        return Void{};
    }

private:
    SimpleCTEVisitHelper<void> cte_helper;
};

std::unordered_map<CTEId, UInt64> CTEInfo::collectCTEReferenceCounts(PlanNodePtr & root)
{
    ReferenceCountsVisitor visitor{*this};
    std::unordered_map<CTEId, UInt64> reference_counts;
    VisitorUtil::accept(*root, visitor, reference_counts);
    return reference_counts;
}

std::set<CTEId> CTEInfo::getCTEIds() const
{
    std::set<CTEId> cte_ids;
    for (const auto & item : common_table_expressions)
        cte_ids.emplace(item.first);
    return cte_ids;
}

void CTEInfo::update(CTEId id, PlanNodePtr plan)
{
    if (!contains(id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CTE {} don't exists", id);

    common_table_expressions[id] = std::move(plan);
}

void CTEInfo::add(CTEId id, PlanNodePtr plan)
{
    if (contains(id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CTE {} already exists", id);

    common_table_expressions[id] = std::move(plan);
    next_cte_id = std::max(id, next_cte_id);
}

}
