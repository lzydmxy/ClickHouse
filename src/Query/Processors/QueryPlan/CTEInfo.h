#pragma once

#include <Core/Types.h>

#include <set>

namespace DB
{

using CTEId = UInt32;

class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;

class CTEInfo
{
public:
    PlanNodePtr & getCTEDef(CTEId cte_id) { return common_table_expressions.at(cte_id); }

    bool contains(CTEId cte_id) const { return common_table_expressions.contains(cte_id); }

    const std::unordered_map<CTEId, PlanNodePtr> & getCTEs() const { return common_table_expressions; }
    std::unordered_map<CTEId, PlanNodePtr> & getCTEs() { return common_table_expressions; }

    void add(CTEId id, PlanNodePtr plan)
    {
        //todo: now just a fake impl for build
        return;
    }

    void update(CTEId id, PlanNodePtr plan);

    bool empty() const { return common_table_expressions.empty(); }

    size_t size() const { return common_table_expressions.size(); }

    void clear() { common_table_expressions.clear(); }

    std::unordered_map<CTEId, UInt64> collectCTEReferenceCounts(PlanNodePtr & root);

    std::set<CTEId> getCTEIds() const
    {
        //todo: now just a fake impl for build
        std::set<CTEId> cte_ids;
        return cte_ids;
    }

    CTEId nextCTEId() { return ++next_cte_id; }

private:
    std::unordered_map<CTEId, PlanNodePtr> common_table_expressions;

    CTEId next_cte_id = 0;

    class ReferenceCountsVisitor;
};

}
