#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
/*
#include <Query/Common/SymbolsExtractor.h>
#include <Query/Optimizer/Utils.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Query/Processors/QueryPlan/CTEInfo.h>
#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/MarkDistinctStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/SymbolMapper.h>
*/

#include <functional>
#include <memory>
#include <unordered_map>

namespace DB
{

class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;

struct PlanNodeAndMappings
{
    PlanNodePtr plan_node;
    NameToNameMap mappings;
};

class PlanSymbolReallocator
{
public:
    /*
     * unalias symbol references that are just aliases of each other.
     * an projection will append to guarante output symbols.
     */
    static PlanNodePtr unalias(const PlanNodePtr & plan, ContextMutablePtr & context)
    {
        //todo: need impl, now just a fake
        return nullptr;
    }

    /* 
     * deep copy a plan with symbol reallocated. 
     * symbol_mapping return all symbol reallocated mapping.
     * Note: output symbol names and order may changed.
     */
    static PlanNodeAndMappings reallocate(const PlanNodePtr & plan, ContextMutablePtr & context)
    {
        //todo: need impl, now just a fake
        PlanNodePtr plan_node;
        NameToNameMap mappings;
        return {plan_node, mappings};
    }

    /* check output stream is overlapping */
    static bool isOverlapping(const DataStream & lho, const DataStream & rho)
    {
        //todo: need impl, now just a fake
        return false;
    }
};

}
