#pragma once

#include <Query/Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Processors/QueryPlan/QueryPlanExt.h>

#include <memory>
#include <utility>
#include <vector>

namespace DB
{
struct PartitionCheckResult
{
    StoragePtr depend_storage;
    size_t unique_id = 0;
    ASTPtr union_query_partition_predicate;
    ASTPtr mview_partition_predicate;
};

std::optional<PartitionCheckResult>
checkMaterializedViewPartitionConsistency(MaterializedViewStructurePtr structure, ContextMutablePtr context);
}
