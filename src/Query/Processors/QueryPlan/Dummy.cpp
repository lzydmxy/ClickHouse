#include <Query/Processors/QueryPlan/Dummy.h>

#include <Query/Processors/QueryPlan/ValuesStepExt.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

std::pair<String, PlanNodePtr> createDummyPlanNode(ContextMutablePtr context)
{
    auto symbol = context->getOptimizerContext()->getSymbolAllocator()->newSymbol("dummy");

    ColumnsWithTypeAndName header;
    Fields data;

    header.emplace_back(std::make_shared<DataTypeUInt8>(), symbol);
    data.emplace_back(0U);

    auto values_step = std::make_shared<ValuesStepExt>(header, data);
    auto node = PlanNodeBase::createPlanNode(context->getOptimizerContext()->nextNodeId(), values_step);
    return {std::move(symbol), std::move(node)};
}
}
