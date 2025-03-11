#pragma once

#include <Core/Types.h>

namespace DB
{

using PlanNodeId = UInt32;

class PlanNodeIdAllocator;
using PlanNodeIdAllocatorPtr = std::shared_ptr<PlanNodeIdAllocator>;

class PlanNodeIdAllocator
{
public:
    PlanNodeIdAllocator() : next_id(1) { }

    explicit PlanNodeIdAllocator(PlanNodeId next_id_) : next_id(next_id_) { }

    PlanNodeId nextId() { return next_id++; }

private:
    PlanNodeId next_id;
};

}
