#pragma once
#include <Query/Processors/QueryPlan/PlanVisitor.h>

namespace DB
{
class PlanNodeCardinality
{
public:
    struct Range
    {
        Range(size_t lowerBound_, size_t upperBound_) : lower_bound(lowerBound_), upper_bound(upperBound_) { }
        size_t lower_bound;
        size_t upper_bound;
    };

    static bool isScalar(PlanNodeBase & node) { return isScalar(extractCardinality(node)); }
    static bool isEmpty(PlanNodeBase & node) { return isEmpty(extractCardinality(node)); }
    static bool isAtMost(PlanNodeBase & node, size_t maxCardinality) { return extractCardinality(node).upper_bound < maxCardinality; }
    static bool isAtLeast(PlanNodeBase & node, size_t minCardinality) { return extractCardinality(node).lower_bound > minCardinality; }
    static Range extractCardinality(PlanNodeBase & node);

private:
    static inline bool isScalar(const Range & range) { return range.lower_bound == 1 && range.upper_bound == 1; }
    static inline bool isEmpty(const Range & range) { return range.lower_bound == 0 && range.upper_bound == 0; }
    static Range intersection(const Range & range, const Range & other)
    {
        return Range{std::max(range.lower_bound, other.lower_bound), std::min(range.lower_bound, other.upper_bound)};
    }

    class Visitor;
};

}
