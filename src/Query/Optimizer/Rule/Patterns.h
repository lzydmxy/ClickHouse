#pragma once

#include <Query/Optimizer/Rule/Pattern.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <sstream>

namespace DB::Patterns
{

template <typename F, typename Step>
concept CallableWithStep = requires(F&& f, Step* s) {
    { f(*s) };
};

class PatternBuilder
{
public:
    explicit PatternBuilder(PatternPtr init): current(std::move(init)) {}
    PatternPtr result() const { return std::move(current); }

    PatternBuilder & capturedAs(const Capture & capture);
    PatternBuilder & capturedAs(const Capture & capture, const PatternProperty & property);
    PatternBuilder & capturedAs(const Capture & capture, const PatternProperty & property, const std::string & name);
    template <typename T>
    PatternBuilder & capturedStepAs(const Capture & capture, const std::function<std::any(const T &)> & step_property)
    {
        return capturedStepAs(capture, step_property, "unknown");
    }
    template <typename T>
    PatternBuilder & capturedStepAs(const Capture & capture, const std::function<std::any(const T &)> & step_property, const std::string & name)
    {
        static_assert(std::is_base_of<IQueryPlanStep, T>::value, "T must inherit from IQueryPlanStep");

        PatternProperty property = [step_property](const PlanNodePtr & node) -> std::any {
            auto * step = dynamic_cast<const T *>(node->getStep().get());

            if (!step)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected plan step found in pattern matching");

            return step_property(*step);
        };
        return capturedAs(capture, property, name);
    }
    PatternBuilder & matching(PatternPredicate predicate);
    PatternBuilder & matching(PatternPredicate predicate, const std::string & name);
    PatternBuilder & matchingCapture(std::function<bool(const Captures &)> capture_predicate);
    PatternBuilder & matchingCapture(std::function<bool(const Captures &)> capture_predicate, const std::string & name);
    template <typename T, typename F>
    PatternBuilder & matchingStep(F step_predicate) {
        return matchingStep<T>(std::move(step_predicate), "unknown");
    }

    template <typename T, typename F>
    PatternBuilder & matchingStep(F step_predicate, const std::string & name) {
        static_assert(std::is_base_of<const IQueryPlanStep, T>::value, "T must inherit from const IQueryPlanStep");
        PatternPredicate predicate = [step_predicate = std::move(step_predicate)](const QueryPlanStepPtr & istep, Captures & captures) -> bool {
            auto * step = dynamic_cast<const T *>(istep.get());
            if (!step)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected plan step found in pattern matching");

            if constexpr (CallableWithStep<decltype(step_predicate), T>)
                return step_predicate(*step);
            else
                return step_predicate(*step, captures);
        };
        return matching(std::move(predicate), name);
    }

    PatternBuilder & withEmpty();
    PatternBuilder & withSingle(const PatternBuilder & sub_builder) { return withSingle(sub_builder.result());}
    PatternBuilder & withAny(const PatternBuilder & sub_builder) { return withAny(sub_builder.result());}
    PatternBuilder & withAll(const PatternBuilder & sub_builder) { return withAll(sub_builder.result());}
    template <typename ... T>
    PatternBuilder & with(const T &... sub_builders)
    {
        PatternPtrs sub_patterns;
        ( (sub_patterns.emplace_back(sub_builders.result())), ...);
        return with(std::move(sub_patterns));
    }

    template <typename... T>
    PatternBuilder & oneOf(const T &... sub_builders)
    {
        PatternPtrs sub_patterns;
        ((sub_patterns.emplace_back(sub_builders.result())), ...);
        return oneOf(std::move(sub_patterns));
    }

private:
    PatternBuilder & withSingle(PatternPtr sub_pattern);
    PatternBuilder & withAny(PatternPtr sub_pattern);
    PatternBuilder & withAll(PatternPtr sub_pattern);
    PatternBuilder & with(PatternPtrs sub_patterns);
    PatternBuilder & oneOf(PatternPtrs sub_patterns);

    mutable PatternPtr current;
};

// typeOf patterns
inline PatternBuilder typeOf(QueryPlanStepType type) { return PatternBuilder(std::make_unique<TypeOfPattern>(type)); }
inline PatternBuilder any() { return typeOf(QueryPlanStepType::AnyStepExt); }
inline PatternBuilder tree() { return typeOf(QueryPlanStepType::Tree); }

inline PatternBuilder project() { return typeOf(QueryPlanStepType::ProjectionStepExt); }
inline PatternBuilder filter() { return typeOf(QueryPlanStepType::FilterStepExt); }
inline PatternBuilder join() { return typeOf(QueryPlanStepType::JoinStepExt); }
inline PatternBuilder multiJoin() { return typeOf(QueryPlanStepType::MultiJoinStepExt); }
inline PatternBuilder aggregating() { return typeOf(QueryPlanStepType::AggregatingStepExt); }
inline PatternBuilder window() { return typeOf(QueryPlanStepType::WindowStep); }
inline PatternBuilder mergingAggregated() { return typeOf(QueryPlanStepType::MergingAggregatedStepExt); }
inline PatternBuilder unionn() { return typeOf(QueryPlanStepType::UnionStepExt); }
inline PatternBuilder intersect() { return typeOf(QueryPlanStepType::IntersectStepExt); }
inline PatternBuilder except() { return typeOf(QueryPlanStepType::ExceptStepExt); }
inline PatternBuilder exchange() { return typeOf(QueryPlanStepType::ExchangeStepExt); }
inline PatternBuilder remoteSource() { return typeOf(QueryPlanStepType::RemoteExchangeSourceStepExt); }
inline PatternBuilder tableScan() { return typeOf(QueryPlanStepType::TableScanStepExt); }
inline PatternBuilder readNothing() { return typeOf(QueryPlanStepType::ReadNothingStep); }
inline PatternBuilder limit() { return typeOf(QueryPlanStepType::LimitStepExt); }
inline PatternBuilder limitBy() { return typeOf(QueryPlanStepType::LimitByStep); }
inline PatternBuilder sorting() { return typeOf(QueryPlanStepType::SortingStepExt); }
inline PatternBuilder mergeSorting() { return typeOf(QueryPlanStepType::MergeSortingStepExt); }
inline PatternBuilder partialSorting() { return typeOf(QueryPlanStepType::PartialSortingStepExt); }
inline PatternBuilder mergingSorted() { return typeOf(QueryPlanStepType::MergingSortedStepExt); }
inline PatternBuilder distinct() { return typeOf(QueryPlanStepType::DistinctStepExt); }
inline PatternBuilder extremes() { return typeOf(QueryPlanStepType::ExtremesStep); }
inline PatternBuilder apply() { return typeOf(QueryPlanStepType::ApplyStepExt); }
inline PatternBuilder enforceSingleRow() { return typeOf(QueryPlanStepType::EnforceSingleRowStepExt); }
inline PatternBuilder assignUniqueId() { return typeOf(QueryPlanStepType::AssignUniqueIdStepExt); }
inline PatternBuilder cte() { return typeOf(QueryPlanStepType::CTERefStepExt); }
inline PatternBuilder buffer() { return typeOf(QueryPlanStepType::BufferStepExt); }
PatternBuilder topN();
inline PatternBuilder topNFiltering() { return typeOf(QueryPlanStepType::TopNFilteringStepExt); }
inline PatternBuilder explainAnalyze() { return typeOf(QueryPlanStepType::ExplainAnalyzeStepExt); }

template <typename... T>
PatternBuilder oneOf(const T &... sub_builders)
{
    PatternPtrs sub_patterns;
    ((sub_patterns.emplace_back(sub_builders.result())), ...);
    return PatternBuilder(std::make_unique<OneOfPattern>(std::move(sub_patterns)));
}

// miscellaneous
inline PatternPredicate predicateNot(const PatternPredicate & predicate)
{
    return [=](const QueryPlanStepPtr & node, Captures & captures) -> bool {return !predicate(node, captures);};
}

}
