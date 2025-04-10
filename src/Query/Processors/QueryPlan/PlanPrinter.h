#pragma once

#include <type_traits>

#include <Poco/JSON/Object.h>

#include <Query/Processors/QueryPlan/QueryPlanStepHelper.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Executor/PlanSegment.h>
#include <Query/Common/ProcessorProfile.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
using PlanCostMap = std::unordered_map<PlanNodeId, double>;
struct PlanSegmentDescription;
using PlanSegmentDescriptionPtr = std::shared_ptr<PlanSegmentDescription>;
using PlanSegmentDescriptions = std::vector<PlanSegmentDescriptionPtr>;
struct PlanSegmentProfile;
using PlanSegmentProfilePtr = std::shared_ptr<PlanSegmentProfile>;
using PlanSegmentProfiles = std::vector<PlanSegmentProfilePtr>;

struct Analysis;
using AnalysisPtr = std::shared_ptr<Analysis>;

//todo: need impl PlanPrinter,PlanPrinter::TextPrinter

class TextPrinterIntent
{
public:
    static constexpr auto VERTICAL_LINE = "│  ";
    static constexpr auto INTERMEDIATE_PREFIX = "├─ ";
    static constexpr auto LAST_PREFIX = "└─ ";
    static constexpr auto EMPTY_PREFIX = "   ";

    TextPrinterIntent() = default;
    explicit TextPrinterIntent(size_t prefix, bool has_children_)
        : current_lines_prefix(std::string(prefix, ' '))
        , next_lines_prefix(std::string(prefix, ' '))
        , hasChildren(has_children_)
    {
    }

    TextPrinterIntent forChild(bool last, bool has_children_) const;
    String print() const { return current_lines_prefix; }
    String detailIntent() const;

private:
    TextPrinterIntent(String current_lines_prefix_, String next_lines_prefix_, bool hasChildren);

    String current_lines_prefix;
    String next_lines_prefix;
    bool hasChildren{true};
};

class NodeDescription;
using NodeDescriptionPtr = std::shared_ptr<NodeDescription>;
using NodeDescriptions = std::vector<NodeDescriptionPtr>;

class NodeDescription
{
public:
    size_t node_id;
    QueryPlanStepType type = QueryPlanStepType::AnyStepExt;
    String step_name;
    std::unordered_map<String, String> step_detail;
    std::unordered_map<String, std::vector<String>> step_vector_detail;
    std::unordered_map<String, NodeDescriptionPtr> descriptions_in_step;
    std::vector<NodeDescriptionPtr> children;

    struct StatisticInfo
    {
        size_t row_count = 0;
    };

    std::optional<StatisticInfo> stats;

    void setStepStatistic(PlanNodePtr node);
    void setStepDetail(QueryPlanStepPtr step);
    Poco::JSON::Object::Ptr jsonNodeDescription(const StepProfiles & node_profiles, bool print_stats, const PlanCostMap & costs = {});
    static NodeDescriptionPtr getPlanDescription(QueryPlan::Node * node);
    static NodeDescriptionPtr getPlanDescription(PlanNodePtr node);
};

struct PlanSegmentDescription
{
    struct OutputInfo
    {
        size_t segment_id;
        String plan_segment_type;
        RExchangeMode mode;
        size_t exchange_id;
        size_t parallel_size;
        bool keep_order;
    };
    struct InputInfo
    {
        size_t segment_id;
        RExchangeMode mode;
        size_t exchange_id;
        size_t exchange_parallel_size;
        bool keep_order;
        bool stable;
    };
    size_t segment_id;
    String segment_type;
    String query_id;

    PlanNodeId root_id;
    PlanNodeId root_child_id;
    PlanNodePtr plan_node = nullptr;

    String cluster_name;
    size_t parallel;
    size_t exchange_parallel_size;
    UInt32 shard_num;
    RExchangeMode mode;
    Names shuffle_keys;
    std::unordered_map<PlanNodeId, size_t> exchange_to_segment;
    std::vector<std::shared_ptr<OutputInfo>> outputs_desc;
    std::vector<std::shared_ptr<InputInfo>> inputs_desc;


    std::vector<String> output_columns;

    NodeDescriptionPtr node_description;

    Poco::JSON::Object::Ptr jsonPlanSegmentDescription(const StepProfiles & profiles, bool is_pipeline = false);
    String jsonPlanSegmentDescriptionAsString(const StepProfiles & profiles)
    {
        //todo: now just a fake impl for build, need to impl
        return "";
    }
    static PlanSegmentDescriptionPtr getPlanSegmentDescription(PlanSegmentPtr & segment, bool record_plan_detail = false) { auto plan_segment_desc = std::make_shared<PlanSegmentDescription>(); return plan_segment_desc;}
};

}
