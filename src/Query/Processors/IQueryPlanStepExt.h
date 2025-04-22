#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

struct RuntimeAttributeDescription
{
    String description;
    std::vector<std::pair<String, String>> name_and_detail;
    // If the attribute information is complex, can use json
    String additional;

    void fillFromProto(const Protos::RuntimeAttributeDescription & proto)
    {
        //todo: hongzhigao, other feat: need impl
        return;
    }

    void toProto(Protos::RuntimeAttributeDescription & proto) const
    {
        //todo: hongzhigao, other feat: need impl
        return;
    }
};

class IQueryPlanStepExt : public IQueryPlanStep
{
public:
    std::unordered_map<String, RuntimeAttributeDescription> & getAttributeDescriptions()
    {
        return attribute_descriptions;
    }
protected:
    /// Text description of runtime attributes
    std::unordered_map<String, RuntimeAttributeDescription> attribute_descriptions;
};

}
