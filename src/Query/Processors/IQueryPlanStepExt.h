#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Query/Protos/plan_node.pb.h>
#include <Query/Protos/plan_segment_service.pb.h>

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
        description = proto.description();
        for (const auto & proto_element : proto.details())
        {
            auto name = proto_element.name();
            auto alias = proto_element.alias();
            name_and_detail.emplace_back(name, alias);
        }
        if (proto.additional().empty())
            additional = proto.additional();
    }

    void toProto(Protos::RuntimeAttributeDescription & proto) const
    {
        proto.set_description(description);
        for (const auto & [name, detail] : name_and_detail)
        {
            auto * proto_element = proto.add_details();
            proto_element->set_name(name);
            proto_element->set_alias(detail);
        }
        proto.set_additional(additional);
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

using QueryPlanStepExtPtr = std::shared_ptr<IQueryPlanStepExt>;

}
