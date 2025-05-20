#include <Query/Processors/QueryPlan/PlanPrinter.h>
#include <Query/Common/PlanSegmentProfile.h>

namespace DB
{

Poco::JSON::Object::Ptr PlanSegmentDescription::jsonPlanSegmentDescription(const StepProfiles & profiles, bool is_pipeline)
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);

    auto f = [](RExchangeMode::Enum xchg_mode) {
        switch (xchg_mode)
        {
            case RExchangeMode::LOCAL_NO_NEED_REPARTITION:
                return "LOCAL_NO_NEED_REPARTITION";
            case RExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                return "LOCAL_MAY_NEED_REPARTITION";
            case RExchangeMode::BROADCAST:
                return "BROADCAST";
            case RExchangeMode::REPARTITION:
                return "REPARTITION";
            case RExchangeMode::GATHER:
                return "GATHER";
            default:
                return "UNKNOWN";
        }
    };

    json->set("SegmentID", segment_id);
    json->set("SegmentType", segment_type);
    String exchange = (segment_id == 0) ? "Output" : f(mode);
    json->set("OutputExchangeMode", exchange);
    if (exchange == "REPARTITION") // print shuffle keys
    {
        Poco::JSON::Array keys;
        for (auto & key : shuffle_keys)
            keys.add(key);
        json->set("ShuffleKeys", keys);
    }
    json->set("ParallelSize", parallel);
    json->set("ClusterName", (cluster_name.empty() ? "server" : cluster_name));
    json->set("ExchangeParallelSize", exchange_parallel_size);
    if (!output_columns.empty())
    {
        Poco::JSON::Array output_array;
        for (const auto & column : output_columns)
            output_array.add(column);
        json->set("OutputColumns", output_array);
    }

    if (!outputs_desc.empty())
    {
        Poco::JSON::Array outputs;
        for (auto & output : outputs_desc)
        {
            Poco::JSON::Object::Ptr output_json = new Poco::JSON::Object(true);
            output_json->set("SegmentID", output->segment_id);
            output_json->set("PlanSegmentType",output->plan_segment_type);
            output_json->set("ExchangeId", output->exchange_id);
            output_json->set("ExchangeMode", f(output->mode));
            output_json->set("ParallelSize", output->parallel_size);
            output_json->set("KeepOrder", output->keep_order);
            outputs.add(output_json);
        }
        json->set("Outputs", outputs);
    }

    if (!inputs_desc.empty())
    {
        Poco::JSON::Array inputs;
        for (auto & input : inputs_desc)
        {
            Poco::JSON::Object::Ptr input_json = new Poco::JSON::Object(true);
            input_json->set("SegmentID", input->segment_id);
            input_json->set("ExchangeId", input->exchange_id);
            input_json->set("ExchangeMode", f(input->mode));
            input_json->set("ExchangeParallelSize", input->exchange_parallel_size);
            input_json->set("KeepOrder", input->keep_order);
            inputs.add(input_json);
        }
        json->set("Inputs", inputs);
    }

    if (node_description && !is_pipeline)
        json->set("QueryPlan", node_description->jsonNodeDescription(profiles, false));
    return json;
}

String PlanSegmentDescription::jsonPlanSegmentDescriptionAsString(const StepProfiles & profiles)
{
    auto json = jsonPlanSegmentDescription(profiles);
    std::ostringstream os;
    json->stringify(os, 1);
    return os.str();
}

Poco::JSON::Object::Ptr NodeDescription::jsonNodeDescription(const StepProfiles & node_profiles, bool print_stats, const PlanCostMap & costs)
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(true);
    json->set("NodeId", node_id);
    json->set("NodeType", step_name);
    for (auto & detail : step_detail)
        json->set(detail.first, detail.second);
    for (auto & vector_detail : step_vector_detail)
    {
        Poco::JSON::Array details;
        for (const auto& item : vector_detail.second)
            details.add(item);
        json->set(vector_detail.first, details);
    }

    if (stats.has_value() && print_stats)
    {
        Poco::JSON::Object::Ptr stats_json = new Poco::JSON::Object(true);
        stats_json->set("RowCount", stats.value().row_count);
        json->set("Statistic", stats_json);
    }

    if (node_profiles.contains(node_id))
    {
        const auto & profile_detail = node_profiles.at(node_id);
        Poco::JSON::Object::Ptr profiles = new Poco::JSON::Object(true);
        profiles->set("WallTimeMs", float(profile_detail->sum_elapsed_us)/profile_detail->worker_cnt/1000);
        profiles->set("MaxWallTimeMs", float(profile_detail->max_elapsed_us)/1000);
        profiles->set("MinWallTimeMs", float(profile_detail->min_elapsed_us)/1000);
        profiles->set("OutputRows", profile_detail->output_rows);
        profiles->set("OutputBytes", profile_detail->output_bytes);
        profiles->set("OutputWaitTimeMs", float(profile_detail->output_wait_sum_elapsed_us)/profile_detail->worker_cnt/1000);
        profiles->set("MaxOutputWaitTimeMs", float(profile_detail->output_wait_max_elapsed_us)/1000);
        profiles->set("MinOutputWaitTimeMs", float(profile_detail->output_wait_min_elapsed_us)/1000);
        Poco::JSON::Array inputs_profile;
        if (!children.empty() && profile_detail->inputs.contains(children[0]->node_id))
        {
            for (auto & child : children)
            {
                auto input_profile = profile_detail->inputs[child->node_id];
                Poco::JSON::Object::Ptr input = new Poco::JSON::Object(true);
                input->set("InputNodeId", child->node_id);
                input->set("InputRows", input_profile.input_rows);
                input->set("InputBytes", input_profile.input_bytes);
                input->set("InputWaitTimeMs", float(input_profile.input_wait_sum_elapsed_us)/profile_detail->worker_cnt/1000);
                input->set("MaxInputWaitTimeMs", float(input_profile.input_wait_max_elapsed_us)/1000);
                input->set("MinInputWaitTimeMs", float(input_profile.input_wait_min_elapsed_us)/1000);
                inputs_profile.add(input);
            }
        }
        else
        {
            for (auto input_profile : profile_detail->inputs)
            {
                Poco::JSON::Object::Ptr input = new Poco::JSON::Object(true);
                input->set("InputNodeId", input_profile.first);
                input->set("InputRows", input_profile.second.input_rows);
                input->set("InputBytes", input_profile.second.input_bytes);
                input->set("InputWaitTimeMs", float(input_profile.second.input_wait_sum_elapsed_us)/profile_detail->worker_cnt/1000);
                input->set("MaxInputWaitTimeMs", float(input_profile.second.input_wait_max_elapsed_us)/1000);
                input->set("MinInputWaitTimeMs", float(input_profile.second.input_wait_min_elapsed_us)/1000);
                inputs_profile.add(input);
            }
        }
        profiles->set("Inputs", inputs_profile);

        double filtered = 0.0;
        if (children.size() > 1)
        {
            size_t max_input_rows = 0;
            for (const auto & child : children)
            {
                if (!node_profiles.contains(child->node_id))
                    continue;
                max_input_rows = std::max(max_input_rows, node_profiles.at(child->node_id)->output_rows);
            }
            if (max_input_rows != 0)
            {
                double max_rows = static_cast<double>(max_input_rows);
                filtered = max_rows > 0 ? ((max_rows - static_cast<double>(profile_detail->output_rows)) * static_cast<double>(100) / max_rows) : 0;
            }
        }
        else if (children.size() == 1)
        {
            if (node_profiles.contains(children[0]->node_id))
            {
                auto child_input_rows = static_cast<double>(node_profiles.at(children[0]->node_id)->output_rows);
                filtered = child_input_rows > 0 ? ((child_input_rows - static_cast<double>(profile_detail->output_rows)) * static_cast<double>(100) / child_input_rows) : 0;
            }
        }
        profiles->set("FilteredRate", filtered);
        json->set("Profiles", profiles);
    }

    if (!descriptions_in_step.empty())
    {
        Poco::JSON::Object::Ptr descriptions = new Poco::JSON::Object(true);
        for (auto & desc : descriptions_in_step)
            descriptions->set(desc.first, desc.second->jsonNodeDescription(node_profiles, print_stats, costs));
        json->set("StepDescriptions", descriptions);
    }

    Poco::JSON::Array children_array;
    for (auto & child : children)
        children_array.add(child->jsonNodeDescription(node_profiles, print_stats, costs));

    if (!children.empty())
        json->set("Children", children_array);
    return json;
}


}

