#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <unordered_map>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{

using String = std::string;

class OptimizerProfile
{
private:
    std::unordered_map<String, String> optimizer_profile_map;
    std::vector<std::pair<String, String>> rule_profile_map;
public:
    void setTime(String name, String time_str, bool is_rule = false)
    {
        if (is_rule)
            rule_profile_map.emplace_back(name, time_str);
        else
            optimizer_profile_map[name] = time_str;
    }

    String getOptimizerProfile(bool print_detail = false)
    {
        std::ostringstream os;

        os << getFormatTime("Optimizer Total")
           << String(2, ' ') << getFormatTime("Rewrite")
           << String(2, ' ') << getFormatTime("Analyzer")
           << String(2, ' ') << getFormatTime("Planning");

        if (!print_detail)
        {
            os << String(2, ' ') << getFormatTime("Optimizer");
        }
        else
        {
            String suffix = " [" + std::to_string(rule_profile_map.size()) + "]" + "\n";
            os << String(2, ' ') << getFormatTime("Optimizer", "-- ", suffix);
            for (auto & item : rule_profile_map)
                os << std::string(4, ' ') << "-- " + item.first + " " + item.second + "\n";
        }

        os << String(2, ' ') << getFormatTime("Plan Normalize")
           << String(2, ' ') << getFormatTime("PlanSegment build");
        return os.str();
    }

    String getFormatTime(String name, String prefix = "-- " , String suffix = "\n")
    {
        if (!optimizer_profile_map.contains(name))
            return prefix + name + suffix;
        return prefix + name + " " + optimizer_profile_map.at(name) + suffix;
    }

    void clear()
    {
        optimizer_profile_map.clear();
        rule_profile_map.clear();
    }

};

}
