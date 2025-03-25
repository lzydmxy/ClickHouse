#include "OptimizerSettings.h"
#include <Common/logger_useful.h>
//#include <Coordination/Defines.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteIntText.h>
#include "config.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTINGS_TRAITS(OptimizerSettingsTraits, LIST_OF_OPTIMIZER_SETTINGS)

IMPLEMENT_SETTING_ENUM(SchedulerMode, ErrorCodes::BAD_ARGUMENTS, {
    {"random", SchedulerMode::RANDOM},
    {"first_order", SchedulerMode::FIRST_ORDER},
    {"random_order", SchedulerMode::RANDOM_ORDER},
    {"cpu_rank", SchedulerMode::CPU_RANK},
    {"memory_rank", SchedulerMode::MEMORY_RANK}
})

IMPLEMENT_SETTING_ENUM(QueryDryRunMode, ErrorCodes::BAD_ARGUMENTS,
    {{"none", QueryDryRunMode::NONE},
     {"skip_send_parts", QueryDryRunMode::SKIP_SEND_PARTS},
     {"skip_read_parts", QueryDryRunMode::SKIP_READ_PARTS},
     {"skip_execute_segment", QueryDryRunMode::SKIP_EXECUTE_SEGMENT},
     {"skip_execute_query", QueryDryRunMode::SKIP_EXECUTE_QUERY}})

void OptimizerSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in Optimizer settings config");
        throw;
    }
}

}
