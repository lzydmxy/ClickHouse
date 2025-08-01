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

IMPLEMENT_SETTING_ENUM(ExpandMode, ErrorCodes::BAD_ARGUMENTS,
    {{"EXPAND", ExpandMode::EXPAND},
     {"UNION", ExpandMode::UNION},
     {"CTE", ExpandMode::CTE}})

IMPLEMENT_SETTING_ENUM(CTEMode, ErrorCodes::BAD_ARGUMENTS,
    {{"INLINED", CTEMode::INLINED},
     {"SHARED", CTEMode::SHARED},
     {"AUTO", CTEMode::AUTO},
     {"ENFORCED", CTEMode::ENFORCED}})

IMPLEMENT_SETTING_ENUM(SpillMode, ErrorCodes::BAD_ARGUMENTS,
    {{"manual", SpillMode::MANUAL},
     {"auto", SpillMode::AUTO}})

IMPLEMENT_SETTING_ENUM(DialectType, ErrorCodes::BAD_ARGUMENTS,
    {{"CLICKHOUSE", DialectType::CLICKHOUSE},
     {"ANSI",       DialectType::ANSI},
     {"MYSQL",      DialectType::MYSQL}})

IMPLEMENT_SETTING_ENUM(StatisticsAccurateSampleNdvMode, ErrorCodes::BAD_ARGUMENTS,
    {{"NEVER", StatisticsAccurateSampleNdvMode::NEVER},
     {"AUTO", StatisticsAccurateSampleNdvMode::AUTO},
     {"ALWAYS", StatisticsAccurateSampleNdvMode::ALWAYS}})

IMPLEMENT_SETTING_ENUM(StatisticsCachePolicy, ErrorCodes::BAD_ARGUMENTS,
    {{"default", StatisticsCachePolicy::Default},
     {"cache", StatisticsCachePolicy::Cache},
     {"catalog", StatisticsCachePolicy::Catalog}})

IMPLEMENT_SETTING_ENUM(MaterializedViewConsistencyCheckMethod, ErrorCodes::BAD_ARGUMENTS,
    {{"NONE", MaterializedViewConsistencyCheckMethod::NONE},
     {"PARTITION", MaterializedViewConsistencyCheckMethod::PARTITION}})


// config & settings examples
// conf/config.xml
// <optimizer>
//     <rpc_port>8106</rpc_port>
// </optimizer>
void OptimizerSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
        {
            if (key == "rpc_port" || key == "statistics_path")
                continue;
            LOG_DEBUG(getLogger("OptiminzerSettings"), "Load settings item {}.{} from config", config_elem, key);
            set(key, config.getString(config_elem + "." + key));
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in Optimizer settings config");
        throw;
    }

    LOG_DEBUG(getLogger("OptiminzerSettings"), "Load settings {} from config", config_elem);
}

}
