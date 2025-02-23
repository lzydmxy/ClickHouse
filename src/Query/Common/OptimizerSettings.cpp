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
}

IMPLEMENT_SETTINGS_TRAITS(OptimizerSettingsTraits, LIST_OF_COORDINATION_SETTINGS)

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
