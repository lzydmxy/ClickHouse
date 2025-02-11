#pragma once

#include <Core/Field.h>
#include <Common/SettingsChanges.h>

namespace DB::Protos
{
class SettingChange;
class SettingsChanges;
}

namespace JDDB
{

using DB::Field;
using DB::WriteBuffer;
using DB::ReadBuffer;

struct SettingChange: public DB::SettingChange
{
    SettingChange() = default;
    SettingChange(DB::SettingChange && setting_change) : DB::SettingChange(std::move(setting_change)) {}
    SettingChange(const DB::SettingChange & setting_change) : DB::SettingChange(setting_change) {}
    void serialize(DB::WriteBuffer & buf) const;
    void deserialize(DB::ReadBuffer & buf);
    void toProto(DB::Protos::SettingChange & proto) const;
    void fillFromProto(const DB::Protos::SettingChange & proto);
};


class SettingsChanges : public std::vector<SettingChange>
{
public:
    using std::vector<SettingChange>::vector;

    bool tryGet(const std::string_view & name, Field & out_value) const;
    const Field * tryGet(const std::string_view & name) const;
    Field * tryGet(const std::string_view & name);

    /// Inserts element if doesn't exists and returns true, otherwise just returns false
    bool insertSetting(std::string_view name, const Field & value);
    /// Sets element to value, inserts if doesn't exist
    void setSetting(std::string_view name, const Field & value);
    /// If element exists - removes it and returns true, otherwise returns false
    bool removeSetting(std::string_view name);

    void merge(const SettingsChanges & other);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
    void toProto(DB::Protos::SettingsChanges & proto) const;
    void fillFromProto(const DB::Protos::SettingsChanges & proto);

    static std::unordered_set<String> WHITELIST_SETTINGS;
};

}
