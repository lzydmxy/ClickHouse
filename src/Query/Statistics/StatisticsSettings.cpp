#include <Query/Statistics/StatisticsSettings.h>

#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>
#include <Query/Statistics/SettingsMap.h>
#include <Query/Protos/optimizer_statistics.pb.h>

namespace DB::QueryStatistics
{
    void StatisticsSettings::toProto(Protos::StatisticsSettings & proto) const
    {
        serializeMapToProto(manager_settings, *proto.mutable_manager_settings());
        for (const auto & [db, db_map] : scope_settings_list)
        {
            for (const auto & [tb, table_settings] : db_map)
            {
                auto * entry_pb = proto.add_scope_settings_list();
                if (db)
                    entry_pb->set_database(*db);
                if (tb)
                    entry_pb->set_table(*tb);

                entry_pb->set_enable_auto_stats(table_settings.enable_auto_stats);
                ProtosSerDerHelper::toProto(table_settings.settings_changes, *entry_pb->mutable_changes());
            }
        }
    }

    void StatisticsSettings::fillFromProto(const Protos::StatisticsSettings & proto)
    {
        manager_settings = AutoStatsManagerSettings(deserializeMapFromProto<String, String>(proto.manager_settings()));
        for (const auto & entry_pb : proto.scope_settings_list())
        {
            std::optional<std::string> db, tb;
            if (entry_pb.has_database())
                db = entry_pb.database();
            if (entry_pb.has_table())
                tb = entry_pb.table();

            auto & db_map = scope_settings_list[db];
            auto & table_settings = db_map[tb];
            table_settings.enable_auto_stats = entry_pb.enable_auto_stats();
            ProtosSerDerHelper::fillFromProto(table_settings.settings_changes, entry_pb.changes());
        }
    }
}
