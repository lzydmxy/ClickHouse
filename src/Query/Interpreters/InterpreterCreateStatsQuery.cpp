#include <Query/Interpreters/InterpreterCreateStatsQuery.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Query/Parsers/ASTStatsQueryExt.h>
// #include <Query/Protos/optimizer_statistics.pb.h>
#include <Query/Statistics/ASTHelpers.h>
// #include <Query/Statistics/AutoStatsTaskLogHelper.h>
#include <Interpreters/InterpreterFactory.h>
#include <Query/Statistics/CollectTarget.h>
#include <Query/Statistics/StatisticsCollector.h>
#include <Query/Statistics/TypeUtils.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int INCORRECT_DATA;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int QUERY_WAS_CANCELLED;
}

using namespace QueryStatistics;

static Block constructInfoBlock(
    const ContextPtr & context, const String & table_name, UInt64 column_count, const String & row_count_or_error, double time, bool only_header = false)
{
    Block block;
    auto append_str_column = [&](String header, String value) {
        ColumnWithTypeAndName tuple;
        tuple.name = header;
        tuple.type = std::make_shared<DataTypeString>();
        auto col = tuple.type->createColumn();
        if (!only_header)
        {
            col->insertData(value.data(), value.size());
        }
        tuple.column = std::move(col);
        block.insert(std::move(tuple));
    };

    auto append_num_column = [&]<typename T>(String header, T value) {
        static_assert(std::is_trivial_v<T>);
        ColumnWithTypeAndName tuple;
        tuple.name = header;
        tuple.type = std::make_shared<DataTypeNumber<T>>();
        auto col = ColumnVector<T>::create();
        if (!only_header)
        {
            col->insertValue(value);
        }
        tuple.column = std::move(col);
        block.insert(std::move(tuple));
    };

    append_str_column("table_name", table_name);
    append_num_column("column_count", column_count);
    append_str_column("row_count_or_error", row_count_or_error);
    if (context->getOptimizerContext()->getSettingsRef().create_stats_time_output)
    {
        append_num_column("elapsed_time", time);
    }
    return block;
}


namespace
{
    class CreateStatsSource : public ISource, WithContext
    {
    public:
        CreateStatsSource(ContextPtr context_, std::vector<CollectTarget> collect_targets_)
            : ISource(getHeader(context_), false), WithContext(context_), collect_targets(std::move(collect_targets_)), log(getLogger("CreateStatsSource"))
        {
        }

        String getName() const override { return "CreateStatsSource"; }

        static Block getHeader(ContextPtr context_)
        {
            auto header = constructInfoBlock(context_, "", 0, "", 0, true);
            return header;
        }

    private:
        Chunk generate() override
        {
            auto context = getContext();
            Stopwatch watch;
            while (counter < collect_targets.size())
            {
                auto collect_target = collect_targets.at(counter++);
                const auto exception_handler = [&] {
                    auto elapsed_time = watch.elapsedSeconds();
                    auto err_info_with_stack = getCurrentExceptionMessage(true);
                    LOG_ERROR(log, "Error when collecting stats: {}", err_info_with_stack);

                    auto err_info = getCurrentExceptionMessage(false);
                    error_infos.emplace(collect_target.table_identifier.getDbTableName(), err_info_with_stack);

                    auto block = constructInfoBlock(
                        context,
                        collect_target.table_identifier.getTableName(),
                        collect_target.columns_desc.size(),
                        err_info,
                        elapsed_time);
                    return Chunk(block.getColumns(), block.rows());
                };

                try
                {
                    auto row_count_opt = collectStatsOnTarget(context, collect_target);
                    if (!row_count_opt)
                        continue;
                    auto row_count = row_count_opt.value();
                    auto elapsed_time = watch.elapsedSeconds();
                    auto block = constructInfoBlock(
                        context,
                        collect_target.table_identifier.getTableName(),
                        collect_target.columns_desc.size(),
                        std::to_string(row_count),
                        elapsed_time);
                    return Chunk(block.getColumns(), block.rows());
                }
                catch (Poco::Exception & e)
                {
                    if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                    {
                        LOG_INFO(log, "create stats is cancelled");
                        throw;
                    }

                    return exception_handler();
                }
                catch (...)
                {
                    return exception_handler();
                }
            }

            if (error_infos.empty())
            {
                // succeed
                return {};
            }

            // handle errors
            String total_error;
            for (const auto & [k, v] : error_infos)
            {
                total_error += fmt::format(FMT_STRING("when collecting table {} having the following error: {}\n"), k, v);
            }
            throw Exception(ErrorCodes::INCORRECT_DATA, "Create stats failed with errors:\n{}", total_error);
        }

        std::map<String, String> error_infos;
        std::vector<CollectTarget> collect_targets;
        size_t counter = 0;
        LoggerPtr log;
    };
}

// static void submitAsyncTasks(ContextPtr context, const std::vector<CollectTarget> & collect_targets)
// {
//     for (const auto & target : collect_targets)
//     {
//         TaskInfoCore core{
//             .task_uuid = UUIDHelpers::generateV4(),
//             .task_type = TaskType::Manual,
//             .table = target.table_identifier,
//             .settings_json = target.settings.toJsonStr(),
//             .stats_row_count = 0,
//             .udi_count = 0,
//             .priority = 100,
//             .retry_times = 0,
//             .status = Status::Created};
//         if (target.implicit_all_columns)
//             core.columns_name = {};
//         else
//         {
//             core.columns_name.clear();
//             for (const auto & col : target.columns_desc)
//             {
//                 core.columns_name.emplace_back(col.name);
//             }
//         }
//
//         AutoStats::writeTaskLog(context, core);
//     }
// }


CollectorSettings analyzeSettings(const ContextPtr& context, const ASTCreateStatsQueryExt * query)
{
    auto query_settings = context->getOptimizerContext()->getSettings();
    if (query->settings_changes_opt)
    {
        auto settings_changes = query->settings_changes_opt.value();
        applyStatisticsSettingsChanges(query_settings, std::move(settings_changes));
    }

    CollectorSettings settings;
    settings.fromContextSettings(query_settings);
    using SampleType = ASTCreateStatsQueryExt::SampleType;

    // old style to specify settings, maximun priority
    if (query->sample_type == SampleType::FullScan)
    {
        settings.set_enable_sample(false);
    }
    else if (query->sample_type == SampleType::Sample)
    {
        settings.set_enable_sample(true);

        if (query->sample_rows)
        {
            settings.set_sample_row_count(*query->sample_rows);
        }

        if (query->sample_ratio)
        {
            settings.set_sample_ratio(static_cast<Float32>(*query->sample_ratio));
        }
    }
    settings.set_if_not_exists(query->if_not_exists);

    return settings;
}

BlockIO InterpreterCreateStatsQuery::execute()
{
    auto context = getContext();
    const auto * query = query_ptr->as<const ASTCreateStatsQueryExt>();
    if (!query)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error not an ASTCreateStatsQueryExt");
    }

    auto catalog = createCatalogAdaptor(context);
    catalog->checkHealth(/*is_write=*/true);

    CollectorSettings settings = analyzeSettings(context, query);

    auto tables = getTablesFromAST(context, query);
    std::vector<CollectTarget> valid_targets;
    for (const auto & table : tables)
    {
        if (catalog->isTableCollectable(table))
        {
            if (settings.if_not_exists() && catalog->hasStatsData(table))
            {
                // skip when if_not_exists is on
                continue;
            }
            CollectTarget target(context, table, settings, query->columns);
            valid_targets.emplace_back(std::move(target));
        }
    }

    if (valid_targets.empty())
    {
        return {};
    }

    using SyncMode = ASTCreateStatsQueryExt::SyncMode;
    auto use_sync_mode
        = query->sync_mode == SyncMode::Default ? context->getOptimizerContext()->getSettingsRef().statistics_enable_async : query->sync_mode == SyncMode::Async;

    if (use_sync_mode)
    {
        // TODO wujianchao implement create stats async
        // submitAsyncTasks(context, std::move(valid_targets));
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Create stats asynchronously is not implemented yet");
    }
    else
    {
        BlockIO io;
        io.pipeline = QueryPipeline(Pipe(std::make_shared<CreateStatsSource>(context, std::move(valid_targets))));
        return io;
    }
}

void registerInterpreterCreateStatsQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    { return std::make_unique<InterpreterCreateStatsQuery>(args.query, args.context); };
    factory.registerInterpreter("InterpreterCreateStatsQuery", create_fn);
}

}
