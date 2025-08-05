#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>

#include <Core/Block.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Query/Interpreters/InterpreterShowStatsQuery.h>
#include <Query/Optimizer/Dump/DDLDumper.h>
#include <Query/Optimizer/Dump/StatsLoader.h>
#include <Query/Parsers/ASTStatsQueryExt.h>
#include <Query/Statistics/ASTHelpers.h>
#include <Query/Statistics/FormattedOutput.h>
#include <Query/Statistics/StatisticsCollector.h>
#include <Query/Statistics/StatsTableBasic.h>
#include <Query/Statistics/TypeUtils.h>
#include <Query/ProtosHelper/ASTSerDerHelper.h>
#include <Query/Core/UUIDExt.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/InterpreterFactory.h>

namespace DB
{

namespace QueryStatistics
{
    std::shared_ptr<StatsTableBasic> getTableStatistics(ContextPtr context, const StatsTableIdentifier & table);
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
}
using Protos::DbStats;
using Protos::DbStats_Version_V1;
using Protos::DbStats_Version_V2;
using namespace QueryStatistics;


std::vector<FormattedOutputData> getTableFormattedOutput(
    ContextPtr context,
    CatalogAdaptorPtr catalog,
    const CollectorSettings & collector_settings,
    const StatsTableIdentifier & table_info,
    const std::vector<String> & column_names)
{
    std::vector<FormattedOutputData> results;
    StatisticsCollector collector_impl(context, catalog, table_info, collector_settings);
    if (column_names.empty())
    {
        collector_impl.readAllFromCatalog();
    }
    else
    {
        auto cols_desc = catalog->filterCollectableColumns(table_info, column_names, true);
        collector_impl.readFromCatalogImpl(cols_desc);
    }


    auto plannode_stats_opt = collector_impl.toPlanNodeStatistics();
    if (!plannode_stats_opt.has_value())
    {
        return {};
    }

    auto plannode_stats = plannode_stats_opt.value();

    auto row_count = plannode_stats->getRowCount();
    {
        FormattedOutputData fod;
        fod.append("identifier", table_info.getTableName() + ".*");
        fod.append("count", row_count);
        results.emplace_back(std::move(fod));
    }

    auto symbols = plannode_stats->getSymbolStatistics();
    auto cols_desc = catalog->getAllCollectableColumns(table_info);
    for (auto & col : cols_desc)
    {
        auto symbol_iter = symbols.find(col.name);
        if (symbol_iter == symbols.end())
        {
            // TODO: should we output empty columns? to ensure no stats is correctly handled
            continue;
        }
        auto & symbol_stats = symbol_iter->second;

        FormattedOutputData fod;

        auto type_name = col.type->getName();
        fod.append("identifier", table_info.getTableName() + "." + col.name);
        fod.append("type", type_name);
        auto ndv = symbol_stats->getNdv();
        fod.append("ndv", ndv);

        auto null_count = symbol_stats->getNullsCount();
        fod.append("count", row_count - null_count);
        fod.append("null_count", null_count);
        fod.append("avg_byte_size", symbol_stats->getAvg());

        fod.append("min", symbol_stats->getMin());
        fod.append("max", symbol_stats->getMax());

        fod.append("has_histogram", !symbol_stats->getHistogram().empty());
        results.emplace_back(std::move(fod));
    }
    return results;
}

void writeDbStats(ContextPtr context, const String & db_name, const String & path)
{
    DbStats db_stats;
    db_stats.set_db_name(db_name);
    db_stats.set_version(PROTO_VERSION);
    auto catalog = createCatalogAdaptor(context);
    auto tables = catalog->getAllTablesID(db_name);
    for (auto & table : tables)
    {
        StatisticsCollector collector(context, catalog, table, {});
        collector.readAllFromCatalog();
        auto table_collection = collector.getTableStats().writeToCollection();
        if (table_collection.empty())
        {
            continue;
        }

        auto table_pb = db_stats.add_tables();
        table_pb->set_table_name(table.getTableName());
        for (auto & [k, v] : table_collection)
        {
            table_pb->mutable_blobs()->operator[](static_cast<int64_t>(k)) = v->serialize();
        }
        for (auto & [col_name, col_stats] : collector.getColumnsStats())
        {
            auto column_pb = table_pb->add_columns();
            auto column_collection = col_stats.writeToCollection();
            if (column_collection.empty())
            {
                continue;
            }
            column_pb->set_column_name(col_name);
            for (auto & [k, v] : column_collection)
            {
                column_pb->mutable_blobs()->operator[](static_cast<int64_t>(k)) = v->serialize();
            }
        }
    }
    std::ofstream fout(path, std::ios::binary);
    db_stats.SerializeToOstream(&fout);
}
void writeDbStatsToJson(ContextPtr context, const String & db_name, const String & folder)
{
    DDLDumper ddl_dumper(folder);
    ddl_dumper.addTableFromDatabase(db_name, context);
    ddl_dumper.dumpStats(folder + "/stats.json");
}

void readDbStats(ContextPtr context, const String & original_db_name, const String & path)
{
    std::ifstream fin(path, std::ios::binary);
    DbStats db_stats;
    ASSERT_PARSE(db_stats.ParseFromIstream(&fin));

    auto version = db_stats.has_version() ? db_stats.version() : DbStats_Version_V1;
    if (version == DbStats_Version_V1)
    {
        version = DbStats_Version_V2;
    }
    if (version != PROTO_VERSION)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "stats version is incorrect");
    }

    auto db_name = original_db_name;
    auto catalog = createCatalogAdaptor(context);
    auto logger = getLogger("load stats");

    auto load_ts = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    for (auto & table_pb : db_stats.tables())
    {
        auto table_name = table_pb.table_name();
        auto table_id_opt = catalog->getTableIdByName(db_name, table_name);
        if (!table_id_opt)
        {
            auto msg = "table " + table_name + " not exist in database " + db_name;
            LOG_WARNING(logger, "table {} not exist in database {}.",table_name, db_name);
            continue;
        }

        StatisticsCollector collector(context, catalog, table_id_opt.value(), {});

        {
            StatsCollection collection;
            for (auto & [k, v] : table_pb.blobs())
            {
                auto tag = static_cast<StatisticsTag>(k);
                auto obj = createStatisticsBase(tag, v);
                if (obj)
                    collection[tag] = std::move(obj);
            }
            StatisticsCollector::TableStats table_stats;
            table_stats.readFromCollection(collection);
            table_stats.basic->setTimestamp(load_ts);
            collector.setTableStats(std::move(table_stats));
        }

        for (auto & column_pb : table_pb.columns())
        {
            auto column_name = column_pb.column_name();
            StatsCollection collection;
            for (auto & [k, v] : column_pb.blobs())
            {
                auto tag = static_cast<StatisticsTag>(k);
                auto obj = createStatisticsBase(tag, v);
                if (obj)
                    collection[tag] = std::move(obj);
            }
            StatisticsCollector::ColumnStats column_stats;
            column_stats.readFromCollection(collection);
            collector.setColumnStats(column_name, std::move(column_stats));
        }
        collector.writeToCatalog();
    }
}

void readDbStatsFromJson(ContextPtr context, const String & json_file)
{
    StatsLoader stats_loader(json_file, context);
    stats_loader.loadStats(/*load_all=*/true);
}

BlockIO InterpreterShowStatsQuery::executeAll()
{
    auto query = query_ptr->as<const ASTShowStatsQueryExt>();

    auto context = getContext();
    auto tables = getTablesFromAST(context, query);
    auto catalog = createCatalogAdaptor(context);

    Blocks blocks;

    for (auto & table_info : tables)
    {
        auto fods = getTableFormattedOutput(context, catalog, collector_settings, table_info, query->columns);
        // adjust here to change the order
        auto block = outputFormattedBlock(
            fods, {"identifier", "type", "count", "null_count", "ndv", "min", "max", "avg_byte_size", "has_histogram"});

        blocks.emplace_back(std::move(block));
    }

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(concatenateBlocks(blocks)));
    return res;
}

std::vector<FormattedOutputData> getColumnFormattedOutput(const String & full_column_name, const SymbolStatistics & symbol_stats)
{
    std::vector<FormattedOutputData> fods;

    // table.col  | bucket_id | range | count | ndv | cumulative_count | cumulative_ndv |

    double cumulative_count = 0;
    double cumulative_ndv = 0;
    auto bucket_id = 0;
    auto & histogram = symbol_stats.getHistogram();
    for (auto & bucket : histogram.getBuckets())
    {
        auto count = bucket.getCount();
        if (count == 0)
        {
            continue;
        }
        FormattedOutputData fod;
        fod.append("identifier", full_column_name);
        fod.append("bucket_id", bucket_id);
        auto low_inc = bucket.isLowerClosed();
        auto high_inc = bucket.isUpperClosed();
        auto low = bucket.getLowerBound();
        auto high = bucket.getUpperBound();
        auto ndv = bucket.getNumDistinct();
        String range
            = (low_inc ? "[" : "(") + boost::lexical_cast<String>(low) + ", " + boost::lexical_cast<String>(high) + (high_inc ? "]" : ")");
        fod.append("range", range);
        fod.append("count", count);
        fod.append("ndv", ndv);
        cumulative_count += count;
        cumulative_ndv += ndv;
        fod.append("cumulative_count", cumulative_count);
        fod.append("cumulative_ndv", cumulative_ndv);
        fods.emplace_back(std::move(fod));
        ++bucket_id;
    }
    return fods;
}

BlocksList getColumnsFormattedOutput(
    ContextPtr context,
    CatalogAdaptorPtr catalog,
    const CollectorSettings & collector_settings,
    const StatsTableIdentifier & table_info,
    const std::vector<String> & target_columns)
{
    StatisticsCollector collector_impl(context, catalog, table_info, collector_settings);

    if (!target_columns.empty())
    {
        auto cols_desc = catalog->filterCollectableColumns(table_info, target_columns, true);
        collector_impl.readFromCatalog(target_columns);
    }
    else
    {
        collector_impl.readAllFromCatalog();
    }

    auto plannode_stats_opt = collector_impl.toPlanNodeStatistics();
    if (!plannode_stats_opt.has_value())
    {
        return {};
    }
    auto plannode_stats = plannode_stats_opt.value();

    BlocksList blocks;
    const auto & plan_stats = plannode_stats->getSymbolStatistics();
    if (plan_stats.empty())
    {
        return {};
    }

    auto cols_desc = catalog->getAllCollectableColumns(table_info);

    for (auto & col_desc : cols_desc)
    {
        auto col_name = col_desc.name;
        if (plan_stats.count(col_name) == 0)
        {
            continue;
        }

        const auto & symbol_stats = plan_stats.at(col_name);

        if (symbol_stats->getHistogram().empty())
        {
            continue;
        }
        auto full_col_name = table_info.getTableName() + "." + col_name;
        auto fods = getColumnFormattedOutput(full_col_name, *symbol_stats);
        auto block = outputFormattedBlock(fods, {"identifier", "bucket_id", "range", "count", "ndv", "cumulative_count", "cumulative_ndv"});
        blocks.emplace_back(std::move(block));
    }
    return blocks;
}


BlockIO InterpreterShowStatsQuery::executeColumn()
{
    auto query = query_ptr->as<const ASTShowStatsQueryExt>();
    auto context = getContext();
    // Block sample_block = getSampleBlock();
    // MutableColumns res_columns = sample_block.cloneEmptyColumns();
    auto tables = getTablesFromAST(context, query);
    auto catalog = createCatalogAdaptor(context);

    Blocks blocks;
    if (!query->columns.empty())
    {
        if (tables.size() != 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "columns specifier is supported only for single table");
        }

        auto table_info = tables[0];
        auto new_blocks = getColumnsFormattedOutput(context, catalog, collector_settings, table_info, query->columns);
        blocks.insert(blocks.end(), new_blocks.begin(), new_blocks.end());
    }
    else
    {
        for (auto & table_info : tables)
        {
            auto new_blocks = getColumnsFormattedOutput(context, catalog, collector_settings, table_info, {});
            blocks.insert(blocks.end(), new_blocks.begin(), new_blocks.end());
        }
    }

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(concatenateBlocks(blocks)));
    return res;
}

static bool isSpecialFunction(const String & name)
{
    static std::set<String> specials({"__save", "__load", "__jsonsave", "__jsonload", "__transfer"});
    return specials.count(name);
}

BlockIO InterpreterShowStatsQuery::executeSpecial()
{
    const auto * query = query_ptr->as<const ASTShowStatsQueryExt>();
    auto context = getContext();
    auto catalog = createCatalogAdaptor(context);

    // when special, cache settings will be invalid
    if (collector_settings.cache_policy() != StatisticsCachePolicy::Default)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cache policy is not supported for special functions");
    }

    if (context->getOptimizerContext()->getSettingsRef().statistics_cache_policy != StatisticsCachePolicy::Default)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "statistics_cache_policy is not supported for special functions, must set it to default");
    }

    // refactor this into an explicit command
    if (query->getTable() == "__save")
    {
        catalog->checkHealth(/*is_write=*/false);
        auto db_name = context->resolveDatabase(query->getDatabase());
        auto path = context->getOptimizerContext()->getSettingsRef().graphviz_path.toString() + "/" + db_name + ".bin";

        writeDbStats(context, db_name, path);
    }
    else if (query->getTable() == "__load")
    {
        catalog->checkHealth(/*is_write=*/true);
        auto db_name = query->getDatabase();
        if (db_name.empty())
            db_name = context->getCurrentDatabase();
        auto path = context->getOptimizerContext()->getSettingsRef().graphviz_path.toString() + "/" + db_name + ".bin";
        if (!std::filesystem::exists(path))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "file {} not exists", path);
        }

        readDbStats(context, db_name, path);
    }
    else if (query->getTable() == "__jsonsave")
    {
        catalog->checkHealth(/*is_write=*/false);
        auto db_name = query->getDatabase();
        if (db_name.empty())
            db_name = context->getCurrentDatabase();
        auto folder = context->getOptimizerContext()->getSettingsRef().graphviz_path.toString() + '/' + db_name;
        writeDbStatsToJson(context, db_name, folder);
    }
    else if (query->getTable() == "__jsonload")
    {
        catalog->checkHealth(/*is_write=*/true);
        auto db_name = query->getDatabase();
        if (db_name.empty())
            db_name = context->getCurrentDatabase();
        auto path = context->getOptimizerContext()->getSettingsRef().graphviz_path.toString() + '/' + db_name + "/stats.json";
        if (!std::filesystem::exists(path))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "json_file {} not exists", path);
        }

        readDbStatsFromJson(context, path);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "unknown special action: {}", query->getTable());
    }
    return {};
}


BlockIO InterpreterShowStatsQuery::executeTable()
{
    auto query = query_ptr->as<const ASTShowStatsQueryExt>();
    auto context = getContext();
    auto catalog = createCatalogAdaptor(context);
    auto tables = getTablesFromAST(context, query);
    std::vector<FormattedOutputData> result;
    for (auto & table : tables)
    {
        FormattedOutputData data;
        auto obj = getTableStatistics(context, table);
        auto storage = catalog->getStorageByTableId(table);
        data.append("database", table.getDatabaseName());
        data.append("table", table.getTableName());
        data.append("engine", storage->getName());
        data.append("unique_key", UUIDHelpers::UUIDToString(table.getUniqueKey(context)));
        data.append("row_count", obj ? std::to_string(obj->getRowCount()) : "");

        String timestamp = "";
        if (obj)
        {
            WriteBufferFromOwnString buffer;
            writeDateTimeText(obj->getTimestamp(), DataTypeDateTime64::default_scale, buffer, DateLUT::serverTimezoneInstance());
        }

        data.append("timestamp", timestamp);
        result.emplace_back(std::move(data));
    }

    auto block = outputFormattedBlock(result, {"database", "table", "engine", "unique_key", "row_count", "timestamp"});
    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(block)));
    return res;
}

BlockIO InterpreterShowStatsQuery::execute()
{
    const auto * query = query_ptr->as<const ASTShowStatsQueryExt>();
    auto context = getContext();
    auto catalog = createCatalogAdaptor(context);

    if (query->cache_policy != StatisticsCachePolicy::Default)
    {
        collector_settings.set_cache_policy(query->cache_policy);
    }
    else if (context->getOptimizerContext()->getSettingsRef().statistics_cache_policy != StatisticsCachePolicy::Default)
    {
        collector_settings.set_cache_policy(context->getOptimizerContext()->getSettingsRef().statistics_cache_policy);
    }

    // when enable_memory_catalog is true, we won't use cache
    if (catalog->getSettingsRef().enable_memory_catalog)
    {
        if (collector_settings.cache_policy() != StatisticsCachePolicy::Default)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "memory catalog don't support cache policy");
    }

    if (isSpecialFunction(query->getTable()))
    {
        return executeSpecial();
    }
    else if (query->kind == StatsQueryKind::COLUMN_STATS)
    {
        catalog->checkHealth(/*is_write=*/false);
        // throw Exception("unimplemented", ErrorCodes::LOGICAL_ERROR);
        return executeColumn();
    }
    else if (query->kind == StatsQueryKind::ALL_STATS)
    {
        catalog->checkHealth(/*is_write=*/false);
        return executeAll();
    }
    else if (query->kind == StatsQueryKind::TABLE_STATS)
    {
        catalog->checkHealth(false);
        return executeTable();
    }
    UNREACHABLE();
}

void registerInterpreterShowStatsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowStatsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowStatsQuery", create_fn);
}

}
