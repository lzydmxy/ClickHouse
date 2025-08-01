#include <Query/Optimizer/tests/gtest_base_plan_test.h>

#include <Databases/DatabaseMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Query/Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Query/Optimizer/Dump/PlanReproducer.h>
#include <Query/Optimizer/PlanOptimizer.h>
#include <Query/Analyzer/QueryRewriter.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Query/Processors/QueryPlan/PlanPrinter.h>
#include <Query/Planner/PlannerExt.h>
#include <Query/Statistics/CacheManager.h>
#include <Query/Statistics/CatalogAdaptor.h>
#include <Poco/NumberParser.h>
#include <Poco/ConsoleChannel.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Query/Core/tests/gtest_protobuf_common.h>
#include <Query/Optimizer/tests/gtest_storage_mock_distirbuted.h>
#include <Query/Analyzer/QueryAnalyzer.h>
#include <Query/Statistics/CacheManager.h>
#include <Query/Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Storages/System/attachSystemTables.h>


#include <fstream>
#include <iostream>

using namespace std::string_literals;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void initLogger(const String & level)
{
    if (!Poco::Logger::root().getChannel())
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel());
        Poco::Logger::root().setChannel(channel);
    }
    Poco::Logger::root().setLevel(level);
}

BasePlanTest::BasePlanTest(const String & database_name_, const std::unordered_map<String, Field> & session_settings)
    : database_name(database_name_), session_context(Context::createCopy(getContext().context))
{
    tryRegisterFunctions();
    tryRegisterFormats();
    UnitTest::tryRegisterStorages();
    tryRegisterAggregateFunctions();

    initLogger("warning");
    session_context->initializeOptimizerContext();
    // tryRegisterStorageMockDistributed();

    SettingsChanges setting_changes;

    setting_changes.emplace_back("enable_optimizer", true);
    setting_changes.emplace_back("enable_memory_catalog", true);
    setting_changes.emplace_back("dialect_type", "ANSI");
    setting_changes.emplace_back("data_type_default_nullable", false);

    for (const auto & item : session_settings)
        setting_changes.emplace_back(item.first, item.second);

    session_context->applySettingsChanges(setting_changes);

    if (DatabaseCatalog::instance().tryGetDatabase(database_name))
        DatabaseCatalog::instance().detachDatabase(session_context, database_name, true, false);

    auto database = std::make_shared<DatabaseMemory>(database_name, session_context);
    DatabaseCatalog::instance().attachDatabase(database_name, database);

    const String system_db_name = "system";
    if (!DatabaseCatalog::instance().tryGetDatabase(system_db_name))
    {
        auto database_system = std::make_shared<DatabaseMemory>(system_db_name, session_context);
        DatabaseCatalog::instance().attachDatabase(system_db_name, database_system);
        attachSystemTablesServer(session_context, *DatabaseCatalog::instance().getSystemDatabase(), false);
    }

    session_context->setCurrentDatabase(database_name);
}

ASTPtr BasePlanTest::parse(const std::string & query, ContextMutablePtr query_context)
{
    const char * begin = query.data();
    const char * end = begin + query.size();

    ParserQuery parser(end);
    auto ast = parseQuery(
        parser, begin, end, "", query_context->getSettingsRef().max_query_size, query_context->getSettingsRef().max_parser_depth, query_context->getSettingsRef().max_parser_backtracks);
    return ast;
}

QueryPlanExtPtr BasePlanTest::plan(const String & query, ContextMutablePtr query_context)
{
    auto ast = parse(query, query_context);

    {
        SelectIntersectExceptQueryVisitor::Data data{
            query_context->getSettingsRef().intersect_default_mode, query_context->getSettingsRef().except_default_mode};
        SelectIntersectExceptQueryVisitor{data}.visit(ast);
    }

    /// Normalize SelectWithUnionQuery
    NormalizeSelectWithUnionQueryVisitor::Data data{query_context->getSettingsRef().union_default_mode};
    NormalizeSelectWithUnionQueryVisitor{data}.visit(ast);

    ast = QueryRewriter().rewrite(ast, query_context);
    AnalysisPtr analysis = QueryAnalyzer::analyze(ast, query_context);
    auto query_plan = PlannerExt().plan(ast, *analysis, query_context);
    PlanOptimizer::optimize(*query_plan, query_context);
    return query_plan;
}

PlanSegmentTreePtr BasePlanTest::planSegment(const String & query, ContextMutablePtr query_context)
{
    auto query_plan = plan(query, query_context);

    auto plan = PlanNodeToNodeVisitor::convert(*query_plan);

    auto plan_segment_tree = std::make_unique<PlanSegmentTree>();
    ClusterInfoContext cluster_info_context{.query_plan = plan, .context = query_context, .plan_segment_tree = plan_segment_tree};
    PlanSegmentContext plan_segment_context = ClusterInfoFinder::find(*query_plan, cluster_info_context);
    PlanSegmentSplitter::split(plan, plan_segment_context);
    return plan_segment_tree;
}

std::string BasePlanTest::execute(const String & query, ContextMutablePtr query_context)
{
    ASTPtr ast = parse(query, query_context);
    if (auto * create = ast->as<ASTCreateQuery>())
    {
        if (create->storage)
        {
            auto * storage = create->storage->as<ASTStorage>();
            storage->engine->name = StorageMockDistributed::ENGINE_NAME;
        }
        InterpreterCreateQuery create_interpreter(ast, query_context);
        create_interpreter.execute();
        return "";
    }

    String res;
    {
        ReadBufferFromString is1(query);
        WriteBufferFromString os1(res);
        executeQuery(is1, os1, false, query_context, {}, {}, std::nullopt);
    }
    return res;
}

ContextMutablePtr BasePlanTest::createQueryContext(std::unordered_map<std::string, Field> settings)
{
    auto query_context = Context::createCopy(session_context);
    query_context->setSessionContext(session_context);
    query_context->setQueryContext(query_context);
    query_context->setCurrentQueryId("test_plan");
    query_context->initializeOptimizerContext();
    query_context->getOptimizerContext()->createPlanNodeIdAllocator();
    query_context->getOptimizerContext()->createSymbolAllocator();
    query_context->getOptimizerContext()->createOptimizerMetrics();
    for (const auto & item : settings)
        query_context->setSetting(item.first, item.second);
    return query_context;
}

std::vector<std::string> AbstractPlanTestSuite::loadQueries()
{
    auto path = getQueriesDir();
    if (!std::filesystem::exists(path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} not found.", path.string());

    std::vector<std::string> queries;
    for (const auto & entry : std::filesystem::directory_iterator(path))
        queries.emplace_back(entry.path().stem().string());

    auto try_convert_to_number = [](const std::string & name) {
        try
        {
            return std::stoi(name.substr(1));
        }
        catch (std::exception &)
        {
            return 0;
        }
    };
    std::sort(queries.begin(), queries.end(), [&](auto & a, auto & b) { return try_convert_to_number(a) < try_convert_to_number(b); });

    return queries;
}

AbstractPlanTestSuite::Query AbstractPlanTestSuite::loadQuery(const std::string & name)
{
    auto file = getQueriesDir() / (name + ".sql");
    std::vector<std::string> splits = loadFile(file, ';');

    std::unordered_map<std::string, Field> settings;
    std::vector<std::pair<std::string, ASTPtr>> sql;

    for (auto & split : splits)
    {
        auto ast = parse(split, session_context);
        if (getAstType(ast) == ASTType::ASTSetQuery)
            for (auto & set : ast->as<ASTSetQuery &>().changes)
                settings.emplace(set.name, set.value);
        else
            sql.emplace_back(split, ast);
    }
    return Query{file.stem().string(), settings, sql};
}

std::string AbstractPlanTestSuite::explain(const std::string & name)
{
    std::string explain;
    auto query = loadQuery(name);
    auto context = createQueryContext(query.settings);
    for (auto & sql : query.sql)
    {
        if (getAstType(sql.second) == DB::ASTType::ASTSelectQuery || getAstType(sql.second) == DB::ASTType::ASTSelectWithUnionQuery)
        {
            auto query_plan = plan(sql.first, context);

            CardinalityEstimator::estimate(*query_plan, context);
            QueryPlanSettings settings{.query_plan_options = {}, .stats = show_statistics};
            explain += DB::PlanPrinter::textLogicalPlan(*query_plan, context, {}, {}, settings);
        }
        else
            execute(sql.first, context);
    }
    return explain;
}

std::string AbstractPlanTestSuite::loadExplain(const std::string & name)
{
    auto file = getExpectedExplainDir() / (name + ".explain");
    if (!std::filesystem::exists(file))
        return "";
    return loadFile(file)[0];
}

void AbstractPlanTestSuite::saveExplain(const std::string & name, const std::string & explain)
{
    auto file = getExpectedExplainDir() / (name + ".explain");
    if (!std::filesystem::exists(file.parent_path()))
        std::filesystem::create_directories(file.parent_path());

    std::ofstream output;
    output.open(file);
    output << explain;
    output.close();
}

void AbstractPlanTestSuite::createTables()
{
    using namespace DB::QueryStatistics;
    CacheManager::initialize(10000, std::chrono::seconds(1000));
    for (auto & file : getTableDDLFiles())
    {
        for (auto & ddl : loadFile(file, ';'))
        {
            ASTPtr ast = parse(ddl, session_context);
            if (auto * create = ast->as<ASTCreateQuery>())
            {
                if (create->storage)
                {
                    auto * storage = create->storage->as<ASTStorage>();
                    storage->engine->name = StorageMockDistributed::ENGINE_NAME;
                }
            }

            InterpreterCreateQuery create_interpreter(ast, session_context);
            create_interpreter.execute();
        }
    }
}

void AbstractPlanTestSuite::loadTableStatistics()
{
    auto path = getStatisticsFile();

    auto context = createQueryContext(std::unordered_map<String, Field>{{"graphviz_path", path.parent_path().parent_path().string()}});

    std::string expected_database = path.parent_path().filename();
    if (expected_database != database_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "database should be {}", expected_database);
    if (!std::filesystem::exists(path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} not found.", path.string());

    // todo: lizhuoyu, need InterpreterShowStatsQuery
    // InterpreterShowStatsQuery interpreter(parse("show stats __jsonload;", context), context);

    // interpreter.execute();
}

std::vector<std::string> AbstractPlanTestSuite::loadFile(const std::filesystem::path & path, char sep)
{
    if (!std::filesystem::exists(path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} not found.", path.string());

    std::vector<std::string> sections;
    std::ifstream in(path);

    if (sep == '\0')
    {
        std::ostringstream ss;
        ss << in.rdbuf();
        sections.emplace_back(ss.str());
    }
    else
    {
        std::string line;
        while (std::getline(in, line, sep))
        {
            boost::algorithm::trim(line);
            if (line.empty())
                continue;
            sections.emplace_back(std::move(line));
        }
    }
    return sections;
}

bool AbstractPlanTestSuite::enforce_regenerate()
{
    return std::getenv("REGENERATE") != nullptr;
}

int AbstractPlanTestSuite::regenerate_task_thread_size()
{
    if (auto * str = std::getenv("REGENERATE_TASK_THREAD_SIZE"))
    {
        int value;
        if (Poco::NumberParser::tryParse(String{str}, value) && value > 0 && value < 100)
        {
            return value;
        }
    }
    return 8;
}

std::unordered_map<String, Field> BasePlanTest::getDefaultOptimizerSettings()
{
    std::unordered_map<std::string, DB::Field> settings;
    if (auto * str = std::getenv("REGENERATE_SETTINGS"); str != nullptr && std::strlen(str) > 0)
    {
        ParserSetQuery parser{true};
        auto set_query = parseQuery(parser, str, 0, 0, 0);
        for (const auto & setting_change : set_query->as<ASTSetQuery>()->changes)
            settings.emplace(setting_change.name, setting_change.value);
    }

#ifndef NDEBUG
    // debug mode may time out.
    settings.emplace("iterative_optimizer_timeout", "30000000");
    settings.emplace("cascades_optimizer_timeout", "30000000");
#endif
    settings.emplace("dialect_type", "ANSI");
    settings.emplace("enable_sharding_optimize", 1);
    settings.emplace("data_type_default_nullable", false);
    settings.emplace("enable_group_by_keys_pruning", true);
    settings.emplace("enable_eliminate_join_by_fk", true);
    settings.emplace("enable_eliminate_complicated_pk_fk_join", true);
    settings.emplace("enable_eliminate_complicated_pk_fk_join_without_top_join", true);
    settings.emplace("enable_add_projection_to_pruning", true);
    settings.emplace("statistics_return_row_count_if_empty", false);
    settings.emplace("enable_positional_arguments", true);
    settings.emplace("rewrite_complex_predicate_by_domain", true);
    return settings;
}
}
