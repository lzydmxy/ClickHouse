#include <Query/Optimizer/Dump/ReproduceUtils.h>
#include <Query/Analyzer/Analysis.h>
#include <Query/Interpreters/InterpreterExplainQueryUseOptimizer.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Query/Optimizer/Dump/DumpUtils.h>
#include <Query/Parsers/ParserSettings.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <IO/Archives/ZipArchiveReader.h>
#include <Query/Planner/PlannerExt.h>
#include <Query/Parsers/ASTExplainQueryExt.h>
#include <filesystem>
#include <fstream>
#include <string>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace DB::ReproduceUtils
{
ASTPtr parse(const std::string & query, ContextPtr query_context)
{
    const char * begin = query.data();
    const char * end = begin + query.size();

    ParserQuery parser(end, false);
    return parseQuery(
        parser,
        begin,
        end,
        "",
        query_context->getSettingsRef().max_query_size,
        query_context->getSettingsRef().max_parser_depth,
        query_context->getSettingsRef().max_parser_backtracks);
}

void executeDDL(ConstASTPtr query, ContextMutablePtr query_context)
{
    String query_str = serializeAST(*query);
    String res;
    ReadBufferFromString is1(query_str);
    WriteBufferFromString os1(res);

    query_context->setCurrentTransaction(nullptr);
    executeQuery(is1, os1, false, query_context, {}, {}, std::nullopt);
}

std::string obtainExplainString(const std::string & select_query, ContextMutablePtr query_context)
{
    ASTPtr ast = parse(select_query, query_context);
    auto explain_query = std::make_shared<ASTExplainQueryExt>(ASTExplainQueryExt::QueryPlan);
    explain_query->setExplainedQuery(ast);
    InterpreterExplainQueryUseOptimizer interpreter(explain_query, query_context);
    //todo: liyang453, other feat: need getInputStream() in BlockIO
    //auto explain_result = interpreter.execute().getInputStream();
    //Block explain_block = explain_result->read();
    Block explain_block; 
    while (explain_block && !explain_block.rows())
    {
        //todo: liyang453, other feat: need getInputStream() in BlockIO
        //explain_block = explain_result->read();
    }
    if (!explain_block.rows())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "explain result is empty");
    }
    // there is only one block with one column
    const auto & column = *(explain_block.getByPosition(0).column);
    std::string res;
    for (size_t i = 0; i < column.size(); ++i)
    {
        res += column[i].get<String>() + '\n';
    }
    return res;
}

const char * toString(DDLStatus stats)
{
    switch (stats)
    {
        case DDLStatus::failed: return "failed to execute ddl";
        case DDLStatus::created: return "created database or table";
        case DDLStatus::reused: return "reused existing database or table";
        case DDLStatus::dropped: return "dropped database or table";
        case DDLStatus::created_and_loaded_stats: return "created table and loaded stats";
        case DDLStatus::reused_and_loaded_stats: return "reused existing table and loaded stats";
        case DDLStatus::unknown: return "unknown status";
    }
}

std::string getFolder(const std::string & file_path)
{
    bool is_zip = file_path.ends_with(".zip");
    std::string folder_path = (is_zip) ? file_path.substr(0, file_path.size() - 4)
                                       : DumpUtils::simplifyPath(file_path);

    if (is_zip)
    {
        std::filesystem::path zip_path = std::filesystem::path(file_path);
        if (!std::filesystem::exists(zip_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "zip file not found: {}", file_path);
        ZipArchiveReader zip_reader(file_path);
        for (auto & file : zip_reader.getAllFiles())
        {
            auto in = zip_reader.readFile(file, /*throw_on_not_found=*/true);
            WriteBufferFromFile out(file);
            copyData(*in, out);
            out.finalize();
        }
        folder_path = folder_path + '/' + DumpUtils::DUMP_RESULT_FILE;
    }

    return folder_path;
}

Poco::JSON::Object::Ptr readJsonFromAbsolutePath(const std::string & absolute_path)
{
    std::filesystem::path file_path(absolute_path);
    if (!std::filesystem::exists(file_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "file not found: {}", absolute_path);
    std::ifstream fin(file_path);
    std::stringstream buffer;
    buffer << fin.rdbuf();
    return Poco::JSON::Parser().parse(buffer.str()).extract<Poco::JSON::Object::Ptr>();
}

}
