#pragma once

#include <string>
#include <Query/Optimizer/tests/gtest_base_plan_test.h>
#include <Query/Optimizer/tests/test_config.h>

namespace DB
{
/**
 * change resource path in test_config.h.in.
 */
class BaseTpcdsPlanTest : public AbstractPlanTestSuite
{
public:
    explicit BaseTpcdsPlanTest(const std::unordered_map<String, Field> & settings, int sf_ = 1000, bool use_sample = false)
        : AbstractPlanTestSuite("tpcds" + std::to_string(sf_) + (use_sample ? "_sample" : ""), settings), sf(sf_)
    {
        if (sf_ != 1000 && sf_ != 100)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "sf only support 100 or 1000");
        createTables();
        dropTableStatistics();
        loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {TPCDS_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override
    {
        return std::filesystem::path(TPCDS_TABLE_STATISTICS_FOLDER) / getDatabaseName() / "stats.json";
    }
    std::filesystem::path getQueriesDir() override { return TPCDS_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override
    {
        std::string dir = getDatabaseName() + label;
        return std::filesystem::path(TPCDS_EXPECTED_EXPLAIN_RESULT) / dir;
    }
    void setLabel(const std::string & label_) { this->label = "_" + label_; }

    int sf;
    std::string label;
};

}
