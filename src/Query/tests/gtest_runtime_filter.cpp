#include <gtest/gtest.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Query/Common/OptimizerSettings.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterBuilder.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterTypes.h>

using namespace DB;

constexpr size_t DEFAULT_BLOOM_FILTER_BYTES = 1024 * 256;

TEST(RuntimeFilterTest, BlockBloomFilter)
{
    BlockBloomFilter bloom_filter{DEFAULT_BLOOM_FILTER_BYTES};
    bloom_filter.addKey(794873);
    bloom_filter.addKey(1190443);
    bloom_filter.addKey(12121237);
    EXPECT_TRUE(bloom_filter.probeKey(794873));
    EXPECT_TRUE(bloom_filter.probeKey(1190443));
    EXPECT_TRUE(bloom_filter.probeKey(12121237));
}

TEST(RuntimeFilterTest, BlockBloomFilterRandomTest)
{
    BlockBloomFilter bloom_filter{DEFAULT_BLOOM_FILTER_BYTES};
    std::vector<int> random_numbers;
    for (int i = 0; i < 10000; i++)
        random_numbers.push_back(rand());

    for (const auto num : random_numbers)
        EXPECT_FALSE(bloom_filter.probeKey(num));

    for (const auto num : random_numbers)
        bloom_filter.addKey(num);

    for (const auto num : random_numbers)
        EXPECT_TRUE(bloom_filter.probeKey(num));
}

/// @brief Merge two bloom filter
TEST(RuntimeFilterTest, MergeBloomFilter)
{
    auto log = &Poco::Logger::get("RuntimeFilter");
    RuntimeFilterId rf_id{1};
    OptimizerSettings setting;
    LinkedHashMap<String, RuntimeFilter> rfs;
    RuntimeFilter rf1(1, RRuntimeFilter::LOCAL);
    RuntimeFilter rf2(2, RRuntimeFilter::DISTRIBUTED);
    rfs.emplace_back("local1", rf1);
    rfs.emplace_back("distr1", rf2);

    std::map<UInt32, RuntimeFilterData>  data_sets;
    /// worker 1
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk1;
    DataTypePtr type_ptr = std::make_shared<DataTypeNumber<Int32>>();
    auto bloom_wk1 = std::make_shared<BloomFilterWithRange>(1024, type_ptr);
    for (int i = 0; i < 1024; ++i)
        bloom_wk1->addKey(i);
    RuntimeFilterVal wk1{true, bloom_wk1, nullptr};
    LOG_TRACE(log, "wk1 {}", wk1.dump());
    runtime_wk1.emplace(rf_id, wk1);
    data_sets.insert(std::make_pair(0, RuntimeFilterData{std::move(runtime_wk1)}));

    /// worker 2
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk2;
    auto bloom_wk2 = std::make_shared<BloomFilterWithRange>(512, type_ptr);
    for (int i = 0; i < 512; ++i)
        bloom_wk2->addKey(i);
    RuntimeFilterVal wk2{true, bloom_wk2, nullptr};
    LOG_TRACE(log, "wk2 {}", wk2.dump());
    runtime_wk2.emplace(rf_id, wk2);
    data_sets.insert(std::make_pair(1, RuntimeFilterData{std::move(runtime_wk2)}));
    /// start build
    RuntimeFilterBuilder builder(setting, rfs);
    auto && dt = builder.merge(std::move(data_sets));
    EXPECT_EQ(dt.bypass, BypassType::NO_BYPASS);
    EXPECT_EQ(dt.runtime_filters.size(), 1);
    EXPECT_EQ(dt.runtime_filters[rf_id].is_bf, true);
}

/// @brief Merge two sets
TEST(RuntimeFilterTest, MergeSet)
{
    auto log = &Poco::Logger::get("RuntimeFilter");
    RuntimeFilterId rf_id{1};
    OptimizerSettings setting;
    LinkedHashMap<String, RuntimeFilter> rfs;
    RuntimeFilter rf1(1, RRuntimeFilter::LOCAL);
    RuntimeFilter rf2(2, RRuntimeFilter::DISTRIBUTED);
    rfs.emplace_back("local1", rf1);
    rfs.emplace_back("distr1", rf2);

    std::map<UInt32, RuntimeFilterData>  data_sets;
    DataTypePtr type_ptr = std::make_shared<DataTypeNumber<Int32>>();

    /// worker 1
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk1;
    auto set_wk1 = std::make_shared<ValueSetWithRange>(type_ptr);
    for (int i = 0; i < 1024; ++i)
        set_wk1->insert(i);
    RuntimeFilterVal wk1{false, nullptr, set_wk1};
    LOG_TRACE(log, "wk1 {}", wk1.dump());
    runtime_wk1.emplace(rf_id, wk1);
    /// worker 2
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk2;
    auto set_wk2 = std::make_shared<ValueSetWithRange>(type_ptr);
    for (int i = 0; i < 512; ++i)
        set_wk2->insert(i);
    RuntimeFilterVal wk2{false, nullptr, set_wk2};
    LOG_TRACE(log, "wk2 {}", wk2.dump());
    runtime_wk2.emplace(rf_id, wk2);

    data_sets.insert(std::make_pair(0, RuntimeFilterData{std::move(runtime_wk1)}));
    data_sets.insert(std::make_pair(1, RuntimeFilterData{std::move(runtime_wk2)}));
    /// start build
    RuntimeFilterBuilder builder(setting, rfs);
    auto && dt = builder.merge(std::move(data_sets));
    EXPECT_EQ(dt.bypass, BypassType::NO_BYPASS);
    EXPECT_EQ(dt.runtime_filters.size(), 1);
    EXPECT_EQ(dt.runtime_filters[rf_id].is_bf, false);
}

/// @brief Merge if num partitions > 0
TEST(RuntimeFilterTest, DiffMerge1)
{
    auto log = &Poco::Logger::get("RuntimeFilter");
    RuntimeFilterId rf_id{1};

    OptimizerSettings setting;
    LinkedHashMap<String, RuntimeFilter> rfs;
    RuntimeFilter rf1(1, RRuntimeFilter::LOCAL);
    RuntimeFilter rf2(2, RRuntimeFilter::DISTRIBUTED);
    rfs.emplace_back("local1", rf1);
    rfs.emplace_back("distr1", rf2);

    std::map<UInt32, RuntimeFilterData>  data_sets;
    /// worker 1
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk1;
    DataTypePtr type_ptr = std::make_shared<DataTypeNumber<Int32>>();
    BloomFilterWithRangePtr bloom_wk1 = std::make_shared<BloomFilterWithRange>(10124, type_ptr);
    for (int i = 0; i < 10124; ++i)
        bloom_wk1->addKey(i);
    bloom_wk1->is_pre_enlarged = true;
    RuntimeFilterVal wk1{true, bloom_wk1, nullptr};
    LOG_TRACE(log, "wk1 {}", wk1.dump());
    runtime_wk1.emplace(rf_id, wk1);
    data_sets.insert(std::make_pair(0, RuntimeFilterData{std::move(runtime_wk1)}));

    /// worker 2
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk2;
    ValueSetWithRangePtr value_wk2 = std::make_shared<ValueSetWithRange>(type_ptr);
    for (int i = 0; i < 1023; ++i)
        value_wk2->insert(i);
    RuntimeFilterVal wk2{false, nullptr, value_wk2};
    LOG_TRACE(log, "wk2 {}", wk2.dump());
    runtime_wk2.emplace(rf_id, wk2);
    data_sets.insert(std::make_pair(1, RuntimeFilterData{std::move(runtime_wk2)}));

    /// worker 3
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk3;
    BloomFilterWithRangePtr bloom_wk3 = std::make_shared<BloomFilterWithRange>(10124, type_ptr);
    for (int i = 0; i < 10124; ++i)
        bloom_wk3->addKey(i);
    bloom_wk3->is_pre_enlarged = false;
    RuntimeFilterVal wk3{true, bloom_wk3, nullptr};
    LOG_TRACE(log, "wk3 {}", wk3.dump());
    runtime_wk3.emplace(rf_id, wk3);
    data_sets.insert(std::make_pair(2, RuntimeFilterData{std::move(runtime_wk3)}));

    /// start build
    RuntimeFilterBuilder builder(setting, rfs);
    auto && dt = builder.merge(std::move(data_sets));
    EXPECT_EQ(dt.bypass, BypassType::NO_BYPASS);
    EXPECT_EQ(dt.runtime_filters.size(), 0);
}

/// @brief Cant merge if no min max
TEST(RuntimeFilterTest, DiffMerge2)
{
    auto log = &Poco::Logger::get("RuntimeFilter");
    RuntimeFilterId rf_id{1};

    OptimizerSettings setting;
    LinkedHashMap<String, RuntimeFilter> rfs;
    RuntimeFilter rf1{1, RRuntimeFilter::LOCAL};
    RuntimeFilter rf2{2, RRuntimeFilter::DISTRIBUTED};
    rfs.emplace_back("local1", rf1);
    rfs.emplace_back("distr1", rf2);

    std::map<UInt32, RuntimeFilterData>  data_sets;
    /// worker 1
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk1;
    DataTypePtr type_ptr = std::make_shared<DataTypeNumber<Int32>>();
    ValueSetWithRangePtr value_wk1 = std::make_shared<ValueSetWithRange>(type_ptr);
    for (int i = 0; i < 10124; ++i)
        value_wk1->insert(i);
    RuntimeFilterVal wk1{false, nullptr, value_wk1 };
    LOG_TRACE(log, "wk1 {}", wk1.dump());
    runtime_wk1.emplace(rf_id, wk1);
    data_sets.insert(std::make_pair(0, RuntimeFilterData{std::move(runtime_wk1)}));

    /// worker 2
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk2;
    ValueSetWithRangePtr value_wk2 = std::make_shared<ValueSetWithRange>(type_ptr);
    for (int i = 0; i < 1023; ++i)
        value_wk2->insert(i);
    RuntimeFilterVal wk2{false, nullptr, value_wk2 };
    LOG_TRACE(log, "wk2 {}", wk2.dump());
    runtime_wk2.emplace(rf_id, wk2);
    data_sets.insert(std::make_pair(1, RuntimeFilterData{std::move(runtime_wk2)}));

    /// worker 3
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk3;
    BloomFilterWithRangePtr bloom_wk3 = std::make_shared<BloomFilterWithRange>(10124, type_ptr);
    for (int i = 0; i < 10124; ++i)
        bloom_wk3->addKey(i);
    bloom_wk3->is_pre_enlarged = true;
    RuntimeFilterVal wk3{true, bloom_wk3, nullptr };
    LOG_TRACE(log, "wk3 {}", wk3.dump());
    runtime_wk3.emplace(rf_id, wk3);
    data_sets.insert(std::make_pair(2, RuntimeFilterData{std::move(runtime_wk3)}));

    /// start build
    RuntimeFilterBuilder builder(setting, rfs);
    auto && dt = builder.merge(std::move(data_sets));
    EXPECT_EQ(dt.bypass, BypassType::NO_BYPASS);
    EXPECT_EQ(dt.runtime_filters.size(), 0);
}
