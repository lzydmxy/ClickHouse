#include <memory>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <base/defines.h>
#include <Query/Common/LinkedHashMap.h>
#include <Query/Common/LinkedHashSet.h>

using namespace DB;

#if defined(ABORT_ON_LOGICAL_ERROR)
    // skip this test, since ASSERT_DEATH is not stable
    #define ASSERT_LOGICAL_ERROR(p)
#else
    #define ASSERT_LOGICAL_ERROR(p) ASSERT_ANY_THROW(p)
#endif

TEST(LinkedHashMapTest, String)
{
    using K = String;
    using V = String;
    auto genK = [](int x) { return fmt::format("K{:05d}", x); };
    auto genV = [](int x) { return fmt::format("V{:05d}", x); };
    int N = 10000;
    int i;
    LinkedHashMap<K, V> mapping;
    ASSERT_TRUE(mapping.empty());
    for (i = 0; i < N; ++i)
    {
        auto index = i * 3 % N;
        ASSERT_EQ(mapping.size(), i);
        mapping.emplace_back(genK(index), genV(index));
        ASSERT_EQ(mapping.size(), i + 1);
    }
    ASSERT_FALSE(mapping.empty());

    i = 0;
    for (auto [k, v] : mapping)
    {
        auto index = i * 3 % N;
        ASSERT_EQ(k, genK(index));
        ASSERT_EQ(v, genV(index));
        ++i;
    }
    ASSERT_EQ(mapping.size(), N);
    for (i = 0; i < N; ++i)
    {
        auto k = genK(i);
        auto v = mapping.at(k);
        ASSERT_TRUE(mapping.count(k));
        ASSERT_EQ(v, genV(i));
    }
    ASSERT_FALSE(mapping.count(genK(N)));
    ASSERT_LOGICAL_ERROR(mapping.at(genK(N)));
}

TEST(LinkedHashMapTest, UniquePtr)
{
    using K = int;
    using V = std::unique_ptr<int>;
    auto genK = [](int x) { return x; };
    auto genV = [](int x) { return std::make_unique<int>(x); };
    int N = 10000;
    int i;
    LinkedHashMap<K, V> mapping;
    ASSERT_TRUE(mapping.empty());
    for (i = 0; i < N; ++i)
    {
        auto index = i * 3 % N;
        ASSERT_EQ(mapping.size(), i);
        mapping.emplace_back(genK(index), genV(index));
        ASSERT_EQ(mapping.size(), i + 1);
    }
    ASSERT_FALSE(mapping.empty());

    i = 0;
    for (auto & [k, v] : mapping)
    {
        auto index = i * 3 % N;
        ASSERT_EQ(k, genK(index));
        ASSERT_EQ(*v, index);
        ++i;
    }

    for (i = 0; i < N; ++i)
    {
        auto k = genK(i);
        auto v = *mapping.at(k);
        ASSERT_TRUE(mapping.count(k));
        ASSERT_EQ(v, *genV(i));
    }
    ASSERT_FALSE(mapping.count(genK(N)));
    ASSERT_LOGICAL_ERROR(mapping.at(genK(N)));
}


TEST(LinkedHashMapTest, operatorBracket)
{
    LinkedHashMap<String, LinkedHashSet<String>> map;
    LinkedHashSet<String> set;
    set.emplace("1");

    map.emplace("foo", std::move(set));
    map["bar"].emplace("100");
    map["foo"].emplace("2");
    map["zzz"];
    std::cout << map.toString() << std::endl;
}
