#pragma once
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST.h>
#include <Query/Common/LinkedHashMap.h>
#include <Query/Common/OptimizerContext.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/ProtosHelper/AddressInfo.h>
#include <Query/Executor/RuntimeFilter/RuntimeFilterTypes.h>


namespace DB
{
using RuntimeFilterId = UInt32;
using BloomFilterWithRangePtr = std::shared_ptr<BloomFilterWithRange>;
using ValueSetWithRangePtr = std::shared_ptr<ValueSetWithRange>;

String distributionToString(RRuntimeFilter::Enum distribution);

enum class BypassType : UInt8
{
    NO_BYPASS = 0,   /// Normal case
    BYPASS_EMPTY_HT, /// Empty right table, which can short circuit the left table scan
    BYPASS_LARGE_HT, /// Too large to build runtime filter, same as the runtime filter abort
};
String bypassTypeToString(BypassType type);

struct InternalDynamicData
{
    Field range{};
    Field bf{};
    Field set{};
    BypassType bypass = BypassType::NO_BYPASS;

    String dump() const
    {
        return bypassTypeToString(bypass) + " range:" + range.dump() + " bf:" + bf.dump() + " set:" + set.dump();
    }
};

struct RuntimeFilter
{
    RuntimeFilterId id;
    RRuntimeFilter::Enum distribution;
    void toProto(RRuntimeFilter & proto) const;
    static RuntimeFilter fromProto(const RRuntimeFilter & proto);
    RuntimeFilter(RuntimeFilterId id_, RRuntimeFilter::Enum distribution_) : id(id_), distribution(distribution_) { }
};

struct RuntimeFilterVal
{
    /* Data: bloom filter and values set is mutual exclusion */
    bool is_bf;
    BloomFilterWithRangePtr bloom_filter;
    ValueSetWithRangePtr values_set; // hash default 1024
    void deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf) const;
    String dump() const;
};

struct RuntimeFilterData
{
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_filters;
    BypassType bypass = BypassType::NO_BYPASS;

    bool isBloomFilter(RuntimeFilterId id) const;
    bool isValueSet(RuntimeFilterId id) const;

    void deserialize(ReadBuffer & istr);
    void serialize(WriteBuffer & ostr) const;
    String dump() const;
};

struct DynamicData
{
    DynamicData() :bf_mutex(std::make_shared<std::shared_mutex>()) {
    }
    BypassType bypass = BypassType::NO_BYPASS;
    bool is_local = false;
    std::variant<RuntimeFilterVal, InternalDynamicData> data;
    std::shared_ptr<std::shared_mutex> bf_mutex;
    BloomFilterWithRangePtr bf;
    String dump()
    {
        if (bypass == BypassType::BYPASS_LARGE_HT)
            return "BYPASS_LARGE_HT";
        else if (bypass == BypassType::BYPASS_EMPTY_HT)
            return "BYPASS_EMPTY_HT";

        std::stringstream ss;
        if (is_local)
            ss << "LOCAL: ";

        if (is_local)
        {
            auto const & d = std::get<RuntimeFilterVal>(data);
            ss << d.dump();
            return ss.str();
        }
        else
        {
            auto const & d = std::get<InternalDynamicData>(data);
            return "range:" + d.range.dump() + " bf:" + d.bf.dump() + " set:" + d.set.dump();
        }
    }
};

class RuntimeFilterBuilder;
using RuntimeFilterBuilderPtr = std::shared_ptr<RuntimeFilterBuilder>;

class RuntimeFilterBuilder
{
public:
    explicit RuntimeFilterBuilder(const OptimizerSettings & settings, const LinkedHashMap<String, RuntimeFilter> & runtime_filters_);

    UInt32 getId() const { return builder_id; }

    const LinkedHashMap<String, RuntimeFilter> & getRuntimeFilters() const { return runtime_filters; }
    bool isLocal(const String & name) {
        return runtime_filters.at(name).distribution == RRuntimeFilter::LOCAL;
    }

    RuntimeFilterData merge(std::map<UInt32, RuntimeFilterData> && data_sets) const;
    std::unordered_map<RuntimeFilterId, InternalDynamicData> extractDistributedValues(RuntimeFilterData && data) const;

private:
    LinkedHashMap<String, RuntimeFilter> runtime_filters;
    UInt32 builder_id;
    bool enable_range_cover = false;
};
}
