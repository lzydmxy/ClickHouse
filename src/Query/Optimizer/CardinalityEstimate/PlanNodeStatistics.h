

#pragma once

#include <Core/Types.h>
#include <Query/Optimizer/CardinalityEstimate/SymbolStatistics.h>

#include <Poco/JSON/Object.h>

namespace DB
{
class SymbolStatistics;
class PlanNodeStatistics;
using PlanNodeStatisticsPtr = std::shared_ptr<PlanNodeStatistics>;

/**
 * Statistics for a table or query plan step.
 */
class PlanNodeStatistics
{
public:
    PlanNodeStatistics(UInt64 rowCount = 0, std::unordered_map<String, SymbolStatisticsPtr> symbolStatistics = {});

    PlanNodeStatistics(const PlanNodeStatistics &) = delete;

    PlanNodeStatisticsPtr copy() const
    {
        return std::make_shared<PlanNodeStatistics>(row_count, symbol_statistics);
    }

    PlanNodeStatistics & operator+=(const PlanNodeStatistics & other)
    {
        row_count += other.row_count;

        for (auto & symbols_stats : symbol_statistics)
        {
            for (auto & other_symbols_stats : other.symbol_statistics)
            {
                if (symbols_stats.first == other_symbols_stats.first)
                {
                    *symbols_stats.second += *other_symbols_stats.second;
                }
            }
        }
        return *this;
    }

    UInt64 getRowCount() const { return row_count; }
    void setRowCount(UInt64 row_count_) { this->row_count = row_count_; }

    std::unordered_map<String, SymbolStatisticsPtr> & getSymbolStatistics() { return symbol_statistics; }
    const SymbolStatisticsPtr & getSymbolStatistics(const String & symbol);

    void updateRowCount(UInt64 row_count_) { row_count = row_count_; }
    void updateSymbolStatistics(const String & symbol, SymbolStatisticsPtr stats) { symbol_statistics[symbol] = stats; }

    UInt64 getOutputSizeInBytes() const;
    UInt64 getColumnSize(Names symbols) const;

    String toString() const;

    Poco::JSON::Object::Ptr toJson() const;

    void pruneSymbols(NameSet && names)
    {
        auto pm_it = symbol_statistics.begin();
        while (pm_it != symbol_statistics.end())
        {
            if (!names.contains(pm_it->first))
            {
                pm_it = symbol_statistics.erase(pm_it);
            }
            else
            {
                ++pm_it;
            }
        }
    }

private:
    UInt64 row_count;
    std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics;
};

}
