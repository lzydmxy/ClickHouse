#include <Query/Statistics/StatsTableIdentifier.h>
#include <Query/Statistics/StatsTableBasic.h>

namespace DB::QueryStatistics
{
std::optional<DateTime64> getVersion(ContextPtr context, const StatsTableIdentifier & table);
std::shared_ptr<StatsTableBasic> getTableStatistics(ContextPtr context, const StatsTableIdentifier & table);
}
