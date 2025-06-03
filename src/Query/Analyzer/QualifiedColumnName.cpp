#include <Query/Analyzer/QualifiedColumnName.h>
#include <Functions/FunctionsHashing.h>

namespace DB
{
std::size_t QualifiedColumnName::hash() const
{
    size_t hash = MurmurHash3Impl64::combineHashes(
        MurmurHash3Impl64::apply(database.c_str(), database.size()), MurmurHash3Impl64::apply(table.c_str(), table.size()));
    hash = MurmurHash3Impl64::combineHashes(hash, MurmurHash3Impl64::apply(column.c_str(), column.size()));
    return hash;
}

}
