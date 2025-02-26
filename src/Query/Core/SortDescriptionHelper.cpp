#include <Query/Core/SortDescriptionHelper.h>

namespace DB
{

std::string format(const SortColumnDescription & sort_desc)
{
    return fmt::format(
        "{} {} {}",
        sort_desc.column_name,
        sort_desc.direction == 1 ? "ASC" : "DESC",
        sort_desc.nulls_direction == 0 ? "ANY" :
            (sort_desc.nulls_direction == sort_desc.direction ? "NULLS LAST" : "NULLS FIRST"));
}

}
