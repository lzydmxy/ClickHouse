#pragma once

#include <utility>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Query/Common/LinkedHashMap.h>

namespace DB
{
using Assignment = std::pair<String, ConstASTPtr>;
// using Assignments = std::vector<Assignment>;
class Assignments : public LinkedHashMap<String, ConstASTPtr>
{
public:
    using LinkedHashMap::LinkedHashMap;
    Assignments copy() const;
};
}
