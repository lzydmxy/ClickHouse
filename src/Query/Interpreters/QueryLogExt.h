#pragma once
#include <Interpreters/QueryLog.h>

namespace DB
{

/// Extend QueryLogElement to add new fields
/// The optional fields in QueryStatusInfo in ProcessList.h do not need to be added here
class QueryLogElementExt : public QueryLogElement
{
public:
    std::shared_ptr<std::vector<String>> segment_profiles;
    Int64 segment_id{};
};

}
