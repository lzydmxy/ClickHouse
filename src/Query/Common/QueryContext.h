#pragma once
#include <Poco/Util/AbstractConfiguration.h>
#include <Query/Common/ExceptionHandler.h>

namespace DB
{

struct QuerySettings;
using QuerySettingsPtr = std::shared_ptr<QuerySettings>;
class PlanSegmentProcessList;
class PlanSegmentProcessListEntry;
using PlanSegmentProcessListEntryPtr = std::shared_ptr<PlanSegmentProcessListEntry>;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

class QueryContext
{
public:
    QueryContext(QuerySettingsPtr query_settings_);

    const QuerySettingsPtr & getQuerySettings() const;

    void initPlanSegmentExHandler();
    ExceptionHandlerPtr getPlanSegmentExHandler() const;

    void setPlanSegmentProcessListEntry(PlanSegmentProcessListEntryPtr segment_process_list_entry_);
    PlanSegmentProcessList & getPlanSegmentProcessList();

    void setProcessListElement(QueryStatusPtr elem);
private:
    QuerySettingsPtr query_settings;
};

using QueryContextPtr = std::shared_ptr<QueryContext>;

}
