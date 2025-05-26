#pragma once

#include <Common/Logger.h>
#include <Query/Common/QueryCommon.h>
#include <Query/Common/ExceptionHandler.h>
#include <Query/Common/OptimizerSettings.h>
#include <Query/Processors/QueryPlan/PlanNodeIdAllocator.h>
#include <Query/Planner/SymbolAllocator.h>
#include <Query/Optimizer/OptimizerMetrics.h>
#include <Query/Processors/QueryPlan/PlanCache.h>
#include <Query/Optimizer/OptimizerProfile.h>
#include <Query/Statistics/StatisticsMemoryStore.h>

namespace DB
{

class ExceptionHandler;
using ExceptionHandlerPtr = std::shared_ptr<ExceptionHandler>;
class PlanSegmentProcessList;
using PlanSegmentProcessListPtr = std::shared_ptr<PlanSegmentProcessList>;
class PlanSegmentProcessListEntry;
using PlanSegmentProcessListEntryPtr = std::shared_ptr<PlanSegmentProcessListEntry>;
using PlanSegmentProcessListEntryWeakPtr = std::weak_ptr<PlanSegmentProcessListEntry>;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class HostWithPorts;

class SegmentScheduler;
using SegmentSchedulerPtr = std::shared_ptr<SegmentScheduler>;

class AddressInfo;
using AddressInfoPtr = std::shared_ptr<AddressInfo>;

class ProcessListEntry;
using ProcessListEntryPtr = std::shared_ptr<ProcessListEntry>;

struct ProcessorProfileLogElement;
template <typename>
class ProfileElementConsumer;

class QueryExchangeLog;
using QueryExchangeLogPtr = std::shared_ptr<QueryExchangeLog>;

class OptimizerMetrics;
using OptimizerMetricsPtr = std::shared_ptr<OptimizerMetrics>;

class SegmentScheduler;
using SegmentSchedulerPtr = std::shared_ptr<SegmentScheduler>;

class PlanCacheManager;

struct Settings;
struct PlanSegmentInstanceID;

using  StatisticsMemoryStorePtr = std::shared_ptr<QueryStatistics::StatisticsMemoryStore>;
class OptimizerContextData;

using ExcludedRules = std::unordered_set<UInt32>;
using ExcludedRulesMap = std::unordered_map<PlanNodeId, ExcludedRules>;

enum ServiceType
{
    standalone,
    node,
    master,
    tso
};

class OptimizerContext
{
public:
    OptimizerContext(const Settings & settings_, const Poco::Util::AbstractConfiguration & config);

    const OptimizerSettings & getSettingsRef() const { return optimizer_settings; }
    OptimizerSettings & getSettingsRef() { return optimizer_settings; }
    const OptimizerSettings getSettings() const { return optimizer_settings; }

    /// milliseconds
    UInt32 getQueryMaxExecutionTime() const;
    TimePoint getQueryExpirationTimeStamp() const;
    void initQueryExpirationTimeStamp();

    void initExceptionHandler();
    ExceptionHandlerPtr getExceptionHandler() const;

    void setCoordinatorAddress(const AddressInfoPtr address);
    AddressInfoPtr getCoordinatorAddress() const;

    void setRPCPort(UInt16 rpc_port_);
    UInt16 getRPCPort();

    void setPlanSegmentProcessListEntry(PlanSegmentProcessListEntryPtr segment_process_list_entry_);
    PlanSegmentProcessListEntryPtr getPlanSegmentProcessListEntry() const;

    void setPlanSegmentProcessList(PlanSegmentProcessListPtr segment_process_list_);
    PlanSegmentProcessListPtr getPlanSegmentProcessList() const;

    void setProcessListEntry(ProcessListEntryPtr process_list_entry_);
    ProcessListEntryPtr getProcessListEntry() const;

    void
    setProcessorProfileElementConsumer(std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> processor_log_element_consumer_);
    std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> getProcessorProfileElementConsumer() const;

    void setProcessListElement(QueryStatusPtr elem);
    QueryStatusPtr getProcessListElement() const;

    void setPlanSegmentInstanceID(const PlanSegmentInstanceID & instance_id);
    PlanSegmentInstanceID getPlanSegmentInstanceID();

    void setSendTCPProgress(std::function<void()> callback);
    std::function<void()> getSendTCPProgress() const;

    HostWithPorts getHostWithPorts() const;
    SegmentSchedulerPtr getSegmentScheduler() const;
    ServiceType getServiceType() const;

    void setIsExplainQuery(const bool & is_explain_query_);
    bool isExplainQuery() const;

    QueryExchangeLogPtr getQueryExchangeLog();

    void addNonDeterministicFunction(const std::string & fun_name, bool within_query_scope) const
    {
        nondeterministic_functions_out_of_query_scope.emplace(fun_name);
        if (within_query_scope)
            nondeterministic_functions_within_query_scope.emplace(fun_name);
    }
    bool isNonDeterministicFunction(const std::string & fun_name) const
    {
        return nondeterministic_functions_within_query_scope.contains(fun_name);
    }
    bool isNonDeterministicFunctionOutOfQueryScope(const std::string & fun_name) const
    {
        return nondeterministic_functions_out_of_query_scope.contains(fun_name);
    }

    void setExecuteSubQueryPath(String path) { graphviz_sub_query_path = std::move(path); }

    void setQueryMaxExecutionTime(UInt32 milli_second);

    String getExecuteSubQueryPath() const
    {
        return graphviz_sub_query_path;
    }
    void removeExecuteSubQueryPath()
    {
        graphviz_sub_query_path = "";
    }

    void setComplexQueryActive(bool complex_query_active);
    bool getComplexQueryActive();

    PlanNodeIdAllocatorPtr & getPlanNodeIdAllocator() { return id_allocator; }
    int incAndGetSubQueryId() { return ++sub_query_id; }
    UInt32 nextNodeId() { return id_allocator->nextId(); }
    void logOptimizerProfile(LoggerPtr log, String prefix, String name, UInt64 time, bool is_rule = false);
    void addQueryPlanInfo(String & query_plan_) { this->query_plan = query_plan_; }
    String getQueryPlan() { return query_plan; }
    void createPlanNodeIdAllocator(int max_id = 1) { id_allocator = std::make_shared<PlanNodeIdAllocator>(max_id); }
    void createSymbolAllocator() { symbol_allocator = std::make_shared<SymbolAllocator>(); }
    OptimizerMetricsPtr & getOptimizerMetrics() { return optimizer_metrics; }
    void createOptimizerMetrics() { optimizer_metrics = std::make_shared<OptimizerMetrics>(); }
    void setPlanCacheManager(std::unique_ptr<PlanCacheManager> && manager);
    void initOptimizerProfile() { optimizer_profile = std::make_unique<OptimizerProfile>(); }
    PlanCacheManager* getPlanCacheManager();
    const SymbolAllocatorPtr & getSymbolAllocator() { return symbol_allocator; }
    StatisticsMemoryStorePtr getStatisticsMemoryStore();

    ExcludedRulesMap & getExcludedRulesMap() { return exclude_rules_map; }

    int getRuleId() const { return rule_id; }
    void setRuleId(int rule_id_) { rule_id = rule_id_; }
    void incRuleId() { ++rule_id; }
	void setTransactionID(UInt64 txt_id_);
	UInt64 getTransactionID();
protected:
    std::shared_ptr<QueryStatistics::StatisticsMemoryStore> stats_memory_store = nullptr;

private:
    OptimizerSettings optimizer_settings;
    UInt32 query_max_execution_time;
    TimePoint query_expiration_timestamp;
    AddressInfoPtr coordinator_address;
    UInt16 rpc_port;
    std::shared_ptr<OptimizerContextData> data;
    ExceptionHandlerPtr exception_handler;
    PlanSegmentProcessListEntryPtr segment_process_list_entry;
    PlanSegmentProcessListPtr plan_segment_process_list;
    ProcessListEntryPtr process_list_entry;
    QueryStatusPtr query_process_element;
    std::function<void()> send_tcp_progress{nullptr};
    bool is_explain_query{false};
    bool complex_query_active{false};
    QueryExchangeLogPtr query_exchange_log;
	UInt64 txt_id{0};
    // make sure a context not be passed to ExprAnalyzer::analyze concurrently
    mutable std::unordered_set<std::string> nondeterministic_functions_within_query_scope;
    mutable std::unordered_set<std::string> nondeterministic_functions_out_of_query_scope;
    PlanNodeIdAllocatorPtr id_allocator = nullptr;
    String query_plan;
    std::shared_ptr<SymbolAllocator> symbol_allocator = nullptr;
    std::shared_ptr<OptimizerMetrics> optimizer_metrics = nullptr;
    std::unique_ptr<PlanCacheManager> plan_cache_manager = nullptr;
    std::shared_ptr<SegmentScheduler> segment_scheduler = nullptr;
    std::shared_ptr<OptimizerProfile> optimizer_profile = nullptr;

    int sub_query_id = 0;
    int rule_id = 3000;
    String graphviz_sub_query_path;

    ExcludedRulesMap exclude_rules_map;
};

using OptimizerContextPtr = std::shared_ptr<OptimizerContext>;

}
