#include "OptimizerContext.h"
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Coordination/Defines.h>
#include <Coordination/KeeperConstants.h>
#include <Server/CloudPlacementInfo.h>
#include <Query/Statistics/StatisticsKeeperStore.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskSelector.h>
#include <IO/S3/Credentials.h>
#include <Interpreters/Context.h>
#include <Query/Common/OptimizerSettings.h>
#include <Query/Executor/SegmentScheduler.h>
#include <Query/Executor/PlanSegmentProcessList.h>
#include <Query/Statistics/StringHash.h>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;

}

OptimizerContextData::OptimizerContextData() = default;
OptimizerContextData::OptimizerContextData(const OptimizerContextData &) = default;

OptimizerContext::OptimizerContext() = default;
OptimizerContext::OptimizerContext(const OptimizerContext & rhs) : OptimizerContextData(rhs) {}

OptimizerContext::OptimizerContext(const Settings & settings_, const Poco::Util::AbstractConfiguration & config)
{
    shared = std::make_shared<OptimizerContextSharedData>();

    optimizer_settings.loadFromConfig("optimizer", config);
    if (settings_.max_execution_time.totalSeconds() != 0)
        query_max_execution_time = std::min(settings_.max_execution_time.totalSeconds() * UInt64(1000), UInt64(UINT32_MAX));
    else if (optimizer_settings.exchange_timeout_ms != 0)
        query_max_execution_time = std::min(UInt64(optimizer_settings.exchange_timeout_ms), UInt64(UINT32_MAX));
    else
        query_max_execution_time = 300 * 1000; // default 300 seconds
    initQueryExpirationTimeStamp();
    setPlanSegmentProcessList(std::make_shared<PlanSegmentProcessList>());
}

void OptimizerContext::setQueryMaxExecutionTime(UInt32 milli_second)
{
    query_max_execution_time = milli_second;
    initQueryExpirationTimeStamp();
}

UInt32 OptimizerContext::getQueryMaxExecutionTime() const
{
    return query_max_execution_time;
}

TimePoint OptimizerContext::getQueryExpirationTimeStamp() const
{
    return query_expiration_timestamp;
}

void OptimizerContext::initQueryExpirationTimeStamp()
{
    query_expiration_timestamp = std::chrono::system_clock::now() + std::chrono::milliseconds(query_max_execution_time);
}

void OptimizerContext::initPlanSegmentExceptionHandler()
{
    plan_segment_exception_handler = std::make_shared<ExceptionHandler>();
}

ExceptionHandlerPtr OptimizerContext::getPlanSegmentExceptionHandler() const
{
    return plan_segment_exception_handler;
}

void OptimizerContext::setCoordinatorAddress(const AddressInfoPtr address)
{
    coordinator_address = address;
}

AddressInfoPtr OptimizerContext::getCoordinatorAddress() const
{
    return coordinator_address;
}

void OptimizerContext::setRPCPort(UInt16 rpc_port_)
{
    rpc_port = rpc_port_;
}

UInt16 OptimizerContext::getRPCPort()
{
    return rpc_port;
}

void OptimizerContext::setPlanSegmentProcessListEntry(PlanSegmentProcessListEntryPtr segment_process_list_entry_)
{
    segment_process_list_entry = segment_process_list_entry_;
}

PlanSegmentProcessListEntryPtr OptimizerContext::getPlanSegmentProcessListEntry() const
{
    return segment_process_list_entry;
}

void OptimizerContext::setPlanSegmentProcessList(PlanSegmentProcessListPtr segment_process_list_)
{
    shared->plan_segment_process_list = segment_process_list_;
}

PlanSegmentProcessListPtr OptimizerContext::getPlanSegmentProcessList() const
{
    return shared->plan_segment_process_list;
}

void OptimizerContext::setProcessListEntry(ProcessListEntryPtr process_list_entry_)
{
    process_list_entry = process_list_entry_;
}

ProcessListEntryPtr OptimizerContext::getProcessListEntry() const
{
    return process_list_entry;
}

void OptimizerContext::setProcessListElement(QueryStatusPtr elem)
{
    query_process_element = elem;
}

QueryStatusPtr OptimizerContext::getProcessListElement() const
{
    return query_process_element;
}

void OptimizerContext::setSendTCPProgress(std::function<void()> callback)
{
    send_tcp_progress = callback;
}

std::function<void()> OptimizerContext::getSendTCPProgress() const
{
    return send_tcp_progress;
}

void OptimizerContext::setPlanSegmentInstanceID(const PlanSegmentInstanceID & instance_id)
{
    plan_segment_instance_id = instance_id;
}

PlanSegmentInstanceID OptimizerContext::getPlanSegmentInstanceID()
{
    return plan_segment_instance_id;
}

void OptimizerContext::setIsExplainQuery(const bool & is_explain_query_)
{
    is_explain_query = is_explain_query_;
}

bool OptimizerContext::isExplainQuery() const
{
    return is_explain_query;
}

QueryExchangeLogPtr OptimizerContext::getQueryExchangeLog()
{
    return shared->query_exchange_log;
}

void OptimizerContext::logOptimizerProfile(LoggerPtr log, String prefix, String name, UInt64 time, bool is_rule)
{
    if (optimizer_settings.log_optimizer_run_time && log)
        LOG_DEBUG(log, "{} {} {}", prefix, name, time);

    if (optimizer_profile)
        optimizer_profile->setTime(name,  std::to_string(time), is_rule);
}

void OptimizerContext::setPlanCacheManager(std::unique_ptr<PlanCacheManager> && manager)
{
    //todo: zhangdongdong92, other feat: need a part shared lock
    //auto lock = getLock(); // checked
    shared->plan_cache_manager = std::move(manager);
}

PlanCacheManager* OptimizerContext::getPlanCacheManager()
{
    //todo: zhangdongdong92, other feat: need a part shared lock
    //auto lock = getLock(); // checked
    return shared->plan_cache_manager ? shared->plan_cache_manager.get() : nullptr;
}

void OptimizerContext::setStatisticsKeeperStore(StatisticsKeeperStorePtr statistics_keeper_store_ptr, ContextMutablePtr)
{
    std::lock_guard lock(mutex);
    if (shared->statistics_keeper_store)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "statistics keeper store thread has already been initialized");
    shared->statistics_keeper_store = std::move(statistics_keeper_store_ptr);
    shared->statistics_keeper_store->startup();

    // auto job = makeLoadJob(
    // {},
    // TablesLoaderBackgroundStartupPoolId,
    // "startup statistics keeper store worker",
    // [this] (AsyncLoader &, const LoadJobPtr &)
    // {
    //     std::lock_guard lock2(mutex);
    //     shared->statistics_keeper_store->startup();
    // });
    //
    // shared->ddl_worker_startup_task = makeLoadTask(context->getAsyncLoader(), {job});
    // shared->ddl_worker_startup_task->schedule();
}

StatisticsKeeperStorePtr OptimizerContext::getStatisticsKeeperStore()
{
    std::lock_guard lock(mutex);
    return shared->statistics_keeper_store ? shared->statistics_keeper_store : nullptr;
}

HostWithPorts OptimizerContext::getHostWithPorts() const
{
    //todo: zhangdongdong92, other feat: need impl, now just a fake impl
    HostWithPorts host;
    return host;
}

void OptimizerContext::setTransactionID(UInt64 txt_id_)
{
    txt_id = txt_id_;
}

// todo: lizhuoyu5, Currently, there is no transaction ID available, so we are temporarily using the hash value of the query_id instead.
// todo: In the future, the transaction ID can be obtained from Keeper.
UInt64 OptimizerContext::getTransactionID(std::string_view query_id)
{
    return QueryStatistics::stringHash64(query_id);
}

std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> OptimizerContext::getProcessorProfileElementConsumer() const
{
    //todo: zhangdongdong92, other feat: need impl, now just a fake impl
    std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> processor_log_element_consumer;
    return processor_log_element_consumer;
}


SegmentSchedulerPtr OptimizerContext::getSegmentScheduler() const
{
    // todo: hongzhigao1, other feat: need a shared->mutex
    if (!shared->segment_scheduler)
        shared->segment_scheduler = std::make_shared<SegmentScheduler>();
    return shared->segment_scheduler;
}

StatisticsMemoryStorePtr OptimizerContext::getStatisticsMemoryStore()
{
    //todo: zhangdongdong92, other feat: need a part shared lock
    // auto lock = getLocalLock();

    if (!this->stats_memory_store)
    {
        this->stats_memory_store = std::make_shared<QueryStatistics::StatisticsMemoryStore>();
    }
    return stats_memory_store;
}

void OptimizerContext::setComplexQueryActive(bool complex_query_active_)
{
    shared->complex_query_active = complex_query_active_;
}

bool OptimizerContext::getComplexQueryActive()
{
    return shared->complex_query_active;
}

String OptimizerContext::getOptimizerProfile(bool print_rule)
{
    if (optimizer_profile)
    {
        String profile = optimizer_profile->getOptimizerProfile(print_rule);
        clearOptimizerProfile();
        return profile;
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "OptimizerProfile is not initialized");
}

void OptimizerContext::clearOptimizerProfile()
{
    if (!optimizer_profile)
        return;
    optimizer_profile->clear();
    optimizer_profile = nullptr;
}

void OptimizerContext::setSetting(std::string_view name, const String & value)
{
    std::lock_guard lock(mutex);
    optimizer_settings.set(name, value);
}

void OptimizerContext::setSetting(std::string_view name, const Field & value)
{
    std::lock_guard lock(mutex);
    optimizer_settings.set(name, value);
}

size_t OptimizerContext::getWorkerSize() const
{
    return cluster == nullptr ? 1 : cluster->getShardCount();
}
String OptimizerContext::getClusterName() const
{
    return cluster == nullptr ? "" : cluster->getName();
}

}
