#include "PlanSegmentProcessList.h"
#include <base/time.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Query/Common/OptimizerContext.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int QUERY_WAS_CANCELLED;
    extern const int LOGICAL_ERROR;
}

PlanSegmentProcessList::EntryPtr PlanSegmentProcessList::insertGroup(ContextMutablePtr query_context, size_t segment_id, bool force)
{
    std::vector<size_t> segment_ids{segment_id};
    auto entries = insertGroup(query_context, segment_ids, force);
    return entries[0];
}

std::vector<PlanSegmentProcessList::EntryPtr>
PlanSegmentProcessList::insertGroup(ContextMutablePtr query_context, std::vector<size_t> & segment_ids, bool force)
{
    auto & settings = query_context->getSettingsRef();
    auto optimizer_context = query_context->getOptimizerContext();
    auto & optimizer_settings = optimizer_context->getSettingsRef();
    auto address = optimizer_context->getCoordinatorAddress();
    const auto & client_info = query_context->getClientInfo();
    const String & initial_query_id = client_info.initial_query_id;
    const String & coordinator_address = extractExchangeHostPort(*address);
    bool is_internal_query = query_context->isInternalQuery();
    bool need_wait_cancel = false;

    auto initial_query_start_time_ms = query_context->getClientInfo().initial_query_start_time_microseconds;
    {
        auto segment_group = getGroup(initial_query_id);
        if (segment_group
            && (segment_group->coordinator_address != coordinator_address
                || segment_group->initial_query_start_time_ms != initial_query_start_time_ms))
        {
            if (!force && (!settings.replace_running_query || segment_group->initial_query_start_time_ms > initial_query_start_time_ms))
                throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
                    "Distributed query with id = {} is already running.", initial_query_id);

            LOG_WARNING(
                logger,
                "Distributed query with id = {} will be replaced by other coordinator: {}",
                initial_query_id,
                address->toString());

            need_wait_cancel = tryCascadeCancel(segment_group, false);
        }
    }

    // TODO: Check running query, see void ProcessList::checkRunningQuery()
    // if (!is_internal_query && !optimizer_context->getProcessListEntry())
    //     query_context->getProcessList().checkRunningQuery(query_context, false, force);

    if (need_wait_cancel)
    {
        std::unique_lock lock(mutex);
        auto replace_running_query_max_wait_ms = settings.replace_running_query_max_wait_ms.totalMilliseconds();
        if (!replace_running_query_max_wait_ms
            || !remove_group.wait_for(lock, std::chrono::milliseconds(replace_running_query_max_wait_ms), [&] {
                    bool inited = false;
                    auto it = initail_query_to_groups.find(initial_query_id);
                    if ( it != initail_query_to_groups.end() && it->second->coordinator_address == coordinator_address)
                    {
                        inited = true;
                    }                            
                    return inited;
               }))
        {
            throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
                "Distributed query with id = {} is already running and can't be stopped", initial_query_id);
        }
    }

    PlanSegmentGroupPtr segment_group;
    {
        std::unique_lock<std::shared_mutex> lock(query_mutex);
        auto it = initail_query_to_groups.find(initial_query_id);
        if (it != initail_query_to_groups.end())
        {
            if (it->second->coordinator_address == coordinator_address &&
                it->second->initial_query_start_time_ms == initial_query_start_time_ms)
            {
                bool emplace = it->second->emplace_null(segment_ids);
                if (emplace)
                    segment_group = it->second;
            }
        }
    }

    if (!segment_group)
    {
        bool use_query_memory_tracker
            = optimizer_settings.exchange_use_query_memory_tracker && (segment_ids.size() != 1 || segment_ids[0] != 0);
        size_t queue_bytes = optimizer_settings.exchange_queue_bytes;
        segment_group = std::make_shared<PlanSegmentGroup>(
            initial_query_id,
            coordinator_address,
            initial_query_start_time_ms,
            use_query_memory_tracker,
            queue_bytes,
            is_internal_query);
        segment_group->emplace_null(segment_ids);

        std::unique_lock<std::shared_mutex> lock(query_mutex);
        initail_query_to_groups.emplace(initial_query_id, segment_group);
    }
    // No need sub query and parent_initial_query_id
    // if (!parent_initial_query_id.empty())
    // {
    //     auto parent_segment_group = getGroup(parent_initial_query_id);
    //     // It's not important to find a real parent.
    //     if (parent_segment_group)
    //         parent_segment_group->addChildQuery(initial_query_id);
    // }

    std::vector<EntryPtr> entries;
    for (size_t segment_id : segment_ids)
    {
        auto entry = std::make_shared<PlanSegmentProcessListEntry>(*this, segment_group, initial_query_id, segment_id);
        entry->setMemoryController(segment_group->memory_controller);
        entries.emplace_back(std::move(entry));
    }
    return entries;
}

void PlanSegmentProcessList::insertProcessList(
    EntryPtr plan_segment_process_entry, size_t segment_id, ContextMutablePtr query_context, bool force)
{
    ProcessList::EntryPtr entry;
    auto context_process_list_entry = query_context->getOptimizerContext()->getProcessListEntry().lock();

    LOG_TRACE(logger, "Insert process list, context entry is null {}, segment_id {}, force {}",
        context_process_list_entry == nullptr, segment_id, force);

    if (context_process_list_entry)
        entry = std::move(context_process_list_entry);
    else
    {
        /// TODO wujianchao handle it
        Stopwatch start_watch{CLOCK_MONOTONIC};
        ParserSelectQuery parser;
        String default_query = "SELECT 1";
        auto default_ast = parseQuery(parser, default_query, 0, 0, 0);
        entry = query_context->getProcessList().insert(default_query, default_ast.get(), query_context, start_watch.getStart());
    }

    plan_segment_process_entry->setQueryStatus(entry->getQueryStatus());
    const auto segment_group = plan_segment_process_entry->getPlanSegmentGroup();
    bool exist = segment_group->modify(segment_id, std::move(entry));
    if (!exist)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Distributed query {}@{}@{} doesn't contain segment_id {}",
                query_context->getInitialQueryId(),
                segment_group->coordinator_address,
                segment_group->initial_query_start_time_ms,
                segment_id);
}

bool PlanSegmentGroup::tryCancel(bool internal)
{
    std::vector<std::shared_ptr<ProcessListEntry>> to_cancel;
    std::unique_lock lock(mutex);
    for (auto & it : segment_queries)
    {
        if (it.second)
            to_cancel.push_back(it.second);
    }
    lock.unlock();

    if (!is_cancelling)
    {
        is_cancelling = true;

        for (auto & entry : to_cancel)
        {
            entry->getQueryStatus()->cancelQuery(true, internal);
        }
    }

    return !to_cancel.empty();
}

void PlanSegmentGroup::addChildQuery(const String & child_initial_query_id)
{
    std::unique_lock lock(mutex);
    children_initial_query_id.emplace(child_initial_query_id);
}

std::set<String> PlanSegmentGroup::getChildrenQuery()
{
    std::unique_lock lock(mutex);
    return children_initial_query_id;
}

bool PlanSegmentProcessList::remove(std::string initial_query_id, size_t segment_id)
{
    auto segment_group = getGroup(initial_query_id);
    if (segment_group)
    {
        if (segment_group->use_query_memory_tracker)
            segment_group->memory_tracker.logPeakMemoryUsage();
        segment_group->erase(segment_id);

        LOG_TRACE(
            logger,
            "Remove segment {} for distributed query {}@{}@{} from PlanSegmentProcessList",
            segment_id,
            initial_query_id,
            segment_group->coordinator_address,
            segment_group->initial_query_start_time_ms);

        if (segment_group->empty())
        {
            // size_t num_erased
            //     = initail_query_to_groups.erase_if(initial_query_id, [](const Container::value_type & v) { return v.second->empty(); });
            size_t num_erased{0};
            {
                std::unique_lock<std::shared_mutex> lock(query_mutex);
                auto it = initail_query_to_groups.find(initial_query_id);
                if (it != initail_query_to_groups.end() && it->second->empty())
                {
                    initail_query_to_groups.erase(it);
                    num_erased = 1;
                }
            }
            LOG_TRACE(
                logger,
                "Remove {} segment group for distributed query {}@{}@{}",
                num_erased,
                initial_query_id,
                segment_group->coordinator_address,
                segment_group->initial_query_start_time_ms);

            if (num_erased)
                remove_group.notify_all();
        }
        return true;
    }

    LOG_ERROR(logger, "Logical error: Cannot found query: {} in PlanSegmentProcessList", initial_query_id);
    return false;
}

CancellationCode PlanSegmentProcessList::tryCancelPlanSegmentGroup(const String & initial_query_id, String coordinator_address)
{
    auto res = CancellationCode::CancelSent;
    bool found = false;

    auto segment_group = getGroup(initial_query_id);
    if (segment_group)
    {
        if (coordinator_address.empty() || segment_group->coordinator_address == coordinator_address)
        {
            found = tryCascadeCancel(segment_group, true);
            LOG_DEBUG(
                logger,
                "Try cancel for distributed query[{}@{}@{}] from PlanSegmentProcessList, result is {}",
                initial_query_id,
                coordinator_address,
                segment_group->initial_query_start_time_ms,
                found);
        }
        else
        {
            LOG_WARNING(
                logger,
                "Fail to cancel distributed query[{}@{}@{}], coordinator_address doesn't match, seg coordinator address is {}",
                initial_query_id,
                coordinator_address,
                segment_group->coordinator_address,
                segment_group->initial_query_start_time_ms);
            return CancellationCode::CancelCannotBeSent;
        }
    }

    if (!found)
    {
        res = CancellationCode::NotFound;
    }
    return res;
}

bool PlanSegmentProcessList::tryCascadeCancel(PlanSegmentGroupPtr segment_group, bool internal)
{
    bool found = segment_group->tryCancel(internal);
    auto ids = segment_group->getChildrenQuery();
    for (const auto & id : ids)
    {
        auto child_segment_group = getGroup(id);
        child_segment_group->tryCancel(internal);
    }
    return found;
}

PlanSegmentGroupPtr PlanSegmentProcessList::getGroup(const String & initial_query_id)
{
    std::shared_lock<std::shared_mutex> lock(query_mutex);
    PlanSegmentGroupPtr segment_group;
    auto it = initail_query_to_groups.find(initial_query_id);
    if (it != initail_query_to_groups.end())
        segment_group = it->second;
    return segment_group;
}

PlanSegmentProcessListEntry::PlanSegmentProcessListEntry(
    PlanSegmentProcessList & parent_, PlanSegmentGroupPtr segment_group_, String initial_query_id_, size_t segment_id_)
    : parent(parent_), segment_group(segment_group_), initial_query_id(std::move(initial_query_id_)), segment_id(segment_id_)
{
}

PlanSegmentProcessListEntry::~PlanSegmentProcessListEntry()
{
    parent.remove(initial_query_id, segment_id);
}

void PlanSegmentProcessListEntry::prepareQueryScope(ContextMutablePtr query_context)
{
    query_scope.emplace(query_context);
}

}
