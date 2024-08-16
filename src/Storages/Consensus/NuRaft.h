#pragma once
#include <libnuraft/buffer.hxx>
#include <libnuraft/nuraft.hxx>
#include <libnuraft/async.hxx>
#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>


namespace Consensus
{

using RunnerID = uint32_t;

using NuBuffer = nuraft::buffer;
using NuBufferPtr = nuraft::ptr<NuBuffer>;
using NuBuffers = std::vector<NuBufferPtr>;
using NuLogEntry = nuraft::log_entry;
using NuLogEntryPtr = nuraft::ptr<NuLogEntry>;
using NuLogEntries = std::vector<NuLogEntryPtr>;
using NuLogEntriesPtr = nuraft::ptr<NuLogEntries>;
using NuLogStorePtr = nuraft::ptr<nuraft::log_store>;
using NuRaftServer = nuraft::raft_server;
using NuRaftServerPtr = nuraft::ptr<NuRaftServer>;
using NuRaftLauncher = nuraft::raft_launcher;
using NuRaftLauncherPtr = nuraft::ptr<NuRaftLauncher>;

using NuServerConfig = nuraft::srv_config;
using NuServerConfigPtr = nuraft::ptr<NuServerConfig>;
using NuClusterConfig = nuraft::cluster_config;
using NuClusterConfigPtr = nuraft::ptr<NuClusterConfig>;
using NuServerState = nuraft::srv_state;
using NuServerStatePtr = nuraft::ptr<NuServerState>;
using CMDResultPtr = nuraft::ptr<nuraft::cmd_result<NuBufferPtr>>;

using NuSnapshot = nuraft::snapshot;
using NuSnapshotPtr = nuraft::ptr<NuSnapshot>;

template<typename T, typename ... TArgs>
inline nuraft::ptr<T> RaftNew(TArgs&&... args) {
    return nuraft::cs_new<T>(std::forward<TArgs>(args)...);
}

template< typename T, typename TE = nuraft::ptr<std::exception> >
using NuAsyncResult = nuraft::async_result<T,TE>;

using nuraft::cluster_config;
using nuraft::int32;
using nuraft::log_store;
using nuraft::ptr;
using nuraft::srv_config;
using nuraft::srv_state;

}
