#include "RpcChannelPool.h"
#include <brpc/options.pb.h>
#include <fmt/format.h>

namespace DB
{

void RpcChannelPool::createExpireTimer()
{
    std::unique_lock<std::shared_mutex> lock(pool_mutex);
    if (rpc_channel_pool_check_interval_seconds > 0 && rpc_channel_pool_expired_seconds > 0)
    {
        expireThread = std::make_unique<std::thread>([this]() {
            while (this->rpc_channel_pool_check_interval_seconds > 0 && this->rpc_channel_pool_expired_seconds > 0)
            {
                std::unique_lock<std::mutex> lock(mutex);
                if (exit)
                    break;
                const auto timeout = std::chrono::seconds(this->rpc_channel_pool_check_interval_seconds);
                cv.wait_for(lock, timeout);
                this->checkAndClearExpiredPool(BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
                this->checkAndClearExpiredPool(BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY);
            }
        });
    }
}

void RpcChannelPool::destroyExpireTimer()
{
    if (expireThread.get())
    {
        try
        {
            std::unique_lock<std::mutex> lock(mutex);
            exit = true;
            cv.notify_all();
            lock.unlock();

            expireThread->join();
            expireThread.reset();
        } catch (...)
        {
            expireThread.reset();
        }
    }
}

size_t RpcChannelPool::checkAndClearExpiredPool(const std::string & client_type)
{
    size_t expired_num = 0;
    time_t current_ts = time(nullptr);
    std::unique_lock<std::shared_mutex> lock(pool_mutex);

    auto & host_port_pool = channel_pool[client_type].host_port_pool;

    std::vector<HostPort> expired_host_ports;
    for (auto & it : host_port_pool)
    {
        if (current_ts > it.second->getRecentUsedTime() + static_cast<long>(rpc_channel_pool_expired_seconds))
        {
            expired_host_ports.emplace_back(it.first);
            ++expired_num;
        }
    }
    for (const auto & host_port : expired_host_ports)
        host_port_pool.erase(host_port);

    if (expired_num != 0)
        LOG_TRACE(log, "Check {} ChannelPool, expired_num is {}.", client_type, expired_num);
    return expired_num;
}

std::shared_ptr<RpcClient> RpcChannelPool::getClient(const String & host_port, const std::string & client_type, bool refresh)
{
    static thread_local std::unordered_map<ClientType, PoolOptionsPtr> local_options_pool;

    auto iter = channel_pool.find(client_type);
    if (unlikely(iter == channel_pool.end()))
    {
        //TODO: Why print message by std cout
        std::cout << Poco::DateTimeFormatter::format(Poco::DateTime(), "%Y.%m.%d %H:%M:%S.%i") << " <Warning> "
                  << "RpcChannelPool::getClient "
                  << "The given client_type:<< client_type << is not in config! <rpc_default> config will be taken.";
        iter = channel_pool.find(BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        if (iter == channel_pool.end())
        {
            std::cout << Poco::DateTimeFormatter::format(Poco::DateTime(), "%Y.%m.%d %H:%M:%S.%i") << " <Error> "
                      << "RpcChannelPool::getClient "
                      << "<rpc_default> is not in config. The default config should be taken but it's not. Check "
                         "BrpcChannelPoolConfigHolder::createTypedConfig. Return nullptr.";
            return nullptr;
        }
    }

    auto local_iter = local_options_pool.find(client_type);
    if (local_iter == local_options_pool.end() || local_iter->second != iter->second.pool_options)
    {
        local_options_pool.emplace(client_type, std::atomic_load(&iter->second.pool_options));
        local_iter = local_options_pool.find(client_type);
    }
    auto & pool_options = local_iter->second;
    auto max_connections = pool_options->max_connections;

    PoolPtr pool = nullptr;
    {
        std::unique_lock<std::shared_mutex> lock(pool_mutex);
        auto & host_pool = iter->second.host_port_pool;
        auto host_it = host_pool.find(host_port);
        if (host_it != host_pool.end())
        {
            pool = host_it->second;
            pool->updateRecentUsedTime();
        }
        else
        {
            pool = std::make_shared<Pool>(max_connections);
            host_pool.emplace(host_port, pool);
        }
    }

    if (likely(pool_options->load_balancer == "rr")) // round robin
    {
        auto & pool_clients = pool->clients;
        auto index = pool->counter % max_connections;
        pool->counter = (index + 1) % max_connections;
        auto client = std::atomic_load(&pool_clients.at(index));
        if (client && client->ok())
        {
            return client;
        }
        else
        {
            auto & connection_pool_options = pool_options->channel_options;
            if (static_cast<brpc::ConnectionType>(connection_pool_options.connection_type) == brpc::ConnectionType::CONNECTION_TYPE_SINGLE)
            {
                connection_pool_options.connection_group = pool_options->pool_name + "_" + std::to_string(index);
            }

            client = std::make_shared<RpcClient>(
                host_port,
                [pool = std::move(pool)]() -> void { pool->ok_.store(false, std::memory_order_relaxed); },
                &connection_pool_options);
            std::atomic_store(&pool_clients[index], client);
            return client;
        }
    }
    else
    {
        // TODO: other load balancer
        std::cout << Poco::DateTimeFormatter::format(Poco::DateTime(), "%Y.%m.%d %H:%M:%S.%i") << " <Warning> "
                  << "RpcChannelPool::getClient "
                  << "The given load balancer is not defined. Return nullptr.";
    }
    return nullptr;
}

} // namespace DB
