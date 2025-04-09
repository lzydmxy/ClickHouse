#include "gtest_exchange_helper.h"
#include <brpc/server.h>

brpc::Server * ExchangeRemoteTest::server = new brpc::Server;
DB::BrpcExchangeReceiverRegistryService * ExchangeRemoteTest::service_impl = new DB::BrpcExchangeReceiverRegistryService(73400320);
