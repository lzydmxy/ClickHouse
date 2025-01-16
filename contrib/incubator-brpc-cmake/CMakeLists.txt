set(ENABLE_BRPC_DEFAULT ${ENABLE_LIBRARIES})

option(ENABLE_BRPC "Use bRPC" ${ENABLE_BRPC_DEFAULT})

if(NOT ENABLE_BRPC)
  message(STATUS "Not using bRPC")
  return()
endif()

set(_bRPC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/incubator-brpc")
set(_bRPC_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/incubator-bprc")
set(_bRPC_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/incubator-brpc")

set(_gFlags_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/gflags/include")

include(protobuf_generate_brpc.cmake)
include(brpc.cmake)

set(bRPC_LIBRARIES brpc-static)

add_library(_ch_contrib_brpc INTERFACE)
target_link_libraries(_ch_contrib_brpc INTERFACE ${bRPC_LIBRARIES})
target_include_directories(_ch_contrib_brpc SYSTEM INTERFACE ${_bRPC_INCLUDE_DIRS} ${_gFlags_INCLUDE_DIR})
#target_include_directories(_ch_contrib_brpc PUBLIC ${bRPC_INCLUDE_DIRS})
#target_include_directories(_ch_contrib_brpc SYSTEM PUBLIC ${bRPC_INCLUDE_DIRS})
add_library(ch_contrib::brpc ALIAS _ch_contrib_brpc)
