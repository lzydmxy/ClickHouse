# PROTOBUF_GENERATE_CPP(dbms_proto_srcs dbms_proto_headers
#     common.proto
#     #enum.proto    
#     #cnch_common.proto
#     #cnch_server_rpc.proto
#     #cnch_worker_rpc.proto
#     #data_models.proto
#     #data_models_mv.proto
#     #lake_models.proto
#     #data_part.proto
#     #plan_segment_manager.proto
#     #registry.proto
#     #resource_manager_rpc.proto
#     #tso.proto
#     #optimizer_statistics.proto
#     #plan_node_utils.proto
#     #runtime_filter.proto
#     #daemon_manager_rpc.proto
#     #plan_node.proto
    
#     #auto_statistics.proto
#     #disk_cache.proto
#     #distributed_file.proto
#     )


# # Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
# set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# # Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
# set (CMAKE_CXX_CLANG_TIDY "")


# # # Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
# # target_compile_options(clickhouse_protos_lib PRIVATE "-w")

# add_library(clickhouse_protos_lib ${dbms_proto_srcs} ${dbms_proto_headers})
# target_include_directories(clickhouse_protos_lib SYSTEM PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/..")
# target_include_directories (clickhouse_protos_lib SYSTEM BEFORE PRIVATE ${Protobuf_INCLUDE_DIR})
# target_link_libraries (clickhouse_protos_lib PRIVATE ${Protobuf_LIBRARY})

#PROTOBUF_GENERATE_GRPC_CPP(dbms_query_proto_srcs dbms_query_proto_headers common.proto)

PROTOBUF_GENERATE_CPP(dbms_query_proto_srcs dbms_query_proto_headers 
    plan_node.proto
    common.proto
)

# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(dbms_query_protos ${dbms_query_proto_headers} ${dbms_query_proto_srcs})
# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
#target_compile_options(dbms_query_proto PRIVATE "-w")
target_include_directories(dbms_query_protos SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (dbms_query_protos PUBLIC ch_contrib::grpc)
