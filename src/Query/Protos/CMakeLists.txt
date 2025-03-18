PROTOBUF_GENERATE_CPP(dbms_query_proto_srcs dbms_query_proto_headers 
    common.proto    
    plan_node.proto
    plan_segment_service.proto
    runtime_filter.proto
    registry.proto
)

# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(clickhouse_query_protos ${dbms_query_proto_headers} ${dbms_query_proto_srcs})
target_include_directories(clickhouse_query_protos SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (clickhouse_query_protos PUBLIC ch_contrib::protobuf ch_contrib::brpc)
