clickhouse_add_executable (exchange_test exchange_remote_helper.cpp exchange_test_tool.cpp)

# target_link_libraries (exchange_test PUBLIC

# )

target_link_libraries (exchange_test PRIVATE
    dbms
    loggers
    clickhouse_functions
    clickhouse_common_io
    ch_contrib::protobuf
    ch_contrib::brpc
    ch_contrib::llvm
)
