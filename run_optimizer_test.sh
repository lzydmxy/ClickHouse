#!/usr/bin/env bash

./build/src/unit_tests_dbms trace --gtest_filter="HostWithPorts*"
./build/src/unit_tests_dbms trace --gtest_filter="LinkedHashMap*"
./build/src/unit_tests_dbms trace --gtest_filter="RuntimeFilter*"
./build/src/unit_tests_dbms trace --gtest_filter="NodeSelectorTest*"
./build/src/unit_tests_dbms trace --gtest_filter="NameResolutionTest*"
./build/src/unit_tests_dbms trace --gtest_filter="JSONSuite*"
./build/src/unit_tests_dbms trace --gtest_filter="ASTEqualsTest*"

# cmake -S . -B build -DENABLE_CCACHE=1 -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=YES -DENABLE_TESTS=ON

# list all tests(11272 tests from 242 test suites.)
# ./build/src/unit_tests_dbms --gtest_list_tests

# ./build/src/unit_tests_dbms \
# --gtest_filter="HostWithPorts*:LinkedHashMap*:RuntimeFilter*:NodeSelectorTest*:NameResolutionTest*:JSONSuite*" \
# --gtest_output=xml:clickhouse_tests_detail.xml # save to file

# ./build/src/unit_tests_dbms \
# --gtest_filter="Parser*:Exchange*:QueryPlan*:Processors*:TestActions*:BucketShuffle*:RPCchannelPool*"
