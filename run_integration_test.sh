#!/usr/bin/env bash

function build_clickhouse_binary() {
    # 1、build binary
    docker run -d \
        -e OUTPUT_DIR=/workdir/ClickHouse/build \
        -e DEB_ARCH=amd64 \
        -e CC=clang-18 \
        -e CXX=clang++-18 \
        -e BUILD_TYPE=None \
        -e CMAKE_FLAGS="$CMAKE_FLAGS -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 -DCOMPILER_CACHE=disabled" \
        -e BUILD_TARGET='clickhouse-bundle' \
        --workdir=/workdir/ClickHouse \
        --volume=${WORKDIR}:/workdir/ClickHouse \
        hub.jdcloud.com/cnp/byconity:dev-env_debian_allways_v3 /bin/bash -c 'git config --global --add safe.directory /workdir/ClickHouse && cmake -S . -B build && cmake --build build'
}

function exec_integration_test() {
    # 2、entrypoint
    export CASES_DIR=${WORKDIR}/tests/integration
    export UTILS_DIR=${WORKDIR}/utils
    export CLICKHOUSE_ROOT=${WORKDIR}/build/programs
    export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=${WORKDIR}/programs/server
    export CLICKHOUSE_TESTS_SERVER_BIN_PATH=${WORKDIR}/build/programs/clickhouse
    export CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH=${WORKDIR}/build/programs/clickhouse-odbc-bridge

    cd ${WORKDIR}/tests/integration || exit
    if [ "$1" = "test" ]; then
        shift
    fi

    # debug integration test runner env
    # tests/integration/runner --command=bash
    ./runner --docker-image-version=fixed_latest "$@"
}

function main() {
    export WORKDIR=${PWD}

    case "$1" in
    #     build)
    #         shift
    #         build_clickhouse_binary "$@"
    #         exit 0
    #         ;;
        *)
            exec_integration_test "$@"
            ;;
    esac
}

main "$@"

#import
# hub.jdcloud.com/cnp/byconity:dev-env_debian_allways_v3 #build clickhouse binary
# hub.jdcloud.com/cnp/integration-tests-runner:fixed_latest #intgration test