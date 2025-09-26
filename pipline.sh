#!/bin/bash

set -e

cd /ClickHouse_new
# 运行流水线编译的镜像时，将宿主机的`/root/ClickHouse_new`目录挂载到容器中：`-v /root/ClickHouse_new:/root/ClickHouse_new`
rm -rf contrib && ln -s /root/ClickHouse_new/contrib/ contrib
mkdir build
cmake -S . -B build
cmake --build build
objcopy --strip-debug build/programs/clickhouse
chmod 777 build/programs/clickhouse
