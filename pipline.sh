#!/bin/bash

set -e

mkdir -p /ClickHouse_new/build
cd /ClickHouse_new/build
date > t11
#cmake .. -DCMAKE_BUILD_TYPE=Release
#ninja -j 16
