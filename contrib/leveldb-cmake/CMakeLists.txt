# Copyright 2017 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.9)
set(_LEVELDB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/leveldb")
set(_LEVELDB_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/leveldb")
# Keep the version below in sync with the one in db.h
#project(leveldb VERSION 1.23.0 LANGUAGES C CXX)

# C standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_C_STANDARD)
    # This project can use C11, but will gracefully decay down to C89.
    set(CMAKE_C_STANDARD 11)
    set(CMAKE_C_STANDARD_REQUIRED OFF)
    set(CMAKE_C_EXTENSIONS OFF)
endif(NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
    # This project requires C++11.
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

if (WIN32)
    set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_WINDOWS)
    # TODO(cmumford): Make UNICODE configurable for Windows.
    add_definitions(-D_UNICODE -DUNICODE)
else (WIN32)
    set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_POSIX)
endif (WIN32)

option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" OFF)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" OFF)
option(LEVELDB_INSTALL "Install LevelDB's header and library" OFF)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

#include(CheckLibraryExists)
#check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
#check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
#check_library_exists(zstd zstd_compress "" HAVE_ZSTD)
#check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXSymbolExists)
# Using check_cxx_symbol_exists() instead of check_c_symbol_exists() because
# we're including the header from C++, and feature detection should use the same
# compiler language that the project will use later. Principles aside, some
# versions of do not expose fdatasync() in <unistd.h> in standard C mode
# (-std=c11), but do expose the function in standard C++ mode (-std=c++11).
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # Disable C++ exceptions.
    string(REGEX REPLACE "/EH[a-z]+" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHs-c-")
    add_definitions(-D_HAS_EXCEPTIONS=0)

    # Disable RTTI.
    string(REGEX REPLACE "/GR" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /GR-")
else(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # Enable strict prototype warnings for C code in clang and gcc.
    if(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-prototypes")
    endif(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")

    # Disable C++ exceptions.
    string(REGEX REPLACE "-fexceptions" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

    # Disable RTTI.
    string(REGEX REPLACE "-frtti" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")

# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

# Used by googletest.
check_cxx_compiler_flag(-Wno-missing-field-initializers
        LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include(CheckCXXSourceCompiles)

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(LEVELDB_PUBLIC_INCLUDE_DIR "include/leveldb")
set(LEVELDB_PORT_CONFIG_DIR "include/port")

configure_file(
        "${_LEVELDB_SOURCE_DIR}/port/port_config.h.in"
        "${_LEVELDB_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(
        "${_LEVELDB_BINARY_DIR}/include"
        "."
)

include_directories(${_LEVELDB_SOURCE_DIR})

if(BUILD_SHARED_LIBS)
    # Only export LEVELDB_EXPORT symbols from the shared library.
    add_compile_options(-fvisibility=hidden)
endif(BUILD_SHARED_LIBS)

# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(leveldb "")
target_sources(leveldb
        PRIVATE
        "${_LEVELDB_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
        "${_LEVELDB_SOURCE_DIR}/db/builder.cc"
        "${_LEVELDB_SOURCE_DIR}/db/builder.h"
        "${_LEVELDB_SOURCE_DIR}/db/c.cc"
        "${_LEVELDB_SOURCE_DIR}/db/db_impl.cc"
        "${_LEVELDB_SOURCE_DIR}/db/db_impl.h"
        "${_LEVELDB_SOURCE_DIR}/db/db_iter.cc"
        "${_LEVELDB_SOURCE_DIR}/db/db_iter.h"
        "${_LEVELDB_SOURCE_DIR}/db/dbformat.cc"
        "${_LEVELDB_SOURCE_DIR}/db/dbformat.h"
        "${_LEVELDB_SOURCE_DIR}/db/dumpfile.cc"
        "${_LEVELDB_SOURCE_DIR}/db/filename.cc"
        "${_LEVELDB_SOURCE_DIR}/db/filename.h"
        "${_LEVELDB_SOURCE_DIR}/db/log_format.h"
        "${_LEVELDB_SOURCE_DIR}/db/log_reader.cc"
        "${_LEVELDB_SOURCE_DIR}/db/log_reader.h"
        "${_LEVELDB_SOURCE_DIR}/db/log_writer.cc"
        "${_LEVELDB_SOURCE_DIR}/db/log_writer.h"
        "${_LEVELDB_SOURCE_DIR}/db/memtable.cc"
        "${_LEVELDB_SOURCE_DIR}/db/memtable.h"
        "${_LEVELDB_SOURCE_DIR}/db/repair.cc"
        "${_LEVELDB_SOURCE_DIR}/db/skiplist.h"
        "${_LEVELDB_SOURCE_DIR}/db/snapshot.h"
        "${_LEVELDB_SOURCE_DIR}/db/table_cache.cc"
        "${_LEVELDB_SOURCE_DIR}/db/table_cache.h"
        "${_LEVELDB_SOURCE_DIR}/db/version_edit.cc"
        "${_LEVELDB_SOURCE_DIR}/db/version_edit.h"
        "${_LEVELDB_SOURCE_DIR}/db/version_set.cc"
        "${_LEVELDB_SOURCE_DIR}/db/version_set.h"
        "${_LEVELDB_SOURCE_DIR}/db/write_batch_internal.h"
        "${_LEVELDB_SOURCE_DIR}/db/write_batch.cc"
        "${_LEVELDB_SOURCE_DIR}/port/port_stdcxx.h"
        "${_LEVELDB_SOURCE_DIR}/port/port.h"
        "${_LEVELDB_SOURCE_DIR}/port/thread_annotations.h"
        "${_LEVELDB_SOURCE_DIR}/table/block_builder.cc"
        "${_LEVELDB_SOURCE_DIR}/table/block_builder.h"
        "${_LEVELDB_SOURCE_DIR}/table/block.cc"
        "${_LEVELDB_SOURCE_DIR}/table/block.h"
        "${_LEVELDB_SOURCE_DIR}/table/filter_block.cc"
        "${_LEVELDB_SOURCE_DIR}/table/filter_block.h"
        "${_LEVELDB_SOURCE_DIR}/table/format.cc"
        "${_LEVELDB_SOURCE_DIR}/table/format.h"
        "${_LEVELDB_SOURCE_DIR}/table/iterator_wrapper.h"
        "${_LEVELDB_SOURCE_DIR}/table/iterator.cc"
        "${_LEVELDB_SOURCE_DIR}/table/merger.cc"
        "${_LEVELDB_SOURCE_DIR}/table/merger.h"
        "${_LEVELDB_SOURCE_DIR}/table/table_builder.cc"
        "${_LEVELDB_SOURCE_DIR}/table/table.cc"
        "${_LEVELDB_SOURCE_DIR}/table/two_level_iterator.cc"
        "${_LEVELDB_SOURCE_DIR}/table/two_level_iterator.h"
        "${_LEVELDB_SOURCE_DIR}/util/arena.cc"
        "${_LEVELDB_SOURCE_DIR}/util/arena.h"
        "${_LEVELDB_SOURCE_DIR}/util/bloom.cc"
        "${_LEVELDB_SOURCE_DIR}/util/cache.cc"
        "${_LEVELDB_SOURCE_DIR}/util/coding.cc"
        "${_LEVELDB_SOURCE_DIR}/util/coding.h"
        "${_LEVELDB_SOURCE_DIR}/util/comparator.cc"
        "${_LEVELDB_SOURCE_DIR}/util/crc32c.cc"
        "${_LEVELDB_SOURCE_DIR}/util/crc32c.h"
        "${_LEVELDB_SOURCE_DIR}/util/env.cc"
        "${_LEVELDB_SOURCE_DIR}/util/filter_policy.cc"
        "${_LEVELDB_SOURCE_DIR}/util/hash.cc"
        "${_LEVELDB_SOURCE_DIR}/util/hash.h"
        "${_LEVELDB_SOURCE_DIR}/util/logging.cc"
        "${_LEVELDB_SOURCE_DIR}/util/logging.h"
        "${_LEVELDB_SOURCE_DIR}/util/mutexlock.h"
        "${_LEVELDB_SOURCE_DIR}/util/no_destructor.h"
        "${_LEVELDB_SOURCE_DIR}/util/options.cc"
        "${_LEVELDB_SOURCE_DIR}/util/random.h"
        "${_LEVELDB_SOURCE_DIR}/util/status.cc"

        # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
        $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/c.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/cache.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/comparator.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/db.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/dumpfile.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/env.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/export.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/iterator.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/options.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/slice.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/status.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/table.h"
        "${_LEVELDB_SOURCE_DIR}/${LEVELDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
)

if (WIN32)
    target_sources(leveldb
            PRIVATE
            "${_LEVELDB_SOURCE_DIR}/util/env_windows.cc"
            "${_LEVELDB_SOURCE_DIR}/util/windows_logger.h"
    )
else (WIN32)
    target_sources(leveldb
            PRIVATE
            "${_LEVELDB_SOURCE_DIR}/util/env_posix.cc"
            "${_LEVELDB_SOURCE_DIR}/util/posix_logger.h"
    )
endif (WIN32)

# MemEnv is not part of the interface and could be pulled to a separate library.
target_sources(leveldb
        PRIVATE
        "${_LEVELDB_SOURCE_DIR}/helpers/memenv/memenv.cc"
        "${_LEVELDB_SOURCE_DIR}/helpers/memenv/memenv.h"
)

target_include_directories(leveldb
        PUBLIC
        $<BUILD_INTERFACE:${_LEVELDB_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(leveldb
        PROPERTIES VERSION "1.23.0" SOVERSION "1")

target_compile_definitions(leveldb
        PRIVATE
        # Used by include/export.h when building shared libraries.
        LEVELDB_COMPILE_LIBRARY
        # Used by port/port.h.
        ${LEVELDB_PLATFORM_NAME}=1
)
if (NOT HAVE_CXX17_HAS_INCLUDE)
    target_compile_definitions(leveldb
            PRIVATE
            LEVELDB_HAS_PORT_CONFIG_H=1
    )
endif(NOT HAVE_CXX17_HAS_INCLUDE)

if(BUILD_SHARED_LIBS)
    target_compile_definitions(leveldb
            PUBLIC
            # Used by include/export.h.
            LEVELDB_SHARED_LIBRARY
    )
endif(BUILD_SHARED_LIBS)

#if(HAVE_CLANG_THREAD_SAFETY)
#    target_compile_options(leveldb
#            PUBLIC
#            -Werror -Wthread-safety)
#endif(HAVE_CLANG_THREAD_SAFETY)

#if(HAVE_CRC32C)
#    target_link_libraries(leveldb crc32c)
#endif(HAVE_CRC32C)
#if(HAVE_SNAPPY)
#    target_link_libraries(leveldb snappy)
#endif(HAVE_SNAPPY)
#if(HAVE_ZSTD)
#    target_link_libraries(leveldb zstd)
#endif(HAVE_ZSTD)
#if(HAVE_TCMALLOC)
#    target_link_libraries(leveldb tcmalloc)
#endif(HAVE_TCMALLOC)

# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(leveldb Threads::Threads)

add_executable(leveldbutil
        "${_LEVELDB_SOURCE_DIR}/db/leveldbutil.cc"
)
target_link_libraries(leveldbutil leveldb)


target_link_libraries(leveldb Threads::Threads)

target_compile_definitions(leveldb PRIVATE HAVE_CRC32C=0)
target_compile_definitions(leveldb PRIVATE HAVE_SNAPPY=0)
target_compile_definitions(leveldb PRIVATE HAVE_Zstd=0)

add_library(ch_contrib::leveldb ALIAS leveldb)