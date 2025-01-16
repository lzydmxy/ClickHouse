# This file was edited for JD.
# Support add_contrib in contrib/CMakeList.txt

cmake_minimum_required(VERSION 2.8.10)
project(brpc C CXX)

option(WITH_GLOG "With glog" OFF)
option(WITH_MESALINK "With MesaLink" OFF)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
option(DEBUG "Print debug logs" ON)
else()
option(DEBUG "Print debug logs" OFF)
endif()
option(WITH_DEBUG_SYMBOLS "With debug symbols" ON)
option(WITH_THRIFT "With thrift framed protocol supported" OFF)
option(WITH_SNAPPY "With snappy" OFF)
option(WITH_RDMA "With RDMA" OFF)
option(BUILD_UNIT_TESTS "Whether to build unit tests" OFF)
option(BUILD_BRPC_TOOLS "Whether to build brpc tools" OFF)
option(DOWNLOAD_GTEST "Download and build a fresh copy of googletest. Requires Internet access." OFF)

# Enable MACOSX_RPATH. Run "cmake --help-policy CMP0042" for policy details.
if(POLICY CMP0042)
    cmake_policy(SET CMP0042 NEW)
endif()

set(BRPC_VERSION 1.3.0)

SET(CPACK_GENERATOR "DEB")
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "brpc authors")
INCLUDE(CPack)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

set(WITH_GLOG_VAL "0")

if(WITH_DEBUG_SYMBOLS)
    set(DEBUG_SYMBOL "-g")
endif()

set(WITH_RDMA_VAL "0")

set(_bRPC_GFLAGS_LIBRARIES ch_contrib::gflag)
set(_bRPC_PROTOBUF_LIBRARIES ch_contrib::protobuf)

include(GNUInstallDirs)

configure_file(${_bRPC_SOURCE_DIR}/config.h.in ${_bRPC_SOURCE_DIR}/src/butil/config.h @ONLY)

set(CMAKE_MODULE_PATH ${_bRPC_SOURCE_DIR}/cmake)

include_directories(
    ${_bRPC_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
)

execute_process(
    COMMAND bash -c "${_bRPC_SOURCE_DIR}/tools/get_brpc_revision.sh ${_bRPC_SOURCE_DIR} | tr -d '\n'"
    OUTPUT_VARIABLE BRPC_REVISION
)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    include(CheckFunctionExists)
    CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
    if(NOT HAVE_CLOCK_GETTIME)
        set(DEFINE_CLOCK_GETTIME "-DNO_CLOCK_GETTIME_IN_MAC")
    endif()
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -Wno-deprecated-declarations -Wno-inconsistent-missing-override")
endif()

set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${DEFINE_CLOCK_GETTIME} -DBRPC_WITH_GLOG=${WITH_GLOG_VAL} -DBRPC_WITH_RDMA=${WITH_RDMA_VAL} -DGFLAGS_NS=${GFLAGS_NS}")

if (SANITIZE STREQUAL "thread")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DTHREAD_SANITIZER")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -mllvm -tsan-instrument-memory-accesses=false")
    endif()
endif()

if (SANITIZE STREQUAL "memory")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DMEMORY_SANITIZER")
endif()

if (SANITIZE STREQUAL "address")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DADDRESS_SANITIZER")
endif()

set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DBTHREAD_USE_FAST_PTHREAD_MUTEX -D__const__=__unused__ -D_GNU_SOURCE -DUSE_SYMBOLIZE -DNO_TCMALLOC -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DBRPC_REVISION=\\\"${BRPC_REVISION}\\\" -D__STRICT_ANSI__")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${DEBUG_SYMBOL} ${THRIFT_CPP_FLAG}")
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_CPP_FLAGS} -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${CMAKE_CPP_FLAGS} -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-unused-parameter -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_DEBUG}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_DEBUG}")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -DNDEBUG -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CPP_FLAGS} -DNDEBUG -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-unused-parameter -fno-omit-frame-pointer")
endif()

macro(use_cxx11)
if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()
endmacro(use_cxx11)

use_cxx11()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    #required by butil/crc32.cc to boost performance for 10x
    if((CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)") AND NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.4))
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4 -msse4.2")
    elseif((CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64"))
        # segmentation fault in libcontext
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-gcse")
    endif()
    if(NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0))
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-aligned-new")
    endif()
endif()

list(APPEND DYNAMIC_LIB ${OPENSSL_SSL_LIBRARY})

set(BRPC_PRIVATE_LIBS "-lgflags -lprotobuf -lleveldb -lprotoc -lssl -lcrypto -ldl -lz")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(DYNAMIC_LIB ${DYNAMIC_LIB} rt)
    set(BRPC_PRIVATE_LIBS "${BRPC_PRIVATE_LIBS} -lrt")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        pthread
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreData"
        "-framework CoreText"
        "-framework Security"
        "-framework Foundation"
        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop")
endif()

set (_bRPC_GFLAGS_INCLUDE_PATH "${ClickHouse_BINARY_DIR}/contrib/gflags/src/include")
#set(_bRPC_GFLAGS_INCLUDE "")
set(_bRPC_GFLAGS_LIBRARY ch_contrib::gflags)

set(_bRPC_SSL_INCLUDE_DIR "")
set(_bRPC_SSL_LIBRARIES OpenSSL::Crypto OpenSSL::SSL)

# set(PROTOC_LIB ${Protobuf_PROTOC_LIBRARY})
# set(PROTOBUF_LIBRARIES ${Protobuf_LIBRARY})

# set(PROTOBUF_INCLUDE_DIRS ${Protobuf_INCLUDE_DIR})

set(PROTOBUF_PROTOC_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})

include_directories(
        ${_bRPC_GFLAGS_INCLUDE}
        ${_bRPC_PROTOBUF_INCLUDE}
#        ${LEVELDB_INCLUDE_PATH}
#        ${OPENSSL_INCLUDE_DIR}
        )

set(DYNAMIC_LIB
    ${_bRPC_GFLAGS_LIBRARY}
    ${_bRPC_PROTOBUF_LIBRARIES}
    ${_bRPC_PROTOC_LIB}
#    ${CMAKE_THREAD_LIBS_INIT}
    dl)

if(WITH_MESALINK)
    list(APPEND DYNAMIC_LIB ${MESALINK_LIB})
else()
    list(APPEND DYNAMIC_LIB ${OPENSSL_SSL_LIBRARY})
endif()

set(BRPC_PRIVATE_LIBS "-lgflags -lprotobuf -lleveldb -lprotoc -lssl -lcrypto -ldl -lz")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(DYNAMIC_LIB ${DYNAMIC_LIB} rt)
    set(BRPC_PRIVATE_LIBS "${BRPC_PRIVATE_LIBS} -lrt")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        pthread
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreData"
        "-framework CoreText"
        "-framework Security"
        "-framework Foundation"
        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop")
endif()

# for *.so
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${_bRPC_BINARY_DIR}/output/lib)
# for *.a
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${_bRPC_BINARY_DIR}/output/lib)

# the reason why not using file(GLOB_RECURSE...) is that we want to
# include different files on different platforms.
set(BUTIL_SOURCES
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/dmg_fp/g_fmt.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/dmg_fp/dtoa_wrapper.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/dynamic_annotations/dynamic_annotations.c
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/icu/icu_utf.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/superfasthash/superfasthash.c
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/modp_b64/modp_b64.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/symbolize/demangle.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/symbolize/symbolize.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/snappy/snappy-sinksource.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/snappy/snappy-stubs-internal.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/snappy/snappy.cc
    ${_bRPC_SOURCE_DIR}/src/butil/third_party/murmurhash3/murmurhash3.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/arena.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/at_exit.cc
    ${_bRPC_SOURCE_DIR}/src/butil/atomicops_internals_x86_gcc.cc
    ${_bRPC_SOURCE_DIR}/src/butil/base64.cc
    ${_bRPC_SOURCE_DIR}/src/butil/big_endian.cc
    ${_bRPC_SOURCE_DIR}/src/butil/cpu.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/alias.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/asan_invalid_access.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/crash_logging.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/debugger.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/debugger_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/dump_without_crashing.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/proc_maps_linux.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/stack_trace.cc
    ${_bRPC_SOURCE_DIR}/src/butil/debug/stack_trace_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/environment.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/file.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/file_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/file_enumerator.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/file_enumerator_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/file_path.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/file_path_constants.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/memory_mapped_file.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/memory_mapped_file_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/scoped_file.cc
    ${_bRPC_SOURCE_DIR}/src/butil/files/scoped_temp_dir.cc
    ${_bRPC_SOURCE_DIR}/src/butil/file_util.cc
    ${_bRPC_SOURCE_DIR}/src/butil/file_util_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/guid.cc
    ${_bRPC_SOURCE_DIR}/src/butil/guid_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/hash.cc
    ${_bRPC_SOURCE_DIR}/src/butil/lazy_instance.cc
    ${_bRPC_SOURCE_DIR}/src/butil/location.cc
    ${_bRPC_SOURCE_DIR}/src/butil/memory/aligned_memory.cc
    ${_bRPC_SOURCE_DIR}/src/butil/memory/ref_counted.cc
    ${_bRPC_SOURCE_DIR}/src/butil/memory/ref_counted_memory.cc
    ${_bRPC_SOURCE_DIR}/src/butil/memory/singleton.cc
    ${_bRPC_SOURCE_DIR}/src/butil/memory/weak_ptr.cc
    ${_bRPC_SOURCE_DIR}/src/butil/posix/file_descriptor_shuffle.cc
    ${_bRPC_SOURCE_DIR}/src/butil/posix/global_descriptors.cc
    ${_bRPC_SOURCE_DIR}/src/butil/process_util.cc
    ${_bRPC_SOURCE_DIR}/src/butil/rand_util.cc
    ${_bRPC_SOURCE_DIR}/src/butil/rand_util_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/fast_rand.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/safe_strerror_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/sha1_portable.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/latin1_string_conversions.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/nullable_string16.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/safe_sprintf.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/string16.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/string_number_conversions.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/string_split.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/string_piece.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/string_util.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/string_util_constants.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/stringprintf.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/utf_offset_string_conversions.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/utf_string_conversion_utils.cc
    ${_bRPC_SOURCE_DIR}/src/butil/strings/utf_string_conversions.cc
    ${_bRPC_SOURCE_DIR}/src/butil/synchronization/cancellation_flag.cc
    ${_bRPC_SOURCE_DIR}/src/butil/synchronization/condition_variable_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/synchronization/waitable_event_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/non_thread_safe_impl.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/platform_thread_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/simple_thread.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/thread_checker_impl.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/thread_collision_warner.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/thread_id_name_manager.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/thread_local_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/thread_local_storage.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/thread_local_storage_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/thread_restrictions.cc
    ${_bRPC_SOURCE_DIR}/src/butil/threading/watchdog.cc
    ${_bRPC_SOURCE_DIR}/src/butil/time/clock.cc
    ${_bRPC_SOURCE_DIR}/src/butil/time/default_clock.cc
    ${_bRPC_SOURCE_DIR}/src/butil/time/default_tick_clock.cc
    ${_bRPC_SOURCE_DIR}/src/butil/time/tick_clock.cc
    ${_bRPC_SOURCE_DIR}/src/butil/time/time.cc
    ${_bRPC_SOURCE_DIR}/src/butil/time/time_posix.cc
    ${_bRPC_SOURCE_DIR}/src/butil/version.cc
    ${_bRPC_SOURCE_DIR}/src/butil/logging.cc
    ${_bRPC_SOURCE_DIR}/src/butil/class_name.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/errno.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/find_cstr.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/status.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/string_printf.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/thread_local.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/unix_socket.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/endpoint.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/fd_utility.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/files/temp_file.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/files/file_watcher.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/time.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/zero_copy_stream_as_streambuf.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/crc32c.cc
    ${_bRPC_SOURCE_DIR}/src/butil/containers/case_ignored_flat_map.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/iobuf.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/binary_printer.cpp
    ${_bRPC_SOURCE_DIR}/src/butil/recordio.cc
    ${_bRPC_SOURCE_DIR}/src/butil/popen.cpp
    )

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(BUTIL_SOURCES ${BUTIL_SOURCES}
        ${_bRPC_SOURCE_DIR}/src/butil/file_util_linux.cc
        ${_bRPC_SOURCE_DIR}/src/butil/threading/platform_thread_linux.cc
        ${_bRPC_SOURCE_DIR}/src/butil/strings/sys_string_conversions_posix.cc)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(BUTIL_SOURCES ${BUTIL_SOURCES}
        ${_bRPC_SOURCE_DIR}/src/butil/mac/bundle_locations.mm
        ${_bRPC_SOURCE_DIR}/src/butil/mac/foundation_util.mm
        ${_bRPC_SOURCE_DIR}/src/butil/file_util_mac.mm
        ${_bRPC_SOURCE_DIR}/src/butil/threading/platform_thread_mac.mm
        ${_bRPC_SOURCE_DIR}/src/butil/strings/sys_string_conversions_mac.mm
        ${_bRPC_SOURCE_DIR}/src/butil/time/time_mac.cc
        ${_bRPC_SOURCE_DIR}/src/butil/mac/scoped_mach_port.cc)
endif()

file(GLOB_RECURSE BVAR_SOURCES "${_bRPC_SOURCE_DIR}/src/bvar/*.cpp")
file(GLOB_RECURSE BTHREAD_SOURCES "${_bRPC_SOURCE_DIR}/src/bthread/*.cpp")
file(GLOB_RECURSE JSON2PB_SOURCES "${_bRPC_SOURCE_DIR}/src/json2pb/*.cpp")
file(GLOB_RECURSE BRPC_SOURCES "${_bRPC_SOURCE_DIR}/src/brpc/*.cpp")
file(GLOB_RECURSE THRIFT_SOURCES "${_bRPC_SOURCE_DIR}/src/brpc/thrift*.cpp")
file(GLOB_RECURSE EXCLUDE_SOURCES "${_bRPC_SOURCE_DIR}/src/brpc/event_dispatcher_*.cpp")

if(WITH_THRIFT)
    message("brpc compile with thrift protocol")
else()
    # Remove thrift sources
    foreach(v ${THRIFT_SOURCES})
        list(REMOVE_ITEM BRPC_SOURCES ${v})
    endforeach()
    set(THRIFT_SOURCES "")
endif()

foreach(v ${EXCLUDE_SOURCES})
    list(REMOVE_ITEM BRPC_SOURCES ${v})
endforeach()

set(MCPACK2PB_SOURCES
    ${_bRPC_SOURCE_DIR}/src/mcpack2pb/field_type.cpp
    ${_bRPC_SOURCE_DIR}/src/mcpack2pb/mcpack2pb.cpp
    ${_bRPC_SOURCE_DIR}/src/mcpack2pb/parser.cpp
    ${_bRPC_SOURCE_DIR}/src/mcpack2pb/serializer.cpp)

#include(CompileProto)

function(compile_proto OUT_HDRS OUT_SRCS DESTDIR HDR_OUTPUT_DIR PROTO_DIR PROTO_FILES)
  foreach(P ${PROTO_FILES})
    string(REPLACE .proto .pb.h HDR ${P})
    set(HDR_RELATIVE ${HDR})
    set(HDR ${DESTDIR}/${HDR})
    string(REPLACE .proto .pb.cc SRC ${P})
    set(SRC ${DESTDIR}/${SRC})
    list(APPEND HDRS ${HDR})
    list(APPEND SRCS ${SRC})
    add_custom_command(
      OUTPUT ${HDR} ${SRC}
      COMMAND ${_bRPC_PROTOBUF_PROTOC_EXECUTABLE} ${PROTOC_FLAGS} 
      -I${PROTO_DIR} 
      --cpp_out=${DESTDIR} ${PROTO_DIR}/${P}
      COMMAND ${CMAKE_COMMAND} -E copy ${HDR} ${HDR_OUTPUT_DIR}/${HDR_RELATIVE}
      DEPENDS ${PROTO_DIR}/${P} protobuf::protoc
    )
  endforeach()
  set(${OUT_HDRS} ${HDRS} PARENT_SCOPE)
  set(${OUT_SRCS} ${SRCS} PARENT_SCOPE)
endfunction()

FUNCTION(AUTO_SOURCES RETURN_VALUE PATTERN SOURCE_SUBDIRS)

	IF ("${SOURCE_SUBDIRS}" STREQUAL "RECURSE")
		SET(PATH ".")
		IF (${ARGC} EQUAL 4)
			LIST(GET ARGV 3 PATH)
		ENDIF ()
	ENDIF()

	IF ("${SOURCE_SUBDIRS}" STREQUAL "RECURSE")
		UNSET(${RETURN_VALUE})
		FILE(GLOB SUBDIR_FILES "${PATH}/${PATTERN}")
		LIST(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})

		FILE(GLOB SUBDIRS RELATIVE ${PATH} ${PATH}/*)

		FOREACH(DIR ${SUBDIRS})
			IF (IS_DIRECTORY ${PATH}/${DIR})
				IF (NOT "${DIR}" STREQUAL "CMAKEFILES")
					FILE(GLOB_RECURSE SUBDIR_FILES "${PATH}/${DIR}/${PATTERN}")
					LIST(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})
				ENDIF()
			ENDIF()
		ENDFOREACH()
	ELSE ()
		FILE(GLOB ${RETURN_VALUE} "${PATTERN}")

		FOREACH (PATH ${SOURCE_SUBDIRS})
			FILE(GLOB SUBDIR_FILES "${PATH}/${PATTERN}")
			LIST(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})
		ENDFOREACH(PATH ${SOURCE_SUBDIRS})
	ENDIF ()

	IF (${FILTER_OUT})
		LIST(REMOVE_ITEM ${RETURN_VALUE} ${FILTER_OUT})
	ENDIF()

	SET(${RETURN_VALUE} ${${RETURN_VALUE}} PARENT_SCOPE)
ENDFUNCTION(AUTO_SOURCES)

# set(PROTO_FILES 
#     idl_options.proto
#     brpc/rtmp.proto
#     brpc/rpc_dump.proto
#     brpc/get_favicon.proto
#     brpc/span.proto
#     brpc/builtin_service.proto
#     brpc/get_js.proto
#     brpc/errno.proto
#     brpc/nshead_meta.proto
#     brpc/options.proto
#     brpc/policy/baidu_rpc_meta.proto
#     brpc/policy/hulu_pbrpc_meta.proto
#     brpc/policy/public_pbrpc_meta.proto
#     brpc/policy/sofa_pbrpc_meta.proto
#     brpc/policy/mongo.proto
#     brpc/trackme.proto
#     brpc/streaming_rpc_meta.proto
#     brpc/proto_base.proto
# )

set(PROTO_FILES 
    ${_bRPC_SOURCE_DIR}/src/idl_options.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/rtmp.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/rpc_dump.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/get_favicon.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/span.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/builtin_service.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/get_js.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/errno.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/nshead_meta.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/options.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/policy/baidu_rpc_meta.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/policy/hulu_pbrpc_meta.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/policy/public_pbrpc_meta.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/policy/sofa_pbrpc_meta.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/policy/mongo.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/trackme.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/streaming_rpc_meta.proto
    ${_bRPC_SOURCE_DIR}/src/brpc/proto_base.proto
)

#set(_bRPC_PROTOBUF_INCLUDE ${Protobuf_INCLUDE_DIR})
set(_bRPC_PROTOBUF_INCLUDE "")
set(_bRPC_PROTOBUF_LIBRARIES ch_contrib::protobuf)
set(_bRPC_PROTOBUF_PROTOC "protoc")
set(_bRPC_PROTOBUF_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
set(_bRPC_PROTOBUF_PROTOC_LIBRARIES ch_contrib::protoc)

#file(MAKE_DIRECTORY ${_bRPC_BINARY_DIR}/output/include/brpc)
#set(PROTOC_FLAGS ${PROTOC_FLAGS} -I${PROTOBUF_INCLUDE_DIR})

# compile_proto(PROTO_HDRS PROTO_SRCS ${_bRPC_BINARY_DIR}
#                                     ${_bRPC_BINARY_DIR}/output/include
#                                     ${_bRPC_SOURCE_DIR}/src
#                                     "${PROTO_FILES}")

AUTO_SOURCES(brpc_PROTO_FILES "*.proto" "RECURSE" "${_bRPC_SOURCE_DIR}/src")
SET(brpc_PROTO_FILES ${brpc_PROTO_FILES} PARENT_SCOPE)

PROTOBUF_GENERATE_BRPC_CPP(PROTO_SRCS PROTO_HDRS 
     ${brpc_PROTO_FILES}
)

#${_bRPC_BINARY_DIR}/output/

add_library(PROTO_LIB OBJECT ${PROTO_SRCS} ${PROTO_HDRS})

set(SOURCES
    ${BVAR_SOURCES}
    ${BTHREAD_SOURCES}
    ${JSON2PB_SOURCES}
    ${MCPACK2PB_SOURCES}
    ${BRPC_SOURCES}
    ${THRIFT_SOURCES}
    )

file(COPY ${_bRPC_SOURCE_DIR}/src/brpc/
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/output/include/brpc/
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
        )
file(COPY ${_bRPC_SOURCE_DIR}/src/
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/output/include/
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
        )
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
        )

# Install pkgconfig
#configure_file(${_bRPC_source_DIR}/cmake/brpc.pc.in ${_bRPC_BINARY_DIR}/brpc.pc @ONLY)

#install(FILES ${_bRPC_BINARY_DIR}/brpc.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${_bRPC_SOURCE_DIR}/src)

add_library(BUTIL_LIB OBJECT ${BUTIL_SOURCES})
add_library(SOURCES_LIB OBJECT ${SOURCES})
add_dependencies(SOURCES_LIB PROTO_LIB)
target_link_libraries(SOURCES_LIB ${ZLIB_LIBRARIES})
target_compile_definitions(SOURCES_LIB PRIVATE NO_SSL)
target_compile_definitions(BUTIL_LIB PRIVATE NO_SSL)

# shared library needs POSITION_INDEPENDENT_CODE
set_property(TARGET ${SOURCES_LIB} PROPERTY POSITION_INDEPENDENT_CODE 1)
set_property(TARGET ${BUTIL_LIB} PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(brpc-static STATIC $<TARGET_OBJECTS:BUTIL_LIB>
                               $<TARGET_OBJECTS:SOURCES_LIB>
                               $<TARGET_OBJECTS:PROTO_LIB>)

# if(BRPC_WITH_THRIFT)
#    target_link_libraries(brpc-static thrift)
# endif()

SET_TARGET_PROPERTIES(brpc-static PROPERTIES OUTPUT_NAME brpc CLEAN_DIRECT_OUTPUT 1)

# for protoc-gen-mcpack
set(EXECUTABLE_OUTPUT_PATH ${_bRPC_BINARY_DIR}/output/bin)
    
# set(protoc_gen_mcpack_SOURCES
#     ${_bRPC_SOURCE_DIR}/src/mcpack2pb/generator.cpp
# )
# add_executable(protoc-gen-mcpack ${protoc_gen_mcpack_SOURCES})

# if(BUILD_SHARED_LIBS)
#     add_library(brpc-shared SHARED $<TARGET_OBJECTS:BUTIL_LIB> 
#                                    $<TARGET_OBJECTS:SOURCES_LIB>
#                                    $<TARGET_OBJECTS:PROTO_LIB>)
#     target_link_libraries(brpc-shared ${DYNAMIC_LIB})
#     if(BRPC_WITH_GLOG)
#         target_link_libraries(brpc-shared ${GLOG_LIB})
#     endif()
#     # if(BRPC_WITH_THRIFT)
#     #     target_link_libraries(brpc-shared thrift)
#     # endif()
#     SET_TARGET_PROPERTIES(brpc-shared PROPERTIES OUTPUT_NAME brpc CLEAN_DIRECT_OUTPUT 1)

#     target_link_libraries(protoc-gen-mcpack brpc-shared ${DYNAMIC_LIB} pthread)

#     install(TARGETS brpc-shared
#             RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#             LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             )
#     target_include_directories(brpc-shared SYSTEM PUBLIC ${GFLAGS_INCLUDE_PATH} INTERFACE ${_bRPC_BINARY_DIR}/output/include)
# else()
    
    #target_link_libraries(protoc-gen-mcpack brpc-static ${DYNAMIC_LIB} pthread)
    target_link_libraries(brpc-static ${DYNAMIC_LIB} pthread)
    target_link_libraries(brpc-static PUBLIC ${_bRPC_GFLAGS_LIBRARY})
    target_include_directories(brpc-static SYSTEM PUBLIC ${_bRPC_GFLAGS_INCLUDE} INTERFACE ${_bRPC_BINARY_DIR}/output/include)

# endif()

install(TARGETS brpc-static
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        )
