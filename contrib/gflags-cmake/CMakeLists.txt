option (ENABLE_GFLAGS "Enable gflags" ${ENABLE_LIBRARIES})

if (NOT ENABLE_GFLAGS)
    message(STATUS "Not using gflags")
    return()
endif()

set(_gFlags_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/gflags/include")
set(_gFlags_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/gflags/src")
set(_gFlags_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/gflags/lib")

configure_file(${_gFlags_SOURCE_DIR}/defines.h.in ${_gFlags_INCLUDE_DIR}/gflags/defines.h)
configure_file(${_gFlags_SOURCE_DIR}/gflags.h.in ${_gFlags_INCLUDE_DIR}/gflags/gflags.h)
configure_file(${_gFlags_SOURCE_DIR}/gflags_declare.h.in ${_gFlags_INCLUDE_DIR}/gflags/gflags_declare.h)
configure_file(${_gFlags_SOURCE_DIR}/gflags_completions.h.in ${_gFlags_INCLUDE_DIR}/gflags/gflags_completions.h)
#configure_file(${_gFlags_SOURCE_DIR}/defines.h.in ${_gFlags_INCLUDE_DIR}/defines.h)

set (GFLAGS_SRCS
    ${_gFlags_SOURCE_DIR}/gflags.cc
    ${_gFlags_SOURCE_DIR}/gflags_completions.cc
    ${_gFlags_SOURCE_DIR}/gflags_reporting.cc
)

#include(gflags.cmake)

set(gFlags_LIBRARIES gflags-static)

add_library (_gflags ${GFLAGS_SRCS})
#add_library(_gflags INTERFACE)
target_link_libraries(_gflags PUBLIC ${gFlags_LIBRARIES})
target_include_directories(_gflags PUBLIC ${_gFlags_INCLUDE_DIR} ${_gFlags_SOURCE_DIR})
add_library(ch_contrib::gflags ALIAS _gflags)