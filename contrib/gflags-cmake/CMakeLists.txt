option (ENABLE_GFLAGS "Enable gflags" ${ENABLE_LIBRARIES})

if (NOT ENABLE_GFLAGS)
    message(STATUS "Not using gflags")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/gflags")

set (SRCS
    "${LIBRARY_DIR}/src/gflags_completions.cc"
    "${LIBRARY_DIR}/src/gflags_reporting.cc"
)

add_library (_gflags ${SRCS})

add_library(ch_contrib::gflags ALIAS _gflags)