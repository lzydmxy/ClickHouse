if (TARGET ch_contrib::brpc)
    add_subdirectory(Protos)
endif()

if (ENABLE_TESTS)
    add_subdirectory (tools)
endif ()
