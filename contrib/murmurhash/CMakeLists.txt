add_library(_murmurhash
    src/MurmurHash2.cpp
    src/MurmurHash3.cpp
    include/murmurhash/MurmurHash2.h
    include/murmurhash/MurmurHash3.h)

target_include_directories(_murmurhash PRIVATE include/murmurhash)
target_include_directories(_murmurhash PUBLIC include)
add_library(ch_contrib::murmurhash ALIAS _murmurhash)
