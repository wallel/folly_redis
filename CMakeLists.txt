cmake_minimum_required(VERSION 3.21)
project(folly_redis)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

#默认debug
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
#_GNU_SOURCE
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
endif()

set(Boost_NO_WARN_NEW_VERSIONS ON)
find_package(folly CONFIG REQUIRED)

add_library(folly_redis
        redis/builders.h
        redis/builders.cpp
        redis/client.cpp
        redis/client.h
        redis/client_interface.h
        redis/cluster_client.h
        redis/cluster_client.cpp
        redis/command.h
        redis/command.cpp
        redis/conn.h
        redis/conn.cpp
        redis/reply.h
        redis/reply.cpp
        redis/redis_export.h
        )
target_include_directories(folly_redis PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(folly_redis PUBLIC Folly::folly)
target_compile_options(folly_redis PUBLIC "$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")
target_compile_options(folly_redis PUBLIC "$<$<CXX_COMPILER_ID:MSVC>:/Zc:__cplusplus>")

####################################
#tests
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/e2239ee6043f73722e7aa812a459f54a28552929.zip
)

FetchContent_MakeAvailable(googletest)
enable_testing()

add_executable(tests tests/reply_test.cpp)

target_link_libraries(tests PRIVATE gtest_main folly_redis)

include(GoogleTest)
gtest_discover_tests(tests)
