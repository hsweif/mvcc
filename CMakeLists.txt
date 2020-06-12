#######################################################
# Basic project information and versions
project(mvcc)
cmake_minimum_required(VERSION 3.2.0)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g2 -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O2 -Wall")

#######################################################
# Check submodule directories
if (NOT IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/src/ext/glog/cmake")
    message(FATAL_ERROR "The glog submodule directory is missing! "
            "You probably did not clone the project with --recursive, or you first checked out "
            "pbrt before it was added. It is possible to recover by running "
            "\"git submodule update --init --recursive\"")
endif ()

#######################################################
# Setup Google Test library and unit test
# Download and unpack googletest at configure time
enable_testing()
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download)
if (result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif ()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download)
if (result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif ()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
        ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
        EXCLUDE_FROM_ALL)

# The gtest/gtest_main targets carry header search path
# dependencies automatically when using CMake 2.8.11 or
# later. Otherwise we have to add them here ourselves.
if (CMAKE_VERSION VERSION_LESS 2.8.11)
    include_directories("${gtest_SOURCE_DIR}/include")
endif ()


#######################################################
# glog
set(WITH_GFLAGS OFF CACHE BOOL "Use gflags")
set(BUILD_SHARED_LIBS OFF CACHE BOOL " " FORCE)
if (WIN32)
    add_definitions(-D GOOGLE_GLOG_DLL_DECL=)
endif ()
add_subdirectory(src/ext/glog)
set_property(TARGET glog logging_unittest demangle_unittest utilities_unittest stl_logging_unittest PROPERTY FOLDER "ext")
include_directories(
        src/ext/glog/src
        ${CMAKE_BINARY_DIR}/src/ext/glog
        src/filesystem
)

#######################################################
# Project setup
include_directories(src)

set(MVCC_SOURCES
        src/parser.cpp
        src/database.cpp
        src/txn_manager.cpp
        src/page.cpp
        )

set(MVCC_HEADERS
        src/fwd.h
        src/parser.h
        src/database.h
        src/txn_manager.h
        src/page.h
        )

FILE(GLOB FILE_SYSTEM_SRC
        src/filesystem/bufmanager
        src/filesystem/fileio
        src/filesystem/utils
        )

add_library(mvcc STATIC
        ${MVCC_SOURCES}
        ${MVCC_HEADERS}
        ${FILE_SYSTEM_SRC}
        )

set(ALL_MVCC_LIBS
        glog
        mvcc
        Threads::Threads
        )
add_executable(main src/main.cpp)
target_link_libraries(main ${ALL_MVCC_LIBS})


#######################################################
# Setup unit test
set(MVCC_TESTS
        tests/test_parser.cpp
        tests/test_txnmanager.cpp
        tests/test_database.cpp
        )
add_executable(test_all ${MVCC_TESTS})
target_link_libraries(test_all gtest_main ${ALL_MVCC_LIBS})
add_test(NAME TestAll COMMAND test_all)


