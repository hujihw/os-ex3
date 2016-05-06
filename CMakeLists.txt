cmake_minimum_required(VERSION 3.3.2)
project(ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES Search.cpp MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h MapReduceClient.cpp)
add_executable(ex3 ${SOURCE_FILES})