cmake_minimum_required(VERSION 3.5)
project(ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES Search.cpp MapReduceFramework.cpp MapManager.cpp MapManager.h ExecReduce.cpp ExecReduce.h Manager.cpp Manager.h)
add_executable(ex3 ${SOURCE_FILES})