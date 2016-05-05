cmake_minimum_required(VERSION 3.5)
project(ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES main.cpp MapReduceFramework.cpp Shuffle.cpp Shuffle.h MapManager.cpp MapManager.h ExecReduce.cpp ExecReduce.h Search.cpp Search.h StringCount.cpp StringCount.h Manager.cpp Manager.h)
add_executable(ex3 ${SOURCE_FILES})