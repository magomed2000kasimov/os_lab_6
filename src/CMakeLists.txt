cmake_minimum_required(VERSION 3.14)
project(maga_lab_06)

set(CMAKE_CXX_STANDARD 17)

add_executable(terminal main_node.cpp)
add_executable(child_node child_node.cpp)
add_library(functions sf.cpp sf.h)

#target_link_libraries(test zmq)
target_link_libraries(functions zmq)
target_link_libraries(terminal zmq functions)
target_link_libraries(child_node zmq functions)


