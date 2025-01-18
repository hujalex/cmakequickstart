cmake_minimum_required(VERSION 3.21)

project(MyExample)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(main main.cpp)
add_executable(arrowio arrowio.cpp)
add_executable(arrowtest arrowtest.cpp)
add_executable(csvtest csvtest.cpp)

target_link_libraries(main PRIVATE Arrow::arrow_shared)
target_link_libraries(arrowio PRIVATE arrow parquet)
target_link_libraries(arrowtest PRIVATE arrow parquet)