# CMake 最低版本号要求
cmake_minimum_required (VERSION 3.2)

# 项目信息
project (cassandra_test)

# 查找当前目录下的所有源文件
# 并将名称保存到 DIR_SRCS 变量
aux_source_directory(. DIR_SRCS)

# 添加子目录
add_subdirectory(cass)
add_subdirectory(spdlog)
add_subdirectory(cxxopts)

# 指定生成目标 
add_executable(DatastaxTest ${DIR_SRCS})

# 添加链接库
target_link_libraries(DatastaxTest cassandra)
target_link_libraries(DatastaxTest spdlog)
target_link_libraries(DatastaxTest cxxopts)
