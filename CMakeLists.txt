cmake_minimum_required(VERSION 3.20)
project(itch_adapter LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(ITCH_SANITIZE "Build with ASan/UBSan" OFF)

add_library(itch INTERFACE)
target_include_directories(itch INTERFACE include)
target_compile_options(itch INTERFACE -Wall -Wextra -Wpedantic)
if(ITCH_SANITIZE)
  target_compile_options(itch INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(itch INTERFACE -fsanitize=address,undefined)
endif()

enable_testing()
add_subdirectory(tests)
