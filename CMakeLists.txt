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
option(ITCH_TSAN "Build with ThreadSanitizer (use a separate build directory; incompatible with ITCH_SANITIZE)" OFF)

# --- Dependencies -----------------------------------------------------------
find_package(Threads REQUIRED)

option(ITCH_WITH_AERON "Build the Aeron transport (downloads Aeron unless ITCH_AERON_SOURCE_DIR is set)" ON)
set(ITCH_AERON_SOURCE_DIR "" CACHE PATH "Use a local Aeron checkout instead of downloading one")
find_package(Boost 1.74 REQUIRED)   # Asio is header-only

find_package(spdlog CONFIG QUIET)
if(NOT spdlog_FOUND)
  include(FetchContent)
  FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.14.1)
  FetchContent_MakeAvailable(spdlog)
endif()

# folly is optional: it only provides the lock-free task queue used by the
# busy-spin WorkerThread mode. Without it, Post() falls back to asio post.
find_package(folly CONFIG QUIET)
if(folly_FOUND)
  message(STATUS "folly found: enabling HAVE_FOLLY")
else()
  message(STATUS "folly not found: WorkerThread spin mode uses asio post")
endif()

# --- Core header-only library -------------------------------------------------
add_library(itch INTERFACE)
target_include_directories(itch INTERFACE include)
target_compile_options(itch INTERFACE -Wall -Wextra -Wpedantic)
if(ITCH_TSAN)
  target_compile_options(itch INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
  target_link_options(itch INTERFACE -fsanitize=thread)
endif()
if(ITCH_SANITIZE)
  target_compile_options(itch INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(itch INTERFACE -fsanitize=address,undefined)
endif()

# --- Network layer (compiled) -------------------------------------------------
# NOTE: src/net/unicast_udp_socket.cpp is currently a copy of the header and is
# intentionally not compiled.
add_library(itch_net STATIC
  src/net/socket_manager.cpp
  src/net/udp_receiver.cpp
  src/net/udp_socket.cpp
  src/net/unicast_udp_socket.cpp)
target_link_libraries(itch_net PUBLIC itch Boost::headers spdlog::spdlog Threads::Threads)
if(folly_FOUND)
  target_link_libraries(itch_net PUBLIC Folly::folly)
  target_compile_definitions(itch_net PUBLIC HAVE_FOLLY)
endif()

# --- Protocol layer + file replay tool ------------------------------------------
add_library(itch_protocol STATIC src/protocol/ndaq_itch_decoder.cpp)
target_link_libraries(itch_protocol PUBLIC itch spdlog::spdlog)

add_executable(itch_file_replay tools/itch_file_replay.cpp)
target_link_libraries(itch_file_replay PRIVATE itch_protocol)

# --- Feed handling: MoldUDP64 sequencing, A/B arbitration, recovery ------------
add_library(itch_feed STATIC src/feed/feed_session.cpp)
if(UNIX)
  target_sources(itch_feed PRIVATE src/feed/mold_sender.cpp)
endif()
target_link_libraries(itch_feed PUBLIC itch)

# --- Feed adapter: sockets -> session -> ring -> books ------------------------------
add_library(itch_feedio STATIC src/feed/feed_handler.cpp src/feed/feed_consumer.cpp)
target_link_libraries(itch_feedio PUBLIC itch_feed itch_net itch_book)

# --- Messaging (server/client, protocol, transports) --------------------------
add_library(itch_messaging STATIC
  src/messaging/protocol.cpp
  src/messaging/channel.cpp
  src/messaging/transport.cpp
  src/messaging/market_data_server.cpp
  src/messaging/market_data_client.cpp)
target_link_libraries(itch_messaging PUBLIC itch_book)

if(UNIX)
  add_library(itch_gateway STATIC src/messaging/tcp_gateway.cpp)
  target_link_libraries(itch_gateway PUBLIC itch_messaging)
endif()

# --- Aeron transport ------------------------------------------------------------
if(ITCH_WITH_AERON)
  include(FetchContent)
  # Only the C media driver and the C++ client are needed (no Java, no archive, no tests).
  set(BUILD_AERON_DRIVER ON CACHE BOOL "" FORCE)
  set(BUILD_AERON_ARCHIVE_API OFF CACHE BOOL "" FORCE)
  set(AERON_TESTS OFF CACHE BOOL "" FORCE)
  set(AERON_UNIT_TESTS OFF CACHE BOOL "" FORCE)
  set(AERON_SYSTEM_TESTS OFF CACHE BOOL "" FORCE)
  set(AERON_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
  set(AERON_BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
  set(AERON_INSTALL_TARGETS OFF CACHE BOOL "" FORCE)
  set(CMAKE_POLICY_VERSION_MINIMUM 3.5)   # Aeron's CMake predates CMake 4's minimum
  if(ITCH_AERON_SOURCE_DIR)
    set(FETCHCONTENT_SOURCE_DIR_AERON "${ITCH_AERON_SOURCE_DIR}")
  endif()
  FetchContent_Declare(aeron
    GIT_REPOSITORY https://github.com/aeron-io/aeron.git
    GIT_TAG 1.53.3
    GIT_SHALLOW TRUE)
  FetchContent_MakeAvailable(aeron)

  add_library(itch_aeron STATIC src/messaging/aeron_transport.cpp)
  target_compile_definitions(itch_aeron PUBLIC ITCH_WITH_AERON)
  target_link_libraries(itch_aeron PUBLIC itch_messaging aeron_client_wrapper aeron_driver_static aeron_static)
  add_executable(itch_md_server tools/itch_md_server.cpp)
  target_link_libraries(itch_md_server PRIVATE itch_aeron)
  if(UNIX)
    add_executable(itch_md_gateway tools/itch_md_gateway.cpp)
    target_link_libraries(itch_md_gateway PRIVATE itch_aeron itch_gateway)
  endif()
  add_executable(itch_md_client tools/itch_md_client.cpp)
  target_link_libraries(itch_md_client PRIVATE itch_aeron)
  # Aeron's headers are not warning-clean under our flags; treat them as system headers.
  target_include_directories(itch_aeron SYSTEM PUBLIC
    "${aeron_SOURCE_DIR}/aeron-client/src/main/cpp_wrapper"
    "${aeron_SOURCE_DIR}/aeron-client/src/main/c"
    "${aeron_SOURCE_DIR}/aeron-driver/src/main/c")
endif()

# --- Feed tools ---------------------------------------------------------------------
add_executable(itch_mold_sender tools/itch_mold_sender.cpp)
target_link_libraries(itch_mold_sender PRIVATE itch_feed)
add_executable(itch_feed_handler tools/itch_feed_handler.cpp)
target_link_libraries(itch_feed_handler PRIVATE itch_feedio)
if(ITCH_WITH_AERON)
  target_link_libraries(itch_feed_handler PRIVATE itch_aeron)
endif()

# --- Order book + book manager ------------------------------------------------
add_library(itch_book STATIC src/book/order_book.cpp src/book/book_manager.cpp)
target_link_libraries(itch_book PUBLIC itch_protocol)

add_executable(itch_book_replay tools/itch_book_replay.cpp)
target_link_libraries(itch_book_replay PRIVATE itch_book)

enable_testing()
add_subdirectory(tests)
