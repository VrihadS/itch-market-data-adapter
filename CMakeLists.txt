cmake_minimum_required(VERSION 3.20)
project(itch_adapter VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)   # -O3 -DNDEBUG on GCC/Clang
endif()

# ── Options ───────────────────────────────────────────────────────────────────────────────
option(ITCH_WITH_AERON  "Build the Aeron market data server/client (downloads Aeron unless ITCH_AERON_SOURCE_DIR is set)" ON)
set(ITCH_AERON_SOURCE_DIR "" CACHE PATH "Use this local Aeron checkout instead of downloading one")
option(ITCH_BUILD_TESTS "Build the test suite" ON)
option(ITCH_NATIVE      "Tune for THIS machine's CPU (-march=native / -mcpu=native); the binaries may not run on other CPUs" OFF)
option(ITCH_LTO         "Link-time optimisation (inlines across libraries)" OFF)
option(ITCH_SANITIZE    "AddressSanitizer + UndefinedBehaviorSanitizer" OFF)
option(ITCH_TSAN        "ThreadSanitizer (use a separate build directory)" OFF)
if(ITCH_SANITIZE AND ITCH_TSAN)
  message(FATAL_ERROR "ITCH_SANITIZE and ITCH_TSAN cannot be combined: build them in separate directories")
endif()

# Optimisation switches that apply to everything built here, including Aeron.
if(ITCH_LTO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT itch_ipo_ok OUTPUT itch_ipo_msg)
  if(itch_ipo_ok)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
  else()
    message(WARNING "ITCH_LTO requested but not supported by this toolchain: ${itch_ipo_msg}")
  endif()
endif()

set(ITCH_CPU_FLAG "")
if(ITCH_NATIVE)
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-march=native" itch_has_march_native)
  if(itch_has_march_native)
    set(ITCH_CPU_FLAG "-march=native")
  else()
    check_cxx_compiler_flag("-mcpu=native" itch_has_mcpu_native)   # Apple Silicon / AArch64 toolchains
    if(itch_has_mcpu_native)
      set(ITCH_CPU_FLAG "-mcpu=native")
    else()
      message(WARNING "ITCH_NATIVE requested but the compiler accepts neither -march=native nor -mcpu=native")
    endif()
  endif()
endif()

# ── Dependencies ──────────────────────────────────────────────────────────────────────────
find_package(Threads REQUIRED)
find_package(Boost 1.74 REQUIRED)       # Asio is header-only; only the headers are used

find_package(spdlog CONFIG QUIET)
if(NOT spdlog_FOUND)
  include(FetchContent)
  FetchContent_Declare(spdlog GIT_REPOSITORY https://github.com/gabime/spdlog.git GIT_TAG v1.14.1 GIT_SHALLOW TRUE)
  FetchContent_MakeAvailable(spdlog)
endif()

# folly is optional: it only supplies the lock-free task queue of WorkerThread's busy-spin mode.
find_package(folly CONFIG QUIET)

# ── Common compile settings (every target links `itch`) ───────────────────────────────────
add_library(itch INTERFACE)
target_include_directories(itch INTERFACE include)
target_compile_options(itch INTERFACE -Wall -Wextra -Wpedantic)
if(ITCH_CPU_FLAG)
  target_compile_options(itch INTERFACE ${ITCH_CPU_FLAG})
endif()
if(ITCH_TSAN)
  target_compile_options(itch INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
  target_link_options(itch INTERFACE -fsanitize=thread)
endif()
if(ITCH_SANITIZE)
  target_compile_options(itch INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(itch INTERFACE -fsanitize=address,undefined)
endif()

# ── Libraries, in dependency order ────────────────────────────────────────────────────────
# ITCH 5.0 decoder (knows nothing about networking or books)
add_library(itch_protocol STATIC src/protocol/ndaq_itch_decoder.cpp)
target_link_libraries(itch_protocol PUBLIC itch spdlog::spdlog)

# Order books and the per-instrument manager
add_library(itch_book STATIC src/book/order_book.cpp src/book/book_manager.cpp)
target_link_libraries(itch_book PUBLIC itch_protocol)

# Sockets: network thread, multicast / unicast UDP receive (Boost.Asio)
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

# MoldUDP64 session logic: sequencing, A/B arbitration, recovery (no sockets inside)
add_library(itch_feed STATIC src/feed/feed_session.cpp)
if(UNIX)
  target_sources(itch_feed PRIVATE src/feed/mold_sender.cpp)       # test sender / fault injector (POSIX sockets)
endif()
target_link_libraries(itch_feed PUBLIC itch)

# The feed adapter: sockets -> session -> ring -> books
add_library(itch_feedio STATIC src/feed/feed_handler.cpp src/feed/feed_consumer.cpp)
target_link_libraries(itch_feedio PUBLIC itch_feed itch_net itch_book)

# Market data distribution protocol, server and client (transport-agnostic)
add_library(itch_messaging STATIC
  src/messaging/protocol.cpp
  src/messaging/channel.cpp
  src/messaging/transport.cpp
  src/messaging/market_data_server.cpp
  src/messaging/market_data_client.cpp)
target_link_libraries(itch_messaging PUBLIC itch_book)

if(UNIX)
  add_library(itch_gateway STATIC src/messaging/tcp_gateway.cpp)    # TCP front door for Python / Windows clients
  target_link_libraries(itch_gateway PUBLIC itch_messaging)
endif()

# ── Aeron transport and the tools that need it ────────────────────────────────────────────
if(ITCH_WITH_AERON)
  include(FetchContent)
  # Only the C media driver and the C++ client are needed (no Java, archive or tests).
  set(BUILD_AERON_DRIVER ON CACHE BOOL "" FORCE)
  set(BUILD_AERON_ARCHIVE_API OFF CACHE BOOL "" FORCE)
  set(AERON_TESTS OFF CACHE BOOL "" FORCE)
  set(AERON_UNIT_TESTS OFF CACHE BOOL "" FORCE)
  set(AERON_SYSTEM_TESTS OFF CACHE BOOL "" FORCE)
  set(AERON_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
  set(AERON_BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
  set(AERON_INSTALL_TARGETS OFF CACHE BOOL "" FORCE)
  set(CMAKE_POLICY_VERSION_MINIMUM 3.5)       # Aeron's CMake predates CMake 4's minimum policy version
  if(ITCH_AERON_SOURCE_DIR)
    set(FETCHCONTENT_SOURCE_DIR_AERON "${ITCH_AERON_SOURCE_DIR}")
  endif()
  FetchContent_Declare(aeron GIT_REPOSITORY https://github.com/aeron-io/aeron.git GIT_TAG 1.53.3 GIT_SHALLOW TRUE)
  FetchContent_MakeAvailable(aeron)

  add_library(itch_aeron STATIC src/messaging/aeron_transport.cpp)
  target_compile_definitions(itch_aeron PUBLIC ITCH_WITH_AERON)
  target_link_libraries(itch_aeron PUBLIC itch_messaging aeron_client_wrapper aeron_driver_static aeron_static)
  # Aeron's headers are not warning-clean under our flags: treat them as system headers.
  target_include_directories(itch_aeron SYSTEM PUBLIC
    "${aeron_SOURCE_DIR}/aeron-client/src/main/cpp_wrapper"
    "${aeron_SOURCE_DIR}/aeron-client/src/main/c"
    "${aeron_SOURCE_DIR}/aeron-driver/src/main/c")

  add_executable(itch_md_server tools/itch_md_server.cpp)
  target_link_libraries(itch_md_server PRIVATE itch_aeron)
  add_executable(itch_md_client tools/itch_md_client.cpp)
  target_link_libraries(itch_md_client PRIVATE itch_aeron)
  if(UNIX)
    add_executable(itch_md_gateway tools/itch_md_gateway.cpp)
    target_link_libraries(itch_md_gateway PRIVATE itch_aeron itch_gateway)
  endif()
endif()

# ── Tools ─────────────────────────────────────────────────────────────────────────────────
add_executable(itch_file_replay tools/itch_file_replay.cpp)          # decode a file, count and sanity-check messages
target_link_libraries(itch_file_replay PRIVATE itch_protocol)

add_executable(itch_book_replay tools/itch_book_replay.cpp)          # decode + build books, verify, show books
target_link_libraries(itch_book_replay PRIVATE itch_book)

add_executable(itch_mold_sender tools/itch_mold_sender.cpp)          # replay a file as MoldUDP64 with faults
target_link_libraries(itch_mold_sender PRIVATE itch_feed)

add_executable(itch_feed_handler tools/itch_feed_handler.cpp)        # the feed adapter
target_link_libraries(itch_feed_handler PRIVATE itch_feedio)
if(ITCH_WITH_AERON)
  target_link_libraries(itch_feed_handler PRIVATE itch_aeron)        # enables --serve
endif()

# ── Tests ─────────────────────────────────────────────────────────────────────────────────
if(ITCH_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# ── Summary ───────────────────────────────────────────────────────────────────────────────
message(STATUS "")
message(STATUS "itch_adapter ${PROJECT_VERSION}: build type ${CMAKE_BUILD_TYPE}, ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  native tuning : ${ITCH_NATIVE} ${ITCH_CPU_FLAG}")
message(STATUS "  LTO           : ${ITCH_LTO}")
message(STATUS "  Aeron         : ${ITCH_WITH_AERON}    tests: ${ITCH_BUILD_TESTS}")
message(STATUS "  sanitizers    : address+UB=${ITCH_SANITIZE} thread=${ITCH_TSAN}")
message(STATUS "  folly         : ${folly_FOUND} (optional)")
message(STATUS "")
