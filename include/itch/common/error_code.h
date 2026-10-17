#pragma once

#include <cstdint>

namespace feed_handler
{

enum class error_code : uint8_t
{
    ok = 0,
    failed_to_initialize,
    invalid_argument,
    not_initialized,
    already_started,
    socket_error,
    bind_failed,
    timeout,
    buffer_overflow,
    parse_error,
    sequence_gap,
    unknown,
};

constexpr const char *to_string(error_code e) noexcept
{
    switch (e)
    {
    case error_code::ok:                  return "ok";
    case error_code::failed_to_initialize: return "failed_to_initialize";
    case error_code::invalid_argument:    return "invalid_argument";
    case error_code::not_initialized:     return "not_initialized";
    case error_code::already_started:     return "already_started";
    case error_code::socket_error:        return "socket_error";
    case error_code::bind_failed:         return "bind_failed";
    case error_code::timeout:             return "timeout";
    case error_code::buffer_overflow:     return "buffer_overflow";
    case error_code::parse_error:         return "parse_error";
    case error_code::sequence_gap:        return "sequence_gap";
    case error_code::unknown:             return "unknown";
    }
    return "unknown";
}

} // namespace feed_handler
