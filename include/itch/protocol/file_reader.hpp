#pragma once
// Reads the raw Nasdaq ITCH 5.0 sample-file framing: each message is preceded
// by a 2-byte big-endian length. (No MoldUDP64 framing in these files.)

#include <cstddef>
#include <cstdint>
#include <cstdio>

namespace itch {

class FileReader
{
  public:
    enum class Status { Ok, Eof, Truncated };

    explicit FileReader(std::FILE *f) : f_(f) {}

    // On Ok, `data`/`len` point into an internal buffer valid until the next call.
    Status Next(const char *&data, size_t &len)
    {
        unsigned char hdr[2];
        const size_t  h = std::fread(hdr, 1, 2, f_);
        if (h == 0) return Status::Eof;
        if (h != 2) return Status::Truncated;

        len = (static_cast<size_t>(hdr[0]) << 8) | hdr[1];
        if (len == 0) { data = buf_; return Status::Ok; }
        if (std::fread(buf_, 1, len, f_) != len) return Status::Truncated;
        data = buf_;
        return Status::Ok;
    }

  private:
    std::FILE *f_;
    char       buf_[65536];
};

} // namespace itch
