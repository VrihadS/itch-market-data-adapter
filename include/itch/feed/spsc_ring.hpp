#pragma once
// Single-producer / single-consumer ring of variable-size records.
//
// The network thread writes records (sequenced ITCH messages and session events); the book thread
// reads them in place and pops. Wait-free on both sides: no locks, no allocation after construction.
//
//   producer:  uint8_t *p = ring.claim(type, size, control); if (!p) {full}; fill p[0..size); ring.publish();
//   consumer:  SpscRing::Record r; while (ring.peek(r)) { use r.data[0..r.size); ring.pop(); }
//
// Records are 8-byte aligned and never wrap: when one would straddle the end of the buffer a pad record
// fills the rest and the record starts at offset 0. `control` records may use the last `control_reserve`
// bytes of free space that data records cannot, so a session reset or an end-of-session marker still
// gets through when a slow consumer has filled the ring with data.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>

namespace feed_handler::feed {

class SpscRing {
public:
    struct Record {
        uint32_t       type;
        uint32_t       size;    // payload bytes
        const uint8_t *data;
    };

    static constexpr size_t   kAlign   = 8;
    static constexpr size_t   kHeader  = 8;
    static constexpr uint32_t kPadType = 0xFFFFFFFFu;

    // `capacity_bytes` is rounded up to a power of two (minimum 4 KiB).
    explicit SpscRing(size_t capacity_bytes, size_t control_reserve_bytes = 64 * 1024)
    {
        size_t cap = 4096;
        while (cap < capacity_bytes) cap <<= 1;
        capacity_ = cap;
        mask_ = cap - 1;
        reserve_ = control_reserve_bytes < cap / 2 ? control_reserve_bytes : cap / 2;
        buf_.reset(static_cast<uint8_t *>(::operator new[](cap, std::align_val_t(64))));
        std::memset(buf_.get(), 0, cap);
    }

    size_t capacity() const { return capacity_; }
    // The largest payload a single record may carry.
    size_t max_payload() const { return capacity_ / 4 - kHeader; }

    // ── producer ───────────────────────────────────────────────────────
    // Returns where to write `size` payload bytes, or nullptr if there is no room (or the record is too
    // large). Call publish() after filling it; at most one claim may be outstanding.
    uint8_t *claim(uint32_t type, uint32_t size, bool control = false)
    {
        if (size > max_payload()) return nullptr;
        const size_t total = record_bytes(size);
        const size_t offset = tail_ & mask_;
        const size_t pad = (offset + total > capacity_) ? capacity_ - offset : 0;
        const size_t need = pad + total + (control ? 0 : reserve_);
        if (capacity_ - (tail_ - head_cache_) < need) {
            head_cache_ = head_pub_.load(std::memory_order_acquire);
            if (capacity_ - (tail_ - head_cache_) < need) return nullptr;
        }
        if (pad) {
            write_header(offset, kPadType, static_cast<uint32_t>(pad - kHeader));
            tail_ += pad;
        }
        const size_t at = tail_ & mask_;
        write_header(at, type, size);
        pending_ = total;
        return buf_.get() + at + kHeader;
    }

    void publish()
    {
        tail_ += pending_;
        pending_ = 0;
        tail_pub_.store(tail_, std::memory_order_release);
    }

    bool push(uint32_t type, const void *data, uint32_t size, bool control = false)
    {
        uint8_t *p = claim(type, size, control);
        if (!p) return false;
        if (size) std::memcpy(p, data, size);
        publish();
        return true;
    }

    // ── consumer ───────────────────────────────────────────────────────
    bool peek(Record &out)
    {
        for (;;) {
            if (head_ == tail_cache_) {
                tail_cache_ = tail_pub_.load(std::memory_order_acquire);
                if (head_ == tail_cache_) return false;
            }
            const size_t at = head_ & mask_;
            uint32_t size, type;
            std::memcpy(&size, buf_.get() + at, 4);
            std::memcpy(&type, buf_.get() + at + 4, 4);
            if (type == kPadType) {                      // skip the filler at the end of the buffer
                head_ += kHeader + size;
                head_pub_.store(head_, std::memory_order_release);
                continue;
            }
            out = Record{type, size, buf_.get() + at + kHeader};
            return true;
        }
    }

    void pop()
    {
        uint32_t size;
        std::memcpy(&size, buf_.get() + (head_ & mask_), 4);
        head_ += record_bytes(size);
        head_pub_.store(head_, std::memory_order_release);
    }

    // Approximate occupancy; safe to call from either thread.
    size_t used_bytes() const
    {
        return static_cast<size_t>(tail_pub_.load(std::memory_order_acquire) - head_pub_.load(std::memory_order_acquire));
    }

private:
    static size_t record_bytes(uint32_t size) { return kHeader + ((static_cast<size_t>(size) + kAlign - 1) & ~(kAlign - 1)); }
    void write_header(size_t at, uint32_t type, uint32_t size)
    {
        std::memcpy(buf_.get() + at, &size, 4);
        std::memcpy(buf_.get() + at + 4, &type, 4);
    }

    struct AlignedDelete { void operator()(uint8_t *p) const { ::operator delete[](p, std::align_val_t(64)); } };
    std::unique_ptr<uint8_t[], AlignedDelete> buf_;
    size_t capacity_ = 0, mask_ = 0, reserve_ = 0;

    // producer-owned
    alignas(64) uint64_t tail_ = 0;
    uint64_t head_cache_ = 0;
    size_t   pending_ = 0;
    // consumer-owned
    alignas(64) uint64_t head_ = 0;
    uint64_t tail_cache_ = 0;
    // shared positions, each on its own cache line
    alignas(64) std::atomic<uint64_t> tail_pub_{0};
    alignas(64) std::atomic<uint64_t> head_pub_{0};
};

}  // namespace feed_handler::feed
