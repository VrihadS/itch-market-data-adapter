#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace feed_handler {

constexpr uint32_t NULL_IDX = std::numeric_limits<uint32_t>::max();

// 32-byte order. Lives in a global pool; refs are unique across the
// whole session, so one shared store is enough for all symbols.
struct alignas(32) Order {
    uint64_t ref;
    int32_t  price;
    uint32_t shares;
    uint32_t prev;      // FIFO prev (index into store)
    uint32_t next;      // FIFO next (index into store)
    uint16_t locate;
    uint8_t  side;      // 0 = buy, 1 = sell
    uint8_t  _pad;
};
static_assert(sizeof(Order) == 32);

// How often the store had to leave its pre-sized buffers and grow on the heap.
// All zero means the run stayed inside the capacity given at construction.
struct StoreStats {
    uint64_t pool_growths      = 0;   // order pool reallocated (copies every order)
    uint64_t free_list_growths = 0;
    uint64_t table_growths     = 0;   // ref table rehashed
    uint64_t total() const { return pool_growths + free_list_growths + table_growths; }
};

// Order pool plus an open-addressing ref -> pool-index table.
//
// Everything is pre-sized from `expected_orders` (the peak number of simultaneously
// live orders), so steady-state operation does not allocate. If more orders go live
// than that, the store keeps working by growing on the heap (vector doubling and a
// table rehash) and counts each such event in stats(). Pre-sizing reserves address
// space; pages are only touched as the pool fills, apart from the ref table, which
// is initialized up front (4 bytes x 2 x expected_orders, rounded to a power of two).
//
// The table holds only 4-byte pool indices (the key is read back from the
// order itself), uses linear probing, and deletes by backward shift, so there
// are no tombstones. Its size follows the number of LIVE orders, not the range
// of ref numbers seen: on a full ITCH day refs run to hundreds of millions
// while only ~2 million orders are live at once. Refs are full 64-bit values.
class OrderStore {
public:
    static constexpr size_t kDefaultExpectedOrders = size_t{1} << 16;

    explicit OrderStore(size_t expected_orders = kDefaultExpectedOrders) {
        orders_.reserve(expected_orders);
        free_.reserve(expected_orders);
        unsigned bits = kMinBits;
        while ((size_t{1} << bits) < expected_orders * 2) ++bits;   // load factor <= 0.5 at capacity
        init_table(bits);
    }

    OrderStore(const OrderStore&)            = delete;
    OrderStore& operator=(const OrderStore&) = delete;

    /// Insert a new order and index it by ref. Caller initializes
    /// the returned slot (price, shares, side, locate, links).
    /// Returns NULL_IDX (and changes nothing) if the ref is already live.
    uint32_t alloc(uint64_t ref) {
        if (find(ref) != NULL_IDX) return NULL_IDX;   // duplicate live ref
        if ((live_ + 1) * 2 > table_.size()) grow();

        uint32_t idx;
        if (!free_.empty()) {
            idx = free_.back();
            free_.pop_back();
        } else {
            if (orders_.size() == orders_.capacity()) ++stats_.pool_growths;
            orders_.emplace_back();
            idx = static_cast<uint32_t>(orders_.size() - 1);
        }
        orders_[idx] = Order{};
        orders_[idx].ref  = ref;
        orders_[idx].prev = NULL_IDX;
        orders_[idx].next = NULL_IDX;
        insert(idx);
        ++live_;
        return idx;
    }

    void free(uint32_t idx) {
        erase(idx);
        --live_;
        if (free_.size() == free_.capacity()) ++stats_.free_list_growths;
        free_.push_back(idx);
    }

    /// Lookup. Returns NULL_IDX on miss. Never allocates.
    uint32_t find(uint64_t ref) const {
        for (size_t i = home(ref);; i = (i + 1) & mask_) {
            const uint32_t idx = table_[i];
            if (idx == NULL_IDX) return NULL_IDX;
            if (orders_[idx].ref == ref) return idx;
        }
    }

    Order&       at(uint32_t idx)       { return orders_[idx]; }
    const Order& at(uint32_t idx) const { return orders_[idx]; }

    size_t live_orders() const { return live_; }

    /// Forgets every order but keeps the pre-sized memory.
    void clear() {
        orders_.clear();
        free_.clear();
        std::fill(table_.begin(), table_.end(), NULL_IDX);
        live_ = 0;
        stats_ = StoreStats{};
    }
    const StoreStats& stats() const { return stats_; }

    /// Bytes held by the pool, free list and index table (capacity, not size).
    size_t memory_bytes() const {
        return orders_.capacity() * sizeof(Order) + free_.capacity() * sizeof(uint32_t) +
               table_.capacity() * sizeof(uint32_t);
    }

private:
    static constexpr unsigned kMinBits = 4;

    size_t home(uint64_t ref) const {
        return static_cast<size_t>((ref * 0x9E3779B97F4A7C15ull) >> shift_);
    }

    void init_table(unsigned bits) {
        table_.assign(size_t{1} << bits, NULL_IDX);
        mask_  = table_.size() - 1;
        shift_ = 64 - bits;
    }

    void insert(uint32_t idx) {
        size_t i = home(orders_[idx].ref);
        while (table_[i] != NULL_IDX) i = (i + 1) & mask_;
        table_[i] = idx;
    }

    void grow() {
        ++stats_.table_growths;
        const unsigned bits = 64 - shift_ + 1;
        std::vector<uint32_t> old;
        old.swap(table_);
        init_table(bits);
        for (uint32_t idx : old)
            if (idx != NULL_IDX) insert(idx);
    }

    // Backward-shift deletion: keeps probe chains intact without tombstones.
    void erase(uint32_t idx) {
        size_t i = home(orders_[idx].ref);
        while (table_[i] != idx) i = (i + 1) & mask_;      // idx is always present
        size_t j = i;
        for (;;) {
            j = (j + 1) & mask_;
            const uint32_t cand = table_[j];
            if (cand == NULL_IDX) break;
            const size_t h = home(orders_[cand].ref);
            // Move cand into the hole at i unless its home lies cyclically in (i, j].
            const bool stays = (i <= j) ? (i < h && h <= j) : (i < h || h <= j);
            if (!stays) { table_[i] = cand; i = j; }
        }
        table_[i] = NULL_IDX;
    }

    std::vector<Order>    orders_;
    std::vector<uint32_t> free_;
    std::vector<uint32_t> table_;
    size_t                mask_  = 0;
    unsigned              shift_ = 0;
    size_t                live_  = 0;
    StoreStats            stats_;
};

} // namespace feed_handler
