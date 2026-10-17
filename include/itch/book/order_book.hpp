#pragma once
#include "order_store.h"
#include <cstddef>
#include <cstdint>
#include <map>

namespace feed_handler {

struct Level {
    uint64_t total_shares = 0;
    uint32_t order_count  = 0;
    uint32_t head         = NULL_IDX;
    uint32_t tail         = NULL_IDX;
};

// Counters for messages the book could not apply. Non-zero values on clean
// data indicate a bug or a feed gap, so they are surfaced rather than ignored.
struct BookStats {
    uint64_t unknown_ref   = 0;   // E/C/X/D/U for a ref that is not live
    uint64_t wrong_locate  = 0;   // order belongs to a different book
    uint64_t rejected_add  = 0;   // ref >= 2^32 or already live
    uint64_t inconsistent  = 0;   // order's level missing from the map (invariant violation)
};

// Result of removing shares from a resting order (execute / cancel / delete).
struct Reduced {
    bool     ok     = false;   // false: order not found (counted in BookStats)
    int32_t  price  = 0;       // the order's display price
    uint8_t  side   = 0;       // 0 = buy, 1 = sell
    uint32_t shares = 0;       // shares actually removed (clamped to what was resting)
};

struct DepthLevel {
    int32_t  price;
    uint64_t shares;
    uint32_t orders;
};

class OrderBook {
public:
    OrderBook(OrderStore& store, uint16_t locate)
        : store_(&store), locate_(locate) {}

    // ── A / F : add a resting order ─────────────────────────────
    // Returns false (and counts it) if the order could not be stored.
    inline bool on_add(uint64_t ref, int32_t price, uint32_t shares, uint8_t side) {
        const uint32_t oi = store_->alloc(ref);
        if (oi == NULL_IDX) { ++stats_.rejected_add; return false; }
        Order& o = store_->at(oi);
        o.price  = price;
        o.shares = shares;
        o.side   = side;
        o.locate = locate_;

        auto& levels = (side == 0) ? bids_ : asks_;
        Level& lv = levels[price];       // creates if missing
        o.prev = lv.tail;
        o.next = NULL_IDX;
        if (lv.tail != NULL_IDX) store_->at(lv.tail).next = oi;
        else                     lv.head = oi;
        lv.tail = oi;
        lv.total_shares += shares;
        ++lv.order_count;
        note_touch(side, price);
        return true;
    }

    // ── E / C : execution at the resting order's display price ──
    // Returns what was executed so callers can report the print.
    inline Reduced on_execute(uint64_t ref, uint32_t shares) { return reduce(ref, shares); }

    // ── X : partial cancel ──────────────────────────────────────
    inline void on_cancel(uint64_t ref, uint32_t shares) { (void)reduce(ref, shares); }

    // ── D : full delete ─────────────────────────────────────────
    inline void on_delete(uint64_t ref) {
        const uint32_t oi = lookup(ref);
        if (oi == NULL_IDX) return;
        const Order& o = store_->at(oi);
        reduce_at(oi, o.shares);
    }

    // ── U : replace = delete old + add new (side & locate carried) ─
    // The new order goes to the back of its level (loses time priority).
    inline void on_replace(uint64_t old_ref, uint64_t new_ref,
                           uint32_t shares, int32_t price) {
        const uint32_t oi = lookup(old_ref);
        if (oi == NULL_IDX) return;
        const uint8_t side = store_->at(oi).side;   // copy BEFORE delete
        on_delete(old_ref);
        on_add(new_ref, price, shares, side);
    }

    // ── best price ──────────────────────────────────────────────
    bool best_bid(int32_t& px, uint64_t& sh) const {
        if (bids_.empty()) return false;
        auto it = bids_.rbegin();
        px = it->first;  sh = it->second.total_shares;
        return true;
    }
    bool best_ask(int32_t& px, uint64_t& sh) const {
        if (asks_.empty()) return false;
        auto it = asks_.begin();
        px = it->first;  sh = it->second.total_shares;
        return true;
    }

    // Copies up to `n` price levels, best first, into `out`; returns how many.
    // side: 0 = bids (highest first), 1 = asks (lowest first).
    size_t depth(uint8_t side, size_t n, DepthLevel* out) const {
        size_t k = 0;
        if (side == 0) {
            for (auto it = bids_.rbegin(); it != bids_.rend() && k < n; ++it, ++k)
                out[k] = {it->first, it->second.total_shares, it->second.order_count};
        } else {
            for (auto it = asks_.begin(); it != asks_.end() && k < n; ++it, ++k)
                out[k] = {it->first, it->second.total_shares, it->second.order_count};
        }
        return k;
    }

    // ── "did the top N levels change?" tracking ─────────────────
    // Call begin_touch(N) before applying a message to a book someone watches;
    // afterwards depth_touched() is true if the message added, removed or resized
    // a level within the best N of its side. begin_touch(0) turns tracking off.
    // Conservative: replacing an order in place reports a touch even though the
    // visible image may be unchanged. Never reports false when the image changed.
    void begin_touch(uint32_t depth) { touch_depth_ = depth; touch_rank_ = UINT32_MAX; }
    bool depth_touched() const { return touch_rank_ < touch_depth_; }

    // Crossed/locked books are legitimate during halts and auctions, so this is
    // a statistic for callers to interpret, not a structural error.
    bool crossed() const {
        if (bids_.empty() || asks_.empty()) return false;
        return bids_.rbegin()->first >= asks_.begin()->first;
    }

    size_t level_count() const { return bids_.size() + asks_.size(); }
    size_t order_count() const;                  // sum over levels (O(levels))
    uint16_t locate() const { return locate_; }
    const BookStats& stats() const { return stats_; }

    // Structural invariants only (level sums/counts, FIFO links, price/side/
    // locate of each order, ref index round-trip). Does NOT fail on crossing.
    bool validate() const;   // in .cpp

private:
    inline uint32_t lookup(uint64_t ref) {
        const uint32_t oi = store_->find(ref);
        if (oi == NULL_IDX) { ++stats_.unknown_ref; return NULL_IDX; }
        if (store_->at(oi).locate != locate_) { ++stats_.wrong_locate; return NULL_IDX; }
        return oi;
    }

    inline Reduced reduce(uint64_t ref, uint32_t shares) {
        const uint32_t oi = lookup(ref);
        if (oi == NULL_IDX) return {};
        return reduce_at(oi, shares);
    }

    // Number of levels strictly better than `price` on `side`, capped at touch_depth_.
    inline uint32_t rank_of(uint8_t side, int32_t price) const {
        uint32_t n = 0;
        if (side == 0) {
            for (auto it = bids_.rbegin(); it != bids_.rend() && it->first > price && n < touch_depth_; ++it) ++n;
        } else {
            for (auto it = asks_.begin(); it != asks_.end() && it->first < price && n < touch_depth_; ++it) ++n;
        }
        return n;
    }

    inline void note_touch(uint8_t side, int32_t price) {
        if (touch_depth_ == 0) return;
        const uint32_t r = rank_of(side, price);
        if (r < touch_rank_) touch_rank_ = r;
    }

    // Remove up to `shares` from the order; unlink it and drop the level when empty.
    inline Reduced reduce_at(uint32_t oi, uint32_t shares) {
        Order& o = store_->at(oi);
        const uint32_t cut   = (shares < o.shares) ? shares : o.shares;
        const int32_t  price = o.price;
        const uint8_t  side  = o.side;
        auto& levels = (side == 0) ? bids_ : asks_;
        auto it = levels.find(price);
        if (it == levels.end()) { ++stats_.inconsistent; return {}; }
        note_touch(side, price);                 // before the level can disappear
        Level& lv = it->second;
        lv.total_shares -= cut;
        o.shares        -= cut;

        if (o.shares == 0) {
            unlink_fifo(lv, oi);
            --lv.order_count;
            store_->free(oi);
            if (lv.order_count == 0) levels.erase(it);
        }
        return {true, price, side, cut};
    }

    inline void unlink_fifo(Level& lv, uint32_t oi) {
        Order& o = store_->at(oi);
        if (o.prev != NULL_IDX) store_->at(o.prev).next = o.next;
        else                    lv.head = o.next;
        if (o.next != NULL_IDX) store_->at(o.next).prev = o.prev;
        else                    lv.tail = o.prev;
        o.prev = o.next = NULL_IDX;
    }

    OrderStore* store_;
    uint16_t    locate_;
    BookStats   stats_;
    uint32_t    touch_depth_ = 0;             // 0 = tracking off
    uint32_t    touch_rank_  = UINT32_MAX;    // best (lowest) rank touched since begin_touch
    std::map<int32_t, Level> bids_;
    std::map<int32_t, Level> asks_;
};

} // namespace feed_handler
