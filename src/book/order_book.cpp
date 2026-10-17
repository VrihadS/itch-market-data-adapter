#include "itch/book/order_book.hpp"
#include <cstdio>

namespace feed_handler {

size_t OrderBook::order_count() const {
    size_t n = 0;
    for (const auto& kv : bids_) n += kv.second.order_count;
    for (const auto& kv : asks_) n += kv.second.order_count;
    return n;
}

bool OrderBook::validate() const {
    for (int side = 0; side < 2; ++side) {
        const auto& levels = (side == 0) ? bids_ : asks_;
        for (const auto& [px, lv] : levels) {
            uint64_t sum  = 0;
            uint32_t n    = 0;
            uint32_t prev = NULL_IDX;
            for (uint32_t oi = lv.head; oi != NULL_IDX; oi = store_->at(oi).next) {
                const Order& o = store_->at(oi);
                if (o.price  != px)       { std::fprintf(stderr, "locate=%u order price mismatch\n", locate_); return false; }
                if (o.side   != side)     { std::fprintf(stderr, "locate=%u order side mismatch\n", locate_);  return false; }
                if (o.locate != locate_)  { std::fprintf(stderr, "locate=%u order locate mismatch\n", locate_); return false; }
                if (o.prev   != prev)     { std::fprintf(stderr, "locate=%u FIFO prev link broken\n", locate_); return false; }
                if (store_->find(o.ref) != oi) { std::fprintf(stderr, "locate=%u ref index does not round-trip\n", locate_); return false; }
                sum += o.shares;
                prev = oi;
                ++n;
            }
            if (prev != lv.tail) { std::fprintf(stderr, "locate=%u level tail mismatch\n", locate_); return false; }
            if (sum != lv.total_shares || n != lv.order_count) {
                std::fprintf(stderr,
                    "level mismatch locate=%u side=%d price=%d sum=%llu exp=%llu "
                    "count=%u exp=%u\n",
                    locate_, side, px,
                    (unsigned long long)sum, (unsigned long long)lv.total_shares,
                    n, lv.order_count);
                return false;
            }
        }
    }
    return true;
}

} // namespace feed_handler
