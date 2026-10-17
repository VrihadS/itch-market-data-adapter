#pragma once
// Routes decoded ITCH messages to one OrderBook per stock locate.
// Implements the decoder's handler interface, so it can be passed straight to
// NasdaqItchDecoder::DecodeMessage (MoldUDP64 framing is handled by the net/feed layers).

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "itch/book/book_listener.hpp"
#include "itch/book/order_book.hpp"
#include "itch/book/order_store.h"
#include "itch/protocol/ndaq_itch_decoder.h"

namespace feed_handler {

struct ManagerStats {
    uint64_t book_messages   = 0;   // A/F/E/C/X/D/U applied
    uint64_t books_created   = 0;
    uint64_t lazy_books      = 0;   // created by an order message before any Stock Directory
    uint64_t bad_side        = 0;   // add with side other than 'B'/'S'
    uint64_t crossing_events = 0;   // messages after which the touched book was crossed/locked
    uint64_t book_events     = 0;   // on_book_update calls made to the listener
    uint64_t trade_events    = 0;   // on_trade calls made to the listener
};

class BookManager : public NasdaqItchMessageHandler {
public:
    static constexpr size_t kMaxLocate = 65536;   // locate is a uint16_t

    // `expected_orders` pre-sizes the order pool and ref table (peak simultaneously live
    // orders; ~1.9M on a busy 2019 day). Exceeding it still works but grows on the heap.
    explicit BookManager(size_t expected_orders = kDefaultExpectedOrders);

    static constexpr size_t kDefaultExpectedOrders = size_t{1} << 22;   // 4,194,304

    // When on, counts every message that leaves its book crossed or locked.
    // Crossing is normal during halts and auctions; this is a statistic, not an error.
    void set_track_crossing(bool on) { track_crossing_ = on; }

    // ── decoder callbacks ──────────────────────────────────────
    void OnStockDirectory(const NasdaqItch::StockDirectory &m) override;
    void OnStockTradingAction(const NasdaqItch::StockTradingAction &m) override;
    void OnAddOrder(const NasdaqItch::AddOrder &m) override;          // A and F
    void OnOrderExecuted(const NasdaqItch::OrderExecuted &m) override; // E and C
    void OnOrderCancel(const NasdaqItch::OrderCancel &m) override;
    void OnOrderDelete(const NasdaqItch::OrderDelete &m) override;
    void OnOrderReplace(const NasdaqItch::OrderReplace &m) override;
    void OnTrade(const NasdaqItch::Trade &m) override;                 // P
    void OnCrossTrade(const NasdaqItch::CrossTrade &m) override;       // Q
    void OnBrokenTrade(const NasdaqItch::BrokenTrade &m) override;     // B

    // ── subscriptions (used by the messaging layer) ────────────
    // The listener is called only for instruments with interest set. `features` is a
    // mask of kInterestTrades / kInterestBook; `depth` (1..) applies to kInterestBook.
    // Must be called from the same thread that feeds messages.
    void set_listener(BookListener *l) { listener_ = l; }
    BookListener *listener() const { return listener_; }
    void set_interest(uint16_t locate, uint8_t features, uint8_t depth);
    uint8_t interest(uint16_t locate) const { return interest_[locate]; }
    uint8_t interest_depth(uint16_t locate) const { return depth_[locate]; }
    std::optional<uint16_t> locate_of(std::string_view symbol) const;   // O(1), from Stock Directory
    uint64_t last_timestamp_ns() const { return last_ts_; }              // of the latest book message

    // ── queries ────────────────────────────────────────────────
    const OrderBook *book(uint16_t locate) const { return books_[locate].get(); }
    OrderBook       *book(uint16_t locate)       { return books_[locate].get(); }
    const char      *symbol(uint16_t locate) const { return symbols_[locate].data(); }
    char             trading_state(uint16_t locate) const { return states_[locate]; }
    // Linear scan over all locates; for tools and tests, not the hot path.
    const OrderBook *find_by_symbol(const char *symbol) const;

    size_t book_count()   const { return stats_.books_created; }
    size_t live_orders()  const { return store_.live_orders(); }
    size_t store_memory_bytes() const { return store_.memory_bytes(); }
    const StoreStats &store_stats() const { return store_.stats(); }
    size_t crossed_books() const;

    const ManagerStats &stats() const { return stats_; }
    BookStats aggregate_book_stats() const;   // summed over all books

    // Empties every book, the order store, symbols and trading states (a new session, or a rebuild from
    // sequence 1). Subscriptions (set_interest) and the listener are kept.
    void reset();

    // A 64-bit fingerprint of everything observable: per book (in locate order) the live order count and the
    // top `depth` levels of both sides. Two managers fed the same messages have the same digest.
    uint64_t state_digest(size_t depth = 10) const;

    // Validates every book's structure and that the store's live-order count
    // equals the sum over books. Crossing is not treated as a failure.
    bool validate_all() const;

private:
    OrderBook &get_or_create(uint16_t locate, bool lazy);
    void       after(OrderBook &b, uint64_t ts);
    uint8_t    pre(uint16_t locate, OrderBook &b);
    void       post(uint16_t locate, OrderBook &b, uint8_t interest, uint64_t ts);
    void       emit_trade(uint16_t locate, char kind, char side, char cross_type, int32_t price,
                          uint64_t shares, uint64_t match, uint64_t ts);

    OrderStore                              store_;
    std::vector<std::unique_ptr<OrderBook>> books_;
    std::vector<std::array<char, 9>>        symbols_;
    std::vector<char>                       states_;
    ManagerStats                            stats_;
    bool                                    track_crossing_ = false;
    BookListener                           *listener_ = nullptr;
    std::vector<uint8_t>                    interest_;
    std::vector<uint8_t>                    depth_;
    std::unordered_map<std::string, uint16_t> locate_by_symbol_;
    uint64_t                                last_ts_ = 0;
};

} // namespace feed_handler
