#include "itch/book/book_manager.hpp"

#include <cstdio>
#include <cstring>

namespace feed_handler {

namespace {
// ITCH side 'B' -> 0 (buy), 'S' -> 1 (sell); anything else is invalid.
inline int SideOf(char c) { return c == 'B' ? 0 : (c == 'S' ? 1 : -1); }
} // namespace

BookManager::BookManager(size_t expected_orders)
    : store_(expected_orders), books_(kMaxLocate), symbols_(kMaxLocate), states_(kMaxLocate, ' '),
      interest_(kMaxLocate, 0), depth_(kMaxLocate, 0)
{
    for (auto &s : symbols_) s.fill('\0');
}

OrderBook &BookManager::get_or_create(uint16_t locate, bool lazy)
{
    auto &slot = books_[locate];
    if (!slot)
    {
        slot = std::make_unique<OrderBook>(store_, locate);
        ++stats_.books_created;
        if (lazy) ++stats_.lazy_books;
    }
    return *slot;
}

void BookManager::after(OrderBook &b, uint64_t ts)
{
    ++stats_.book_messages;
    last_ts_ = ts;
    if (track_crossing_ && b.crossed()) ++stats_.crossing_events;
}

// Arms depth-change tracking for a watched book; returns the locate's interest flags.
uint8_t BookManager::pre(uint16_t locate, OrderBook &b)
{
    const uint8_t in = interest_[locate];
    if (in & kInterestBook) b.begin_touch(depth_[locate]);
    return in;
}

void BookManager::post(uint16_t locate, OrderBook &b, uint8_t in, uint64_t ts)
{
    if ((in & kInterestBook) && listener_ && b.depth_touched())
    {
        ++stats_.book_events;
        listener_->on_book_update(locate, b, ts);
    }
}

void BookManager::emit_trade(uint16_t locate, char kind, char side, char cross_type, int32_t price,
                             uint64_t shares, uint64_t match, uint64_t ts)
{
    if (!listener_ || !(interest_[locate] & kInterestTrades)) return;
    ++stats_.trade_events;
    listener_->on_trade(TradeEvent{locate, kind, side, cross_type, price, shares, match, ts});
}

void BookManager::set_interest(uint16_t locate, uint8_t features, uint8_t depth)
{
    interest_[locate] = features;
    depth_[locate]    = (features & kInterestBook) ? depth : 0;
    if (!(features & kInterestBook) && books_[locate]) books_[locate]->begin_touch(0);
}

void BookManager::reset()
{
    for (auto &b : books_) b.reset();
    store_.clear();
    for (auto &s : symbols_) s.fill('\0');
    std::fill(states_.begin(), states_.end(), ' ');
    locate_by_symbol_.clear();
    stats_ = ManagerStats{};
    last_ts_ = 0;
}

uint64_t BookManager::state_digest(size_t depth) const
{
    uint64_t h = 1469598103934665603ull;               // FNV-1a
    auto mix = [&h](uint64_t v) { for (int i = 0; i < 8; ++i) { h ^= (v & 0xFF); h *= 1099511628211ull; v >>= 8; } };
    std::vector<DepthLevel> lv(depth);
    for (size_t loc = 0; loc < kMaxLocate; ++loc)
    {
        const OrderBook *b = books_[loc].get();
        if (!b || (b->order_count() == 0 && b->level_count() == 0)) continue;   // an empty book and a missing one look the same
        mix(loc);
        mix(b->order_count());
        for (uint8_t side = 0; side < 2; ++side)
        {
            const size_t n = b->depth(side, depth, lv.data());
            mix(n);
            for (size_t i = 0; i < n; ++i) { mix(static_cast<uint32_t>(lv[i].price)); mix(lv[i].shares); mix(lv[i].orders); }
        }
    }
    mix(store_.live_orders());
    return h;
}

std::optional<uint16_t> BookManager::locate_of(std::string_view symbol) const
{
    // Stock Directory symbols are stored right-trimmed; ignore padding in the query.
    while (!symbol.empty() && (symbol.back() == ' ' || symbol.back() == '\0')) symbol.remove_suffix(1);
    const auto it = locate_by_symbol_.find(std::string(symbol));
    if (it == locate_by_symbol_.end()) return std::nullopt;
    return it->second;
}

void BookManager::OnStockDirectory(const NasdaqItch::StockDirectory &m)
{
    get_or_create(m.stock_locate, /*lazy=*/false);
    std::memcpy(symbols_[m.stock_locate].data(), m.stock, 9);
    locate_by_symbol_[m.stock] = m.stock_locate;
}

void BookManager::OnStockTradingAction(const NasdaqItch::StockTradingAction &m)
{
    states_[m.stock_locate] = m.trading_state;
}

void BookManager::OnAddOrder(const NasdaqItch::AddOrder &m)
{
    const int side = SideOf(m.buy_sell);
    if (side < 0) { ++stats_.bad_side; return; }
    OrderBook &b = get_or_create(m.stock_locate, /*lazy=*/true);
    const uint8_t in = pre(m.stock_locate, b);
    b.on_add(m.order_reference_number, m.price, m.shares, static_cast<uint8_t>(side));
    after(b, m.timestamp);
    post(m.stock_locate, b, in, m.timestamp);
}

void BookManager::OnOrderExecuted(const NasdaqItch::OrderExecuted &m)
{
    OrderBook &b = get_or_create(m.stock_locate, true);
    const uint8_t in = pre(m.stock_locate, b);
    const Reduced r = b.on_execute(m.order_reference_number, m.executed_shares);
    after(b, m.timestamp);
    // Time & sales: report the print before the resulting book change.
    if (r.ok && (in & kInterestTrades))
    {
        const char side = r.side == 0 ? 'B' : 'S';
        if (!m.has_price)                   // 'E': executes at the order's display price
            emit_trade(m.stock_locate, 'E', side, 0, r.price, r.shares, m.match_number, m.timestamp);
        else if (m.printable == 'Y')        // 'C': explicit price; non-printable executions are not prints
            emit_trade(m.stock_locate, 'C', side, 0, m.execution_price, r.shares, m.match_number, m.timestamp);
    }
    post(m.stock_locate, b, in, m.timestamp);
}

void BookManager::OnOrderCancel(const NasdaqItch::OrderCancel &m)
{
    OrderBook &b = get_or_create(m.stock_locate, true);
    const uint8_t in = pre(m.stock_locate, b);
    b.on_cancel(m.order_reference_number, m.cancelled_shares);
    after(b, m.timestamp);
    post(m.stock_locate, b, in, m.timestamp);
}

void BookManager::OnOrderDelete(const NasdaqItch::OrderDelete &m)
{
    OrderBook &b = get_or_create(m.stock_locate, true);
    const uint8_t in = pre(m.stock_locate, b);
    b.on_delete(m.order_reference_number);
    after(b, m.timestamp);
    post(m.stock_locate, b, in, m.timestamp);
}

void BookManager::OnOrderReplace(const NasdaqItch::OrderReplace &m)
{
    OrderBook &b = get_or_create(m.stock_locate, true);
    const uint8_t in = pre(m.stock_locate, b);
    b.on_replace(m.original_order_reference_number, m.new_order_reference_number, m.shares, m.price);
    after(b, m.timestamp);
    post(m.stock_locate, b, in, m.timestamp);
}

void BookManager::OnTrade(const NasdaqItch::Trade &m)
{
    emit_trade(m.stock_locate, 'P', m.buy_sell, 0, m.price, m.shares, m.match_number, m.timestamp);
}

void BookManager::OnCrossTrade(const NasdaqItch::CrossTrade &m)
{
    emit_trade(m.stock_locate, 'Q', 0, m.cross_type, m.cross_price, m.shares, m.match_number, m.timestamp);
}

void BookManager::OnBrokenTrade(const NasdaqItch::BrokenTrade &m)
{
    emit_trade(m.stock_locate, 'B', 0, 0, 0, 0, m.match_number, m.timestamp);
}

const OrderBook *BookManager::find_by_symbol(const char *symbol) const
{
    const auto loc = locate_of(symbol);
    return loc ? books_[*loc].get() : nullptr;
}

size_t BookManager::crossed_books() const
{
    size_t n = 0;
    for (const auto &b : books_)
        if (b && b->crossed()) ++n;
    return n;
}

BookStats BookManager::aggregate_book_stats() const
{
    BookStats t;
    for (const auto &b : books_)
    {
        if (!b) continue;
        const BookStats &s = b->stats();
        t.unknown_ref  += s.unknown_ref;
        t.wrong_locate += s.wrong_locate;
        t.rejected_add += s.rejected_add;
        t.inconsistent += s.inconsistent;
    }
    return t;
}

bool BookManager::validate_all() const
{
    bool   ok  = true;
    size_t sum = 0;
    for (const auto &b : books_)
    {
        if (!b) continue;
        if (!b->validate()) ok = false;
        sum += b->order_count();
    }
    if (sum != store_.live_orders())
    {
        std::fprintf(stderr, "live orders: books=%zu store=%zu\n", sum, store_.live_orders());
        ok = false;
    }
    return ok;
}

} // namespace feed_handler
