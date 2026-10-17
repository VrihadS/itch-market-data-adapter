// Replays a raw ITCH 5.0 file through decoder -> BookManager (one OrderBook per locate).
//   gzip -dc day.gz | itch_book_replay [-n max_messages] [--until HH:MM:SS] [--verify] [--validate-every N]
//                                      [--show SYM[,SYM...]] [--depth N] [--expect-orders N] [--digest]
//
// --verify  also maintains an independent reference model (hash map of live orders plus
//           per-locate price maps) and compares best bid/ask after every book message.
//           Slower; use it to prove the book, not to time it.
// --expect-orders  pre-size the order pool/ref table for N live orders (default 4194304);
//           beyond that the store grows on the heap and reports it
// --until   stop before the first message stamped at or after this time (ET, since midnight)
// --show    print the order book of each symbol when the replay stops (top --depth levels, default 5)
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>
#include <unordered_map>

#include <spdlog/spdlog.h>

#include "itch/book/book_manager.hpp"
#include "itch/protocol/file_reader.hpp"

using namespace feed_handler;
namespace N = feed_handler::NasdaqItch;

namespace
{

// Reference model: deliberately simple and sharing no code with OrderBook/OrderStore.
class VerifyingManager : public BookManager
{
  public:
    explicit VerifyingManager(size_t expected_orders) : BookManager(expected_orders) {}
    uint64_t mismatches = 0, model_unknown = 0;

    void OnAddOrder(const N::AddOrder &m) override
    {
        BookManager::OnAddOrder(m);
        const uint8_t sd = m.buy_sell == 'B' ? 0 : 1;
        refs_[m.order_reference_number] = {m.stock_locate, m.price, m.shares, sd};
        add(m.stock_locate, m.price, m.shares, sd);
        check(m.stock_locate);
    }
    void OnOrderExecuted(const N::OrderExecuted &m) override
    {
        BookManager::OnOrderExecuted(m);
        reduce(m.order_reference_number, m.executed_shares);
        check(m.stock_locate);
    }
    void OnOrderCancel(const N::OrderCancel &m) override
    {
        BookManager::OnOrderCancel(m);
        reduce(m.order_reference_number, m.cancelled_shares);
        check(m.stock_locate);
    }
    void OnOrderDelete(const N::OrderDelete &m) override
    {
        BookManager::OnOrderDelete(m);
        reduce(m.order_reference_number, UINT32_MAX);
        check(m.stock_locate);
    }
    void OnOrderReplace(const N::OrderReplace &m) override
    {
        BookManager::OnOrderReplace(m);
        auto it = refs_.find(m.original_order_reference_number);
        if (it == refs_.end()) ++model_unknown;
        else
        {
            const Ref o = it->second;
            reduce(m.original_order_reference_number, UINT32_MAX);
            refs_[m.new_order_reference_number] = {o.loc, m.price, m.shares, o.side};
            add(o.loc, m.price, m.shares, o.side);
        }
        check(m.stock_locate);
    }
    size_t model_live() const { return refs_.size(); }

  private:
    struct Ref { uint16_t loc; int32_t px; uint32_t sh; uint8_t side; };
    std::unordered_map<uint64_t, Ref>                      refs_;
    std::unordered_map<uint16_t, std::map<int32_t, uint64_t>> bids_, asks_;

    void add(uint16_t l, int32_t px, uint32_t sh, uint8_t side) { (side ? asks_ : bids_)[l][px] += sh; }
    void reduce(uint64_t ref, uint32_t sh)
    {
        auto it = refs_.find(ref);
        if (it == refs_.end()) { ++model_unknown; return; }
        Ref &o = it->second;
        const uint32_t f = std::min(sh, o.sh);
        auto &lv = (o.side ? asks_ : bids_)[o.loc];
        auto pit = lv.find(o.px);
        pit->second -= f;
        if (!pit->second) lv.erase(pit);
        o.sh -= f;
        if (!o.sh) refs_.erase(it);
    }
    void check(uint16_t l)
    {
        const OrderBook *b = book(l);
        if (!b) return;
        int32_t p; uint64_t s; bool bad = false;
        auto &mb = bids_[l]; auto &ma = asks_[l];
        bool h = b->best_bid(p, s);
        bad |= h != !mb.empty() || (h && (p != mb.rbegin()->first || s != mb.rbegin()->second));
        h = b->best_ask(p, s);
        bad |= h != !ma.empty() || (h && (p != ma.begin()->first || s != ma.begin()->second));
        if (bad && mismatches++ < 5) std::fprintf(stderr, "MISMATCH at book message %llu, locate %u\n",
                                                  (unsigned long long)stats().book_messages, l);
    }
};

std::string Px(int32_t p) { char b[32]; std::snprintf(b, sizeof b, "%d.%04d", p / 10000, p % 10000); return b; }

void ShowBook(const BookManager &mgr, const std::string &sym, size_t depth)
{
    const OrderBook *b = mgr.find_by_symbol(sym.c_str());
    if (!b) { std::printf("\n%s: no such symbol\n", sym.c_str()); return; }
    std::vector<DepthLevel> asks(depth), bids(depth);
    asks.resize(b->depth(1, depth, asks.data()));
    bids.resize(b->depth(0, depth, bids.data()));
    int32_t bp = 0, ap = 0; uint64_t bs = 0, as = 0;
    const bool hb = b->best_bid(bp, bs), ha = b->best_ask(ap, as);
    std::printf("\n=== %s  (locate %u, trading state '%c', %zu orders, %zu price levels) ===\n",
                sym.c_str(), b->locate(), mgr.trading_state(b->locate()), b->order_count(), b->level_count());
    std::printf("  %-6s %12s %12s %8s\n", "side", "price", "shares", "orders");
    for (size_t i = asks.size(); i-- > 0;)
        std::printf("  %-6s %12s %12llu %8u\n", "ASK", Px(asks[i].price).c_str(), (unsigned long long)asks[i].shares, asks[i].orders);
    if (hb && ha)
        std::printf("  ------ spread %s%s\n", Px(ap - bp).c_str(), ap <= bp ? "  (CROSSED/LOCKED)" : "");
    else
        std::printf("  ------ one-sided book\n");
    for (const auto &l : bids)
        std::printf("  %-6s %12s %12llu %8u\n", "BID", Px(l.price).c_str(), (unsigned long long)l.shares, l.orders);
}

uint64_t BE48(const char *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 6; ++i) v = (v << 8) | static_cast<unsigned char>(p[i]);
    return v;
}

} // namespace

int main(int argc, char **argv)
{
    spdlog::set_level(spdlog::level::off);

    const char *path = nullptr;
    uint64_t    max_msgs = UINT64_MAX, validate_every = 10000000;
    bool        verify = false, print_digest = false;
    size_t      expect_orders = BookManager::kDefaultExpectedOrders;
    uint64_t    until_ns = UINT64_MAX;
    size_t      depth = 5;
    std::vector<std::string> show;
    for (int i = 1; i < argc; ++i)
    {
        if (!std::strcmp(argv[i], "-n") && i + 1 < argc) max_msgs = std::strtoull(argv[++i], nullptr, 10);
        else if (!std::strcmp(argv[i], "--validate-every") && i + 1 < argc) validate_every = std::strtoull(argv[++i], nullptr, 10);
        else if (!std::strcmp(argv[i], "--verify")) verify = true;
        else if (!std::strcmp(argv[i], "--digest")) print_digest = true;
        else if (!std::strcmp(argv[i], "--expect-orders") && i + 1 < argc) expect_orders = std::strtoull(argv[++i], nullptr, 10);
        else if (!std::strcmp(argv[i], "--depth") && i + 1 < argc) depth = std::strtoull(argv[++i], nullptr, 10);
        else if (!std::strcmp(argv[i], "--until") && i + 1 < argc)
        {
            int h = 0, m = 0, sec = 0;
            if (std::sscanf(argv[++i], "%d:%d:%d", &h, &m, &sec) < 2) { std::fprintf(stderr, "bad --until\n"); return 2; }
            until_ns = (uint64_t(h) * 3600 + uint64_t(m) * 60 + uint64_t(sec)) * 1000000000ull;
        }
        else if (!std::strcmp(argv[i], "--show") && i + 1 < argc)
        {
            std::string list = argv[++i]; size_t pos = 0;
            while (pos <= list.size()) { size_t c = list.find(',', pos); if (c == std::string::npos) c = list.size(); if (c > pos) show.push_back(list.substr(pos, c - pos)); pos = c + 1; }
        }
        else path = argv[i];
    }
    std::FILE *f = path ? std::fopen(path, "rb") : stdin;
    if (!f) { std::perror("open"); return 2; }
    std::setvbuf(f, nullptr, _IOFBF, 1 << 20);

    VerifyingManager   vm(expect_orders);
    BookManager        plain(expect_orders);
    BookManager       &mgr = verify ? static_cast<BookManager &>(vm) : plain;
    mgr.set_track_crossing(true);

    itch::FileReader  reader(f);
    NasdaqItchDecoder decoder;
    uint64_t total = 0, next_validate = validate_every, validate_fail = 0, decode_fail = 0;
    bool truncated = false;
    uint64_t last_ts = 0;

    const auto t0 = std::chrono::steady_clock::now();
    const char *data; size_t len;
    while (total < max_msgs)
    {
        const auto st = reader.Next(data, len);
        if (st == itch::FileReader::Status::Eof) break;
        if (st == itch::FileReader::Status::Truncated) { truncated = true; break; }
        if (len == 0) continue;
        if (until_ns != UINT64_MAX && len >= 11 && BE48(data + 5) >= until_ns) break;
        last_ts = len >= 11 ? BE48(data + 5) : last_ts;
        size_t used = 0;
        if (!decoder.DecodeMessage(data, len, used, mgr)) ++decode_fail;
        ++total;
        if (mgr.stats().book_messages >= next_validate)
        {
            if (!mgr.validate_all()) ++validate_fail;
            next_validate += validate_every;
        }
    }
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const bool final_ok = mgr.validate_all();

    const auto &st = mgr.stats();
    const BookStats bs = mgr.aggregate_book_stats();
    std::printf("messages read: %llu   book messages applied: %llu   decode failures: %llu\n",
                (unsigned long long)total, (unsigned long long)st.book_messages, (unsigned long long)decode_fail);
    std::printf("books: %llu (created without a Stock Directory: %llu)   live orders: %zu   books crossed now: %zu\n",
                (unsigned long long)st.books_created, (unsigned long long)st.lazy_books, mgr.live_orders(), mgr.crossed_books());
    std::printf("messages leaving a book crossed/locked: %llu (normal during halts/auctions)\n",
                (unsigned long long)st.crossing_events);
    std::printf("unapplied: unknown_ref=%llu wrong_locate=%llu rejected_add=%llu inconsistent=%llu bad_side=%llu\n",
                (unsigned long long)bs.unknown_ref, (unsigned long long)bs.wrong_locate,
                (unsigned long long)bs.rejected_add, (unsigned long long)bs.inconsistent, (unsigned long long)st.bad_side);
    std::printf("validate_all(): final=%s  periodic failures=%llu\n", final_ok ? "ok" : "FAIL", (unsigned long long)validate_fail);
    if (verify)
        std::printf("reference model: best bid/ask mismatches=%llu  live orders book=%zu model=%zu  model-unknown-ref=%llu\n",
                    (unsigned long long)vm.mismatches, vm.live_orders(), vm.model_live(), (unsigned long long)vm.model_unknown);
    std::printf("end of input: %s\n", truncated ? "TRUNCATED mid-message (partial download)" : "clean EOF");
    std::printf("stopped at %02d:%02d:%02d.%09llu ET\n", int(last_ts / 3600000000000ull), int(last_ts / 60000000000ull % 60),
                int(last_ts / 1000000000ull % 60), (unsigned long long)(last_ts % 1000000000ull));
    const StoreStats &ss = mgr.store_stats();
    std::printf("order store: %.1f MB (pool + ref index), pre-sized for %zu live orders\n", mgr.store_memory_bytes() / 1e6, expect_orders);
    std::printf("heap fallbacks (grew past the pre-sized capacity): %llu  [pool=%llu free_list=%llu ref_table=%llu]\n",
                (unsigned long long)ss.total(), (unsigned long long)ss.pool_growths,
                (unsigned long long)ss.free_list_growths, (unsigned long long)ss.table_growths);
    std::printf("elapsed %.1fs%s\n", secs, verify ? " (includes reference model; not a benchmark)" : " (includes gzip pipe; not a benchmark)");

    for (const auto &sym : show) ShowBook(mgr, sym, depth);
    if (print_digest) std::printf("books digest: %016llx (%zu books, %zu live orders)\n", (unsigned long long)mgr.state_digest(), mgr.book_count(), mgr.live_orders());

    const bool bad = !final_ok || validate_fail || decode_fail || (verify && (vm.mismatches || vm.live_orders() != vm.model_live()));
    return bad ? 1 : 0;
}
