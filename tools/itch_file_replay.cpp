// Replays a raw ITCH 5.0 file (2-byte length-prefixed messages) through the
// decoder and cross-checks it. Usage:
//   gzip -dc day.gz | itch_file_replay [-n max_messages]
//   itch_file_replay day.itch [-n max_messages]
// Logging (off by default; per-message logging dominates runtime):
//   --log-level trace|debug|info|warn|error   enable decoder logging
//   --log-file PATH      log destination (default: logs/itch_file_replay.log, truncated each run)
//   --conformance        full field dump per message at info level (use with a small -n)
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include "itch/protocol/file_reader.hpp"
#include "itch/protocol/ndaq_itch_decoder.h"

using namespace feed_handler;
namespace N = feed_handler::NasdaqItch;

namespace
{

struct TypeInfo { const char *name; size_t size; };

const std::map<char, TypeInfo> kTypes = {
    {'S', {"SystemEvent", N::kSystemEventSize}},
    {'R', {"StockDirectory", N::kStockDirectorySize}},
    {'H', {"StockTradingAction", N::kStockTradingActionSize}},
    {'Y', {"RegSHO", N::kRegSHOSize}},
    {'L', {"MarketParticipantPosition", N::kMarketParticipantSize}},
    {'V', {"MWCBDeclineLevel", N::kMWCBDeclineSize}},
    {'W', {"MWCBStatus", N::kMWCBStatusSize}},
    {'K', {"IPOQuotingPeriodUpdate", N::kIPOQuotingSize}},
    {'J', {"LULDAuctionCollar", N::kLULDAuctionCollarSize}},
    {'h', {"OperationalHalt", N::kOperationalHaltSize}},
    {'A', {"AddOrder", N::kAddOrderSize}},
    {'F', {"AddOrderMPID", N::kAddOrderMpidSize}},
    {'E', {"OrderExecuted", N::kOrderExecutedSize}},
    {'C', {"OrderExecutedPrice", N::kOrderExecutedPriceSize}},
    {'X', {"OrderCancel", N::kOrderCancelSize}},
    {'D', {"OrderDelete", N::kOrderDeleteSize}},
    {'U', {"OrderReplace", N::kOrderReplaceSize}},
    {'P', {"Trade", N::kTradeSize}},
    {'Q', {"CrossTrade", N::kCrossTradeSize}},
    {'B', {"BrokenTrade", N::kBrokenTradeSize}},
    {'I', {"NOII", N::kNOIISize}},
    {'N', {"RetailInterest", N::kRetailInterestSize}},
    {'O', {"DirectListingCapRaise", N::kDLCRSize}},
};

// Counts every callback and applies cheap field sanity checks.
struct CountingHandler : NasdaqItchMessageHandler
{
    uint64_t cb[256] = {};      // callbacks keyed by the dispatching type byte
    uint64_t bad_fields = 0;
    uint64_t ts_regressions = 0;
    uint64_t last_ts = 0;
    uint64_t first_ts = 0, max_ts = 0;
    uint64_t total_shares_added = 0;
    uint16_t max_locate = 0;

    void Ts(uint64_t ts, uint16_t locate)
    {
        if (!first_ts) first_ts = ts;
        if (ts < last_ts) ++ts_regressions;
        last_ts = ts;
        if (ts > max_ts) max_ts = ts;
        if (locate > max_locate) max_locate = locate;
    }
    void Bad(bool cond) { if (cond) ++bad_fields; }

    void OnSystemEvent(const N::SystemEvent &m) override { ++cb['S']; Ts(m.timestamp, m.stock_locate); }
    void OnStockDirectory(const N::StockDirectory &m) override
    {
        ++cb['R']; Ts(m.timestamp, m.stock_locate);
        Bad(m.stock[0] == '\0' || m.round_lot_size == 0);
    }
    void OnStockTradingAction(const N::StockTradingAction &m) override { ++cb['H']; Ts(m.timestamp, m.stock_locate); }
    void OnRegSHO(const N::RegSHO &m) override { ++cb['Y']; Ts(m.timestamp, m.stock_locate); }
    void OnMarketParticipantPosition(const N::MarketParticipantPosition &m) override
    {
        ++cb['L']; Ts(m.timestamp, m.stock_locate);
        Bad(m.mpid[0] == '\0' || m.stock[0] == '\0' ||
            (m.primary_market_maker != 'Y' && m.primary_market_maker != 'N'));
    }
    void OnMWCBDeclineLevel(const N::MWCBDeclineLevel &m) override { ++cb['V']; Ts(m.timestamp, m.stock_locate); }
    void OnMWCBStatus(const N::MWCBStatus &m) override { ++cb['W']; Ts(m.timestamp, m.stock_locate); }
    void OnIPOQuotingPeriodUpdate(const N::IPOQuotingPeriodUpdate &m) override { ++cb['K']; Ts(m.timestamp, m.stock_locate); }
    void OnLULDAuctionCollar(const N::LULDAuctionCollar &m) override { ++cb['J']; Ts(m.timestamp, m.stock_locate); }
    void OnOperationalHalt(const N::OperationalHalt &m) override { ++cb['h']; Ts(m.timestamp, m.stock_locate); }
    void OnAddOrder(const N::AddOrder &m) override
    {
        ++cb[static_cast<unsigned char>(m.has_mpid ? 'F' : 'A')]; Ts(m.timestamp, m.stock_locate);
        total_shares_added += m.shares;
        Bad(m.stock[0] == '\0' || (m.buy_sell != 'B' && m.buy_sell != 'S') || m.shares == 0 || m.price <= 0 ||
            (m.has_mpid && m.mpid[0] == '\0'));
    }
    void OnOrderExecuted(const N::OrderExecuted &m) override
    {
        ++cb[static_cast<unsigned char>(m.has_price ? 'C' : 'E')]; Ts(m.timestamp, m.stock_locate);
        Bad(m.executed_shares == 0 || m.match_number == 0 ||
            (m.has_price && m.printable != 'Y' && m.printable != 'N'));
    }
    void OnOrderCancel(const N::OrderCancel &m) override { ++cb['X']; Ts(m.timestamp, m.stock_locate); Bad(m.cancelled_shares == 0); }
    void OnOrderDelete(const N::OrderDelete &m) override { ++cb['D']; Ts(m.timestamp, m.stock_locate); }
    void OnOrderReplace(const N::OrderReplace &m) override
    {
        ++cb['U']; Ts(m.timestamp, m.stock_locate);
        Bad(m.shares == 0 || m.price <= 0 || m.original_order_reference_number == m.new_order_reference_number);
    }
    void OnTrade(const N::Trade &m) override { ++cb['P']; Ts(m.timestamp, m.stock_locate); Bad(m.stock[0] == '\0' || m.shares == 0); }
    void OnCrossTrade(const N::CrossTrade &m) override
    {
        ++cb['Q']; Ts(m.timestamp, m.stock_locate);
        Bad(m.stock[0] == '\0' || (m.cross_type != 'O' && m.cross_type != 'C' && m.cross_type != 'H' && m.cross_type != 'I'));
    }
    void OnBrokenTrade(const N::BrokenTrade &m) override { ++cb['B']; Ts(m.timestamp, m.stock_locate); }
    void OnNOII(const N::NOII &m) override { ++cb['I']; Ts(m.timestamp, m.stock_locate); Bad(m.stock[0] == '\0'); }
    void OnRetailInterest(const N::RetailInterest &m) override { ++cb['N']; Ts(m.timestamp, m.stock_locate); }
    void OnDirectListingWithCapitalRaise(const N::DirectListingWithCapitalRaise &m) override { ++cb['O']; Ts(m.timestamp, m.stock_locate); }
    void OnUnknown(char, const char *, size_t) override { ++cb['?']; }
};

} // namespace

int main(int argc, char **argv)
{
    const char *path = nullptr;
    uint64_t    max_msgs = UINT64_MAX;
    std::string log_level = "off";
    std::string log_file  = "logs/itch_file_replay.log";
    bool        conformance = false;
    for (int i = 1; i < argc; ++i)
    {
        if (!std::strcmp(argv[i], "-n") && i + 1 < argc) max_msgs = std::strtoull(argv[++i], nullptr, 10);
        else if (!std::strcmp(argv[i], "--log-level") && i + 1 < argc) log_level = argv[++i];
        else if (!std::strcmp(argv[i], "--log-file") && i + 1 < argc) log_file = argv[++i];
        else if (!std::strcmp(argv[i], "--conformance")) conformance = true;
        else path = argv[i];
    }
    if (conformance && log_level == "off") log_level = "info";

    // Logs go to a file, not stderr, so the summary on stdout stays readable.
    const auto lvl = spdlog::level::from_str(log_level);
    if (lvl != spdlog::level::off)
    {
        const auto dir = std::filesystem::path(log_file).parent_path();
        if (!dir.empty()) std::filesystem::create_directories(dir);
        auto logger = std::make_shared<spdlog::logger>(
            "itch", std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file, /*truncate=*/true));
        spdlog::set_default_logger(logger);
        std::fprintf(stderr, "logging %s to %s\n", log_level.c_str(),
                     std::filesystem::absolute(log_file).c_str());
    }
    spdlog::set_level(lvl);

    std::FILE *f = path ? std::fopen(path, "rb") : stdin;
    if (!f) { std::perror("open"); return 2; }
    std::setvbuf(f, nullptr, _IOFBF, 1 << 20);

    itch::FileReader   reader(f);
    NasdaqItchDecoder  decoder;
    decoder.set_conformance(conformance);
    CountingHandler    h;

    uint64_t seen[256] = {};         // messages seen on the wire, by type byte (counted before decoding)
    uint64_t size_mismatch[256] = {};// length prefix != spec size
    uint64_t decode_fail = 0, consumed_mismatch = 0, total = 0, bytes = 0;
    bool     truncated = false;

    const auto t0 = std::chrono::steady_clock::now();
    const char *data; size_t len;
    for (;;)
    {
        if (total >= max_msgs) break;
        const auto st = reader.Next(data, len);
        if (st == itch::FileReader::Status::Eof) break;
        if (st == itch::FileReader::Status::Truncated) { truncated = true; break; }
        if (len == 0) continue;

        const unsigned char t = static_cast<unsigned char>(data[0]);
        ++seen[t];
        const auto it = kTypes.find(static_cast<char>(t));
        if (it != kTypes.end() && it->second.size != len) ++size_mismatch[t];

        size_t used = 0;
        if (!decoder.DecodeMessage(data, len, used, h)) ++decode_fail;
        else if (used != len) ++consumed_mismatch;
        ++total;
        bytes += len + 2;
    }
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    std::printf("%-26s %c %12s %12s %9s\n", "type", ' ', "on-wire", "dispatched", "size!=spec");
    uint64_t dispatch_diff = 0, size_bad = 0;
    for (int t = 0; t < 256; ++t)
    {
        if (!seen[t] && !h.cb[t]) continue;
        const auto it = kTypes.find(static_cast<char>(t));
        const bool known = it != kTypes.end();
        std::printf("%-26s %c %12llu %12llu %9llu%s\n", known ? it->second.name : "(unknown)",
                    known ? static_cast<char>(t) : '?', (unsigned long long)seen[t],
                    (unsigned long long)h.cb[t], (unsigned long long)size_mismatch[t],
                    (known && seen[t] != h.cb[t]) ? "   <-- COUNT MISMATCH" : "");
        size_bad += size_mismatch[t];
        if (known && seen[t] != h.cb[t]) ++dispatch_diff;
    }
    std::printf("\nmessages: %llu (%.1f MB framed)   decode failures: %llu   consumed!=len: %llu\n",
                (unsigned long long)total, bytes / 1e6, (unsigned long long)decode_fail,
                (unsigned long long)consumed_mismatch);
    std::printf("size-vs-spec mismatches: %llu   types with count mismatch: %llu\n",
                (unsigned long long)size_bad, (unsigned long long)dispatch_diff);
    std::printf("field sanity violations: %llu   timestamp regressions: %llu\n",
                (unsigned long long)h.bad_fields, (unsigned long long)h.ts_regressions);
    std::printf("timestamp range: %.6f s .. %.6f s since midnight   max stock locate: %u\n",
                h.first_ts / 1e9, h.max_ts / 1e9, h.max_locate);
    std::printf("end of input: %s\n", truncated ? "TRUNCATED mid-message (expected for a partial download)" : "clean EOF");
    std::printf("elapsed %.2fs  (%.1f M msg/s incl. file I/O; not a benchmark)\n", secs, total / secs / 1e6);

    spdlog::shutdown();
    return (decode_fail || consumed_mismatch || size_bad || dispatch_diff) ? 1 : 0;
}
