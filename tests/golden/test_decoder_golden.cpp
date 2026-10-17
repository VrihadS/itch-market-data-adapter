// Decoder checks against hand-built messages whose offsets come from the spec tables
// (docs/spec/NQTVITCHSpecification.pdf), not from the decoder's own structs.
#include <cstring>
#include <vector>

#include <spdlog/spdlog.h>

#include "check.h"
#include "itch/protocol/ndaq_itch_decoder.h"

using namespace feed_handler;
using Bytes = std::vector<unsigned char>;
namespace N = feed_handler::NasdaqItch;

static void be(Bytes &b, size_t off, uint64_t v, int n) { for (int i = 0; i < n; ++i) b[off + i] = (v >> (8 * (n - 1 - i))) & 0xFF; }
static Bytes hdr(char t, size_t size) { Bytes b(size, 0); b[0] = t; be(b, 1, 7, 2); be(b, 3, 9, 2); be(b, 5, 0x0102030405ull, 6); return b; }
static void put(Bytes &b, size_t off, const char *s) { std::memcpy(&b[off], s, std::strlen(s)); }

struct H : NasdaqItchMessageHandler {
  N::MarketParticipantPosition L{}; N::StockDirectory R{}; N::AddOrder A{}; N::OrderExecuted E{};
  N::OrderReplace U{}; N::RetailInterest Nn{}; N::DirectListingWithCapitalRaise O{}; N::NOII I{}; N::CrossTrade Q{};
  int calls = 0;
  void OnMarketParticipantPosition(const N::MarketParticipantPosition &m) override { L = m; ++calls; }
  void OnStockDirectory(const N::StockDirectory &m) override { R = m; ++calls; }
  void OnAddOrder(const N::AddOrder &m) override { A = m; ++calls; }
  void OnOrderExecuted(const N::OrderExecuted &m) override { E = m; ++calls; }
  void OnOrderReplace(const N::OrderReplace &m) override { U = m; ++calls; }
  void OnRetailInterest(const N::RetailInterest &m) override { Nn = m; ++calls; }
  void OnDirectListingWithCapitalRaise(const N::DirectListingWithCapitalRaise &m) override { O = m; ++calls; }
  void OnNOII(const N::NOII &m) override { I = m; ++calls; }
  void OnCrossTrade(const N::CrossTrade &m) override { Q = m; ++calls; }
};

static bool dec(NasdaqItchDecoder &d, H &h, const Bytes &b, size_t len, size_t &used) {
  return d.DecodeMessage(reinterpret_cast<const char *>(b.data()), len, used, h);
}

int main() {
  spdlog::set_level(spdlog::level::off);
  NasdaqItchDecoder d; H h; size_t used = 0;

  { // 'L' Market Participant Position: 26 bytes; MPID@11(4) Stock@15(8) PMM@23 Mode@24 State@25
    Bytes b = hdr('L', 26); put(b, 11, "GSCO"); put(b, 15, "AAPL    "); b[23] = 'Y'; b[24] = 'N'; b[25] = 'A';
    CHECK(dec(d, h, b, b.size(), used) && used == 26);
    CHECK(!std::strcmp(h.L.mpid, "GSCO") && !std::strcmp(h.L.stock, "AAPL"));
    CHECK(h.L.primary_market_maker == 'Y' && h.L.market_maker_mode == 'N' && h.L.market_participant_state == 'A');
    CHECK(h.L.stock_locate == 7 && h.L.tracking_number == 9 && h.L.timestamp == 0x0102030405ull);
    CHECK(!dec(d, h, b, 18, used));                              // short input rejected
  }
  { // 'R' Stock Directory: 39 bytes; ETP leverage is a 4-byte integer @34, Inverse @38
    Bytes b = hdr('R', 39); put(b, 11, "AAAU    "); b[19] = 'P'; b[20] = 'N'; be(b, 21, 100, 4); b[25] = 'N'; b[26] = 'Q';
    put(b, 27, "I "); b[29] = 'P'; b[30] = 'N'; b[31] = ' '; b[32] = '2'; b[33] = 'Y'; be(b, 34, 3, 4); b[38] = 'Y';
    CHECK(dec(d, h, b, b.size(), used) && used == 39);
    CHECK(!std::strcmp(h.R.stock, "AAAU") && h.R.round_lot_size == 100 && h.R.issue_classification == 'Q');
    CHECK(!std::strcmp(h.R.issue_sub_type, "I") && h.R.luld_reference_price_tier == '2');
    CHECK(h.R.etp_indicator == 'Y' && h.R.etp_leverage_factor == 3 && h.R.inverse_indicator == 'Y');
  }
  { // 'A' / 'F' Add Order: ref@11(8) side@19 shares@20 stock@24(8) price@32; MPID@36 for F
    Bytes b = hdr('A', 36); be(b, 11, 0x1122334455667788ull, 8); b[19] = 'B'; be(b, 20, 300, 4); put(b, 24, "MSFT    "); be(b, 32, 1234567, 4);
    CHECK(dec(d, h, b, b.size(), used) && used == 36);
    CHECK(h.A.order_reference_number == 0x1122334455667788ull && h.A.buy_sell == 'B' && h.A.shares == 300);
    CHECK(!std::strcmp(h.A.stock, "MSFT") && h.A.price == 1234567 && !h.A.has_mpid);
    Bytes f = hdr('F', 40); be(f, 11, 5, 8); f[19] = 'S'; be(f, 20, 1, 4); put(f, 24, "X       "); be(f, 32, 1, 4); put(f, 36, "NITE");
    CHECK(dec(d, h, f, f.size(), used) && used == 40 && h.A.has_mpid && !std::strcmp(h.A.mpid, "NITE"));
  }
  { // 'E' and 'U'
    Bytes e = hdr('E', 31); be(e, 11, 42, 8); be(e, 19, 17, 4); be(e, 23, 99, 8);
    CHECK(dec(d, h, e, e.size(), used) && h.E.order_reference_number == 42 && h.E.executed_shares == 17 && h.E.match_number == 99 && !h.E.has_price);
    Bytes u = hdr('U', 35); be(u, 11, 1, 8); be(u, 19, 2, 8); be(u, 27, 55, 4); be(u, 31, 777, 4);
    CHECK(dec(d, h, u, u.size(), used) && h.U.original_order_reference_number == 1 && h.U.new_order_reference_number == 2 && h.U.shares == 55 && h.U.price == 777);
  }
  { // 'Q' Cross Trade: shares are 8 bytes @11, stock@19, price@27, match@31, type@39
    Bytes q = hdr('Q', 40); be(q, 11, 5000000000ull, 8); put(q, 19, "AAPL    "); be(q, 27, 1500000, 4); be(q, 31, 123, 8); q[39] = 'C';
    CHECK(dec(d, h, q, q.size(), used) && used == 40 && h.Q.shares == 5000000000ull && h.Q.cross_price == 1500000 && h.Q.cross_type == 'C');
  }
  { // 'I' NOII: 50 bytes
    Bytes n = hdr('I', 50); be(n, 11, 1000, 8); be(n, 19, 200, 8); n[27] = 'B'; put(n, 28, "TSLA    "); be(n, 36, 10, 4); be(n, 40, 20, 4); be(n, 44, 30, 4); n[48] = 'O'; n[49] = 'L';
    CHECK(dec(d, h, n, n.size(), used) && used == 50 && h.I.paired_shares == 1000 && h.I.imbalance_shares == 200 && h.I.imbalance_direction == 'B');
    CHECK(h.I.far_price == 10 && h.I.near_price == 20 && h.I.current_reference_price == 30 && h.I.cross_type == 'O' && h.I.price_variation_indicator == 'L');
  }
  { // 'N' Retail Interest (20) and 'O' Direct Listing with Capital Raise (48)
    Bytes r = hdr('N', 20); put(r, 11, "ZZZ     "); r[19] = 'A';
    CHECK(dec(d, h, r, r.size(), used) && used == 20 && !std::strcmp(h.Nn.stock, "ZZZ") && h.Nn.interest_flag == 'A');
    Bytes o = hdr('O', 48); put(o, 11, "NEWCO   "); o[19] = 'Y'; be(o, 20, 1, 4); be(o, 24, 2, 4); be(o, 28, 3, 4); be(o, 32, 0x0A0B0C0D0E0F1011ull, 8); be(o, 40, 5, 4); be(o, 44, 6, 4);
    CHECK(dec(d, h, o, o.size(), used) && used == 48 && h.O.open_eligibility_status == 'Y');
    CHECK(h.O.minimum_allowable_price == 1 && h.O.maximum_allowable_price == 2 && h.O.near_execution_price == 3);
    CHECK(h.O.near_execution_time == 0x0A0B0C0D0E0F1011ull && h.O.lower_price_range_collar == 5 && h.O.upper_price_range_collar == 6);
  }
  { // truncated input for every known type must be rejected without dispatching
    const int before = h.calls;
    for (char t : {'S','R','H','Y','L','V','W','K','J','h','A','F','E','C','X','D','U','P','Q','B','I','N','O'}) {
      Bytes b = hdr(t, 60);
      CHECK(!dec(d, h, b, 5, used) && used == 0);
    }
    CHECK(h.calls == before);
  }
  std::puts("ok");
  return 0;
}
