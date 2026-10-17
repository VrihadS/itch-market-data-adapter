#pragma once
// Events the BookManager raises for instruments someone has subscribed to.
// Nothing is raised, and no extra work is done, for instruments with no interest set.

#include <cstdint>

namespace feed_handler {

class OrderBook;

// Interest flags (also the values of the messaging protocol's feature bits).
constexpr uint8_t kInterestTrades = 0x1;   // time & sales
constexpr uint8_t kInterestBook   = 0x2;   // L2 depth

// One print for time & sales.
//   kind: 'E' execution of a displayed order (at its display price)
//         'C' execution at a price different from the display price (printable only)
//         'P' trade against a non-displayed order
//         'Q' cross trade (opening / closing / halt / intraday cross; see cross_type)
//         'B' broken trade: `match_number` identifies the print being cancelled
//   side: side of the resting order ('B'/'S') for E/C/P, 0 otherwise
//   cross_type: 'O','C','H','I' for Q, 0 otherwise
struct TradeEvent {
    uint16_t locate;
    char     kind;
    char     side;
    char     cross_type;
    int32_t  price;            // 4 implied decimals
    uint64_t shares;
    uint64_t match_number;
    uint64_t exchange_ts_ns;   // since midnight
};

class BookListener {
public:
    virtual ~BookListener() = default;

    // Called after a message changed the top `depth` levels (either side) of a
    // book whose interest includes kInterestBook. The flag is conservative: a
    // call means "may have changed", never "did not change when it did".
    virtual void on_book_update(uint16_t locate, const OrderBook &book, uint64_t exchange_ts_ns) = 0;

    // Called for each print, for instruments whose interest includes kInterestTrades.
    virtual void on_trade(const TradeEvent &ev) = 0;
};

} // namespace feed_handler
