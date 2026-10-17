#pragma once

#include <cstddef>
#include <cstdint>

namespace feed_handler
{

namespace NasdaqItch
{

// ── message sizes ────────────────────────────────────────────
constexpr size_t kSystemEventSize         = 12;   // 'S'
constexpr size_t kStockDirectorySize      = 39;   // 'R'
constexpr size_t kStockTradingActionSize  = 25;   // 'H'
constexpr size_t kRegSHOSize              = 20;   // 'Y'
constexpr size_t kMarketParticipantSize   = 26;   // 'L'
constexpr size_t kMWCBDeclineSize         = 35;   // 'V'
constexpr size_t kMWCBStatusSize          = 12;   // 'W'
constexpr size_t kIPOQuotingSize          = 28;   // 'K'
constexpr size_t kLULDAuctionCollarSize   = 35;   // 'J'
constexpr size_t kOperationalHaltSize     = 21;   // 'h'
constexpr size_t kAddOrderSize            = 36;   // 'A'
constexpr size_t kAddOrderMpidSize        = 40;   // 'F'
constexpr size_t kOrderExecutedSize       = 31;   // 'E'
constexpr size_t kOrderExecutedPriceSize  = 36;   // 'C'
constexpr size_t kOrderCancelSize         = 23;   // 'X'
constexpr size_t kOrderDeleteSize         = 19;   // 'D'
constexpr size_t kOrderReplaceSize        = 35;   // 'U'
constexpr size_t kTradeSize               = 44;   // 'P'
constexpr size_t kCrossTradeSize          = 40;   // 'Q'
constexpr size_t kBrokenTradeSize         = 19;   // 'B'
constexpr size_t kNOIISize                = 50;   // 'I'
constexpr size_t kRetailInterestSize      = 20;   // 'N'  <-- ADDED
constexpr size_t kDLCRSize                = 48;   // 'O'  <-- ADDED

// ── 4.1.1  System Event 'S' ──────────────────────────────────
struct SystemEvent
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     event_code;
};

// ── 4.2.1  Stock Directory 'R' ───────────────────────────────
struct StockDirectory
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     stock[9];
    char     market_category;
    char     financial_status_indicator;
    uint32_t round_lot_size;
    char     round_lots_only;
    char     issue_classification;
    char     issue_sub_type[3];
    char     authenticity;
    char     short_sale_threshold_indicator;
    char     ipo_indicator;
    char     luld_reference_price_tier;
    char     etp_indicator;
    uint32_t etp_leverage_factor;      // <-- FIXED: was char, now uint32_t
    char     inverse_indicator;        // <-- FIXED: at offset 38
};

// ── 4.3.1  Stock Trading Action 'H' ─────────────────────────
struct StockTradingAction
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     stock[9];
    char     trading_state;
    char     reserved;
    char     reason[5];
};

// ── 4.3.2  Reg SHO 'Y' ───────────────────────────────────────
struct RegSHO
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     stock[9];
    char     reg_sho_action;
};

// ── 4.6.1  Market Participant Position 'L' ───────────────────
struct MarketParticipantPosition
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     mpid[5];
    char     stock[9];
    char     primary_market_maker;
    char     market_maker_mode;
    char     market_participant_state;
};

// ── 4.6.2  MWCB Decline Level 'V' ───────────────────────────
struct MWCBDeclineLevel
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t level1;
    uint64_t level2;
    uint64_t level3;
};

// ── 4.6.3  MWCB Status 'W' ───────────────────────────────────
struct MWCBStatus
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     breached_level;
};

// ── 4.6.4  IPO Quoting Period Update 'K' ─────────────────────
struct IPOQuotingPeriodUpdate
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     stock[9];
    uint32_t ipo_quotation_release_time;
    char     ipo_quotation_release_qualifier;
    int32_t  ipo_price;
};

// ── 4.6.5  LULD Auction Collar 'J' ──────────────────────────
struct LULDAuctionCollar
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     stock[9];
    uint32_t reference_price;
    uint32_t upper_price;
    uint32_t lower_price;
    uint32_t auction_collar_extension;
};

// ── 4.6.6  Operational Halt 'h' ─────────────────────────────
struct OperationalHalt
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     stock[9];
    char     market_code;
    char     halt_reason;
};

// ── 4.4.1  Add Order 'A' / 'F' ───────────────────────────────
struct AddOrder
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t order_reference_number;
    char     buy_sell;
    uint32_t shares;
    char     stock[9];
    int32_t  price;
    char     mpid[5];
    bool     has_mpid;
};

// ── 4.4.2.1 / 4.4.2.2  Order Executed 'E' / 'C' ─────────────
struct OrderExecuted
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t order_reference_number;
    uint32_t executed_shares;
    uint64_t match_number;
    int32_t  execution_price;
    char     printable;
    bool     has_price;
};

// ── 4.4.2.3  Order Cancel 'X' ────────────────────────────────
struct OrderCancel
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t order_reference_number;
    uint32_t cancelled_shares;
};

// ── 4.4.2.4  Order Delete 'D' ────────────────────────────────
struct OrderDelete
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t order_reference_number;
};

// ── 4.4.2.5  Order Replace 'U' ───────────────────────────────
struct OrderReplace
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t original_order_reference_number;
    uint64_t new_order_reference_number;
    uint32_t shares;
    int32_t  price;
};

// ── 4.5.1  Trade (Non-Cross) 'P' ────────────────────────────
struct Trade
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t order_reference_number;
    char     buy_sell;
    uint32_t shares;
    char     stock[9];
    int32_t  price;
    uint64_t match_number;
};

// ── 4.5.2  Cross Trade 'Q' ───────────────────────────────────
struct CrossTrade
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t shares;
    char     stock[9];
    int32_t  cross_price;
    uint64_t match_number;
    char     cross_type;
};

// ── 4.5.3  Broken Trade 'B' ──────────────────────────────────
struct BrokenTrade
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t match_number;
};

// ── 4.5.4  NOII 'I' ──────────────────────────────────────────
struct NOII
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    uint64_t paired_shares;
    uint64_t imbalance_shares;
    char     imbalance_direction;
    char     stock[9];
    int32_t  far_price;
    int32_t  near_price;
    int32_t  current_reference_price;
    char     cross_type;
    char     price_variation_indicator;
};

// ── 4.7  Retail Interest 'N' ────────────────────────  ADDED
struct RetailInterest
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     stock[9];
    char     interest_flag;   // 'B','S','A','N'
};

// ── 4.8  Direct Listing w/ Capital Raise 'O' ─────────  ADDED
struct DirectListingWithCapitalRaise
{
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp;
    char     stock[9];
    char     open_eligibility_status;    // 'Y'/'N'
    int32_t  minimum_allowable_price;
    int32_t  maximum_allowable_price;
    int32_t  near_execution_price;
    uint64_t near_execution_time;
    int32_t  lower_price_range_collar;
    int32_t  upper_price_range_collar;
};

} // namespace NasdaqItch

class NasdaqItchMessageHandler
{
public:
    virtual ~NasdaqItchMessageHandler() = default;

    virtual void OnSystemEvent              (const NasdaqItch::SystemEvent&)              {}
    virtual void OnStockDirectory           (const NasdaqItch::StockDirectory&)           {}
    virtual void OnStockTradingAction       (const NasdaqItch::StockTradingAction&)       {}
    virtual void OnRegSHO                   (const NasdaqItch::RegSHO&)                   {}
    virtual void OnMarketParticipantPosition(const NasdaqItch::MarketParticipantPosition&){}
    virtual void OnMWCBDeclineLevel         (const NasdaqItch::MWCBDeclineLevel&)         {}
    virtual void OnMWCBStatus               (const NasdaqItch::MWCBStatus&)               {}
    virtual void OnIPOQuotingPeriodUpdate   (const NasdaqItch::IPOQuotingPeriodUpdate&)   {}
    virtual void OnLULDAuctionCollar        (const NasdaqItch::LULDAuctionCollar&)        {}
    virtual void OnOperationalHalt          (const NasdaqItch::OperationalHalt&)          {}
    virtual void OnAddOrder                 (const NasdaqItch::AddOrder&)                 {}
    virtual void OnOrderExecuted            (const NasdaqItch::OrderExecuted&)            {}
    virtual void OnOrderCancel              (const NasdaqItch::OrderCancel&)              {}
    virtual void OnOrderDelete              (const NasdaqItch::OrderDelete&)              {}
    virtual void OnOrderReplace             (const NasdaqItch::OrderReplace&)             {}
    virtual void OnTrade                    (const NasdaqItch::Trade&)                    {}
    virtual void OnCrossTrade               (const NasdaqItch::CrossTrade&)               {}
    virtual void OnBrokenTrade              (const NasdaqItch::BrokenTrade&)              {}
    virtual void OnNOII                     (const NasdaqItch::NOII&)                     {}
    virtual void OnRetailInterest           (const NasdaqItch::RetailInterest&)           {}  // ADDED
    virtual void OnDirectListingWithCapitalRaise(
                                                const NasdaqItch::DirectListingWithCapitalRaise&) {}  // ADDED
    virtual void OnUnknown(char /*type*/, const char* /*data*/, size_t /*len*/)           {}
};

class NasdaqItchDecoder
{
public:
    NasdaqItchDecoder() = default;

    void set_conformance(bool enable) { conformance_ = enable; }
    bool conformance() const          { return conformance_; }

    bool DecodeMessage(const char* data, size_t len,
                       size_t& bytes_consumed,
                       NasdaqItchMessageHandler& handler);

private:
    bool conformance_ = false;
};

} // namespace feed_handler