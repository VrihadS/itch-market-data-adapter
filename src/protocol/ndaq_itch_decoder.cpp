#include "itch/protocol/ndaq_itch_decoder.h"

#include <cstring>
#include <spdlog/spdlog.h>

namespace feed_handler
{

namespace
{

// ─────────────────────────────────────────────────────────────
// Wire readers (all ITCH fields are big-endian)
// ─────────────────────────────────────────────────────────────
inline uint8_t  U8 (const char *p) { return static_cast<uint8_t>(p[0]); }
inline uint16_t U16(const char *p)
{
    return static_cast<uint16_t>((U8(p) << 8) | U8(p + 1));
}
inline uint32_t U32(const char *p)
{
    return (static_cast<uint32_t>(U8(p    )) << 24) |
           (static_cast<uint32_t>(U8(p + 1)) << 16) |
           (static_cast<uint32_t>(U8(p + 2)) <<  8) |
            static_cast<uint32_t>(U8(p + 3));
}
inline uint64_t U48(const char *p)
{
    return (static_cast<uint64_t>(U8(p    )) << 40) |
           (static_cast<uint64_t>(U8(p + 1)) << 32) |
           (static_cast<uint64_t>(U8(p + 2)) << 24) |
           (static_cast<uint64_t>(U8(p + 3)) << 16) |
           (static_cast<uint64_t>(U8(p + 4)) <<  8) |
            static_cast<uint64_t>(U8(p + 5));
}
inline uint64_t U64(const char *p)
{
    return (static_cast<uint64_t>(U32(p)) << 32) |
            static_cast<uint64_t>(U32(p + 4));
}
inline int32_t I32(const char *p) { return static_cast<int32_t>(U32(p)); }

// Copy `len` ASCII bytes into `dst`, NUL-terminate, right-strip spaces.
inline void CopyAlpha(char *dst, const char *src, size_t len)
{
    std::memcpy(dst, src, len);
    dst[len] = '\0';
    while (len > 0 && dst[len - 1] == ' ')
        dst[--len] = '\0';
}

} // namespace

// ─────────────────────────────────────────────────────────────
// DecodeMessage
// ─────────────────────────────────────────────────────────────
bool NasdaqItchDecoder::DecodeMessage(const char *data, size_t len,
                                      size_t &bytes_consumed,
                                      NasdaqItchMessageHandler &handler)
{
    bytes_consumed = 0;
    if (len == 0)
        return false;

    const char type = data[0];

    switch (type)
    {
    // ── 1.1  System Event  'S'  12 bytes ────────────────────────────────────
    case 'S':
    {
        if (len < NasdaqItch::kSystemEventSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kSystemEventSize);
            return false;
        }

        NasdaqItch::SystemEvent m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        m.event_code      = data[11];

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE SystemEvent: type='{}' locate={} tracking={} ts={} event_code='{}'",
                         type, m.stock_locate, m.tracking_number, m.timestamp, m.event_code);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ SystemEvent: type='{}' ts={} event_code='{}'",
                          type, m.timestamp, m.event_code);

        handler.OnSystemEvent(m);
        bytes_consumed = NasdaqItch::kSystemEventSize;
        return true;
    }

    // ── 1.2.1  Stock Directory  'R'  39 bytes ──────────────────────────────
    //   offset 34: ETP Leverage Factor (4 bytes)
    //   offset 38: Inverse Indicator   (1 byte)
    case 'R':
    {
        if (len < NasdaqItch::kStockDirectorySize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kStockDirectorySize);
            return false;
        }

        NasdaqItch::StockDirectory m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        CopyAlpha(m.stock, data + 11, 8);
        m.market_category                = data[19];
        m.financial_status_indicator     = data[20];
        m.round_lot_size                 = U32(data + 21);
        m.round_lots_only                = data[25];
        m.issue_classification           = data[26];
        CopyAlpha(m.issue_sub_type, data + 27, 2);
        m.authenticity                   = data[29];
        m.short_sale_threshold_indicator = data[30];
        m.ipo_indicator                  = data[31];
        m.luld_reference_price_tier      = data[32];
        m.etp_indicator                  = data[33];
        m.etp_leverage_factor            = U32(data + 34);   // 4 bytes
        m.inverse_indicator              = data[38];         // 1 byte

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE StockDirectory: type='{}' locate={} ts={} stock='{}' category='{}' "
                         "fin_status='{}' round_lot={} round_lots_only='{}' issue_class='{}' issue_subtype='{}' "
                         "authenticity='{}' short_sale_thresh='{}' ipo='{}' luld_tier='{}' etp='{}' "
                         "etp_leverage={} inverse='{}'",
                         type, m.stock_locate, m.timestamp, m.stock, m.market_category,
                         m.financial_status_indicator, m.round_lot_size, m.round_lots_only,
                         m.issue_classification, m.issue_sub_type, m.authenticity,
                         m.short_sale_threshold_indicator, m.ipo_indicator,
                         m.luld_reference_price_tier, m.etp_indicator,
                         m.etp_leverage_factor, m.inverse_indicator);
        else
            spdlog::info("NASDAQ StockDirectory: type='{}' stock='{}' category='{}' round_lot={}",
                         type, m.stock, m.market_category, m.round_lot_size);

        handler.OnStockDirectory(m);
        bytes_consumed = NasdaqItch::kStockDirectorySize;
        return true;
    }

    // ── 1.2.2  Stock Trading Action  'H'  25 bytes ─────────────────────────
    case 'H':
    {
        if (len < NasdaqItch::kStockTradingActionSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kStockTradingActionSize);
            return false;
        }

        NasdaqItch::StockTradingAction m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        CopyAlpha(m.stock, data + 11, 8);
        m.trading_state   = data[19];
        m.reserved        = data[20];
        CopyAlpha(m.reason, data + 21, 4);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE StockTradingAction: type='{}' locate={} ts={} stock='{}' state='{}' reason='{}'",
                         type, m.stock_locate, m.timestamp, m.stock, m.trading_state, m.reason);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ StockTradingAction: type='{}' stock='{}' state='{}' reason='{}'",
                          type, m.stock, m.trading_state, m.reason);

        handler.OnStockTradingAction(m);
        bytes_consumed = NasdaqItch::kStockTradingActionSize;
        return true;
    }

    // ── 1.2.3  Reg SHO Short Sale Price Test  'Y'  20 bytes ────────────────
    case 'Y':
    {
        if (len < NasdaqItch::kRegSHOSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kRegSHOSize);
            return false;
        }

        NasdaqItch::RegSHO m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        CopyAlpha(m.stock, data + 11, 8);
        m.reg_sho_action  = data[19];

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE RegSHO: type='{}' stock='{}' action='{}'",
                         type, m.stock, m.reg_sho_action);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ RegSHO: type='{}' stock='{}' action='{}'",
                          type, m.stock, m.reg_sho_action);

        handler.OnRegSHO(m);
        bytes_consumed = NasdaqItch::kRegSHOSize;
        return true;
    }

    // ── 1.2.4  Market Participant Position  'L'  26 bytes ──────────────────
    //   offset 11: MPID (4)  15: Stock (8)  23: PMM  24: Mode  25: State
    case 'L':
    {
        if (len < NasdaqItch::kMarketParticipantSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kMarketParticipantSize);
            return false;
        }

        NasdaqItch::MarketParticipantPosition m{};
        m.stock_locate             = U16(data + 1);
        m.tracking_number          = U16(data + 3);
        m.timestamp                = U48(data + 5);
        CopyAlpha(m.mpid, data + 11, 4);
        CopyAlpha(m.stock, data + 15, 8);
        m.primary_market_maker     = data[23];
        m.market_maker_mode        = data[24];
        m.market_participant_state = data[25];

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE MarketParticipantPosition: type='{}' mpid='{}' stock='{}' pmm='{}' mode='{}' state='{}'",
                         type, m.mpid, m.stock, m.primary_market_maker, m.market_maker_mode,
                         m.market_participant_state);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ MarketParticipantPosition: type='{}' mpid='{}' state='{}'",
                          type, m.mpid, m.market_participant_state);

        handler.OnMarketParticipantPosition(m);
        bytes_consumed = NasdaqItch::kMarketParticipantSize;
        return true;
    }

    // ── 1.2.5.1  MWCB Decline Level  'V'  35 bytes ─────────────────────────
    case 'V':
    {
        if (len < NasdaqItch::kMWCBDeclineSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kMWCBDeclineSize);
            return false;
        }

        NasdaqItch::MWCBDeclineLevel m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        m.level1          = U64(data + 11);
        m.level2          = U64(data + 19);
        m.level3          = U64(data + 27);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE MWCBDeclineLevel: type='{}' L1={} L2={} L3={}",
                         type, m.level1, m.level2, m.level3);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ MWCBDeclineLevel: type='{}' L1={} L2={} L3={}",
                          type, m.level1, m.level2, m.level3);

        handler.OnMWCBDeclineLevel(m);
        bytes_consumed = NasdaqItch::kMWCBDeclineSize;
        return true;
    }

    // ── 1.2.5.2  MWCB Status  'W'  12 bytes ────────────────────────────────
    case 'W':
    {
        if (len < NasdaqItch::kMWCBStatusSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kMWCBStatusSize);
            return false;
        }

        NasdaqItch::MWCBStatus m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        m.breached_level  = data[11];

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE MWCBStatus: type='{}' breached='{}'",
                         type, m.breached_level);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ MWCBStatus: type='{}' breached='{}'",
                          type, m.breached_level);

        handler.OnMWCBStatus(m);
        bytes_consumed = NasdaqItch::kMWCBStatusSize;
        return true;
    }

    // ── 1.2.6  IPO Quoting Period Update  'K'  28 bytes ────────────────────
    case 'K':
    {
        if (len < NasdaqItch::kIPOQuotingSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kIPOQuotingSize);
            return false;
        }

        NasdaqItch::IPOQuotingPeriodUpdate m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        CopyAlpha(m.stock, data + 11, 8);
        m.ipo_quotation_release_time      = U32(data + 19);
        m.ipo_quotation_release_qualifier = data[23];
        m.ipo_price                       = I32(data + 24);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE IPOQuotingPeriodUpdate: type='{}' stock='{}' release_time={} qualifier='{}' ipo_price={}",
                         type, m.stock, m.ipo_quotation_release_time,
                         m.ipo_quotation_release_qualifier, m.ipo_price);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ IPOQuotingPeriodUpdate: type='{}' stock='{}' ipo_price={}",
                          type, m.stock, m.ipo_price);

        handler.OnIPOQuotingPeriodUpdate(m);
        bytes_consumed = NasdaqItch::kIPOQuotingSize;
        return true;
    }

    // ── 1.2.7  LULD Auction Collar  'J'  35 bytes ──────────────────────────
    case 'J':
    {
        if (len < NasdaqItch::kLULDAuctionCollarSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kLULDAuctionCollarSize);
            return false;
        }

        NasdaqItch::LULDAuctionCollar m{};
        m.stock_locate             = U16(data + 1);
        m.tracking_number          = U16(data + 3);
        m.timestamp                = U48(data + 5);
        CopyAlpha(m.stock, data + 11, 8);
        m.reference_price          = U32(data + 19);
        m.upper_price              = U32(data + 23);
        m.lower_price              = U32(data + 27);
        m.auction_collar_extension = U32(data + 31);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE LULDAuctionCollar: type='{}' stock='{}' ref={} upper={} lower={} extension={}",
                         type, m.stock, m.reference_price, m.upper_price,
                         m.lower_price, m.auction_collar_extension);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ LULDAuctionCollar: type='{}' stock='{}' upper={} lower={}",
                          type, m.stock, m.upper_price, m.lower_price);

        handler.OnLULDAuctionCollar(m);
        bytes_consumed = NasdaqItch::kLULDAuctionCollarSize;
        return true;
    }

    // ── 1.2.8  Operational Halt  'h'  21 bytes ─────────────────────────────
    case 'h':
    {
        if (len < NasdaqItch::kOperationalHaltSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kOperationalHaltSize);
            return false;
        }

        NasdaqItch::OperationalHalt m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        CopyAlpha(m.stock, data + 11, 8);
        m.market_code     = data[19];
        m.halt_reason     = data[20];

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE OperationalHalt: type='{}' stock='{}' market='{}' reason='{}'",
                         type, m.stock, m.market_code, m.halt_reason);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ OperationalHalt: type='{}' stock='{}' market='{}' reason='{}'",
                          type, m.stock, m.market_code, m.halt_reason);

        handler.OnOperationalHalt(m);
        bytes_consumed = NasdaqItch::kOperationalHaltSize;
        return true;
    }

    // ── 1.3.1  Add Order — No MPID  'A'  36 bytes ──────────────────────────
    case 'A':
    {
        if (len < NasdaqItch::kAddOrderSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kAddOrderSize);
            return false;
        }

        NasdaqItch::AddOrder m{};
        m.stock_locate           = U16(data + 1);
        m.tracking_number        = U16(data + 3);
        m.timestamp              = U48(data + 5);
        m.order_reference_number = U64(data + 11);
        m.buy_sell               = data[19];
        m.shares                 = U32(data + 20);
        CopyAlpha(m.stock, data + 24, 8);
        m.price                  = I32(data + 32);
        m.has_mpid               = false;
        m.mpid[0]                = '\0';

        handler.OnAddOrder(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE AddOrder: type='{}' locate={} ts={} ref={} side='{}' shares={} stock='{}' price={}",
                         type, m.stock_locate, m.timestamp, m.order_reference_number,
                         m.buy_sell, m.shares, m.stock, m.price);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ AddOrder: type='{}' ref={} stock='{}' side='{}' price={} shares={}",
                          type, m.order_reference_number, m.stock, m.buy_sell,
                          m.price, m.shares);

        bytes_consumed = NasdaqItch::kAddOrderSize;
        return true;
    }

    // ── 1.3.2  Add Order — With MPID  'F'  40 bytes ────────────────────────
    case 'F':
    {
        if (len < NasdaqItch::kAddOrderMpidSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kAddOrderMpidSize);
            return false;
        }

        NasdaqItch::AddOrder m{};
        m.stock_locate           = U16(data + 1);
        m.tracking_number        = U16(data + 3);
        m.timestamp              = U48(data + 5);
        m.order_reference_number = U64(data + 11);
        m.buy_sell               = data[19];
        m.shares                 = U32(data + 20);
        CopyAlpha(m.stock, data + 24, 8);
        m.price                  = I32(data + 32);
        CopyAlpha(m.mpid, data + 36, 4);
        m.has_mpid               = true;

        handler.OnAddOrder(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE AddOrder(MPID): type='{}' locate={} ts={} ref={} side='{}' shares={} stock='{}' price={} mpid='{}'",
                         type, m.stock_locate, m.timestamp, m.order_reference_number,
                         m.buy_sell, m.shares, m.stock, m.price, m.mpid);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ AddOrder(MPID): type='{}' ref={} stock='{}' side='{}' price={} shares={} mpid='{}'",
                          type, m.order_reference_number, m.stock, m.buy_sell,
                          m.price, m.shares, m.mpid);

        bytes_consumed = NasdaqItch::kAddOrderMpidSize;
        return true;
    }

    // ── 1.4.1  Order Executed  'E'  31 bytes ───────────────────────────────
    case 'E':
    {
        if (len < NasdaqItch::kOrderExecutedSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kOrderExecutedSize);
            return false;
        }

        NasdaqItch::OrderExecuted m{};
        m.stock_locate           = U16(data + 1);
        m.tracking_number        = U16(data + 3);
        m.timestamp              = U48(data + 5);
        m.order_reference_number = U64(data + 11);
        m.executed_shares        = U32(data + 19);
        m.match_number           = U64(data + 23);
        m.execution_price        = 0;
        m.printable              = 'Y';
        m.has_price              = false;

        handler.OnOrderExecuted(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE OrderExecuted: type='{}' ref={} exec_shares={} match={}",
                         type, m.order_reference_number, m.executed_shares, m.match_number);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ OrderExecuted: type='{}' ref={} exec_shares={} match={}",
                          type, m.order_reference_number, m.executed_shares, m.match_number);

        bytes_consumed = NasdaqItch::kOrderExecutedSize;
        return true;
    }

    // ── 1.4.2  Order Executed with Price  'C'  36 bytes ────────────────────
    case 'C':
    {
        if (len < NasdaqItch::kOrderExecutedPriceSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kOrderExecutedPriceSize);
            return false;
        }

        NasdaqItch::OrderExecuted m{};
        m.stock_locate           = U16(data + 1);
        m.tracking_number        = U16(data + 3);
        m.timestamp              = U48(data + 5);
        m.order_reference_number = U64(data + 11);
        m.executed_shares        = U32(data + 19);
        m.match_number           = U64(data + 23);
        m.printable              = data[31];
        m.execution_price        = I32(data + 32);
        m.has_price              = true;

        handler.OnOrderExecuted(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE OrderExecuted(price): type='{}' ref={} exec_shares={} match={} price={} printable='{}'",
                         type, m.order_reference_number, m.executed_shares,
                         m.match_number, m.execution_price, m.printable);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ OrderExecuted(price): type='{}' ref={} exec_shares={} match={} price={} printable='{}'",
                          type, m.order_reference_number, m.executed_shares,
                          m.match_number, m.execution_price, m.printable);

        bytes_consumed = NasdaqItch::kOrderExecutedPriceSize;
        return true;
    }

    // ── 1.4.3  Order Cancel  'X'  23 bytes ─────────────────────────────────
    case 'X':
    {
        if (len < NasdaqItch::kOrderCancelSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kOrderCancelSize);
            return false;
        }

        NasdaqItch::OrderCancel m{};
        m.stock_locate           = U16(data + 1);
        m.tracking_number        = U16(data + 3);
        m.timestamp              = U48(data + 5);
        m.order_reference_number = U64(data + 11);
        m.cancelled_shares       = U32(data + 19);

        handler.OnOrderCancel(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE OrderCancel: type='{}' ref={} cancelled_shares={}",
                         type, m.order_reference_number, m.cancelled_shares);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ OrderCancel: type='{}' ref={} cancelled_shares={}",
                          type, m.order_reference_number, m.cancelled_shares);

        bytes_consumed = NasdaqItch::kOrderCancelSize;
        return true;
    }

    // ── 1.4.4  Order Delete  'D'  19 bytes ─────────────────────────────────
    case 'D':
    {
        if (len < NasdaqItch::kOrderDeleteSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kOrderDeleteSize);
            return false;
        }

        NasdaqItch::OrderDelete m{};
        m.stock_locate           = U16(data + 1);
        m.tracking_number        = U16(data + 3);
        m.timestamp              = U48(data + 5);
        m.order_reference_number = U64(data + 11);

        handler.OnOrderDelete(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE OrderDelete: type='{}' ref={}",
                         type, m.order_reference_number);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ OrderDelete: type='{}' ref={}",
                          type, m.order_reference_number);

        bytes_consumed = NasdaqItch::kOrderDeleteSize;
        return true;
    }

    // ── 1.4.5  Order Replace  'U'  35 bytes ────────────────────────────────
    case 'U':
    {
        if (len < NasdaqItch::kOrderReplaceSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kOrderReplaceSize);
            return false;
        }

        NasdaqItch::OrderReplace m{};
        m.stock_locate                    = U16(data + 1);
        m.tracking_number                 = U16(data + 3);
        m.timestamp                       = U48(data + 5);
        m.original_order_reference_number = U64(data + 11);
        m.new_order_reference_number      = U64(data + 19);
        m.shares                          = U32(data + 27);
        m.price                           = I32(data + 31);

        handler.OnOrderReplace(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE OrderReplace: type='{}' old_ref={} new_ref={} shares={} price={}",
                         type, m.original_order_reference_number,
                         m.new_order_reference_number, m.shares, m.price);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ OrderReplace: type='{}' old_ref={} new_ref={} shares={} price={}",
                          type, m.original_order_reference_number,
                          m.new_order_reference_number, m.shares, m.price);

        bytes_consumed = NasdaqItch::kOrderReplaceSize;
        return true;
    }

    // ── 1.5.1  Trade (Non-Cross)  'P'  44 bytes ────────────────────────────
    case 'P':
    {
        if (len < NasdaqItch::kTradeSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kTradeSize);
            return false;
        }

        NasdaqItch::Trade m{};
        m.stock_locate           = U16(data + 1);
        m.tracking_number        = U16(data + 3);
        m.timestamp              = U48(data + 5);
        m.order_reference_number = U64(data + 11);
        m.buy_sell               = data[19];
        m.shares                 = U32(data + 20);
        CopyAlpha(m.stock, data + 24, 8);
        m.price                  = I32(data + 32);
        m.match_number           = U64(data + 36);

        handler.OnTrade(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE Trade: type='{}' match={} ref={} side='{}' shares={} stock='{}' price={}",
                         type, m.match_number, m.order_reference_number,
                         m.buy_sell, m.shares, m.stock, m.price);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ Trade: type='{}' match={} stock='{}' side='{}' shares={} price={}",
                          type, m.match_number, m.stock, m.buy_sell, m.shares, m.price);

        bytes_consumed = NasdaqItch::kTradeSize;
        return true;
    }

    // ── 1.5.2  Cross Trade  'Q'  40 bytes ──────────────────────────────────
    case 'Q':
    {
        if (len < NasdaqItch::kCrossTradeSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kCrossTradeSize);
            return false;
        }

        NasdaqItch::CrossTrade m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        m.shares          = U64(data + 11);
        CopyAlpha(m.stock, data + 19, 8);
        m.cross_price     = I32(data + 27);
        m.match_number    = U64(data + 31);
        m.cross_type      = data[39];

        handler.OnCrossTrade(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE CrossTrade: type='{}' match={} shares={} stock='{}' price={} cross_type='{}'",
                         type, m.match_number, m.shares, m.stock, m.cross_price, m.cross_type);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ CrossTrade: type='{}' match={} stock='{}' shares={} price={} type='{}'",
                          type, m.match_number, m.stock, m.shares, m.cross_price, m.cross_type);

        bytes_consumed = NasdaqItch::kCrossTradeSize;
        return true;
    }

    // ── 1.5.3  Broken Trade / Order Execution  'B'  19 bytes ───────────────
    case 'B':
    {
        if (len < NasdaqItch::kBrokenTradeSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kBrokenTradeSize);
            return false;
        }

        NasdaqItch::BrokenTrade m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        m.match_number    = U64(data + 11);

        handler.OnBrokenTrade(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE BrokenTrade: type='{}' match={}",
                         type, m.match_number);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ BrokenTrade: type='{}' match={}",
                          type, m.match_number);

        bytes_consumed = NasdaqItch::kBrokenTradeSize;
        return true;
    }

    // ── 1.6  NOII  'I'  50 bytes ───────────────────────────────────────────
    case 'I':
    {
        if (len < NasdaqItch::kNOIISize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kNOIISize);
            return false;
        }

        NasdaqItch::NOII m{};
        m.stock_locate              = U16(data + 1);
        m.tracking_number           = U16(data + 3);
        m.timestamp                 = U48(data + 5);
        m.paired_shares             = U64(data + 11);
        m.imbalance_shares          = U64(data + 19);
        m.imbalance_direction       = data[27];
        CopyAlpha(m.stock, data + 28, 8);
        m.far_price                 = I32(data + 36);
        m.near_price                = I32(data + 40);
        m.current_reference_price   = I32(data + 44);
        m.cross_type                = data[48];
        m.price_variation_indicator = data[49];

        handler.OnNOII(m);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE NOII: type='{}' stock='{}' paired={} imbalance={} dir='{}' far={} near={} ref={} cross='{}' pvi='{}'",
                         type, m.stock, m.paired_shares, m.imbalance_shares,
                         m.imbalance_direction, m.far_price, m.near_price,
                         m.current_reference_price, m.cross_type,
                         m.price_variation_indicator);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ NOII: type='{}' stock='{}' paired={} imbalance={} ref={}",
                          type, m.stock, m.paired_shares, m.imbalance_shares,
                          m.current_reference_price);

        bytes_consumed = NasdaqItch::kNOIISize;
        return true;
    }

    // ── 1.7  Retail Interest  'N'  20 bytes ────────────────────────────────
    //   RPI program ceased 2014-12-31 but message remains in current spec.
    case 'N':
    {
        if (len < NasdaqItch::kRetailInterestSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kRetailInterestSize);
            return false;
        }

        NasdaqItch::RetailInterest m{};
        m.stock_locate    = U16(data + 1);
        m.tracking_number = U16(data + 3);
        m.timestamp       = U48(data + 5);
        CopyAlpha(m.stock, data + 11, 8);
        m.interest_flag   = data[19];   // 'B','S','A','N'

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE RetailInterest: type='{}' stock='{}' flag='{}'",
                         type, m.stock, m.interest_flag);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ RetailInterest: type='{}' stock='{}' flag='{}'",
                          type, m.stock, m.interest_flag);

        handler.OnRetailInterest(m);
        bytes_consumed = NasdaqItch::kRetailInterestSize;
        return true;
    }

    // ── 1.8  Direct Listing with Capital Raise  'O'  48 bytes ──────────────
    //   Added April 28, 2023.
    case 'O':
    {
        if (len < NasdaqItch::kDLCRSize)
        {
            spdlog::error("NASDAQ DecodeMessage: type='{}' too short: got {} need {}",
                          type, len, NasdaqItch::kDLCRSize);
            return false;
        }

        NasdaqItch::DirectListingWithCapitalRaise m{};
        m.stock_locate             = U16(data + 1);
        m.tracking_number          = U16(data + 3);
        m.timestamp                = U48(data + 5);
        CopyAlpha(m.stock, data + 11, 8);
        m.open_eligibility_status  = data[19];
        m.minimum_allowable_price  = I32(data + 20);
        m.maximum_allowable_price  = I32(data + 24);
        m.near_execution_price     = I32(data + 28);
        m.near_execution_time      = U64(data + 32);
        m.lower_price_range_collar = I32(data + 40);
        m.upper_price_range_collar = I32(data + 44);

        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE DLCR: type='{}' stock='{}' eligible='{}' min={} max={} near={} near_time={} lower_collar={} upper_collar={}",
                         type, m.stock, m.open_eligibility_status,
                         m.minimum_allowable_price, m.maximum_allowable_price,
                         m.near_execution_price, m.near_execution_time,
                         m.lower_price_range_collar, m.upper_price_range_collar);
        else if (spdlog::should_log(spdlog::level::debug))
            spdlog::debug("NASDAQ DLCR: type='{}' stock='{}' eligible='{}' near={}",
                          type, m.stock, m.open_eligibility_status,
                          m.near_execution_price);

        handler.OnDirectListingWithCapitalRaise(m);
        bytes_consumed = NasdaqItch::kDLCRSize;
        return true;
    }

    // ── Unknown message type ───────────────────────────────────────────────
    default:
        if (conformance_)
            spdlog::info("NASDAQ CONFORMANCE unknown msg type='{}' len={}", type, len);
        else
            spdlog::info("NasdaqItchDecoder: unknown msg type='{}' len={}", type, len);

        handler.OnUnknown(type, data, len);
        bytes_consumed = len;
        return true;
    }
}

} // namespace feed_handler