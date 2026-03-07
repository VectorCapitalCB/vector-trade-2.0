package cl.vc.inyectorcandle.model;

import java.math.BigDecimal;

public record InstrumentStats(
        InstrumentKey key,
        long totalTrades,
        BigDecimal totalVolume,
        BigDecimal totalTurnover,
        BigDecimal lastPrice,
        BigDecimal bestBid,
        BigDecimal bestAsk,
        BigDecimal variationPct,
        BigDecimal dailyVariationPct,
        BigDecimal vwapIntraday,
        BigDecimal sma20,
        BigDecimal ema20,
        BigDecimal rsi14,
        BigDecimal macdLine,
        BigDecimal macdSignal,
        BigDecimal macdHistogram
) {
}
