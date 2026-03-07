package cl.vc.inyectorcandle.model;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

public record Candle(
        InstrumentKey key,
        Duration timeframe,
        Instant bucketStart,
        Instant bucketEnd,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume,
        BigDecimal turnover,
        long trades
) {
}
