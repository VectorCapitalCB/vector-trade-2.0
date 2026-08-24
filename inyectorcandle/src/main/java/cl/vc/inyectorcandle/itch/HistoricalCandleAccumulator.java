package cl.vc.inyectorcandle.itch;

import cl.vc.inyectorcandle.model.Candle;
import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.MarketSession;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Acumula velas historicas desde ejecuciones ITCH sin pasar por el feed en vivo. */
final class HistoricalCandleAccumulator {
    private final MarketSession session;
    private final List<Duration> timeframes;
    private final Map<BucketKey, MutableCandle> candles = new HashMap<>();

    HistoricalCandleAccumulator(MarketSession session, List<Duration> timeframes) {
        this.session = session;
        this.timeframes = timeframes == null ? List.of() : List.copyOf(timeframes);
    }

    void apply(InstrumentKey key, Instant eventTime, BigDecimal price, BigDecimal qty, BigDecimal amount) {
        if (key == null || eventTime == null || price == null || price.signum() <= 0) {
            return;
        }
        ZonedDateTime local = eventTime.atZone(session.zone());
        if (local.toLocalTime().isBefore(session.open()) || local.toLocalTime().isAfter(session.close())) {
            return;
        }

        for (Duration timeframe : timeframes) {
            if (timeframe == null || timeframe.isZero() || timeframe.isNegative()) {
                continue;
            }
            Instant bucketStart = floorFromSessionStart(local, eventTime, timeframe);
            BucketKey bucketKey = new BucketKey(key, timeframe, bucketStart);
            candles.computeIfAbsent(bucketKey, ignored -> new MutableCandle())
                    .apply(price, qty, amount);
        }
    }

    List<Candle> snapshot() {
        List<Candle> result = new ArrayList<>(candles.size());
        candles.forEach((key, value) -> result.add(value.toCandle(key)));
        result.sort(Comparator.comparing(Candle::bucketStart)
                .thenComparing(candle -> candle.key().id())
                .thenComparing(Candle::timeframe));
        return result;
    }

    private Instant floorFromSessionStart(ZonedDateTime local, Instant instant, Duration timeframe) {
        Instant sessionStart = local.toLocalDate().atTime(session.open()).atZone(session.zone()).toInstant();
        long millis = timeframe.toMillis();
        long elapsed = Duration.between(sessionStart, instant).toMillis();
        return sessionStart.plusMillis(Math.floorDiv(elapsed, millis) * millis);
    }

    private record BucketKey(InstrumentKey instrument, Duration timeframe, Instant start) {
    }

    private static final class MutableCandle {
        private BigDecimal open;
        private BigDecimal high;
        private BigDecimal low;
        private BigDecimal close;
        private BigDecimal volume = BigDecimal.ZERO;
        private BigDecimal turnover = BigDecimal.ZERO;
        private long trades;

        void apply(BigDecimal price, BigDecimal qty, BigDecimal amount) {
            if (open == null) {
                open = price;
                high = price;
                low = price;
            } else {
                high = high.max(price);
                low = low.min(price);
            }
            close = price;
            if (qty != null) {
                volume = volume.add(qty);
            }
            turnover = turnover.add(amount != null ? amount
                    : qty == null ? BigDecimal.ZERO : price.multiply(qty));
            trades++;
        }

        Candle toCandle(BucketKey key) {
            return new Candle(key.instrument(), key.timeframe(), key.start(), key.start().plus(key.timeframe()),
                    open, high, low, close, volume, turnover, trades);
        }
    }
}
