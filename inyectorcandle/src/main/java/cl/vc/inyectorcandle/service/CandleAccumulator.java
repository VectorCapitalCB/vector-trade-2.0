package cl.vc.inyectorcandle.service;

import cl.vc.inyectorcandle.model.Candle;
import cl.vc.inyectorcandle.model.InstrumentKey;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

final class CandleAccumulator {
    private final InstrumentKey key;
    private final Duration timeframe;

    private Instant bucketStart;
    private Instant bucketEnd;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private BigDecimal volume = BigDecimal.ZERO;
    private BigDecimal turnover = BigDecimal.ZERO;
    private long trades;

    CandleAccumulator(InstrumentKey key, Duration timeframe) {
        this.key = key;
        this.timeframe = timeframe;
    }

    Candle rolloverAndApply(Instant eventTime, BigDecimal price, BigDecimal qty, BigDecimal amount) {
        Instant eventBucketStart = floor(eventTime, timeframe);
        Candle finalized = null;

        if (bucketStart != null && eventBucketStart.isAfter(bucketStart)) {
            finalized = toCandle();
            resetBucket(eventBucketStart);
        } else if (bucketStart == null) {
            resetBucket(eventBucketStart);
        }

        apply(price, qty, amount);
        return finalized;
    }

    Candle flush() {
        if (bucketStart == null) {
            return null;
        }
        return toCandle();
    }

    private void resetBucket(Instant newBucketStart) {
        bucketStart = newBucketStart;
        bucketEnd = newBucketStart.plus(timeframe);
        open = null;
        high = null;
        low = null;
        close = null;
        volume = BigDecimal.ZERO;
        turnover = BigDecimal.ZERO;
        trades = 0;
    }

    private void apply(BigDecimal price, BigDecimal qty, BigDecimal amount) {
        if (price == null) {
            return;
        }

        if (open == null) {
            open = price;
            high = price;
            low = price;
            close = price;
        } else {
            if (price.compareTo(high) > 0) {
                high = price;
            }
            if (price.compareTo(low) < 0) {
                low = price;
            }
            close = price;
        }

        if (qty != null) {
            volume = volume.add(qty);
        }

        if (amount != null) {
            turnover = turnover.add(amount);
        } else if (qty != null) {
            turnover = turnover.add(price.multiply(qty));
        }

        trades += 1;
    }

    private Candle toCandle() {
        return new Candle(key, timeframe, bucketStart, bucketEnd, open, high, low, close, volume, turnover, trades);
    }

    static Instant floor(Instant instant, Duration timeframe) {
        long millis = timeframe.toMillis();
        if (millis <= 0) {
            throw new IllegalArgumentException("timeframe must be positive");
        }
        long floored = (instant.toEpochMilli() / millis) * millis;
        return Instant.ofEpochMilli(floored).truncatedTo(ChronoUnit.MILLIS);
    }
}
