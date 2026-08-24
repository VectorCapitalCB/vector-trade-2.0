package cl.vc.blotter.model;

import java.time.Instant;

public record TradeCandle(
        String symbol,
        int bucketMinutes,
        Instant start,
        double open,
        double high,
        double low,
        double close,
        double volume
) {
}
