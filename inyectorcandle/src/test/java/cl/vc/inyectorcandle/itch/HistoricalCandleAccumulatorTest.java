package cl.vc.inyectorcandle.itch;

import cl.vc.inyectorcandle.model.Candle;
import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.MarketSession;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HistoricalCandleAccumulatorTest {
    private static final MarketSession SESSION = MarketSession.of("America/Santiago", "09:05", "17:00");
    private static final InstrumentKey SQM_B = InstrumentKey.fromValues("SQM-B", "T2", "BCS", "CLP", "CS");

    @Test
    void buildsOhlcVolumeAndSessionAlignedBuckets() {
        HistoricalCandleAccumulator accumulator = new HistoricalCandleAccumulator(
                SESSION, List.of(Duration.ofMinutes(1), Duration.ofMinutes(5), Duration.ofHours(4)));

        accumulator.apply(SQM_B, instant("2026-08-10T09:04:59-04:00"), bd("90"), bd("1"), null);
        accumulator.apply(SQM_B, instant("2026-08-10T09:05:00-04:00"), bd("100"), bd("10"), null);
        accumulator.apply(SQM_B, instant("2026-08-10T09:09:59-04:00"), bd("105"), bd("5"), null);
        accumulator.apply(SQM_B, instant("2026-08-10T09:10:00-04:00"), bd("98"), bd("7"), null);
        accumulator.apply(SQM_B, instant("2026-08-10T13:05:00-04:00"), bd("110"), bd("3"), null);

        List<Candle> fiveMinutes = accumulator.snapshot().stream()
                .filter(candle -> candle.timeframe().equals(Duration.ofMinutes(5)))
                .toList();
        assertEquals(3, fiveMinutes.size());
        assertEquals(instant("2026-08-10T09:05:00-04:00"), fiveMinutes.get(0).bucketStart());
        assertEquals(bd("100"), fiveMinutes.get(0).open());
        assertEquals(bd("105"), fiveMinutes.get(0).high());
        assertEquals(bd("100"), fiveMinutes.get(0).low());
        assertEquals(bd("105"), fiveMinutes.get(0).close());
        assertEquals(bd("15"), fiveMinutes.get(0).volume());
        assertEquals(2L, fiveMinutes.get(0).trades());

        List<Candle> fourHours = accumulator.snapshot().stream()
                .filter(candle -> candle.timeframe().equals(Duration.ofHours(4)))
                .toList();
        assertEquals(2, fourHours.size());
        assertEquals(instant("2026-08-10T09:05:00-04:00"), fourHours.get(0).bucketStart());
        assertEquals(instant("2026-08-10T13:05:00-04:00"), fourHours.get(1).bucketStart());
        assertEquals(bd("98"), fourHours.get(0).close());
        assertEquals(bd("110"), fourHours.get(1).close());
    }

    @Test
    void mapsKnownNuamFinancialProducts() {
        assertEquals("CS", ItchLogReplay.securityType(5));
        assertEquals("ETF", ItchLogReplay.securityType(14));
        assertEquals("CFI", ItchLogReplay.securityType(16));
        assertEquals("T2", ItchLogReplay.settlement(5));
    }

    private static Instant instant(String value) {
        return OffsetDateTime.parse(value).toInstant();
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
