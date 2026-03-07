package cl.vc.inyectorcandle.service;

import cl.vc.inyectorcandle.model.Candle;
import cl.vc.inyectorcandle.model.InstrumentKey;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CandleServiceTest {

    @Test
    void shouldFinalizeOneMinuteCandleWhenBucketChanges() {
        InstrumentKey key = InstrumentKey.fromValues("SQM-B", "T2", "XSGO", "CLP");
        CandleService service = new CandleService(key, List.of(Duration.ofMinutes(1)));

        Instant t0 = Instant.parse("2026-03-06T12:00:10Z");
        Instant t1 = Instant.parse("2026-03-06T12:00:50Z");
        Instant t2 = Instant.parse("2026-03-06T12:01:01Z");

        assertTrue(service.onTrade(t0, new BigDecimal("100"), new BigDecimal("10"), null).isEmpty());
        assertTrue(service.onTrade(t1, new BigDecimal("105"), new BigDecimal("5"), null).isEmpty());

        List<Candle> completed = service.onTrade(t2, new BigDecimal("102"), new BigDecimal("4"), null);

        assertEquals(1, completed.size());
        Candle candle = completed.get(0);
        assertEquals(new BigDecimal("100"), candle.open());
        assertEquals(new BigDecimal("105"), candle.high());
        assertEquals(new BigDecimal("100"), candle.low());
        assertEquals(new BigDecimal("105"), candle.close());
        assertEquals(new BigDecimal("15"), candle.volume());
        assertEquals(2, candle.trades());
    }
}
