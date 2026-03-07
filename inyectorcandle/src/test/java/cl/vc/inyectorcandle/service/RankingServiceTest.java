package cl.vc.inyectorcandle.service;

import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.InstrumentStats;
import cl.vc.inyectorcandle.model.MarketRankingSnapshot;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RankingServiceTest {

    @Test
    void shouldBuildTopAndBottomRankings() {
        RankingService service = new RankingService();

        InstrumentStats a = new InstrumentStats(InstrumentKey.fromValues("AAA", "T2", "XSGO", "CLP"), 10,
                new BigDecimal("100"), new BigDecimal("1000"), null, null, null,
                null, new BigDecimal("1.50"), null, null, null, null, null, null, null);
        InstrumentStats b = new InstrumentStats(InstrumentKey.fromValues("BBB", "T2", "XSGO", "CLP"), 10,
                new BigDecimal("10"), new BigDecimal("100"), null, null, null,
                null, new BigDecimal("-2.00"), null, null, null, null, null, null, null);
        InstrumentStats c = new InstrumentStats(InstrumentKey.fromValues("CCC", "T2", "XSGO", "CLP"), 10,
                new BigDecimal("300"), new BigDecimal("2000"), null, null, null,
                null, new BigDecimal("5.25"), null, null, null, null, null, null, null);

        MarketRankingSnapshot snapshot = service.build(List.of(a, b, c), 2);

        assertEquals("CCC", snapshot.topByTurnover().get(0).key().symbol());
        assertEquals("BBB", snapshot.bottomByTurnover().get(0).key().symbol());
        assertEquals("CCC", snapshot.topByVolume().get(0).key().symbol());
        assertEquals("BBB", snapshot.bottomByVolume().get(0).key().symbol());
        assertEquals("CCC", snapshot.topGainers().get(0).key().symbol());
        assertEquals("BBB", snapshot.topLosers().get(0).key().symbol());
    }
}
