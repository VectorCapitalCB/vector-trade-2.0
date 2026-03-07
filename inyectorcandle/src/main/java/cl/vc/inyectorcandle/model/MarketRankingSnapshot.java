package cl.vc.inyectorcandle.model;

import java.time.Instant;
import java.util.List;

public record MarketRankingSnapshot(
        Instant generatedAt,
        List<InstrumentStats> topByTurnover,
        List<InstrumentStats> bottomByTurnover,
        List<InstrumentStats> topByVolume,
        List<InstrumentStats> bottomByVolume,
        List<InstrumentStats> topGainers,
        List<InstrumentStats> topLosers
) {
}
