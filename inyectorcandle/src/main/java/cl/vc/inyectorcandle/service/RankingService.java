package cl.vc.inyectorcandle.service;

import cl.vc.inyectorcandle.model.InstrumentStats;
import cl.vc.inyectorcandle.model.MarketRankingSnapshot;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

public class RankingService {

    public MarketRankingSnapshot build(List<InstrumentStats> allStats, int size) {
        Comparator<InstrumentStats> byTurnover = Comparator.comparing(InstrumentStats::totalTurnover, this::compareNullableBigDecimal);
        Comparator<InstrumentStats> byVolume = Comparator.comparing(InstrumentStats::totalVolume, this::compareNullableBigDecimal);
        Comparator<InstrumentStats> byDailyVariation = Comparator.comparing(InstrumentStats::dailyVariationPct, this::compareNullableBigDecimal);

        List<InstrumentStats> topByTurnover = allStats.stream()
                .sorted(byTurnover.reversed())
                .limit(size)
                .toList();

        List<InstrumentStats> bottomByTurnover = allStats.stream()
                .sorted(byTurnover)
                .limit(size)
                .toList();

        List<InstrumentStats> topByVolume = allStats.stream()
                .sorted(byVolume.reversed())
                .limit(size)
                .toList();

        List<InstrumentStats> bottomByVolume = allStats.stream()
                .sorted(byVolume)
                .limit(size)
                .toList();

        List<InstrumentStats> topGainers = allStats.stream()
                .sorted(byDailyVariation.reversed())
                .limit(size)
                .toList();

        List<InstrumentStats> topLosers = allStats.stream()
                .sorted(byDailyVariation)
                .limit(size)
                .toList();

        return new MarketRankingSnapshot(
                Instant.now(),
                topByTurnover,
                bottomByTurnover,
                topByVolume,
                bottomByVolume,
                topGainers,
                topLosers
        );
    }

    private int compareNullableBigDecimal(BigDecimal a, BigDecimal b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        return a.compareTo(b);
    }
}
