package cl.vc.inyectorcandle.service;

import cl.vc.inyectorcandle.model.Candle;
import cl.vc.inyectorcandle.model.InstrumentKey;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CandleService {
    private final Map<Duration, CandleAccumulator> accumulators = new HashMap<>();

    public CandleService(InstrumentKey key, List<Duration> timeframes) {
        for (Duration timeframe : timeframes) {
            accumulators.put(timeframe, new CandleAccumulator(key, timeframe));
        }
    }

    public List<Candle> onTrade(Instant time, BigDecimal price, BigDecimal qty, BigDecimal amount) {
        List<Candle> finalized = new ArrayList<>();
        for (CandleAccumulator accumulator : accumulators.values()) {
            Candle completed = accumulator.rolloverAndApply(time, price, qty, amount);
            if (completed != null) {
                finalized.add(completed);
            }
        }
        return finalized;
    }

    public List<Candle> flushAll() {
        List<Candle> all = new ArrayList<>();
        for (CandleAccumulator accumulator : accumulators.values()) {
            Candle candle = accumulator.flush();
            if (candle != null) {
                all.add(candle);
            }
        }
        return all;
    }
}
