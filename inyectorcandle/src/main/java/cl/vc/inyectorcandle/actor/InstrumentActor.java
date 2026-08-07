package cl.vc.inyectorcandle.actor;

import cl.vc.inyectorcandle.model.Candle;
import cl.vc.inyectorcandle.model.InstrumentDailyStats;
import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.InstrumentStats;
import cl.vc.inyectorcandle.model.MarketDataEvent;
import cl.vc.inyectorcandle.model.TradeEvent;
import cl.vc.inyectorcandle.mongo.MongoMarketRepository;
import cl.vc.inyectorcandle.service.CandleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.field.MDEntryType;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InstrumentActor {
    private static final Logger LOG = LoggerFactory.getLogger(InstrumentActor.class);
    private static final MathContext MC = MathContext.DECIMAL64;
    private static final BigDecimal HUNDRED = new BigDecimal("100");
    private static final int SMA_PERIOD = 20;
    private static final int RSI_PERIOD = 14;
    private static final ZoneId MARKET_ZONE = ZoneId.of("America/Santiago");

    private final InstrumentKey key;
    private final MongoMarketRepository repository;
    private final CandleService candleService;
    private final BlockingQueue<InstrumentCommand> mailbox = new LinkedBlockingQueue<>();
    private final Thread worker;
    private final long statsThrottleNs;
    private final long openCandleFlushNs;

    private long lastStatsAtNs;
    private long lastOpenCandleAtNs;

    private volatile boolean running = true;

    private long totalTrades = 0;
    private BigDecimal totalVolume = BigDecimal.ZERO;
    private BigDecimal totalTurnover = BigDecimal.ZERO;
    private BigDecimal lastPrice;
    private BigDecimal bestBid;
    private BigDecimal bestAsk;
    private BigDecimal variationPct;
    private BigDecimal dailyVariationPct;
    private BigDecimal vwapIntraday;
    private BigDecimal sma20;
    private BigDecimal ema20;
    private BigDecimal rsi14;
    private BigDecimal macdLine;
    private BigDecimal macdSignal;
    private BigDecimal macdHistogram;

    private final Deque<BigDecimal> smaPrices = new ArrayDeque<>();
    private BigDecimal smaSum = BigDecimal.ZERO;
    private BigDecimal prevTradePrice;
    private BigDecimal ema12;
    private BigDecimal ema26;
    private BigDecimal rsiAvgGain;
    private BigDecimal rsiAvgLoss;
    private BigDecimal rsiSeedGain = BigDecimal.ZERO;
    private BigDecimal rsiSeedLoss = BigDecimal.ZERO;
    private int rsiSeedCount;
    private LocalDate currentTradingDay;
    private long dayStartEpochMs;
    private long dayEndEpochMs;
    private long flushedTrades = -1;
    private BigDecimal previousClose;
    private BigDecimal intradayVolume = BigDecimal.ZERO;
    private BigDecimal intradayTurnover = BigDecimal.ZERO;
    private BigDecimal dayOpen;
    private BigDecimal dayHigh;
    private BigDecimal dayLow;
    private long dayTrades;
    private final Map<String, TradeEvent> activeTrades = new HashMap<>();

    public InstrumentActor(InstrumentKey key, List<Duration> timeframes, MongoMarketRepository repository,
                           long statsThrottleMs, long openCandleFlushMs) {
        this.key = key;
        this.repository = repository;
        this.candleService = new CandleService(key, timeframes);
        this.statsThrottleNs = Math.max(0L, statsThrottleMs) * 1_000_000L;
        this.openCandleFlushNs = Math.max(0L, openCandleFlushMs) * 1_000_000L;
        this.worker = new Thread(this::runLoop, "actor-" + key.id());
        this.worker.start();
    }

    public void tell(InstrumentCommand command) {
        if (running) {
            mailbox.offer(command);
        }
    }

    public InstrumentStats snapshot() {
        return new InstrumentStats(
                key,
                totalTrades,
                totalVolume,
                totalTurnover,
                lastPrice,
                bestBid,
                bestAsk,
                variationPct,
                dailyVariationPct,
                vwapIntraday,
                sma20,
                ema20,
                rsi14,
                macdLine,
                macdSignal,
                macdHistogram
        );
    }

    /**
     * Estado del dia en curso con la misma forma que deja el inyector de logs, para que el front
     * vea "hoy" igual que cualquier dia historico. Null si el instrumento aun no opero.
     */
    public InstrumentDailyStats dailySnapshot(String market, String fallbackCurrency) {
        if (currentTradingDay == null || dayOpen == null) {
            return null;
        }
        String currency = key.currency() == null || key.currency().startsWith("UNKNOWN")
                ? fallbackCurrency : key.currency();
        return new InstrumentDailyStats(market, key.symbol(), null, currency, currentTradingDay,
                dayOpen, dayHigh, dayLow, lastPrice, lastPrice, previousClose,
                intradayVolume, intradayTurnover, null, null, null, null, 0L, dayTrades, Instant.now());
    }

    public void stop() {
        tell(new InstrumentCommand.Stop());
        try {
            while (worker.isAlive()) {
                worker.join(1_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting actor {} to stop", key.id());
        }
    }

    private void runLoop() {
        while (running) {
            try {
                InstrumentCommand cmd = mailbox.take();
                if (cmd instanceof InstrumentCommand.OnMarketData onMarketData) {
                    onMarketData(onMarketData.event());
                } else if (cmd instanceof InstrumentCommand.OnTrade onTrade) {
                    onTrade(onTrade.event());
                } else if (cmd instanceof InstrumentCommand.Flush) {
                    onFlush();
                } else if (cmd instanceof InstrumentCommand.Stop) {
                    onStop();
                }
            } catch (Exception e) {
                LOG.error("Actor {} failed processing command", key.id(), e);
            }
        }
    }

    private void onMarketData(MarketDataEvent event) {
        repository.insertMarketDataEvent(event);

        if (event.mdEntryType() == MDEntryType.BID && event.price() != null) {
            bestBid = event.price();
        }
        if (event.mdEntryType() == MDEntryType.OFFER && event.price() != null) {
            bestAsk = event.price();
        }

        publishStats(false);
    }

    private void onTrade(TradeEvent trade) {
        rollDayIfNeeded(trade.eventTime());

        if (trade.mdEntryId() == null || trade.mdEntryId().isBlank()) {
            applyNewTrade(trade);
            return;
        }

        switch (trade.mdUpdateAction()) {
            case '0' -> applyUpsertTrade(trade);
            case '1' -> applyChangeTrade(trade);
            case '2' -> applyDeleteTrade(trade);
            default -> applyUpsertTrade(trade);
        }
    }

    private void applyNewTrade(TradeEvent trade) {
        repository.insertTrade(trade);

        totalTrades += 1;
        if (trade.quantity() != null) {
            totalVolume = totalVolume.add(trade.quantity());
        }

        BigDecimal amount = trade.amount();
        if (amount == null && trade.price() != null && trade.quantity() != null) {
            amount = trade.price().multiply(trade.quantity());
        }
        if (amount != null) {
            totalTurnover = totalTurnover.add(amount);
        }

        if (trade.price() != null) {
            lastPrice = trade.price();
            updateDailyMetrics(trade.price(), trade.quantity(), amount);
            updateIndicators(trade.price());
        }

        persistCandles(candleService.onTrade(trade.eventTime(), trade.price(), trade.quantity(), amount));
        publishStats(false);
    }

    private void applyUpsertTrade(TradeEvent trade) {
        TradeEvent previous = activeTrades.put(trade.mdEntryId(), trade);
        repository.insertTrade(trade);

        if (previous == null) {
            applyTradeDelta(trade, true);
            return;
        }

        applyTradeDelta(previous, false);
        applyTradeDelta(trade, true);
    }

    private void applyChangeTrade(TradeEvent trade) {
        TradeEvent previous = activeTrades.get(trade.mdEntryId());
        if (previous == null) {
            LOG.debug("Ignoring trade change without previous state instrument={} mdEntryId={}", key.id(), trade.mdEntryId());
            return;
        }

        activeTrades.put(trade.mdEntryId(), trade);
        repository.insertTrade(trade);
        applyTradeDelta(previous, false);
        applyTradeDelta(trade, true);
        publishStats(false);
    }

    private void applyDeleteTrade(TradeEvent trade) {
        TradeEvent previous = activeTrades.remove(trade.mdEntryId());
        repository.insertTrade(trade);
        if (previous == null) {
            LOG.debug("Ignoring trade delete without previous state instrument={} mdEntryId={}", key.id(), trade.mdEntryId());
            return;
        }

        applyTradeDelta(previous, false);
        publishStats(false);
    }

    private void applyTradeDelta(TradeEvent trade, boolean add) {
        BigDecimal sign = add ? BigDecimal.ONE : BigDecimal.ONE.negate();

        totalTrades += add ? 1 : -1;

        if (trade.quantity() != null) {
            totalVolume = totalVolume.add(trade.quantity().multiply(sign, MC), MC);
        }

        BigDecimal amount = trade.amount();
        if (amount == null && trade.price() != null && trade.quantity() != null) {
            amount = trade.price().multiply(trade.quantity(), MC);
        }
        if (amount != null) {
            totalTurnover = totalTurnover.add(amount.multiply(sign, MC), MC);
        }

        if (!add) {
            return;
        }

        if (trade.price() != null) {
            lastPrice = trade.price();
            updateDailyMetrics(trade.price(), trade.quantity(), amount);
            updateIndicators(trade.price());
        }

        persistCandles(candleService.onTrade(trade.eventTime(), trade.price(), trade.quantity(), amount));
        publishStats(false);
    }

    /**
     * Barrido periodico: el throttle deja congelado el ultimo tramo de cada rafaga hasta el tick
     * siguiente, y un instrumento que despues no vuelve a operar nunca lo persiste. El chequeo
     * contra {@code flushedTrades} evita reescribir a los instrumentos quietos.
     */
    private void onFlush() {
        if (flushedTrades == totalTrades) {
            return;
        }
        flushedTrades = totalTrades;
        lastOpenCandleAtNs = System.nanoTime();
        candleService.flushAll().forEach(repository::upsertCandle);
        publishStats(true);
    }

    private void onStop() {
        running = false;
        candleService.flushAll().forEach(repository::upsertCandle);
        publishStats(true);
    }

    /**
     * Persiste las velas cerradas siempre, y las abiertas con throttle: escribir la vela en curso
     * en cada tick multiplicaria por N (timeframes) la escritura sin aportar informacion nueva.
     */
    private void persistCandles(List<Candle> finalized) {
        for (Candle candle : finalized) {
            repository.upsertCandle(candle);
        }

        long now = System.nanoTime();
        if (!finalized.isEmpty() || now - lastOpenCandleAtNs >= openCandleFlushNs) {
            lastOpenCandleAtNs = now;
            candleService.flushAll().forEach(repository::upsertCandle);
        }
    }

    private void publishStats(boolean force) {
        long now = System.nanoTime();
        if (!force && now - lastStatsAtNs < statsThrottleNs) {
            return;
        }
        lastStatsAtNs = now;
        repository.upsertInstrumentStats(snapshot());
    }

    private void updateIndicators(BigDecimal price) {
        updateSma(price);
        ema20 = ema(ema20, price, 20);
        ema12 = ema(ema12, price, 12);
        ema26 = ema(ema26, price, 26);
        if (ema12 != null && ema26 != null) {
            macdLine = ema12.subtract(ema26, MC);
            macdSignal = ema(macdSignal, macdLine, 9);
            if (macdSignal != null) {
                macdHistogram = macdLine.subtract(macdSignal, MC);
            }
        }

        updateRsi(price);
        prevTradePrice = price;
    }

    /**
     * Cambio de jornada. Los contadores que publica {@code instrument_stats} son del dia, no del
     * proceso: sin este corte arrastran la sesion anterior, y sin la recuperacion desde Mongo un
     * reinicio a media rueda deja la pestana de estadisticas con solo el tramo posterior al arranque.
     * <p>
     * La comparacion es contra los limites del dia en epoch-millis para no pagar un
     * {@code atZone().toLocalDate()} por tick.
     */
    private void rollDayIfNeeded(Instant eventTime) {
        long eventMs = eventTime.toEpochMilli();
        if (eventMs >= dayStartEpochMs && eventMs < dayEndEpochMs) {
            return;
        }

        LocalDate tradingDay = eventTime.atZone(MARKET_ZONE).toLocalDate();
        currentTradingDay = tradingDay;
        dayStartEpochMs = tradingDay.atStartOfDay(MARKET_ZONE).toInstant().toEpochMilli();
        dayEndEpochMs = tradingDay.plusDays(1).atStartOfDay(MARKET_ZONE).toInstant().toEpochMilli();

        totalTrades = 0;
        totalVolume = BigDecimal.ZERO;
        totalTurnover = BigDecimal.ZERO;
        intradayVolume = BigDecimal.ZERO;
        intradayTurnover = BigDecimal.ZERO;
        vwapIntraday = null;
        dayOpen = null;
        dayHigh = null;
        dayLow = null;
        dayTrades = 0;

        try {
            previousClose = repository.findPreviousClose(key, tradingDay, MARKET_ZONE);
        } catch (Exception e) {
            LOG.warn("Cannot load previous close for {}", key.id(), e);
            previousClose = null;
        }
        try {
            // El trade en curso todavia no se inserto, asi que lo recuperado es exactamente lo que
            // dejaron corridas anteriores del proceso.
            restoreDay(repository.loadDayTotals(key, tradingDay, MARKET_ZONE));
        } catch (Exception e) {
            LOG.warn("Cannot restore day {} for {}", tradingDay, key.id(), e);
        }
    }

    private void restoreDay(MongoMarketRepository.DayTotals totals) {
        if (totals == null || totals.trades() == 0) {
            return;
        }
        totalTrades = totals.trades();
        totalVolume = totals.volume();
        totalTurnover = totals.turnover();
        dayTrades = totals.trades();
        intradayVolume = totals.volume();
        intradayTurnover = totals.turnover();
        dayOpen = totals.open();
        dayHigh = totals.high();
        dayLow = totals.low();
        lastPrice = totals.last();
        if (intradayVolume.signum() > 0) {
            vwapIntraday = intradayTurnover.divide(intradayVolume, 6, RoundingMode.HALF_UP);
        }
        LOG.info("Estado del dia recuperado instrument={} trades={} volumen={} monto={}",
                key.id(), totalTrades, totalVolume, totalTurnover);
    }

    private void updateDailyMetrics(BigDecimal price, BigDecimal qty, BigDecimal amount) {
        if (dayOpen == null) {
            dayOpen = price;
            dayHigh = price;
            dayLow = price;
        } else {
            if (price.compareTo(dayHigh) > 0) {
                dayHigh = price;
            }
            if (price.compareTo(dayLow) < 0) {
                dayLow = price;
            }
        }
        dayTrades++;

        if (qty != null && qty.compareTo(BigDecimal.ZERO) > 0) {
            intradayVolume = intradayVolume.add(qty, MC);
            BigDecimal usedAmount = amount != null ? amount : price.multiply(qty, MC);
            intradayTurnover = intradayTurnover.add(usedAmount, MC);
            if (intradayVolume.compareTo(BigDecimal.ZERO) > 0) {
                vwapIntraday = intradayTurnover.divide(intradayVolume, 6, RoundingMode.HALF_UP);
            }
        }

        if (previousClose != null && previousClose.compareTo(BigDecimal.ZERO) != 0) {
            dailyVariationPct = price.subtract(previousClose, MC)
                    .multiply(HUNDRED, MC)
                    .divide(previousClose, 6, RoundingMode.HALF_UP);
            variationPct = dailyVariationPct;
        }
    }

    private void updateSma(BigDecimal price) {
        smaPrices.addLast(price);
        smaSum = smaSum.add(price, MC);
        if (smaPrices.size() > SMA_PERIOD) {
            BigDecimal removed = smaPrices.removeFirst();
            smaSum = smaSum.subtract(removed, MC);
        }
        if (smaPrices.size() == SMA_PERIOD) {
            sma20 = smaSum.divide(BigDecimal.valueOf(SMA_PERIOD), 6, RoundingMode.HALF_UP);
        }
    }

    private void updateRsi(BigDecimal price) {
        if (prevTradePrice == null) {
            return;
        }
        BigDecimal delta = price.subtract(prevTradePrice, MC);
        BigDecimal gain = delta.signum() > 0 ? delta : BigDecimal.ZERO;
        BigDecimal loss = delta.signum() < 0 ? delta.negate(MC) : BigDecimal.ZERO;

        if (rsiSeedCount < RSI_PERIOD) {
            rsiSeedGain = rsiSeedGain.add(gain, MC);
            rsiSeedLoss = rsiSeedLoss.add(loss, MC);
            rsiSeedCount++;
            if (rsiSeedCount == RSI_PERIOD) {
                rsiAvgGain = rsiSeedGain.divide(BigDecimal.valueOf(RSI_PERIOD), 8, RoundingMode.HALF_UP);
                rsiAvgLoss = rsiSeedLoss.divide(BigDecimal.valueOf(RSI_PERIOD), 8, RoundingMode.HALF_UP);
                rsi14 = computeRsi(rsiAvgGain, rsiAvgLoss);
            }
            return;
        }

        BigDecimal period = BigDecimal.valueOf(RSI_PERIOD);
        BigDecimal periodMinusOne = BigDecimal.valueOf(RSI_PERIOD - 1L);
        rsiAvgGain = rsiAvgGain.multiply(periodMinusOne, MC).add(gain, MC).divide(period, 8, RoundingMode.HALF_UP);
        rsiAvgLoss = rsiAvgLoss.multiply(periodMinusOne, MC).add(loss, MC).divide(period, 8, RoundingMode.HALF_UP);
        rsi14 = computeRsi(rsiAvgGain, rsiAvgLoss);
    }

    private BigDecimal computeRsi(BigDecimal avgGain, BigDecimal avgLoss) {
        if (avgGain == null || avgLoss == null) {
            return null;
        }
        if (avgLoss.compareTo(BigDecimal.ZERO) == 0) {
            if (avgGain.compareTo(BigDecimal.ZERO) == 0) {
                return new BigDecimal("50.000000");
            }
            return new BigDecimal("100.000000");
        }
        BigDecimal rs = avgGain.divide(avgLoss, 10, RoundingMode.HALF_UP);
        return HUNDRED.subtract(HUNDRED.divide(BigDecimal.ONE.add(rs, MC), 6, RoundingMode.HALF_UP), MC);
    }

    private BigDecimal ema(BigDecimal previousEma, BigDecimal value, int period) {
        if (value == null) {
            return previousEma;
        }
        if (previousEma == null) {
            return value;
        }
        BigDecimal alpha = BigDecimal.valueOf(2.0d / (period + 1.0d));
        return previousEma.add(alpha.multiply(value.subtract(previousEma, MC), MC), MC);
    }
}
