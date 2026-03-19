package cl.vc.inyectorcandle.actor;

import cl.vc.inyectorcandle.model.Candle;
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
    private BigDecimal previousClose;
    private BigDecimal intradayVolume = BigDecimal.ZERO;
    private BigDecimal intradayTurnover = BigDecimal.ZERO;
    private final Map<String, TradeEvent> activeTrades = new HashMap<>();

    public InstrumentActor(InstrumentKey key, List<Duration> timeframes, MongoMarketRepository repository) {
        this.key = key;
        this.repository = repository;
        this.candleService = new CandleService(key, timeframes);
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

        repository.upsertInstrumentStats(snapshot());
    }

    private void onTrade(TradeEvent trade) {
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
            updateDailyMetrics(trade.eventTime(), trade.price(), trade.quantity(), amount);
            updateIndicators(trade.price());
        }

        List<Candle> finalized = candleService.onTrade(trade.eventTime(), trade.price(), trade.quantity(), amount);
        for (Candle candle : finalized) {
            repository.upsertCandle(candle);
        }

        repository.upsertInstrumentStats(snapshot());
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
        repository.upsertInstrumentStats(snapshot());
    }

    private void applyDeleteTrade(TradeEvent trade) {
        TradeEvent previous = activeTrades.remove(trade.mdEntryId());
        repository.insertTrade(trade);
        if (previous == null) {
            LOG.debug("Ignoring trade delete without previous state instrument={} mdEntryId={}", key.id(), trade.mdEntryId());
            return;
        }

        applyTradeDelta(previous, false);
        repository.upsertInstrumentStats(snapshot());
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
            updateDailyMetrics(trade.eventTime(), trade.price(), trade.quantity(), amount);
            updateIndicators(trade.price());
        }

        List<Candle> finalized = candleService.onTrade(trade.eventTime(), trade.price(), trade.quantity(), amount);
        for (Candle candle : finalized) {
            repository.upsertCandle(candle);
        }

        repository.upsertInstrumentStats(snapshot());
    }

    private void onStop() {
        running = false;
        candleService.flushAll().forEach(repository::upsertCandle);
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

    private void updateDailyMetrics(Instant eventTime, BigDecimal price, BigDecimal qty, BigDecimal amount) {
        LocalDate tradingDay = eventTime.atZone(MARKET_ZONE).toLocalDate();
        if (currentTradingDay == null || !currentTradingDay.equals(tradingDay)) {
            currentTradingDay = tradingDay;
            intradayVolume = BigDecimal.ZERO;
            intradayTurnover = BigDecimal.ZERO;
            vwapIntraday = null;
            try {
                previousClose = repository.findPreviousClose(key, tradingDay, MARKET_ZONE);
            } catch (Exception e) {
                LOG.warn("Cannot load previous close for {}", key.id(), e);
                previousClose = null;
            }
        }

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
