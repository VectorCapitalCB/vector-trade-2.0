package cl.vc.inyectorcandle.mongo;

import cl.vc.inyectorcandle.model.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MongoMarketRepository implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MongoMarketRepository.class);
    private static final long PROGRESS_LOG_EVERY = 1_000L;

    private final MongoClient client;
    private final MongoCollection<Document> securitiesCollection;
    private final MongoCollection<Document> marketDataCollection;
    private final MongoCollection<Document> tradesCollection;
    private final MongoCollection<Document> candlesCollection;
    private final MongoCollection<Document> instrumentStatsCollection;
    private final MongoCollection<Document> rankingsCollection;
    private final AtomicLong insertedMarketData = new AtomicLong();
    private final AtomicLong insertedTrades = new AtomicLong();
    private final AtomicLong upsertedCandles = new AtomicLong();
    private final AtomicLong upsertedSecurities = new AtomicLong();
    private final AtomicLong upsertedStats = new AtomicLong();
    private final AtomicLong upsertedRankings = new AtomicLong();

    public MongoMarketRepository(String uri, String databaseName) {
        this.client = MongoClients.create(uri);
        MongoDatabase database = client.getDatabase(databaseName);
        this.securitiesCollection = database.getCollection("securities");
        this.marketDataCollection = database.getCollection("md_events");
        this.tradesCollection = database.getCollection("trades");
        this.candlesCollection = database.getCollection("candles");
        this.instrumentStatsCollection = database.getCollection("instrument_stats");
        this.rankingsCollection = database.getCollection("market_rankings");
        LOG.info("MongoMarketRepository conectado. database={}", databaseName);
    }

    public void upsertSecurity(SecurityDefinition security) {
        Document doc = new Document("_id", security.key().id())
                .append("symbol", security.key().symbol())
                .append("settlement", security.key().settlement())
                .append("destination", security.key().destination())
                .append("currency", security.key().currency())
                .append("securityType", security.key().securityType())
                .append("securityId", security.securityId())
                .append("securityDesc", security.securityDesc())
                .append("bookingRefId", security.bookingRefId())
                .append("updatedAt", Instant.now().toString());

        securitiesCollection.replaceOne(Filters.eq("_id", security.key().id()), doc, new ReplaceOptions().upsert(true));
        logProgress("securities.upsert", upsertedSecurities.incrementAndGet());
    }

    public void insertMarketDataEvent(MarketDataEvent event) {
        Document doc = new Document("instrumentId", event.key().id())
                .append("symbol", event.key().symbol())
                .append("settlement", event.key().settlement())
                .append("destination", event.key().destination())
                .append("currency", event.key().currency())
                .append("securityType", event.key().securityType())
                .append("eventTime", event.eventTime().toString())
                .append("mdEntryType", String.valueOf(event.mdEntryType()))
                .append("price", toDouble(event.price()))
                .append("size", toDouble(event.size()))
                .append("mdEntryId", event.mdEntryId())
                .append("mdReqId", event.mdReqId())
                .append("sourceMsgType", event.sourceMsgType());

        marketDataCollection.insertOne(doc);
        logProgress("md_events.insert", insertedMarketData.incrementAndGet());
    }

    public void insertTrade(TradeEvent trade) {
        Document doc = new Document("instrumentId", trade.key().id())
                .append("symbol", trade.key().symbol())
                .append("settlement", trade.key().settlement())
                .append("destination", trade.key().destination())
                .append("currency", trade.key().currency())
                .append("securityType", trade.key().securityType())
                .append("eventTime", trade.eventTime().toString())
                .append("price", toDouble(trade.price()))
                .append("quantity", toDouble(trade.quantity()))
                .append("amount", toDouble(trade.amount()))
                .append("aggressorSide", trade.aggressorSide())
                .append("mdEntryId", trade.mdEntryId())
                .append("mdReqId", trade.mdReqId())
                .append("sourceMsgType", trade.sourceMsgType());

        tradesCollection.insertOne(doc);
        logProgress("trades.insert", insertedTrades.incrementAndGet());
    }

    public void upsertCandle(Candle candle) {
        String id = candle.key().id() + "|" + candle.timeframe() + "|" + candle.bucketStart();
        Document doc = new Document("_id", id)
                .append("instrumentId", candle.key().id())
                .append("symbol", candle.key().symbol())
                .append("settlement", candle.key().settlement())
                .append("destination", candle.key().destination())
                .append("currency", candle.key().currency())
                .append("securityType", candle.key().securityType())
                .append("timeframe", candle.timeframe().toString())
                .append("bucketStart", candle.bucketStart().toString())
                .append("bucketEnd", candle.bucketEnd().toString())
                .append("open", toDouble(candle.open()))
                .append("high", toDouble(candle.high()))
                .append("low", toDouble(candle.low()))
                .append("close", toDouble(candle.close()))
                .append("volume", toDouble(candle.volume()))
                .append("turnover", toDouble(candle.turnover()))
                .append("trades", candle.trades())
                .append("updatedAt", Instant.now().toString());

        candlesCollection.replaceOne(Filters.eq("_id", id), doc, new ReplaceOptions().upsert(true));
        logProgress("candles.upsert", upsertedCandles.incrementAndGet());
    }

    public void upsertInstrumentStats(InstrumentStats stats) {
        Document doc = new Document("_id", stats.key().id())
                .append("symbol", stats.key().symbol())
                .append("settlement", stats.key().settlement())
                .append("destination", stats.key().destination())
                .append("currency", stats.key().currency())
                .append("securityType", stats.key().securityType())
                .append("totalTrades", stats.totalTrades())
                .append("totalVolume", toDouble(stats.totalVolume()))
                .append("totalTurnover", toDouble(stats.totalTurnover()))
                .append("lastPrice", toDouble(stats.lastPrice()))
                .append("bestBid", toDouble(stats.bestBid()))
                .append("bestAsk", toDouble(stats.bestAsk()))
                .append("variationPct", toDouble(stats.variationPct()))
                .append("dailyVariationPct", toDouble(stats.dailyVariationPct()))
                .append("vwapIntraday", toDouble(stats.vwapIntraday()))
                .append("sma20", toDouble(stats.sma20()))
                .append("ema20", toDouble(stats.ema20()))
                .append("rsi14", toDouble(stats.rsi14()))
                .append("macdLine", toDouble(stats.macdLine()))
                .append("macdSignal", toDouble(stats.macdSignal()))
                .append("macdHistogram", toDouble(stats.macdHistogram()))
                .append("updatedAt", Instant.now().toString());

        instrumentStatsCollection.replaceOne(Filters.eq("_id", stats.key().id()), doc, new ReplaceOptions().upsert(true));
        logProgress("instrument_stats.upsert", upsertedStats.incrementAndGet());
    }

    public void upsertRanking(MarketRankingSnapshot ranking) {
        Document doc = new Document("_id", "latest")
                .append("generatedAt", ranking.generatedAt().toString())
                .append("topByTurnover", toStatsDocs(ranking.topByTurnover()))
                .append("bottomByTurnover", toStatsDocs(ranking.bottomByTurnover()))
                .append("topByVolume", toStatsDocs(ranking.topByVolume()))
                .append("bottomByVolume", toStatsDocs(ranking.bottomByVolume()))
                .append("topGainers", toStatsDocs(ranking.topGainers()))
                .append("topLosers", toStatsDocs(ranking.topLosers()));

        rankingsCollection.replaceOne(Filters.eq("_id", "latest"), doc, new ReplaceOptions().upsert(true));
        logProgress("market_rankings.upsert", upsertedRankings.incrementAndGet());
    }

    public BigDecimal findPreviousClose(InstrumentKey key, LocalDate tradingDay, ZoneId zoneId) {
        Instant dayStart = tradingDay.atStartOfDay(zoneId).toInstant();
        String dayStartIso = dayStart.toString();

        Document dailyCandle = candlesCollection.find(Filters.and(
                        Filters.eq("instrumentId", key.id()),
                        Filters.eq("timeframe", Duration.ofDays(1).toString()),
                        Filters.lt("bucketStart", dayStartIso)
                ))
                .sort(Sorts.descending("bucketStart"))
                .limit(1)
                .first();
        BigDecimal close = toBigDecimal(dailyCandle == null ? null : dailyCandle.get("close"));
        if (close != null) {
            return close;
        }

        Document lastTrade = tradesCollection.find(Filters.and(
                        Filters.eq("instrumentId", key.id()),
                        Filters.lt("eventTime", dayStartIso)
                ))
                .sort(Sorts.descending("eventTime"))
                .limit(1)
                .first();
        return toBigDecimal(lastTrade == null ? null : lastTrade.get("price"));
    }

    public void purgeDay(LocalDate day, ZoneId zoneId) {
        Instant dayStart = day.atStartOfDay(zoneId).toInstant();
        Instant dayEnd = day.plusDays(1).atStartOfDay(zoneId).toInstant();

        DeleteResult mdDeleted = marketDataCollection.deleteMany(Filters.and(
                Filters.gte("eventTime", dayStart.toString()),
                Filters.lt("eventTime", dayEnd.toString())
        ));

        DeleteResult tradesDeleted = tradesCollection.deleteMany(Filters.and(
                Filters.gte("eventTime", dayStart.toString()),
                Filters.lt("eventTime", dayEnd.toString())
        ));

        DeleteResult candlesDeleted = candlesCollection.deleteMany(Filters.and(
                Filters.gte("bucketStart", dayStart.toString()),
                Filters.lt("bucketStart", dayEnd.toString())
        ));

        rankingsCollection.deleteOne(Filters.eq("_id", "latest"));

        LOG.info("Mongo purge day={} md={} trades={} candles={}",
                day, mdDeleted.getDeletedCount(), tradesDeleted.getDeletedCount(), candlesDeleted.getDeletedCount());
    }

    private void logProgress(String op, long count) {
        if (count == 1 || count % PROGRESS_LOG_EVERY == 0) {
            LOG.info("Mongo progreso {} count={}", op, count);
        }
    }

    private static List<Document> toStatsDocs(List<InstrumentStats> stats) {
        return stats.stream().map(s -> new Document("instrumentId", s.key().id())
                        .append("symbol", s.key().symbol())
                        .append("settlement", s.key().settlement())
                        .append("destination", s.key().destination())
                        .append("currency", s.key().currency())
                        .append("securityType", s.key().securityType())
                        .append("totalTrades", s.totalTrades())
                        .append("totalVolume", toDouble(s.totalVolume()))
                        .append("totalTurnover", toDouble(s.totalTurnover()))
                        .append("lastPrice", toDouble(s.lastPrice()))
                        .append("variationPct", toDouble(s.variationPct()))
                        .append("dailyVariationPct", toDouble(s.dailyVariationPct()))
                        .append("vwapIntraday", toDouble(s.vwapIntraday()))
                        .append("sma20", toDouble(s.sma20()))
                        .append("ema20", toDouble(s.ema20()))
                        .append("rsi14", toDouble(s.rsi14()))
                        .append("macdLine", toDouble(s.macdLine()))
                        .append("macdSignal", toDouble(s.macdSignal()))
                        .append("macdHistogram", toDouble(s.macdHistogram())))
                .toList();
    }

    private static Double toDouble(BigDecimal value) {
        return value == null ? null : value.doubleValue();
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return BigDecimal.valueOf(number.doubleValue());
        }
        try {
            return new BigDecimal(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() {
        LOG.info("Mongo resumen final: md_events={} trades={} candles={} securities={} instrument_stats={} rankings={}",
                insertedMarketData.get(),
                insertedTrades.get(),
                upsertedCandles.get(),
                upsertedSecurities.get(),
                upsertedStats.get(),
                upsertedRankings.get());
        client.close();
    }
}
