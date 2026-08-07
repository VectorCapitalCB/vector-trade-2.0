package cl.vc.inyectorcandle.mongo;

import cl.vc.inyectorcandle.model.*;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class MongoMarketRepository implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MongoMarketRepository.class);
    private static final long PROGRESS_LOG_EVERY = 1_000L;
    private static final ReplaceOptions UPSERT = new ReplaceOptions().upsert(true);

    private static final BigDecimal HUNDRED = new BigDecimal("100");

    private final String marketName;
    private final MongoClient client;
    private final MongoWriteQueue writeQueue;
    private final MongoCollection<Document> securitiesCollection;
    private final MongoCollection<Document> marketDataCollection;
    private final MongoCollection<Document> tradesCollection;
    private final MongoCollection<Document> candlesCollection;
    private final MongoCollection<Document> instrumentStatsCollection;
    private final MongoCollection<Document> rankingsCollection;
    private final MongoCollection<Document> bolsaStatsHistoryCollection;
    private final MongoCollection<Document> instrumentDailyCollection;
    private final MongoCollection<Document> brokerDailyCollection;
    private final AtomicLong insertedMarketData = new AtomicLong();
    private final AtomicLong insertedTrades = new AtomicLong();
    private final AtomicLong upsertedCandles = new AtomicLong();
    private final AtomicLong upsertedSecurities = new AtomicLong();
    private final AtomicLong upsertedStats = new AtomicLong();
    private final AtomicLong upsertedRankings = new AtomicLong();
    private final AtomicLong upsertedDaily = new AtomicLong();
    private final AtomicLong upsertedHistory = new AtomicLong();
    private final AtomicLong upsertedBrokers = new AtomicLong();
    private static final UpdateOptions UPSERT_UPDATE = new UpdateOptions().upsert(true);

    public MongoMarketRepository(String uri, String databaseName, int writeQueueSize, int writeBatchSize,
                                 long writeFlushMs, String marketName) {
        this.marketName = marketName;
        this.client = MongoClients.create(uri);
        // W1 sin journal: la durabilidad la da el feed, no vale frenar el hot path por fsync.
        MongoDatabase database = client.getDatabase(databaseName).withWriteConcern(WriteConcern.W1.withJournal(false));
        this.securitiesCollection = database.getCollection("securities");
        this.marketDataCollection = database.getCollection("md_events");
        this.tradesCollection = database.getCollection("trades");
        this.candlesCollection = database.getCollection("candles");
        this.instrumentStatsCollection = database.getCollection("instrument_stats");
        this.rankingsCollection = database.getCollection("market_rankings");
        this.bolsaStatsHistoryCollection = database.getCollection("bolsa_stats_history");
        this.instrumentDailyCollection = database.getCollection("instrument_daily");
        this.brokerDailyCollection = database.getCollection("broker_daily");
        this.writeQueue = new MongoWriteQueue(writeQueueSize, writeBatchSize, writeFlushMs);
        ensureIndexes();
        LOG.info("MongoMarketRepository conectado. database={}", databaseName);
    }

    /**
     * Sin estos indices las consultas de vector-candle-service (por symbol+timeframe, por dia)
     * hacen collection scan contra el mismo Mongo que esta recibiendo la escritura del feed.
     */
    private void ensureIndexes() {
        createIndex(candlesCollection, Indexes.compoundIndex(
                Indexes.ascending("instrumentId", "timeframe"), Indexes.descending("bucketStart")));
        createIndex(candlesCollection, Indexes.ascending("symbol", "timeframe", "_id"));
        createIndex(candlesCollection, Indexes.ascending("bucketStart"));
        createIndex(tradesCollection, Indexes.compoundIndex(
                Indexes.ascending("instrumentId"), Indexes.descending("eventTime")));
        createIndex(tradesCollection, Indexes.ascending("eventTime"));
        createIndex(marketDataCollection, Indexes.ascending("eventTime"));
        createIndex(bolsaStatsHistoryCollection, Indexes.ascending("snapshotKey"));
        createIndex(bolsaStatsHistoryCollection, Indexes.ascending("timeframe", "snapshotAt"));
        createIndex(instrumentDailyCollection, Indexes.compoundIndex(
                Indexes.ascending("market", "tradingDay"), Indexes.descending("turnover")));
        createIndex(instrumentDailyCollection, Indexes.ascending("symbol", "tradingDay"));
        createIndex(brokerDailyCollection, Indexes.compoundIndex(
                Indexes.ascending("market", "tradingDay"), Indexes.descending("turnover")));
    }

    private void createIndex(MongoCollection<Document> collection, Bson keys) {
        try {
            collection.createIndex(keys, new IndexOptions().background(true));
        } catch (Exception e) {
            LOG.warn("No se pudo crear indice en {}: {}", collection.getNamespace(), e.getMessage());
        }
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

        enqueue(securitiesCollection, security.key().id(), doc, false);
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

        // droppable: md_events es traza; si la cola se satura vale mas no frenar el feed.
        writeQueue.submit(marketDataCollection, new InsertOneModel<>(doc), true);
        logProgress("md_events.insert", insertedMarketData.incrementAndGet());
    }

    public void insertTrade(TradeEvent trade) {
        String mdEntryId = trade.mdEntryId();
        String id = trade.key().id() + "|" + ((mdEntryId == null || mdEntryId.isBlank())
                ? trade.eventTime() + "|" + trade.price() + "|" + trade.quantity() + "|" + trade.sourceMsgType()
                : mdEntryId);
        Document doc = new Document("_id", id)
                .append("instrumentId", trade.key().id())
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
                .append("mdUpdateAction", String.valueOf(trade.mdUpdateAction()))
                .append("deleted", trade.mdUpdateAction() == '2')
                .append("mdReqId", trade.mdReqId())
                .append("sourceMsgType", trade.sourceMsgType())
                .append("updatedAt", Instant.now().toString());

        enqueue(tradesCollection, id, doc, false);
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

        enqueue(candlesCollection, id, doc, false);
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

        // droppable: es un snapshot idempotente, el siguiente tick lo vuelve a publicar.
        enqueue(instrumentStatsCollection, stats.key().id(), doc, true);
        logProgress("instrument_stats.upsert", upsertedStats.incrementAndGet());
    }

    public void upsertInstrumentDaily(InstrumentDailyStats stats) {
        upsertInstrumentDaily(stats, "FIX");
    }

    public void upsertInstrumentDaily(InstrumentDailyStats stats, String source) {
        Document doc = new Document("_id", stats.id())
                .append("market", stats.market())
                .append("symbol", stats.symbol())
                .append("securityId", stats.securityId())
                .append("currency", stats.currency())
                .append("tradingDay", stats.tradingDay().toString())
                .append("open", toDouble(stats.open()))
                .append("high", toDouble(stats.high()))
                .append("low", toDouble(stats.low()))
                .append("last", toDouble(stats.last()))
                .append("close", toDouble(stats.close()))
                .append("previousClose", toDouble(stats.previousClose()))
                .append("volume", toDouble(stats.volume()))
                .append("turnover", toDouble(stats.turnover()))
                .append("vwap", toDouble(stats.vwap()))
                .append("variationPct", toDouble(stats.variationPct()))
                .append("auctionPrice", toDouble(stats.auctionPrice()))
                .append("lowLimit", toDouble(stats.lowLimit()))
                .append("highLimit", toDouble(stats.highLimit()))
                .append("referencePrice", toDouble(stats.referencePrice()))
                .append("updates", stats.updates())
                .append("tradeCount", stats.tradeCount())
                .append("source", source)
                .append("updatedAt", stats.lastUpdate().toString());

        enqueue(instrumentDailyCollection, stats.id(), doc, false);
        logProgress("instrument_daily.upsert", upsertedDaily.incrementAndGet());
    }

    /**
     * Merge no destructivo: el OHLCV oficial del feed FIX manda y no se pisa. ITCH solo aporta
     * {@code tradeCount} (que el 35=W no publica) y crea el documento cuando el instrumento o el
     * dia no vinieron por FIX, que es el caso de la enorme mayoria de la plaza.
     */
    public void upsertInstrumentDailyFromItch(InstrumentDailyStats stats, long tradeCount) {
        // Pipeline en vez de $setOnInsert: este ultimo solo actua al crear el documento, y en los
        // dias en que FIX listo el mercado completo hay instrumentos con doc pero sin OHLCV, que
        // ITCH sabe que si operaron. Con $ifNull cada campo se rellena solo si FIX no lo trajo.
        Document fields = new Document("market", stats.market())
                .append("symbol", stats.symbol())
                .append("tradingDay", stats.tradingDay().toString())
                .append("tradeCount", tradeCount)
                .append("updatedAt", stats.lastUpdate().toString())
                .append("source", fillIfNull("source", "FIX+ITCH"))
                .append("securityId", fillIfNull("securityId", stats.securityId()))
                .append("currency", fillIfNull("currency", stats.currency()))
                .append("open", fillIfNull("open", toDouble(stats.open())))
                .append("high", fillIfNull("high", toDouble(stats.high())))
                .append("low", fillIfNull("low", toDouble(stats.low())))
                .append("last", fillIfNull("last", toDouble(stats.last())))
                .append("close", fillIfNull("close", toDouble(stats.close())))
                .append("volume", fillIfNull("volume", toDouble(stats.volume())))
                .append("turnover", fillIfNull("turnover", toDouble(stats.turnover())))
                .append("vwap", fillIfNull("vwap", toDouble(stats.vwap())))
                .append("variationPct", fillIfNull("variationPct", toDouble(stats.variationPct())))
                .append("auctionPrice", fillIfNull("auctionPrice", toDouble(stats.auctionPrice())));

        writeQueue.submit(instrumentDailyCollection, new UpdateOneModel<>(
                Filters.eq("_id", stats.id()),
                List.of(new Document("$set", fields)),
                UPSERT_UPDATE), false);
        logProgress("instrument_daily.itch", upsertedDaily.incrementAndGet());
    }

    /** {@code $ifNull} contra el valor ya presente: lo oficial de FIX manda, ITCH rellena huecos. */
    private static Document fillIfNull(String field, Object itchValue) {
        return new Document("$ifNull", java.util.Arrays.asList("$" + field, itchValue));
    }

    public void upsertBrokerDaily(BrokerDailyStats stats) {
        Document doc = new Document("_id", stats.id())
                .append("market", stats.market())
                .append("broker", stats.broker())
                .append("tradingDay", stats.tradingDay().toString())
                .append("trades", stats.trades())
                .append("volume", toDouble(stats.volume()))
                .append("turnover", toDouble(stats.turnover()))
                .append("buyVolume", toDouble(stats.buyVolume()))
                .append("sellVolume", toDouble(stats.sellVolume()))
                .append("buyTurnover", toDouble(stats.buyTurnover()))
                .append("sellTurnover", toDouble(stats.sellTurnover()))
                .append("netTurnover", toDouble(stats.netTurnover()))
                .append("updatedAt", Instant.now().toString());

        enqueue(brokerDailyCollection, stats.id(), doc, false);
        logProgress("broker_daily.upsert", upsertedBrokers.incrementAndGet());
    }

    /** Lee lo ya escrito para reconstruir el agregado del dia sobre ambas fuentes juntas. */
    public List<InstrumentDailyStats> loadInstrumentDaily(String market, LocalDate day) {
        writeQueue.flush();
        List<InstrumentDailyStats> out = new ArrayList<>();
        for (Document d : instrumentDailyCollection.find(Filters.and(
                Filters.eq("market", market), Filters.eq("tradingDay", day.toString())))) {
            out.add(new InstrumentDailyStats(
                    market,
                    d.getString("symbol"),
                    d.getString("securityId"),
                    d.getString("currency"),
                    day,
                    toBigDecimal(d.get("open")),
                    toBigDecimal(d.get("high")),
                    toBigDecimal(d.get("low")),
                    toBigDecimal(d.get("last")),
                    toBigDecimal(d.get("close")),
                    toBigDecimal(d.get("previousClose")),
                    toBigDecimal(d.get("volume")),
                    toBigDecimal(d.get("turnover")),
                    toBigDecimal(d.get("auctionPrice")),
                    toBigDecimal(d.get("lowLimit")),
                    toBigDecimal(d.get("highLimit")),
                    toBigDecimal(d.get("referencePrice")),
                    d.get("updates") instanceof Number n ? n.longValue() : 0L,
                    d.get("tradeCount") instanceof Number t ? t.longValue() : 0L,
                    Instant.now()));
        }
        return out;
    }

    /**
     * Escribe el agregado diario con el esquema exacto que espera vector-candle-service en
     * {@code bolsa_stats_history}. Asi el boton "Aplicar Fecha" del front encuentra el dia
     * historico sin que nadie tenga que tocar el front ni el candle-service.
     */
    public void upsertMarketDayHistory(MarketDaySummary s) {
        Document doc = new Document("snapshotAt", s.sessionEnd().toString())
                .append("snapshotKey", s.snapshotKey())
                .append("timeframe", "1d")
                .append("exchange", s.market())
                .append("id", s.snapshotKey())
                .append("total_volumen", s.totalVolumen())
                .append("monto_total", s.montoTotal())
                .append("hora_inicio", s.sessionStart().toString())
                .append("hora_fin", s.sessionEnd().toString())
                .append("volatilidad_promedio", s.volatilidadPromedio())
                .append("rango_promedio", s.rangoPromedio())
                .append("indice_promedio", s.indicePromedio())
                .append("indice_maximo", s.indiceMaximo())
                .append("indice_minimo", s.indiceMinimo())
                .append("liquidez_media", s.liquidezMedia())
                .append("numero_total_trades", s.numeroTotalTrades())
                .append("sentimiento_positivo", s.sentimientoPositivo())
                .append("sentimiento_negativo", s.sentimientoNegativo())
                .append("capitalizacion_total", s.capitalizacionTotal())
                .append("capitalizacion_promedio", s.capitalizacionPromedio())
                .append("precio_promedio_acumulado", s.precioPromedioAcumulado())
                .append("precio_maximo_acumulado", s.precioMaximoAcumulado())
                .append("tendencia_general", s.tendenciaGeneral())
                .append("tendencia_promedio", s.tendenciaPromedio())
                .append("mas_tranzado", toRankinDocs(s.masTranzado()))
                .append("mas_volatil", toRankinDocs(s.masVolatil()))
                .append("best_rankin", toRankinDocs(s.mejores()))
                .append("menos_cayo", toRankinDocs(s.mejores()))
                .append("worse_rankin", toRankinDocs(s.peores()))
                .append("mas_cayo", toRankinDocs(s.peores()));

        writeQueue.submit(bolsaStatsHistoryCollection,
                new ReplaceOneModel<>(Filters.eq("snapshotKey", s.snapshotKey()), doc, UPSERT), false);
        logProgress("bolsa_stats_history.upsert", upsertedHistory.incrementAndGet());
    }

    private static List<Document> toRankinDocs(List<InstrumentDailyStats> rows) {
        return rows.stream().map(r -> new Document("id", r.id())
                        .append("exchange", r.market())
                        .append("symbol", r.symbol())
                        // el feed 35=W no tiene dimension de liquidacion; T2 es el default del proto
                        .append("settl", "T2")
                        .append("securityType", "CS")
                        .append("variacion_pct", toDouble(r.variationPct()))
                        .append("precio_ultimo", toDouble(r.last()))
                        .append("precio_maximo", toDouble(r.high()))
                        .append("precio_minimo", toDouble(r.low()))
                        .append("precio_promedio", toDouble(r.vwap()))
                        .append("vwap", toDouble(r.vwap()))
                        .append("volumen", toDouble(r.volume()))
                        .append("monto", toDouble(r.turnover())))
                .toList();
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

        enqueue(rankingsCollection, "latest", doc, false);
        logProgress("market_rankings.upsert", upsertedRankings.incrementAndGet());
    }

    private void enqueue(MongoCollection<Document> collection, String id, Document doc, boolean droppable) {
        writeQueue.submit(collection, new ReplaceOneModel<>(Filters.eq("_id", id), doc, UPSERT), droppable);
    }

    /**
     * Cierre oficial del ultimo dia habil anterior segun {@code instrument_daily}. Es la referencia
     * correcta para la variacion del dia y no depende de que existan velas ni trades del dia previo,
     * que es justo lo que falta cuando el proceso arranca con la base recien poblada.
     */
    public BigDecimal findPreviousDailyClose(String market, String symbol, LocalDate tradingDay) {
        Document doc = instrumentDailyCollection.find(Filters.and(
                        Filters.eq("market", market),
                        Filters.eq("symbol", symbol),
                        Filters.lt("tradingDay", tradingDay.toString()),
                        Filters.ne("close", null)))
                .sort(Sorts.descending("tradingDay"))
                .limit(1)
                .first();
        return doc == null ? null : toBigDecimal(doc.get("close"));
    }

    /**
     * Encadena los cierres: para cada simbolo recorre sus dias en orden y rellena el
     * {@code previousClose} que falte con el {@code close} del dia anterior, recalculando la
     * variacion. No pisa el {@code previousClose} oficial que ya trajo el 269=5 del feed FIX.
     *
     * @return cuantos documentos quedaron con cierre previo nuevo
     */
    public int fillMissingPreviousClose(String market) {
        writeQueue.flush();
        List<Document> docs = new ArrayList<>();
        instrumentDailyCollection.find(Filters.eq("market", market))
                .sort(Sorts.ascending("symbol", "tradingDay"))
                .into(docs);

        String currentSymbol = null;
        BigDecimal lastClose = null;
        int filled = 0;

        for (Document doc : docs) {
            String symbol = doc.getString("symbol");
            if (!symbol.equals(currentSymbol)) {
                currentSymbol = symbol;
                lastClose = null;
            }

            BigDecimal previous = toBigDecimal(doc.get("previousClose"));
            if (previous == null && lastClose != null) {
                previous = lastClose;
                BigDecimal last = toBigDecimal(doc.get("last"));
                Document set = new Document("previousClose", toDouble(previous));
                if (last != null && previous.signum() != 0) {
                    set.append("variationPct", toDouble(last.subtract(previous, MathContext.DECIMAL64)
                            .multiply(HUNDRED, MathContext.DECIMAL64)
                            .divide(previous, 6, RoundingMode.HALF_UP)));
                }
                writeQueue.submit(instrumentDailyCollection, new UpdateOneModel<>(
                        Filters.eq("_id", doc.get("_id")), new Document("$set", set)), false);
                filled++;
            }

            BigDecimal close = toBigDecimal(doc.get("close"));
            if (close != null) {
                lastClose = close;
            }
        }

        writeQueue.flush();
        LOG.info("Cierres encadenados: {} documentos recibieron previousClose de {} revisados", filled, docs.size());
        return filled;
    }

    public List<LocalDate> distinctTradingDays(String market) {
        List<LocalDate> days = new ArrayList<>();
        for (String value : instrumentDailyCollection.distinct("tradingDay", Filters.eq("market", market), String.class)) {
            days.add(LocalDate.parse(value));
        }
        days.sort(LocalDate::compareTo);
        return days;
    }

    public BigDecimal findPreviousClose(InstrumentKey key, LocalDate tradingDay, ZoneId zoneId) {
        writeQueue.flush();

        BigDecimal officialClose = findPreviousDailyClose(marketName, key.symbol(), tradingDay);
        if (officialClose != null) {
            return officialClose;
        }

        // Usar medianoche UTC como límite para candles diarias.
        // Los buckets diarios se almacenan como 2026-03-09T00:00:00Z (medianoche UTC),
        // por lo que comparar contra medianoche Santiago (03:00Z) incluiría la candle
        // del día actual. Usando medianoche UTC se excluye correctamente el día actual.
        String candleDayStart = tradingDay.atStartOfDay(ZoneOffset.UTC).toInstant().toString();
        // Para trades usamos medianoche Santiago (zona del mercado).
        String tradeDayStart = tradingDay.atStartOfDay(zoneId).toInstant().toString();

        Document dailyCandle = candlesCollection.find(Filters.and(
                        Filters.eq("instrumentId", key.id()),
                        Filters.eq("timeframe", Duration.ofDays(1).toString()),
                        Filters.lt("bucketStart", candleDayStart)
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
                        Filters.lt("eventTime", tradeDayStart)
                ))
                .sort(Sorts.descending("eventTime"))
                .limit(1)
                .first();
        return toBigDecimal(lastTrade == null ? null : lastTrade.get("price"));
    }

    /** Totales del dia ya persistidos en {@code trades} para un instrumento. */
    public record DayTotals(long trades, BigDecimal volume, BigDecimal turnover,
                            BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal last) {
    }

    /**
     * Reconstruye desde {@code trades} lo que un actor ya inyecto del dia. Sin esto un reinicio a
     * media rueda arranca los contadores de {@code instrument_stats} / {@code instrument_daily} en
     * cero y el front pierde toda la sesion previa al arranque.
     * <p>
     * Corre una sola vez por instrumento y por dia (en el cambio de jornada del actor) y usa el
     * indice {@code (instrumentId, eventTime)}, asi que en la apertura -cuando el dia todavia no
     * tiene trades- el rango esta vacio y no cuesta nada.
     */
    public DayTotals loadDayTotals(InstrumentKey key, LocalDate day, ZoneId zoneId) {
        String from = day.atStartOfDay(zoneId).toInstant().toString();
        String to = day.plusDays(1).atStartOfDay(zoneId).toInstant().toString();

        Document match = new Document("instrumentId", key.id())
                .append("eventTime", new Document("$gte", from).append("$lt", to))
                .append("deleted", new Document("$ne", true));
        Document amount = new Document("$ifNull", List.of("$amount",
                new Document("$multiply", List.of("$price", "$quantity"))));

        Document row = tradesCollection.aggregate(List.of(
                        new Document("$match", match),
                        new Document("$sort", new Document("eventTime", 1)),
                        new Document("$group", new Document("_id", null)
                                .append("trades", new Document("$sum", 1))
                                .append("volume", new Document("$sum", "$quantity"))
                                .append("turnover", new Document("$sum", amount))
                                .append("open", new Document("$first", "$price"))
                                .append("high", new Document("$max", "$price"))
                                .append("low", new Document("$min", "$price"))
                                .append("last", new Document("$last", "$price")))))
                .first();

        if (row == null) {
            return null;
        }
        return new DayTotals(
                row.get("trades") instanceof Number n ? n.longValue() : 0L,
                zeroIfNull(toBigDecimal(row.get("volume"))),
                zeroIfNull(toBigDecimal(row.get("turnover"))),
                toBigDecimal(row.get("open")),
                toBigDecimal(row.get("high")),
                toBigDecimal(row.get("low")),
                toBigDecimal(row.get("last")));
    }

    private static BigDecimal zeroIfNull(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    public void purgeDay(LocalDate day, ZoneId zoneId) {
        writeQueue.flush();
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

        String snapshotKey = "1d:" + day;
        DeleteResult historyDeleted = bolsaStatsHistoryCollection.deleteOne(Filters.eq("snapshotKey", snapshotKey));

        LOG.info("Mongo purge day={} md={} trades={} candles={} bolsaStatsHistory={}",
                day, mdDeleted.getDeletedCount(), tradesDeleted.getDeletedCount(), candlesDeleted.getDeletedCount(),
                historyDeleted.getDeletedCount());
    }

    public void logInjectionAnalysis(Set<LocalDate> days, ZoneId zoneId, int topN) {
        writeQueue.flush();
        if (days == null || days.isEmpty()) {
            LOG.info("[INYECCION][ANALISIS] Sin dias para analizar");
            return;
        }
        List<LocalDate> sorted = new ArrayList<>(days);
        sorted.sort(LocalDate::compareTo);
        for (LocalDate day : sorted) {
            logDayInjectionAnalysis(day, zoneId, topN);
        }
    }

    private void logDayInjectionAnalysis(LocalDate day, ZoneId zoneId, int topN) {
        Instant dayStart = day.atStartOfDay(zoneId).toInstant();
        Instant dayEnd = day.plusDays(1).atStartOfDay(zoneId).toInstant();

        Map<String, TradeSummary> bySymbol = new LinkedHashMap<>();
        for (Document doc : tradesCollection.find(Filters.and(
                Filters.gte("eventTime", dayStart.toString()),
                Filters.lt("eventTime", dayEnd.toString())
        ))) {
            String symbol = stringVal(doc.get("symbol")).trim().toUpperCase();
            if (symbol.isBlank() || "TEST-STGOX".equals(symbol)) {
                continue;
            }

            Instant t = parseInstantSafe(stringVal(doc.get("eventTime")));
            double price = numberVal(doc.get("price"));
            double qty = numberVal(doc.get("quantity"));
            double amount = numberVal(doc.get("amount"));
            if (amount <= 0 && price > 0 && qty > 0) {
                amount = price * qty;
            }
            if (price <= 0) {
                continue;
            }

            String settlement = stringVal(doc.get("settlement"));
            TradeSummary s = bySymbol.computeIfAbsent(symbol, k -> new TradeSummary(symbol, settlement));
            s.update(t, price, qty, amount);
        }

        if (bySymbol.isEmpty()) {
            LOG.info("[INYECCION][ANALISIS][{}] sin trades para analizar", day);
            return;
        }

        List<TradeSummary> rows = new ArrayList<>(bySymbol.values());
        double totalVol = rows.stream().mapToDouble(r -> r.volume).sum();
        double totalMonto = rows.stream().mapToDouble(r -> r.amount).sum();
        long totalTrades = rows.stream().mapToLong(r -> r.count).sum();
        double sentimientoPos = rows.stream().filter(r -> r.variationPct() > 0).count() * 100.0 / rows.size();
        double sentimientoNeg = rows.stream().filter(r -> r.variationPct() < 0).count() * 100.0 / rows.size();
        double volProm = rows.stream().mapToDouble(r -> Math.abs(r.variationPct())).average().orElse(0.0) / 100.0;
        double rangoProm = rows.stream().mapToDouble(r -> r.high - r.low).average().orElse(0.0);
        double indiceProm = totalVol > 0 ? rows.stream().mapToDouble(r -> r.last * r.volume).sum() / totalVol : 0.0;
        double indiceMax = totalVol > 0 ? rows.stream().mapToDouble(r -> r.high * r.volume).sum() / totalVol : 0.0;
        double indiceMin = totalVol > 0 ? rows.stream().mapToDouble(r -> r.low * r.volume).sum() / totalVol : 0.0;
        double tendenciaProm = rows.stream().mapToDouble(TradeSummary::variationPct).average().orElse(0.0);

        LOG.info("[INYECCION][ANALISIS][{}] totalVolumen={} montoTotal={} numeroTrades={} volatilidadPromedio={} rangoPromedio={} indicePromedio={} indiceMaximo={} indiceMinimo={} sentimientoPositivo={} sentimientoNegativo={} tendenciaPromedio={}",
                day, fmt(totalVol), fmt(totalMonto), totalTrades, fmt(volProm), fmt(rangoProm), fmt(indiceProm), fmt(indiceMax), fmt(indiceMin), fmt(sentimientoPos), fmt(sentimientoNeg), fmt(tendenciaProm));

        logTop(day, "TOP_10_MAS_TRANZADO", rows.stream()
                .sorted(Comparator.comparingDouble((TradeSummary r) -> r.volume).reversed())
                .limit(Math.max(1, topN))
                .toList());
        logTop(day, "TOP_10_MENOS_TRANZADO", rows.stream()
                .sorted(Comparator.comparingDouble(r -> r.volume))
                .limit(Math.max(1, topN))
                .toList());
        logTop(day, "TOP_10_MAS_VOLATIL", rows.stream()
                .sorted(Comparator.comparingDouble((TradeSummary r) -> Math.abs(r.variationPct())).reversed())
                .limit(Math.max(1, topN))
                .toList());
        logTop(day, "TOP_10_MEJORES", rows.stream()
                .sorted(Comparator.comparingDouble(TradeSummary::variationPct).reversed())
                .limit(Math.max(1, topN))
                .toList());
        logTop(day, "TOP_10_PEORES", rows.stream()
                .sorted(Comparator.comparingDouble(TradeSummary::variationPct))
                .limit(Math.max(1, topN))
                .toList());
    }

    private void logTop(LocalDate day, String title, List<TradeSummary> rows) {
        String joined = rows.stream()
                .limit(10)
                .map(r -> String.format(Locale.US, "%s(vol=%s,monto=%s,varPct=%s,last=%s,settl=%s)",
                        r.symbol, fmt(r.volume), fmt(r.amount), fmt(r.variationPct()), fmt(r.last), r.settlement))
                .toList()
                .stream()
                .reduce((a, b) -> a + " | " + b)
                .orElse("");
        LOG.info("[INYECCION][ANALISIS][{}][{}] {}", day, title, joined);
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

    private static String stringVal(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static double numberVal(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static Instant parseInstantSafe(String value) {
        try {
            return Instant.parse(value);
        } catch (Exception e) {
            return Instant.EPOCH;
        }
    }

    private static String fmt(double v) {
        return String.format(Locale.US, "%.6f", v);
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
        writeQueue.close();
        LOG.info("Mongo resumen final: md_events={} trades={} candles={} securities={} instrument_stats={} rankings={} instrument_daily={}",
                insertedMarketData.get(),
                insertedTrades.get(),
                upsertedCandles.get(),
                upsertedSecurities.get(),
                upsertedStats.get(),
                upsertedRankings.get(),
                upsertedDaily.get());
        client.close();
    }

    private static class TradeSummary {
        final String symbol;
        final String settlement;
        double first;
        double last;
        double high;
        double low;
        double volume;
        double amount;
        long count;
        Instant firstTs;
        Instant lastTs;

        TradeSummary(String symbol, String settlement) {
            this.symbol = symbol;
            this.settlement = settlement == null ? "" : settlement;
        }

        void update(Instant ts, double price, double qty, double amount) {
            if (firstTs == null || ts.isBefore(firstTs)) {
                firstTs = ts;
                first = price;
            }
            if (lastTs == null || ts.isAfter(lastTs)) {
                lastTs = ts;
                last = price;
            }
            if (high == 0 || price > high) {
                high = price;
            }
            if (low == 0 || price < low) {
                low = price;
            }
            volume += Math.max(0, qty);
            this.amount += Math.max(0, amount);
            count++;
        }

        double variationPct() {
            if (first <= 0 || last <= 0) {
                return 0.0;
            }
            return ((last - first) / first) * 100.0;
        }
    }
}
