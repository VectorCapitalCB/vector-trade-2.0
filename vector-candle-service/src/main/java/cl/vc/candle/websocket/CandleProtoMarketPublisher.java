package cl.vc.candle.websocket;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.ws.vectortrade.MessageUtilVT;
import com.google.protobuf.Message;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CandleProtoMarketPublisher extends Thread {
    private static final Logger log = LoggerFactory.getLogger(CandleProtoMarketPublisher.class);

    private final Properties properties;
    private final Set<Session> snapshotSent = ConcurrentHashMap.newKeySet();
    private final Set<Session> historySent = ConcurrentHashMap.newKeySet();
    private final Set<String> excludedSymbols;
    private volatile ObjectId lastTradeId;
    private volatile LocalDate lastDailyStatsLogDate;

    public CandleProtoMarketPublisher(Properties properties) {
        this.properties = properties;
        this.excludedSymbols = parseExcludedSymbols(properties.getProperty("mongo.market.exclude.symbols", "TEST-STGOX"));
        setName("candle-proto-market-publisher");
        setDaemon(true);
    }

    @Override
    public void run() {
        String mongoUri = properties.getProperty("mongo.candle.uri", "mongodb://127.0.0.1:27017");
        String databaseName = properties.getProperty("mongo.candle.database", "market_data");
        int pollMs = parseInt(properties.getProperty("mongo.market.poll.ms"), 2000);
        int bootstrapTrades = parseInt(properties.getProperty("mongo.market.bootstrap.trades"), 300);
        int bootstrapDays = parseInt(properties.getProperty("mongo.market.bootstrap.days"), 5);
        int bootstrapMaxTrades = parseInt(properties.getProperty("mongo.market.bootstrap.max.trades"), 300000);
        int tradeBatch = parseInt(properties.getProperty("mongo.market.trade.batch"), 300);
        int topN = parseInt(properties.getProperty("mongo.market.stats.topn"), 20);
        int historyDays = parseInt(properties.getProperty("mongo.market.history.days"), 30);
        String historyCollectionName = properties.getProperty("mongo.market.history.collection", "bolsa_stats_history");

        try (MongoClient client = new MongoClient(new MongoClientURI(mongoUri))) {
            MongoDatabase database = client.getDatabase(databaseName);
            MongoCollection<Document> trades = database.getCollection("trades");
            MongoCollection<Document> instrumentStats = database.getCollection("instrument_stats");
            MongoCollection<Document> bolsaHistory = database.getCollection(historyCollectionName);
            log.info("Proto market publisher iniciado db={} pollMs={} topN={}", databaseName, pollMs, topN);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    sendBootstrapTradesIfNeeded(trades, bootstrapTrades, bootstrapDays, bootstrapMaxTrades);
                    sendHistoricalStatsIfNeeded(bolsaHistory, historyDays);
                    publishIncrementalTrades(trades, tradeBatch);
                    publishBolsaStats(trades, instrumentStats, bolsaHistory, topN);
                } catch (Exception e) {
                    log.error("Error publicando mercado proto", e);
                }
                Thread.sleep(Math.max(500, pollMs));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error iniciando proto market publisher", e);
        }
    }

    private void sendBootstrapTradesIfNeeded(MongoCollection<Document> trades,
                                             int bootstrapTrades,
                                             int bootstrapDays,
                                             int bootstrapMaxTrades) {
        Set<Session> sessions = CandleSubscriptions.allSessions();
        if (sessions.isEmpty()) {
            snapshotSent.clear();
            historySent.clear();
            return;
        }

        List<Session> pending = sessions.stream().filter(s -> !snapshotSent.contains(s)).collect(Collectors.toList());
        if (pending.isEmpty()) {
            return;
        }

        List<MarketDataMessage.TradeGeneral> rows = new ArrayList<>();
        ObjectId max = null;
        ZoneId marketZone = ZoneId.of("America/Santiago");
        LocalDate latestDay = resolveLatestTradeDay(trades, marketZone);
        int safeDays = Math.max(1, bootstrapDays);
        int safeMaxTrades = Math.max(1, bootstrapMaxTrades);
        LocalDate fromDay = latestDay.minusDays(safeDays - 1L);

        for (Document doc : trades.find().sort(Sorts.descending("_id"))) {
            Instant t = parseInstantOrNull(getString(doc, "eventTime", null), marketZone);
            if (t != null) {
                LocalDate d = t.atZone(marketZone).toLocalDate();
                if (d.isBefore(fromDay)) {
                    break;
                }
            }
            if (isExcludedSymbol(getString(doc, "symbol", ""))) {
                continue;
            }
            rows.add(toTradeGeneral(doc));
            if (rows.size() >= safeMaxTrades) {
                break;
            }
            ObjectId id = doc.getObjectId("_id");
            if (id != null && (max == null || id.compareTo(max) > 0)) {
                max = id;
            }
        }

        if (rows.isEmpty()) {
            for (Document doc : trades.find().sort(Sorts.descending("_id")).limit(Math.max(1, bootstrapTrades))) {
                if (isExcludedSymbol(getString(doc, "symbol", ""))) {
                    continue;
                }
                rows.add(toTradeGeneral(doc));
                ObjectId id = doc.getObjectId("_id");
                if (id != null && (max == null || id.compareTo(max) > 0)) {
                    max = id;
                }
            }
        }

        log.info("Bootstrap trades enviados dias={} fromDay={} latestDay={} rows={}", safeDays, fromDay, latestDay, rows.size());
        rows.sort(Comparator.comparing(t -> t.hasT() ? t.getT().getSeconds() : 0L));
        if (!rows.isEmpty()) {
            MarketDataMessage.TradeGeneral firstRow = rows.get(0);
            MarketDataMessage.TradeGeneral lastRow = rows.get(rows.size() - 1);
            Instant firstTs = firstRow.hasT() ? Instant.ofEpochSecond(firstRow.getT().getSeconds(), firstRow.getT().getNanos()) : null;
            Instant lastTs = lastRow.hasT() ? Instant.ofEpochSecond(lastRow.getT().getSeconds(), lastRow.getT().getNanos()) : null;
            log.info("Bootstrap sample ts first={} last={} firstSymbol={} lastSymbol={}",
                    firstTs == null ? "-" : firstTs.atZone(marketZone).toLocalDateTime(),
                    lastTs == null ? "-" : lastTs.atZone(marketZone).toLocalDateTime(),
                    firstRow.getSymbol(),
                    lastRow.getSymbol());
        }
        if (max != null && (lastTradeId == null || max.compareTo(lastTradeId) > 0)) {
            lastTradeId = max;
        }

        MarketDataMessage.SnapshotTradeGeneral snapshot = MarketDataMessage.SnapshotTradeGeneral.newBuilder()
                .addAllTrades(rows)
                .build();
        byte[] payload = toPayload(snapshot);
        for (Session session : pending) {
            if (!sendBinary(session, payload)) {
                continue;
            }
            snapshotSent.add(session);
        }
    }

    private void sendHistoricalStatsIfNeeded(MongoCollection<Document> history, int historyDays) {
        Set<Session> sessions = CandleSubscriptions.allSessions();
        if (sessions.isEmpty()) {
            historySent.clear();
            return;
        }
        List<Session> pending = sessions.stream().filter(s -> !historySent.contains(s)).collect(Collectors.toList());
        if (pending.isEmpty()) {
            return;
        }

        Instant from = Instant.now().minus(historyDays, ChronoUnit.DAYS);
        FindIterable<Document> docs = history.find(Filters.gte("snapshotAt", from.toString()))
                .sort(Sorts.ascending("snapshotAt"));

        List<byte[]> payloads = new ArrayList<>();
        for (Document d : docs) {
            try {
                payloads.add(toPayload(historyDocToBolsaStats(d)));
            } catch (Exception e) {
                log.warn("No se pudo serializar BolsaStats historico _id={}", d.getObjectId("_id"), e);
            }
        }

        for (Session session : pending) {
            boolean ok = true;
            for (byte[] payload : payloads) {
                if (!sendBinary(session, payload)) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                historySent.add(session);
            }
        }
    }

    private void publishIncrementalTrades(MongoCollection<Document> trades, int tradeBatch) {
        FindIterable<Document> cursor = (lastTradeId == null)
                ? trades.find().sort(Sorts.ascending("_id")).limit(Math.max(1, tradeBatch))
                : trades.find(Filters.gt("_id", lastTradeId)).sort(Sorts.ascending("_id")).limit(Math.max(1, tradeBatch));

        for (Document doc : cursor) {
            if (isExcludedSymbol(getString(doc, "symbol", ""))) {
                continue;
            }
            ObjectId id = doc.getObjectId("_id");
            if (id != null) {
                lastTradeId = id;
            }
            MarketDataMessage.TradeGeneral trade = toTradeGeneral(doc);
            byte[] payload = toPayload(trade);
            broadcast(payload);
        }
    }

    private void publishBolsaStats(MongoCollection<Document> trades,
                                   MongoCollection<Document> instrumentStats,
                                   MongoCollection<Document> history,
                                   int topN) {
        ZoneId marketZone = ZoneId.of("America/Santiago");
        LocalDate marketDay = resolveLatestTradeDay(trades, marketZone);
        Map<String, TradeAgg> tradeAggByKey = loadTradeAggForDay(trades, marketDay, marketZone);
        Map<String, Document> instrumentStatsByKey = new LinkedHashMap<>();
        List<MarketDataMessage.RankinSymbol> rows = new ArrayList<>();
        double totalVol = 0.0;
        double totalMonto = 0.0;
        double capTotal = 0.0;
        double precioUltimoSum = 0.0;
        double precioMaxSum = 0.0;
        double varPos = 0.0;
        double varNeg = 0.0;
        double tendenciaSum = 0.0;
        long totalTrades = 0L;
        double liqWeighted = 0.0;
        double liqWeight = 0.0;

        for (Document doc : instrumentStats.find()) {
            if (isExcludedSymbol(getString(doc, "symbol", ""))) {
                continue;
            }
            MarketDataMessage.RankinSymbol rank = toRankin(doc);
            String key = buildKey(rank.getSymbol(), rank.getSecurityExchange(), rank.getSettlType());
            instrumentStatsByKey.put(key, doc);
        }

        // Main source for daily stats/rankings: trades of the resolved market day.
        for (TradeAgg agg : tradeAggByKey.values()) {
            MarketDataMessage.RankinSymbol rank = agg.toRank();
            Document statsDoc = instrumentStatsByKey.get(agg.key);
            if (statsDoc != null) {
                rank = enrichTradeRankWithInstrumentDoc(rank, statsDoc);
            }
            rows.add(rank);
        }

        // Fallback if no daily trades were found.
        if (rows.isEmpty()) {
            for (Document doc : instrumentStatsByKey.values()) {
                rows.add(toRankin(doc));
            }
        }

        rows = dedupeRankRows(rows);

        for (MarketDataMessage.RankinSymbol rank : rows) {

            double volumen = rank.getVolumen();
            double monto = rank.getMonto();
            double precio = rank.getPrecioUltimo();
            double precioMax = rank.getPrecioMaximo();
            double variacion = rank.getVariacionPct();
            double cap = precio * volumen;

            totalVol += volumen;
            totalMonto += monto;
            capTotal += cap;
            precioUltimoSum += precio;
            precioMaxSum += precioMax;
            tendenciaSum += variacion;
            if (variacion > 0) {
                varPos++;
            } else if (variacion < 0) {
                varNeg++;
            }

            if (monto > 0) {
                liqWeighted += (volumen / monto) * monto;
                liqWeight += monto;
            }
        }

        totalTrades = tradeAggByKey.values().stream().mapToLong(a -> a.count).sum();

        int assets = rows.size();
        double sentimientoPositivo = assets > 0 ? (varPos / assets) * 100.0 : 0.0;
        double sentimientoNegativo = assets > 0 ? (varNeg / assets) * 100.0 : 0.0;
        double capPromedio = assets > 0 ? capTotal / assets : 0.0;
        double precioPromAcum = assets > 0 ? precioUltimoSum / assets : 0.0;
        double precioMaxAcum = assets > 0 ? precioMaxSum : 0.0;
        double tendenciaProm = assets > 0 ? tendenciaSum / assets : 0.0;
        double rangoProm = assets > 0 ? rows.stream().mapToDouble(r -> r.getPrecioMaximo() - r.getPrecioMinimo()).average().orElse(0.0) : 0.0;
        double volProm = assets > 0 ? rows.stream().mapToDouble(r -> Math.abs(r.getVariacionPct())).average().orElse(0.0) / 100.0 : 0.0;
        double indiceProm = totalVol > 0 ? rows.stream().mapToDouble(r -> r.getPrecioUltimo() * r.getVolumen()).sum() / totalVol : 0.0;
        double indiceMax = totalVol > 0 ? rows.stream().mapToDouble(r -> r.getPrecioMaximo() * r.getVolumen()).sum() / totalVol : 0.0;
        double indiceMin = totalVol > 0 ? rows.stream().mapToDouble(r -> r.getPrecioMinimo() * r.getVolumen()).sum() / totalVol : 0.0;
        double liquidezMedia = liqWeight > 0 ? liqWeighted / liqWeight : (totalMonto > 0 ? totalVol / totalMonto : 0.0);

        String tendenciaGeneral = "neutral";
        if (sentimientoPositivo > 50.0) {
            tendenciaGeneral = "alcista";
        } else if (sentimientoNegativo > 50.0) {
            tendenciaGeneral = "bajista";
        }

        List<MarketDataMessage.RankinSymbol> masVolatil = rows.stream()
                .sorted(Comparator.comparingDouble((MarketDataMessage.RankinSymbol r) -> Math.abs(r.getVariacionPct())).reversed())
                .limit(topN)
                .collect(Collectors.toList());
        List<MarketDataMessage.RankinSymbol> masCayo = rows.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getVariacionPct))
                .limit(topN)
                .collect(Collectors.toList());
        List<MarketDataMessage.RankinSymbol> menosCayo = rows.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getVariacionPct).reversed())
                .limit(topN)
                .collect(Collectors.toList());
        List<MarketDataMessage.RankinSymbol> masTranzado = rows.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getMonto).reversed())
                .limit(topN)
                .collect(Collectors.toList());
        List<MarketDataMessage.RankinSymbol> menosTranzado = rows.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getMonto))
                .limit(topN)
                .collect(Collectors.toList());
        List<MarketDataMessage.RankinSymbol> bestRankin = rows.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getVariacionPct).reversed())
                .limit(topN)
                .collect(Collectors.toList());
        List<MarketDataMessage.RankinSymbol> worseRankin = rows.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getVariacionPct))
                .limit(topN)
                .collect(Collectors.toList());

        Instant firstTradeInstant = tradeAggByKey.values().stream()
                .map(a -> a.firstTs)
                .filter(java.util.Objects::nonNull)
                .min(Comparator.naturalOrder())
                .orElse(marketDay.atStartOfDay(marketZone).toInstant());
        Instant lastTradeInstant = tradeAggByKey.values().stream()
                .map(a -> a.lastTs)
                .filter(java.util.Objects::nonNull)
                .max(Comparator.naturalOrder())
                .orElse(Instant.now());

        MarketDataMessage.BolsaStats stats = MarketDataMessage.BolsaStats.newBuilder()
                .setId("candle-bolsa-stats")
                .setTotalVolumen(totalVol)
                .setMontoTotal(totalMonto)
                .setHoraInicio(firstTradeInstant.toString())
                .setHoraFin(lastTradeInstant.toString())
                .setVolatilidadPromedio(volProm)
                .setRangoPromedio(rangoProm)
                .setIndicePromedio(indiceProm)
                .setIndiceMaximo(indiceMax)
                .setIndiceMinimo(indiceMin)
                .setLiquidezMedia(liquidezMedia)
                .setNumeroTotalTrades(totalTrades)
                .setSentimientoPositivo(sentimientoPositivo)
                .setSentimientoNegativo(sentimientoNegativo)
                .setCapitalizacionTotal(capTotal)
                .setCapitalizacionPromedio(capPromedio)
                .setPrecioPromedioAcumulado(precioPromAcum)
                .setPrecioMaximoAcumulado(precioMaxAcum)
                .setTendenciaGeneral(tendenciaGeneral)
                .setTendenciaPromedio(tendenciaProm)
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .addAllMasVolatil(masVolatil)
                .addAllMasCayo(masCayo)
                .addAllMenosCayo(menosCayo)
                .addAllMasTranzado(masTranzado)
                .addAllBestRankin(bestRankin)
                .addAllWorseRankin(worseRankin)
                .build();

        maybeLogDailyStats(marketDay, stats, masTranzado, menosTranzado, masVolatil, bestRankin, worseRankin, tradeAggByKey.size(), rows.size());
        persistBolsaStatsHistory(history, stats, lastTradeInstant);
        broadcast(toPayload(stats));
    }

    private void persistBolsaStatsHistory(MongoCollection<Document> history, MarketDataMessage.BolsaStats stats, Instant referenceInstant) {
        Instant now = referenceInstant == null ? Instant.now() : referenceInstant;
        ZonedDateTime zdt = now.atZone(ZoneId.of("America/Santiago"));
        String dayKey = zdt.toLocalDate().toString();
        String hourKey = zdt.truncatedTo(ChronoUnit.HOURS).format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH"));
        String hourlySnapshotKey = "1h:" + hourKey;
        String dailySnapshotKey = "1d:" + dayKey;
        ReplaceOptions upsert = new ReplaceOptions().upsert(true);
        history.replaceOne(Filters.eq("snapshotKey", hourlySnapshotKey),
                toHistoryDocument(stats, now, "1h", hourlySnapshotKey),
                upsert);
        history.replaceOne(Filters.eq("snapshotKey", dailySnapshotKey),
                toHistoryDocument(stats, now, "1d", dailySnapshotKey),
                upsert);
    }

    private Document toHistoryDocument(MarketDataMessage.BolsaStats s, Instant snapshotAt, String timeframe, String snapshotKey) {
        Document d = new Document();
        d.put("snapshotAt", snapshotAt.toString());
        d.put("snapshotKey", snapshotKey);
        d.put("timeframe", timeframe);
        d.put("exchange", s.getSecurityExchange().name());
        d.put("id", s.getId());
        d.put("total_volumen", s.getTotalVolumen());
        d.put("monto_total", s.getMontoTotal());
        d.put("hora_inicio", s.getHoraInicio());
        d.put("hora_fin", s.getHoraFin());
        d.put("volatilidad_promedio", s.getVolatilidadPromedio());
        d.put("rango_promedio", s.getRangoPromedio());
        d.put("indice_promedio", s.getIndicePromedio());
        d.put("indice_maximo", s.getIndiceMaximo());
        d.put("indice_minimo", s.getIndiceMinimo());
        d.put("liquidez_media", s.getLiquidezMedia());
        d.put("numero_total_trades", s.getNumeroTotalTrades());
        d.put("sentimiento_positivo", s.getSentimientoPositivo());
        d.put("sentimiento_negativo", s.getSentimientoNegativo());
        d.put("capitalizacion_total", s.getCapitalizacionTotal());
        d.put("capitalizacion_promedio", s.getCapitalizacionPromedio());
        d.put("precio_promedio_acumulado", s.getPrecioPromedioAcumulado());
        d.put("precio_maximo_acumulado", s.getPrecioMaximoAcumulado());
        d.put("tendencia_general", s.getTendenciaGeneral());
        d.put("tendencia_promedio", s.getTendenciaPromedio());
        d.put("mas_volatil", toRankinDocs(s.getMasVolatilList()));
        d.put("mas_cayo", toRankinDocs(s.getMasCayoList()));
        d.put("menos_cayo", toRankinDocs(s.getMenosCayoList()));
        d.put("mas_tranzado", toRankinDocs(s.getMasTranzadoList()));
        d.put("best_rankin", toRankinDocs(s.getBestRankinList()));
        d.put("worse_rankin", toRankinDocs(s.getWorseRankinList()));
        return d;
    }

    private List<Document> toRankinDocs(List<MarketDataMessage.RankinSymbol> rows) {
        List<Document> out = new ArrayList<>();
        for (MarketDataMessage.RankinSymbol r : rows) {
            Document d = new Document();
            d.put("id", r.getId());
            d.put("exchange", r.getSecurityExchange().name());
            d.put("symbol", r.getSymbol());
            d.put("settl", r.getSettlType().name());
            d.put("securityType", r.getSecurityType().name());
            d.put("variacion_pct", r.getVariacionPct());
            d.put("precio_ultimo", r.getPrecioUltimo());
            d.put("precio_maximo", r.getPrecioMaximo());
            d.put("precio_minimo", r.getPrecioMinimo());
            d.put("precio_promedio", r.getPrecioPromedio());
            d.put("vwap", r.getVwap());
            d.put("twap", r.getTwap());
            d.put("volumen", r.getVolumen());
            d.put("monto", r.getMonto());
            d.put("rsi", r.getRsi());
            d.put("ma", r.getMa());
            d.put("macd", r.getMacd());
            d.put("liquid_ratio", r.getLiquidRatio());
            d.put("implied_volatility", r.getImpliedVolatility());
            out.add(d);
        }
        return out;
    }

    private MarketDataMessage.BolsaStats historyDocToBolsaStats(Document d) {
        String snapshotAt = getString(d, "snapshotAt", Instant.now().toString());
        String timeframe = getString(d, "timeframe", "1d");
        MarketDataMessage.BolsaStats.Builder b = MarketDataMessage.BolsaStats.newBuilder()
                .setId("hist:" + timeframe + ":" + snapshotAt)
                .setTotalVolumen(getDouble(d, "total_volumen"))
                .setMontoTotal(getDouble(d, "monto_total"))
                .setHoraInicio(getString(d, "hora_inicio", ""))
                .setHoraFin(getString(d, "hora_fin", ""))
                .setVolatilidadPromedio(getDouble(d, "volatilidad_promedio"))
                .setRangoPromedio(getDouble(d, "rango_promedio"))
                .setIndicePromedio(getDouble(d, "indice_promedio"))
                .setIndiceMaximo(getDouble(d, "indice_maximo"))
                .setIndiceMinimo(getDouble(d, "indice_minimo"))
                .setLiquidezMedia(getDouble(d, "liquidez_media"))
                .setNumeroTotalTrades(getLong(d, "numero_total_trades"))
                .setSentimientoPositivo(getDouble(d, "sentimiento_positivo"))
                .setSentimientoNegativo(getDouble(d, "sentimiento_negativo"))
                .setCapitalizacionTotal(getDouble(d, "capitalizacion_total"))
                .setCapitalizacionPromedio(getDouble(d, "capitalizacion_promedio"))
                .setPrecioPromedioAcumulado(getDouble(d, "precio_promedio_acumulado"))
                .setPrecioMaximoAcumulado(getDouble(d, "precio_maximo_acumulado"))
                .setTendenciaGeneral(getString(d, "tendencia_general", "neutral"))
                .setTendenciaPromedio(getDouble(d, "tendencia_promedio"))
                .setSecurityExchange(parseSecurityExchange(getString(d, "exchange", "BCS")));

        b.addAllMasVolatil(fromRankinDocs(d.getList("mas_volatil", Document.class)));
        b.addAllMasCayo(fromRankinDocs(d.getList("mas_cayo", Document.class)));
        b.addAllMenosCayo(fromRankinDocs(d.getList("menos_cayo", Document.class)));
        b.addAllMasTranzado(fromRankinDocs(d.getList("mas_tranzado", Document.class)));
        b.addAllBestRankin(fromRankinDocs(d.getList("best_rankin", Document.class)));
        b.addAllWorseRankin(fromRankinDocs(d.getList("worse_rankin", Document.class)));
        return b.build();
    }

    private List<MarketDataMessage.RankinSymbol> fromRankinDocs(List<Document> docs) {
        List<MarketDataMessage.RankinSymbol> out = new ArrayList<>();
        if (docs == null) {
            return out;
        }
        for (Document d : docs) {
            MarketDataMessage.RankinSymbol.Builder b = MarketDataMessage.RankinSymbol.newBuilder()
                    .setId(getString(d, "id", ""))
                    .setSecurityExchange(parseSecurityExchange(getString(d, "exchange", "BCS")))
                    .setSymbol(getString(d, "symbol", ""))
                    .setSettlType(parseSettlType(getString(d, "settl", "T2")))
                    .setSecurityType(parseSecurityType(getString(d, "securityType", "CS")))
                    .setVariacionPct(getDouble(d, "variacion_pct"))
                    .setPrecioUltimo(getDouble(d, "precio_ultimo"))
                    .setPrecioMaximo(getDouble(d, "precio_maximo"))
                    .setPrecioMinimo(getDouble(d, "precio_minimo"))
                    .setPrecioPromedio(getDouble(d, "precio_promedio"))
                    .setVwap(getDouble(d, "vwap"))
                    .setTwap(getDouble(d, "twap"))
                    .setVolumen(getDouble(d, "volumen"))
                    .setMonto(getDouble(d, "monto"))
                    .setRsi(getDouble(d, "rsi"))
                    .setMa(getDouble(d, "ma"))
                    .setMacd(getDouble(d, "macd"))
                    .setLiquidRatio(getDouble(d, "liquid_ratio"))
                    .setImpliedVolatility(getDouble(d, "implied_volatility"));
            out.add(b.build());
        }
        return out;
    }

    private void maybeLogDailyStats(LocalDate marketDay,
                                    MarketDataMessage.BolsaStats stats,
                                    List<MarketDataMessage.RankinSymbol> masTranzado,
                                    List<MarketDataMessage.RankinSymbol> menosTranzado,
                                    List<MarketDataMessage.RankinSymbol> masVolatil,
                                    List<MarketDataMessage.RankinSymbol> topGainers,
                                    List<MarketDataMessage.RankinSymbol> topLosers,
                                    int tradeAggCount,
                                    int rankRowsCount) {
        if (marketDay.equals(lastDailyStatsLogDate)) {
            return;
        }
        lastDailyStatsLogDate = marketDay;

        log.info("[BOLSA][DIARIO][{}][DB=MONGO][TF=1D][SYNC] totalVolumen={} montoTotal={} capitalizacionTotal={} numeroTrades={} volatilidadPromedio={} rangoPromedio={} indicePromedio={} indiceMaximo={} indiceMinimo={} sentimientoPositivo={} sentimientoNegativo={} tendenciaGeneral={} tendenciaPromedio={} horaInicio={} horaFin={} symbolsAggTrades={} symbolsRanking={}",
                marketDay,
                round(stats.getTotalVolumen()),
                round(stats.getMontoTotal()),
                round(stats.getCapitalizacionTotal()),
                stats.getNumeroTotalTrades(),
                round(stats.getVolatilidadPromedio()),
                round(stats.getRangoPromedio()),
                round(stats.getIndicePromedio()),
                round(stats.getIndiceMaximo()),
                round(stats.getIndiceMinimo()),
                round(stats.getSentimientoPositivo()),
                round(stats.getSentimientoNegativo()),
                stats.getTendenciaGeneral(),
                round(stats.getTendenciaPromedio()),
                stats.getHoraInicio(),
                stats.getHoraFin(),
                tradeAggCount,
                rankRowsCount
        );

        logRank("TOP_10_MAS_TRANZADO", masTranzado);
        logRank("TOP_10_MENOS_TRANZADO", menosTranzado);
        logRank("TOP_10_MAS_VOLATIL", masVolatil);
        logRank("TOP_10_MEJORES", topGainers);
        logRank("TOP_10_PEORES", topLosers);
    }

    private void logRank(String title, List<MarketDataMessage.RankinSymbol> list) {
        String rows = list.stream()
                .limit(10)
                .map(r -> String.format("%s(vol=%s,monto=%s,varPct=%s,last=%s,settl=%s)",
                        r.getSymbol(),
                        round(r.getVolumen()),
                        round(r.getMonto()),
                        round(r.getVariacionPct()),
                        round(r.getPrecioUltimo()),
                        r.getSettlType().name()))
                .collect(Collectors.joining(" | "));
        log.info("[BOLSA][DIARIO][{}] {}", title, rows);
    }

    private MarketDataMessage.RankinSymbol toRankin(Document doc) {
        String symbol = getString(doc, "symbol", "");
        RoutingMessage.SettlType settlType = parseSettlType(getString(doc, "settlement", "T2"));
        RoutingMessage.SecurityType securityType = parseSecurityType(getString(doc, "securityType", "CS"));
        MarketDataMessage.SecurityExchangeMarketData securityExchange = parseSecurityExchange(getString(doc, "destination", "BCS"));

        double last = getDouble(doc, "lastPrice");
        double bid = getDouble(doc, "bestBid");
        double ask = getDouble(doc, "bestAsk");
        double dayLow = getDoubleMany(doc, "dayLow", "low", "minPrice");
        double dayHigh = getDoubleMany(doc, "dayHigh", "high", "maxPrice");
        double min = minPositive(dayLow, bid, last, ask);
        double max = Math.max(dayHigh, Math.max(last, Math.max(bid, ask)));
        double volume = getDouble(doc, "totalVolume");
        double monto = getDouble(doc, "totalTurnover");
        double varPct = getDoubleMany(doc, "variationPct", "variacionPct", "changePct", "pctChange", "priceChangePercent", "dailyChangePct");
        if (Math.abs(varPct) < 0.000001d) {
            double prevClose = getDoubleMany(doc, "previousClose", "prevClose", "closePrevious", "priorClose", "referencePrice");
            if (prevClose > 0 && last > 0) {
                varPct = ((last - prevClose) / prevClose) * 100.0d;
            }
        }
        double vwap = getDouble(doc, "vwapIntraday");
        double ma = getDouble(doc, "sma20");
        double rsi = getDouble(doc, "rsi14");
        double macd = getDouble(doc, "macdLine");

        MarketDataMessage.RankinSymbol.Builder b = MarketDataMessage.RankinSymbol.newBuilder()
                .setId(symbol + securityExchange.name() + settlType.name() + securityType.name())
                .setSecurityExchange(securityExchange)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityType(securityType)
                .setVariacionPct(varPct)
                .setPrecioUltimo(last)
                .setPrecioMaximo(max)
                .setPrecioMinimo(min)
                .setPrecioPromedio(last)
                .setVwap(vwap)
                .setTwap(ma)
                .setVolumen(volume)
                .setMonto(monto)
                .setRsi(rsi)
                .setMa(ma)
                .setMacd(macd)
                .setLiquidRatio(monto > 0 ? (volume / monto) : 0.0)
                .setImpliedVolatility(Math.abs(varPct));
        return b.build();
    }

    private List<MarketDataMessage.RankinSymbol> dedupeRankRows(List<MarketDataMessage.RankinSymbol> rows) {
        LinkedHashMap<String, MarketDataMessage.RankinSymbol> map = new LinkedHashMap<>();
        for (MarketDataMessage.RankinSymbol r : rows) {
            String key = (r.getSymbol() + "|" + r.getSecurityExchange().name() + "|" + r.getSettlType().name()).toUpperCase();
            MarketDataMessage.RankinSymbol prev = map.get(key);
            if (prev == null) {
                map.put(key, r);
                continue;
            }
            boolean replace = r.getMonto() > prev.getMonto()
                    || r.getVolumen() > prev.getVolumen()
                    || Math.abs(r.getVariacionPct()) > Math.abs(prev.getVariacionPct());
            if (replace) {
                map.put(key, r);
            }
        }
        return new ArrayList<>(map.values());
    }

    private String buildKey(String symbol, MarketDataMessage.SecurityExchangeMarketData ex, RoutingMessage.SettlType settl) {
        String s = symbol == null ? "" : symbol.trim().toUpperCase();
        return s + "|" + ex.name() + "|" + settl.name();
    }

    private MarketDataMessage.RankinSymbol mergeRankWithTradeAgg(MarketDataMessage.RankinSymbol rank, TradeAgg agg) {
        MarketDataMessage.RankinSymbol.Builder b = rank.toBuilder();
        if (rank.getVolumen() <= 0 && agg.volume > 0) {
            b.setVolumen(agg.volume);
        }
        if (rank.getMonto() <= 0 && agg.amount > 0) {
            b.setMonto(agg.amount);
        }
        if (rank.getPrecioUltimo() <= 0 && agg.last > 0) {
            b.setPrecioUltimo(agg.last);
            b.setPrecioPromedio(agg.last);
        }
        if (rank.getPrecioMaximo() <= 0 || rank.getPrecioMaximo() == rank.getPrecioMinimo()) {
            if (agg.high > 0) b.setPrecioMaximo(agg.high);
            if (agg.low > 0) b.setPrecioMinimo(agg.low);
        }
        if (Math.abs(rank.getVariacionPct()) < 0.000001d && agg.first > 0 && agg.last > 0) {
            b.setVariacionPct(((agg.last - agg.first) / agg.first) * 100.0d);
        }
        return b.build();
    }

    private MarketDataMessage.RankinSymbol enrichTradeRankWithInstrumentDoc(MarketDataMessage.RankinSymbol tradeRank, Document doc) {
        if (doc == null) {
            return tradeRank;
        }
        MarketDataMessage.RankinSymbol.Builder b = tradeRank.toBuilder();
        b.setRsi(getDouble(doc, "rsi14"));
        b.setMa(getDouble(doc, "sma20"));
        b.setMacd(getDouble(doc, "macdLine"));
        b.setVwap(getDouble(doc, "vwapIntraday"));
        b.setTwap(getDouble(doc, "ema20"));
        // Keep liquid ratio tied to daily traded values for strict injector/candle alignment.
        b.setImpliedVolatility(Math.abs(tradeRank.getVariacionPct()));
        return b.build();
    }

    private LocalDate resolveLatestTradeDay(MongoCollection<Document> trades, ZoneId zone) {
        // eventTime can come from different historical injector formats; use recent inserts and parse robustly.
        FindIterable<Document> latestCursor = trades.find().sort(Sorts.descending("_id")).limit(500);
        for (Document doc : latestCursor) {
            String symbol = getString(doc, "symbol", "");
            if (isExcludedSymbol(symbol)) {
                continue;
            }
            String eventTimeRaw = getString(doc, "eventTime", null);
            Instant parsed = parseInstantOrNull(eventTimeRaw, zone);
            if (parsed != null) {
                return parsed.atZone(zone).toLocalDate();
            }
        }
        return LocalDate.now(zone);
    }

    private Map<String, TradeAgg> loadTradeAggForDay(MongoCollection<Document> trades, LocalDate day, ZoneId zone) {
        Map<String, TradeAgg> out = new LinkedHashMap<>();
        String dayPrefix = day.toString();
        // Day-prefix filter is robust for ISO eventTime with/without timezone suffix.
        FindIterable<Document> cursor = trades.find(Filters.regex("eventTime", "^" + dayPrefix))
                .sort(Sorts.ascending("eventTime"));

        for (Document doc : cursor) {
            String symbol = getString(doc, "symbol", "");
            if (isExcludedSymbol(symbol)) {
                continue;
            }
            MarketDataMessage.SecurityExchangeMarketData ex = parseSecurityExchange(getString(doc, "destination", "BCS"));
            RoutingMessage.SettlType settl = parseSettlType(getString(doc, "settlement", "T2"));
            RoutingMessage.SecurityType secType = parseSecurityType(getString(doc, "securityType", "CS"));
            String key = buildKey(symbol, ex, settl);
            TradeAgg agg = out.computeIfAbsent(key, k -> new TradeAgg(key, symbol, ex, settl, secType));

            Instant t = parseInstant(getString(doc, "eventTime", null), zone);
            double price = getDouble(doc, "price");
            double qty = getDouble(doc, "quantity");
            double amount = getDouble(doc, "amount");
            if (amount <= 0 && price > 0 && qty > 0) {
                amount = price * qty;
            }
            agg.update(t, price, qty, amount);
        }
        return out;
    }

    private MarketDataMessage.TradeGeneral toTradeGeneral(Document doc) {
        String symbol = getString(doc, "symbol", "");
        RoutingMessage.SettlType settlType = parseSettlType(getString(doc, "settlement", "T2"));
        RoutingMessage.SecurityType securityType = parseSecurityType(getString(doc, "securityType", "CS"));
        MarketDataMessage.SecurityExchangeMarketData securityExchange = parseSecurityExchange(getString(doc, "destination", "BCS"));

        String id = getString(doc, "mdEntryId", "");
        if (id.isBlank() && doc.getObjectId("_id") != null) {
            id = doc.getObjectId("_id").toHexString();
        }
        Instant t = parseInstant(getString(doc, "eventTime", null), ZoneId.of("America/Santiago"));
        String side = getString(doc, "aggressorSide", "");

        return MarketDataMessage.TradeGeneral.newBuilder()
                .setId(id)
                .setIdGenerico(id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityType(securityType)
                .setSecurityExchange(securityExchange)
                .setPrice(getDouble(doc, "price"))
                .setQty(getDouble(doc, "quantity"))
                .setAmount(getDouble(doc, "amount"))
                .setBuyer("B".equalsIgnoreCase(side) ? "BUY" : "")
                .setSeller("S".equalsIgnoreCase(side) ? "SELL" : "")
                .setT(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(t.getEpochSecond())
                        .setNanos(t.getNano())
                        .build())
                .build();
    }

    private void broadcast(byte[] payload) {
        for (Session session : CandleSubscriptions.allSessions()) {
            sendBinary(session, payload);
        }
    }

    private boolean sendBinary(Session session, byte[] payload) {
        try {
            if (session != null && session.isOpen()) {
                session.getRemote().sendBytes(ByteBuffer.wrap(payload));
                return true;
            }
        } catch (Exception e) {
            log.debug("No se pudo enviar payload proto a session {}", session, e);
        }
        return false;
    }

    private byte[] toPayload(Message message) {
        ByteBuffer bb = MessageUtilVT.serializeMessageByteBuffer(message);
        if (bb == null) {
            return new byte[0];
        }
        byte[] out = new byte[bb.remaining()];
        bb.get(out);
        return out;
    }

    private static String getString(Document doc, String key, String fallback) {
        Object v = doc.get(key);
        return v == null ? fallback : String.valueOf(v);
    }

    private static double getDouble(Document doc, String key) {
        Object v = doc.get(key);
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        if (v == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(String.valueOf(v));
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static double getDoubleMany(Document doc, String... keys) {
        for (String key : keys) {
            double v = getDouble(doc, key);
            if (Math.abs(v) > 0.0000001d) {
                return v;
            }
        }
        return 0.0;
    }

    private static long getLong(Document doc, String key) {
        Object v = doc.get(key);
        if (v instanceof Number n) {
            return n.longValue();
        }
        if (v == null) {
            return 0L;
        }
        try {
            return Long.parseLong(String.valueOf(v));
        } catch (Exception e) {
            return 0L;
        }
    }

    private static Instant parseInstant(String value, ZoneId fallbackZone) {
        Instant parsed = parseInstantOrNull(value, fallbackZone);
        return parsed == null ? Instant.now() : parsed;
    }

    private static Instant parseInstantOrNull(String value, ZoneId fallbackZone) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String normalized = value.trim();
        try {
            return Instant.parse(normalized);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return ZonedDateTime.parse(normalized).toInstant();
        } catch (DateTimeParseException ignored) {
        }
        if (normalized.contains(" ") && !normalized.contains("T")) {
            normalized = normalized.replace(' ', 'T');
        }
        try {
            return java.time.LocalDateTime.parse(normalized).atZone(fallbackZone).toInstant();
        } catch (DateTimeParseException ignored) {
        }
        return null;
    }

    private static RoutingMessage.SettlType parseSettlType(String value) {
        try {
            return RoutingMessage.SettlType.valueOf(value);
        } catch (Exception e) {
            return RoutingMessage.SettlType.T2;
        }
    }

    private static RoutingMessage.SecurityType parseSecurityType(String value) {
        try {
            return RoutingMessage.SecurityType.valueOf(value);
        } catch (Exception e) {
            return RoutingMessage.SecurityType.CS;
        }
    }

    private static MarketDataMessage.SecurityExchangeMarketData parseSecurityExchange(String value) {
        try {
            return MarketDataMessage.SecurityExchangeMarketData.valueOf(value);
        } catch (Exception e) {
            return MarketDataMessage.SecurityExchangeMarketData.BCS;
        }
    }

    private static double minPositive(double... values) {
        double min = Double.MAX_VALUE;
        for (double v : values) {
            if (v > 0 && v < min) {
                min = v;
            }
        }
        return min == Double.MAX_VALUE ? 0.0 : min;
    }

    private static int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return fallback;
        }
    }

    private static String round(double value) {
        return String.format(java.util.Locale.US, "%.6f", value);
    }

    private boolean isExcludedSymbol(String symbol) {
        if (symbol == null) {
            return false;
        }
        return excludedSymbols.contains(symbol.trim().toUpperCase());
    }

    private static Set<String> parseExcludedSymbols(String csv) {
        Set<String> out = new HashSet<>();
        if (csv == null || csv.isBlank()) {
            return out;
        }
        Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(String::toUpperCase)
                .forEach(out::add);
        return out;
    }

    private static class TradeAgg {
        final String key;
        final String symbol;
        final MarketDataMessage.SecurityExchangeMarketData ex;
        final RoutingMessage.SettlType settl;
        final RoutingMessage.SecurityType secType;
        double first;
        double last;
        double high;
        double low;
        double volume;
        double amount;
        long count;
        Instant firstTs;
        Instant lastTs;

        TradeAgg(String key, String symbol, MarketDataMessage.SecurityExchangeMarketData ex, RoutingMessage.SettlType settl, RoutingMessage.SecurityType secType) {
            this.key = key;
            this.symbol = symbol;
            this.ex = ex;
            this.settl = settl;
            this.secType = secType;
        }

        void update(Instant t, double price, double qty, double amt) {
            if (price <= 0) {
                return;
            }
            if (firstTs == null || (t != null && t.isBefore(firstTs))) {
                firstTs = t;
                first = price;
            }
            if (lastTs == null || (t != null && t.isAfter(lastTs))) {
                lastTs = t;
                last = price;
            }
            if (high == 0 || price > high) {
                high = price;
            }
            if (low == 0 || price < low) {
                low = price;
            }
            volume += Math.max(0, qty);
            amount += Math.max(0, amt);
            count++;
        }

        MarketDataMessage.RankinSymbol toRank() {
            double prev = first > 0 ? first : last;
            double varPct = (prev > 0 && last > 0) ? ((last - prev) / prev) * 100.0d : 0.0d;
            return MarketDataMessage.RankinSymbol.newBuilder()
                    .setId(symbol + ex.name() + settl.name() + secType.name())
                    .setSecurityExchange(ex)
                    .setSymbol(symbol)
                    .setSettlType(settl)
                    .setSecurityType(secType)
                    .setVariacionPct(varPct)
                    .setPrecioUltimo(last)
                    .setPrecioMaximo(high)
                    .setPrecioMinimo(low > 0 ? low : last)
                    .setPrecioPromedio(last)
                    .setVwap(0.0)
                    .setTwap(0.0)
                    .setVolumen(volume)
                    .setMonto(amount)
                    .setRsi(0.0)
                    .setMa(0.0)
                    .setMacd(0.0)
                    .setLiquidRatio(amount > 0 ? (volume / amount) : 0.0)
                    .setImpliedVolatility(Math.abs(varPct))
                    .build();
        }
    }
}
