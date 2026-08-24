package cl.vc.candle.websocket;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.eclipse.jetty.websocket.api.Session;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CandleMongoPublisher extends Thread {
    private static final Logger log = LoggerFactory.getLogger(CandleMongoPublisher.class);
    // El feed publica Long.MIN_VALUE/1000 cuando la bolsa no informa el campo (limites, referencia).
    private static final double ABSENT_NUMBER = 1e15;
    private static volatile CandleMongoPublisher INSTANCE;

    private final Properties properties;
    /**
     * _id de la ultima vela publicada por key. Object, no ObjectId: el _id de candles es el String
     * "instrumentId|timeframe|bucketStart" (ver MongoMarketRepository.upsertCandle). Tipar esto como
     * ObjectId reventaba con ClassCastException en el primer subscribe y mataba el hilo completo,
     * dejando ademas dailyCollection=null y load_instrument_daily inservible.
     */
    private final Map<CandleSubscriptionKey, Object> lastSeenByKey = new ConcurrentHashMap<>();
    private final Set<CandleSubscriptionKey> initialized = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile MongoCollection<Document> dailyCollection;
    private volatile MongoCollection<Document> closePriceCollection;
    private volatile MongoClient closePriceClient;

    public CandleMongoPublisher(Properties properties) {
        this.properties = properties;
        INSTANCE = this;
        setName("candle-mongo-publisher");
        setDaemon(true);
    }

    public static CandleMongoPublisher getInstance() {
        return INSTANCE;
    }

    @Override
    public void run() {
        String mongoUri = properties.getProperty("mongo.candle.uri", "mongodb://127.0.0.1:27017");
        String databaseName = properties.getProperty("mongo.candle.database", "market_data");
        String collectionName = properties.getProperty("mongo.candle.collection", "candles");
        String symbolField = properties.getProperty("mongo.candle.field.symbol", "symbol");
        String timeframeField = properties.getProperty("mongo.candle.field.timeframe", "timeframe");
        int pollMs = parseInt(properties.getProperty("mongo.candle.poll.ms"), 1000);
        int batchSize = parseInt(properties.getProperty("mongo.candle.batch.size"), 300);
        int bootstrapLimit = parseInt(properties.getProperty("mongo.candle.bootstrap.limit"), 200);
        log.info("Conectando a MongoDB uri={} db={} collection={}", mongoUri, databaseName, collectionName);

        try (MongoClient client = new MongoClient(new MongoClientURI(mongoUri))) {
            MongoDatabase database = client.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            dailyCollection = database.getCollection(properties.getProperty("mongo.daily.collection", "instrument_daily"));
            connectClosePriceHistory();
            log.info("Mongo publisher iniciado pollMs={} batchSize={} bootstrapLimit={}", pollMs, batchSize, bootstrapLimit);

            while (!Thread.currentThread().isInterrupted()) {
                for (CandleSubscriptionKey key : CandleSubscriptions.activeKeys()) {
                    processKey(collection, key, symbolField, timeframeField, batchSize, bootstrapLimit);
                }
                Thread.sleep(Math.max(250, pollMs));
            }
        } catch (InterruptedException e) {
            log.warn("Mongo publisher interrumpido");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error en mongo publisher", e);
        } finally {
            dailyCollection = null;
            closePriceCollection = null;
            if (closePriceClient != null) {
                try {
                    closePriceClient.close();
                } catch (Exception ignored) {
                    // no-op
                }
                closePriceClient = null;
            }
        }
    }

    private void connectClosePriceHistory() {
        String uri = properties.getProperty("mongo.close.uri", "").trim();
        if (uri.isEmpty()) {
            log.warn("Historico close_prices deshabilitado: falta mongo.close.uri");
            return;
        }
        MongoClient client = null;
        try {
            client = new MongoClient(new MongoClientURI(uri));
            String databaseName = properties.getProperty("mongo.close.database", "close_prices");
            client.getDatabase(databaseName).runCommand(new Document("ping", 1));
            closePriceClient = client;
            closePriceCollection = client.getDatabase(databaseName)
                    .getCollection(properties.getProperty("mongo.close.collection", "close_prices"));
            log.info("Historico close_prices disponible en modo lectura db={} collection={}",
                    databaseName, properties.getProperty("mongo.close.collection", "close_prices"));
        } catch (Exception e) {
            closePriceCollection = null;
            if (client != null) {
                try {
                    client.close();
                } catch (Exception ignored) {
                    // no-op
                }
            }
            log.warn("No se pudo habilitar historico close_prices: {}", e.toString());
        }
    }

    /**
     * Serie diaria historica tomada bajo demanda desde close_prices. No escribe, no crea indices y
     * conserva solo el documento mas reciente de cada dia por simbolo.
     */
    public boolean sendClosePriceHistory(Session session, String market, String symbol, int requestedLimit) {
        MongoCollection<Document> collection = closePriceCollection;
        if (collection == null || symbol == null || symbol.isBlank()) {
            return false;
        }

        int limit = Math.max(1, Math.min(requestedLimit, 500));
        Bson filter = Filters.and(
                Filters.eq("symbol", symbol),
                Filters.eq("securityExchange", market));
        FindIterable<Document> docs = collection.find(filter)
                .sort(Sorts.descending("time"))
                .limit(limit * 3);

        Map<String, JSONObject> latestByDay = new LinkedHashMap<>();
        for (Document doc : docs) {
            String time = doc.getString("time");
            if (time == null || time.length() < 10) {
                continue;
            }
            String day = time.substring(0, 10);
            if (latestByDay.containsKey(day)) {
                continue;
            }
            JSONObject row = closePriceRow(doc, symbol, day);
            if (row != null) {
                latestByDay.put(day, row);
                if (latestByDay.size() >= limit) {
                    break;
                }
            }
        }

        List<JSONObject> chronological = new ArrayList<>(latestByDay.values());
        Collections.reverse(chronological);
        JSONArray rows = new JSONArray();
        chronological.forEach(rows::put);

        JSONObject payload = new JSONObject()
                .put("type", "close_price_history")
                .put("market", market)
                .put("symbol", symbol)
                .put("count", rows.length())
                .put("rows", rows);
        log.info("close_prices historico market={} symbol={} rows={} limit={}", market, symbol, rows.length(), limit);
        return send(session, payload.toString());
    }

    static JSONObject closePriceRow(Document doc, String symbol, String day) {
        try {
            String protobufData = doc.getString("protobufData");
            if (protobufData == null || protobufData.isBlank()) {
                return null;
            }
            JSONObject statistic = new JSONObject(protobufData);
            JSONObject ohlcv = statistic.optJSONObject("ohlcv");
            double close = positiveNumber(ohlcv, "close");
            if (close <= 0d) close = statistic.optDouble("close", 0d);
            if (close <= 0d) close = statistic.optDouble("last", 0d);
            if (close <= 0d) {
                return null;
            }

            double open = positiveNumber(ohlcv, "open");
            double high = positiveNumber(ohlcv, "high");
            double low = positiveNumber(ohlcv, "low");
            double volume = ohlcv == null ? 0d : Math.max(0d, ohlcv.optDouble("volume", 0d));
            if (open <= 0d) open = close;
            if (high <= 0d) high = Math.max(open, close);
            if (low <= 0d) low = Math.min(open, close);

            return new JSONObject()
                    .put("symbol", symbol)
                    .put("date", day)
                    .put("open", open)
                    .put("high", Math.max(high, Math.max(open, close)))
                    .put("low", Math.min(low, Math.min(open, close)))
                    .put("close", close)
                    .put("volume", volume);
        } catch (Exception e) {
            return null;
        }
    }

    private static double positiveNumber(JSONObject source, String key) {
        return source == null ? 0d : source.optDouble(key, 0d);
    }

    /**
     * Estadistica oficial de la bolsa para un dia: todos los instrumentos ordenados por turnover desc.
     * Consulta on-demand (la coleccion cambia una vez al dia), sin polling.
     */
    public boolean sendInstrumentDailyByDay(Session session, String market, String tradingDay, int limit) {
        MongoCollection<Document> collection = dailyCollection;
        if (collection == null) {
            return false;
        }
        FindIterable<Document> docs = collection
                .find(Filters.and(Filters.eq("market", market), Filters.eq("tradingDay", tradingDay)))
                .sort(Sorts.descending("turnover"));
        if (limit > 0) {
            docs = docs.limit(limit);
        }
        JSONArray rows = new JSONArray();
        for (Document doc : docs) {
            rows.put(toInstrumentDailyJson(doc));
        }
        JSONObject payload = new JSONObject()
                .put("type", "instrument_daily")
                .put("mode", "day")
                .put("market", market)
                .put("date", tradingDay)
                .put("limit", limit)
                .put("count", rows.length())
                .put("rows", rows);
        log.info("instrument_daily dia market={} date={} rows={} limit={}", market, tradingDay, rows.length(), limit);
        return send(session, payload.toString());
    }

    /**
     * Serie historica oficial de un simbolo entre dos dias (ambos inclusive), ordenada por dia asc.
     */
    public boolean sendInstrumentDailyBySymbol(Session session, String market, String symbol, String fromDay, String toDay) {
        MongoCollection<Document> collection = dailyCollection;
        if (collection == null) {
            return false;
        }
        FindIterable<Document> docs = collection
                .find(Filters.and(
                        Filters.eq("market", market),
                        Filters.eq("symbol", symbol),
                        Filters.gte("tradingDay", fromDay),
                        Filters.lte("tradingDay", toDay)))
                .sort(Sorts.ascending("tradingDay"));
        JSONArray rows = new JSONArray();
        for (Document doc : docs) {
            rows.put(toInstrumentDailyJson(doc));
        }
        JSONObject payload = new JSONObject()
                .put("type", "instrument_daily")
                .put("mode", "history")
                .put("market", market)
                .put("symbol", symbol)
                .put("from", fromDay)
                .put("to", toDay)
                .put("count", rows.length())
                .put("rows", rows);
        log.info("instrument_daily historico market={} symbol={} from={} to={} rows={}", market, symbol, fromDay, toDay, rows.length());
        return send(session, payload.toString());
    }

    private JSONObject toInstrumentDailyJson(Document doc) {
        JSONObject o = new JSONObject();
        putText(o, "id", doc.get("_id"));
        putText(o, "market", doc.get("market"));
        putText(o, "symbol", doc.get("symbol"));
        putText(o, "securityId", doc.get("securityId"));
        putText(o, "currency", doc.get("currency"));
        putText(o, "tradingDay", doc.get("tradingDay"));
        putNumber(o, "open", doc.get("open"));
        putNumber(o, "high", doc.get("high"));
        putNumber(o, "low", doc.get("low"));
        putNumber(o, "last", doc.get("last"));
        putNumber(o, "close", doc.get("close"));
        putNumber(o, "previousClose", doc.get("previousClose"));
        putNumber(o, "volume", doc.get("volume"));
        putNumber(o, "turnover", doc.get("turnover"));
        putNumber(o, "vwap", doc.get("vwap"));
        putNumber(o, "variationPct", doc.get("variationPct"));
        putNumber(o, "auctionPrice", doc.get("auctionPrice"));
        putNumber(o, "lowLimit", doc.get("lowLimit"));
        putNumber(o, "highLimit", doc.get("highLimit"));
        putNumber(o, "referencePrice", doc.get("referencePrice"));
        Object updates = doc.get("updates");
        o.put("updates", updates instanceof Number n ? n.longValue() : 0L);
        putText(o, "updatedAt", doc.get("updatedAt"));
        return o;
    }

    private static void putText(JSONObject o, String key, Object value) {
        o.put(key, value == null ? JSONObject.NULL : String.valueOf(value));
    }

    private static void putNumber(JSONObject o, String key, Object value) {
        double v = value instanceof Number n ? n.doubleValue() : Double.NaN;
        boolean absent = Double.isNaN(v) || Double.isInfinite(v) || Math.abs(v) >= ABSENT_NUMBER;
        o.put(key, absent ? JSONObject.NULL : v);
    }

    private boolean send(Session session, String payload) {
        try {
            if (session != null && session.isOpen()) {
                session.getRemote().sendString(payload);
                return true;
            }
        } catch (Exception e) {
            log.warn("No se pudo enviar payload a session={}", session, e);
        }
        return false;
    }

    private void processKey(MongoCollection<Document> collection,
                            CandleSubscriptionKey key,
                            String symbolField,
                            String timeframeField,
                            int batchSize,
                            int bootstrapLimit) {
        if (!initialized.contains(key)) {
            log.info("Enviando bootstrap symbol={} timeframe={}", key.getSymbol(), key.getTimeframe());
            sendBootstrap(collection, key, symbolField, timeframeField, bootstrapLimit);
            initialized.add(key);
            return;
        }

        Bson baseFilter = Filters.and(
                Filters.eq(symbolField, key.getSymbol()),
                Filters.eq(timeframeField, key.getTimeframe())
        );

        Object lastSeen = lastSeenByKey.get(key);
        Bson filter = lastSeen == null ? baseFilter : Filters.and(baseFilter, Filters.gt("_id", lastSeen));

        FindIterable<Document> docs = collection.find(filter).sort(Sorts.ascending("_id")).limit(Math.max(1, batchSize));
        for (Document doc : docs) {
            Object id = doc.get("_id");
            if (id != null) {
                lastSeenByKey.put(key, id);
            }
            JSONObject payload = new JSONObject()
                    .put("type", "candle")
                    .put("symbol", key.getSymbol())
                    .put("timeframe", key.getTimeframe())
                    .put("candle", new JSONObject(doc.toJson()));
            broadcast(key, payload.toString());
        }
    }

    private void sendBootstrap(MongoCollection<Document> collection,
                               CandleSubscriptionKey key,
                               String symbolField,
                               String timeframeField,
                               int bootstrapLimit) {
        Bson filter = Filters.and(
                Filters.eq(symbolField, key.getSymbol()),
                Filters.eq(timeframeField, key.getTimeframe())
        );

        FindIterable<Document> docs = collection.find(filter).sort(Sorts.descending("_id")).limit(Math.max(1, bootstrapLimit));
        List<Document> rows = new ArrayList<>();
        for (Document doc : docs) {
            rows.add(doc);
        }
        Collections.reverse(rows);

        JSONArray candles = new JSONArray();
        Object max = null;
        for (Document row : rows) {
            candles.put(new JSONObject(row.toJson()));
            Object id = row.get("_id");
            if (id != null) {
                max = id;
            }
        }
        if (max != null) {
            lastSeenByKey.put(key, max);
        }

        JSONObject payload = new JSONObject()
                .put("type", "bootstrap")
                .put("symbol", key.getSymbol())
                .put("timeframe", key.getTimeframe())
                .put("candles", candles);
        broadcast(key, payload.toString());
    }

    private void broadcast(CandleSubscriptionKey key, String payload) {
        for (Session session : CandleSubscriptions.sessions(key)) {
            try {
                if (session != null && session.isOpen()) {
                    session.getRemote().sendString(payload);
                }
            } catch (Exception e) {
                log.warn("No se pudo enviar payload a session={} symbol={} timeframe={}",
                        session, key.getSymbol(), key.getTimeframe(), e);
            }
        }
    }

    private int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return fallback;
        }
    }
}
