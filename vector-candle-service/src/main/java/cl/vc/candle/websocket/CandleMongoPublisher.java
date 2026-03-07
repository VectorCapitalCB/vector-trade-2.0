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
import org.bson.types.ObjectId;
import org.eclipse.jetty.websocket.api.Session;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CandleMongoPublisher extends Thread {
    private static final Logger log = LoggerFactory.getLogger(CandleMongoPublisher.class);

    private final Properties properties;
    private final Map<CandleSubscriptionKey, ObjectId> lastSeenByKey = new ConcurrentHashMap<>();
    private final Set<CandleSubscriptionKey> initialized = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public CandleMongoPublisher(Properties properties) {
        this.properties = properties;
        setName("candle-mongo-publisher");
        setDaemon(true);
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
        }
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

        ObjectId lastSeen = lastSeenByKey.get(key);
        Bson filter = lastSeen == null ? baseFilter : Filters.and(baseFilter, Filters.gt("_id", lastSeen));

        FindIterable<Document> docs = collection.find(filter).sort(Sorts.ascending("_id")).limit(Math.max(1, batchSize));
        for (Document doc : docs) {
            ObjectId id = doc.getObjectId("_id");
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
        ObjectId max = null;
        for (Document row : rows) {
            candles.put(new JSONObject(row.toJson()));
            ObjectId id = row.getObjectId("_id");
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
