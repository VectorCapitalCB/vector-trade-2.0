package cl.vc.news.scraper;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class NewsMongoRepository {
    private static final Logger log = LoggerFactory.getLogger(NewsMongoRepository.class);
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("America/Santiago"));

    private static MongoCollection<Document> collection;
    private static int bootstrapLimit = 300;

    public static synchronized void init(Properties properties) {
        if (collection != null) {
            return;
        }

        String uri = properties.getProperty("mongo.news.uri", "mongodb://127.0.0.1:27017");
        String dbName = properties.getProperty("mongo.news.database", "vector_trade");
        String collectionName = properties.getProperty("mongo.news.collection", "vector_trade_news");
        bootstrapLimit = parseInt(properties.getProperty("mongo.news.bootstrap.limit"), 300);

        MongoClient client = new MongoClient(new MongoClientURI(uri));
        MongoDatabase database = client.getDatabase(dbName);
        ensureCollection(database, collectionName);
        collection = database.getCollection(collectionName);

        collection.createIndex(Indexes.ascending("hash"),
                new IndexOptions().name("idx_hash_unique").background(true).unique(true));
        collection.createIndex(Indexes.descending("publishedAt"),
                new IndexOptions().name("idx_published_at").background(true));
        collection.createIndex(Indexes.descending("scrapedAt"),
                new IndexOptions().name("idx_scraped_at").background(true));

        log.info("Mongo news initialized db={} collection={}", dbName, collectionName);
    }

    public static boolean saveIfNew(NewsRow row) {
        if (collection == null || row == null || isBlank(row.hash) || isBlank(row.message)) {
            return false;
        }

        long now = System.currentTimeMillis();
        Document setOnInsert = new Document("hash", row.hash)
                .append("source", safe(row.source))
                .append("title", safe(row.title))
                .append("url", safe(row.url))
                .append("message", row.message)
                .append("impact", safe(row.impact))
                .append("type", safe(row.type))
                .append("publishedAt", row.publishedAt <= 0 ? now : row.publishedAt)
                .append("scrapedAt", row.scrapedAt <= 0 ? now : row.scrapedAt)
                .append("createdAt", now);
        Document set = new Document("lastSeenAt", now);

        UpdateResult result = collection.updateOne(
                Filters.eq("hash", row.hash),
                new Document("$setOnInsert", setOnInsert).append("$set", set),
                new UpdateOptions().upsert(true)
        );
        return result.getUpsertedId() != null;
    }

    public static List<NewsRow> lastRows(Integer limit) {
        if (collection == null) {
            return Collections.emptyList();
        }
        int safeLimit = Math.max(1, Math.min(limit == null ? bootstrapLimit : limit, 1000));
        FindIterable<Document> cursor = collection.find()
                .sort(Sorts.descending("publishedAt", "_id"))
                .limit(safeLimit);

        List<NewsRow> rows = new ArrayList<>();
        for (Document d : cursor) {
            String message = d.getString("message");
            if (!isBlank(message)) {
                NewsRow row = new NewsRow();
                row.message = message;
                row.url = d.getString("url");
                row.title = d.getString("title");
                row.source = d.getString("source");
                row.impact = d.getString("impact");
                row.type = d.getString("type");
                Object p = d.get("publishedAt");
                row.publishedAt = p instanceof Number ? ((Number) p).longValue() : 0L;
                rows.add(row);
            }
        }
        Collections.reverse(rows);
        return rows;
    }

    public static String composeMessage(String source, String title, long publishedAtMillis) {
        String ts = TS_FMT.format(Instant.ofEpochMilli(Math.max(0, publishedAtMillis)));
        String src = isBlank(source) ? "source" : source.trim();
        String ttl = isBlank(title) ? "(sin titulo)" : title.trim();
        return "[" + ts + "] " + src + " | " + ttl;
    }

    private static String safe(String value) {
        return value == null ? "" : value.trim();
    }

    private static int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return fallback;
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void ensureCollection(MongoDatabase database, String collectionName) {
        boolean exists = false;
        for (String name : database.listCollectionNames()) {
            if (collectionName.equals(name)) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            database.createCollection(collectionName);
            log.info("Mongo collection created: {}.{}", database.getName(), collectionName);
        }
    }

    public static class NewsRow {
        public String source;
        public String title;
        public String url;
        public String hash;
        public String message;
        public String impact;
        public String type;
        public long publishedAt;
        public long scrapedAt;
    }
}
