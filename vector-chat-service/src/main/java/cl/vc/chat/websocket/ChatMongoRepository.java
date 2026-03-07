package cl.vc.chat.websocket;

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
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ChatMongoRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChatMongoRepository.class);
    private static MongoCollection<Document> collection;
    private static MongoCollection<Document> usersCollection;
    private static int historyLimit = 500;

    public static synchronized void init(Properties properties) {
        if (collection != null && usersCollection != null) {
            return;
        }

        String uri = properties.getProperty("mongo.chat.uri", "mongodb://127.0.0.1:27017");
        String dbName = properties.getProperty("mongo.chat.database", "vector_trade");
        String collectionName = properties.getProperty("mongo.chat.collection", "chat-service-vt");
        String usersCollectionName = properties.getProperty("mongo.chat.users.collection", "chat_users");
        historyLimit = parseInt(properties.getProperty("mongo.chat.history.limit", "500"), 500);

        MongoClient client = new MongoClient(new MongoClientURI(uri));
        MongoDatabase database = client.getDatabase(dbName);

        ensureCollection(database, collectionName);
        ensureCollection(database, usersCollectionName);

        collection = database.getCollection(collectionName);
        usersCollection = database.getCollection(usersCollectionName);

        collection.createIndex(Indexes.ascending("conversationId", "timestamp"),
                new IndexOptions().name("idx_conversation_timestamp").background(true));
        collection.createIndex(Indexes.ascending("timestamp"),
                new IndexOptions().name("idx_timestamp").background(true));

        usersCollection.createIndex(Indexes.ascending("usernameNorm"),
                new IndexOptions().name("idx_username_norm_unique").background(true).unique(true));
        usersCollection.createIndex(Indexes.descending("lastSeenAt"),
                new IndexOptions().name("idx_last_seen").background(true));

        LOGGER.info("Mongo chat initialized db={} messagesCollection={} usersCollection={}", dbName, collectionName, usersCollectionName);
    }

    public static Document save(String fromUsername, String toUsername, String message) {
        String conversationId = conversationId(fromUsername, toUsername);
        Document row = new Document("conversationId", conversationId)
                .append("fromUsername", fromUsername)
                .append("toUsername", toUsername)
                .append("fromUsernameNorm", normalize(fromUsername))
                .append("toUsernameNorm", normalize(toUsername))
                .append("message", message)
                .append("timestamp", System.currentTimeMillis());
        collection.insertOne(row);
        return row;
    }

    public static List<Document> history(String username, String withUsername, int limit) {
        int safeLimit = Math.max(1, Math.min(limit > 0 ? limit : historyLimit, 1000));
        String conversationId = conversationId(username, withUsername);

        FindIterable<Document> cursor = collection.find(Filters.eq("conversationId", conversationId))
                .sort(Sorts.descending("_id"))
                .limit(safeLimit);

        List<Document> rows = new ArrayList<>();
        for (Document d : cursor) {
            rows.add(d);
        }
        Collections.reverse(rows);
        return rows;
    }

    public static void upsertUser(String username) {
        String normalized = normalize(username);
        if (normalized.isEmpty()) {
            return;
        }

        long now = System.currentTimeMillis();
        usersCollection.updateOne(
                Filters.eq("usernameNorm", normalized),
                new Document("$set", new Document("username", username.trim())
                        .append("usernameNorm", normalized)
                        .append("lastSeenAt", now))
                        .append("$setOnInsert", new Document("createdAt", now)),
                new UpdateOptions().upsert(true)
        );
    }

    public static List<String> listKnownUsers(int limit) {
        int safeLimit = Math.max(1, Math.min(limit > 0 ? limit : 200, 1000));
        Set<String> merged = new LinkedHashSet<>();

        for (Document d : usersCollection.find().sort(Sorts.descending("lastSeenAt")).limit(safeLimit)) {
            String username = d.getString("username");
            if (username != null && !username.trim().isEmpty()) {
                merged.add(username.trim());
            }
        }

        for (String from : collection.distinct("fromUsername", String.class)) {
            if (from != null && !from.trim().isEmpty()) {
                merged.add(from.trim());
            }
            if (merged.size() >= safeLimit) {
                break;
            }
        }
        if (merged.size() < safeLimit) {
            for (String to : collection.distinct("toUsername", String.class)) {
                if (to != null && !to.trim().isEmpty()) {
                    merged.add(to.trim());
                }
                if (merged.size() >= safeLimit) {
                    break;
                }
            }
        }

        return new ArrayList<>(merged).subList(0, Math.min(merged.size(), safeLimit));
    }

    public static List<Document> conversations(String username, int limit) {
        String normalized = normalize(username);
        if (normalized.isEmpty()) {
            return Collections.emptyList();
        }

        int safeLimit = Math.max(1, Math.min(limit > 0 ? limit : 100, 500));
        int scanLimit = Math.min(safeLimit * 100, 5000);

        FindIterable<Document> cursor = collection.find(Filters.or(
                        Filters.eq("fromUsernameNorm", normalized),
                        Filters.eq("toUsernameNorm", normalized)
                ))
                .sort(Sorts.descending("timestamp"))
                .limit(scanLimit);

        Map<String, Document> byPeer = new LinkedHashMap<>();
        for (Document d : cursor) {
            String from = d.getString("fromUsername");
            String to = d.getString("toUsername");
            String peer = normalized.equals(normalize(from)) ? to : from;
            String peerNorm = normalize(peer);
            if (peerNorm.isEmpty() || byPeer.containsKey(peerNorm)) {
                continue;
            }

            Document row = new Document("withUsername", peer)
                    .append("conversationId", d.getString("conversationId"))
                    .append("lastMessage", d.getString("message"))
                    .append("timestamp", d.getLong("timestamp"))
                    .append("fromUsername", from)
                    .append("toUsername", to);
            byPeer.put(peerNorm, row);

            if (byPeer.size() >= safeLimit) {
                break;
            }
        }

        return new ArrayList<>(byPeer.values());
    }

    public static String idHex(Document d) {
        ObjectId id = d.getObjectId("_id");
        return id == null ? "" : id.toHexString();
    }

    private static String conversationId(String a, String b) {
        String x = normalize(a);
        String y = normalize(b);
        return x.compareTo(y) <= 0 ? x + "|" + y : y + "|" + x;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private static int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return fallback;
        }
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
            LOGGER.info("Mongo collection created: {}.{}", database.getName(), collectionName);
        }
    }
}
