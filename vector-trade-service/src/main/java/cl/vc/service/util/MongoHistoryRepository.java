package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Historico multi-dia de ordenes y ejecuciones en Mongo. Lo que vive en Redis es estado intradia
 * (expira a las 17:59 y {@code purgeOrdersNotFromToday} lo borra al arrancar); esto es lo que
 * permite consultar meses atras a que precio se compro y se vendio.
 *
 * DISEÑO (rendimiento): {@link #recordExecution} y {@link #recordOrderTerminal} se llaman desde el
 * hilo del actor, asi que SOLO arman el Document y lo encolan -> nunca tocan la red. Un unico hilo
 * daemon drena la cola por lotes y hace insertMany. Si la cola se llena se descarta y se cuenta
 * (mejor perder historico que frenar el ruteo); {@link #status()} expone ese contador.
 *
 * Config (application.properties):
 *   history.enabled           (default false)
 *   history.mongo.connection  (default: reusa mongo.connection)
 *   history.mongo.db          (default vector_history)
 *   history.queue.size        (default 50000)
 *   history.batch.size        (default 500)
 *
 * Idempotencia: indice unico por execId en 'executions'. Los reenvios del sellside con el mismo
 * execId chocan contra el indice y se ignoran (insertMany unordered), sin duplicar el historico.
 */
@Slf4j
public class MongoHistoryRepository {

    private static final String COL_EXECUTIONS = "executions";
    private static final String COL_ORDERS = "orders";

    private static volatile MongoClient client;
    private static volatile MongoCollection<Document> executions;
    private static volatile MongoCollection<Document> orders;
    private static volatile boolean initTried = false;
    private static volatile boolean connected = false;
    private static volatile String lastError = null;
    private static volatile String currentDb = null;

    private static volatile BlockingQueue<PendingWrite> queue;
    private static volatile int batchSize = 500;
    private static final AtomicLong dropped = new AtomicLong();
    private static final AtomicLong writtenExecutions = new AtomicLong();
    private static final AtomicLong writtenOrders = new AtomicLong();

    private record PendingWrite(boolean execution, Document doc) {
    }

    /** Arranca conexion + hilo escritor en background, UNA sola vez. No bloquea al que llama. */
    public static void warmStart() {
        if (initTried) return;
        synchronized (MongoHistoryRepository.class) {
            if (initTried) return;
            initTried = true;
            Thread t = new Thread(MongoHistoryRepository::connectAndStartWriter, "mongo-history-init");
            t.setDaemon(true);
            t.start();
        }
    }

    private static void connectAndStartWriter() {
        if (!connect()) return;
        Thread writer = new Thread(MongoHistoryRepository::drainLoop, "mongo-history-writer");
        writer.setDaemon(true);
        writer.start();
    }

    private static synchronized boolean connect() {
        closeQuietly();
        executions = null;
        orders = null;
        connected = false;
        lastError = null;
        try {
            Properties p = MainApp.getProperties();
            if (!Boolean.parseBoolean(p.getProperty("history.enabled", "false"))) {
                lastError = "history.enabled=false (feature apagado)";
                log.info("[History] historico deshabilitado (history.enabled=false)");
                return false;
            }
            String uri = p.getProperty("history.mongo.connection", p.getProperty("mongo.connection"));
            if (uri == null || uri.isBlank()) {
                lastError = "falta history.mongo.connection (y mongo.connection)";
                log.warn("[History] history.enabled=true pero no hay URI de Mongo; historico off");
                return false;
            }
            String dbName = p.getProperty("history.mongo.db", "vector_history");

            MongoClient c = MongoClients.create(uri);
            c.getDatabase(dbName).runCommand(new Document("ping", 1));
            client = c;
            executions = c.getDatabase(dbName).getCollection(COL_EXECUTIONS);
            orders = c.getDatabase(dbName).getCollection(COL_ORDERS);
            currentDb = dbName;

            ensureIndexes();

            batchSize = readPositiveInt(p, "history.batch.size", 500);
            queue = new ArrayBlockingQueue<>(readPositiveInt(p, "history.queue.size", 50_000));
            connected = true;
            log.info("[History] conectado a {}/{{}, {}}", dbName, COL_EXECUTIONS, COL_ORDERS);
            return true;
        } catch (Exception e) {
            lastError = e.getMessage();
            log.error("[History] no se pudo conectar: {}", e.toString());
            return false;
        }
    }

    private static void ensureIndexes() {
        executions.createIndex(Indexes.ascending("execId"), new IndexOptions().unique(true));
        executions.createIndex(Indexes.compoundIndex(Indexes.ascending("account"), Indexes.descending("transactTime")));
        executions.createIndex(Indexes.compoundIndex(Indexes.ascending("account"),
                Indexes.ascending("symbol"), Indexes.descending("transactTime")));
        orders.createIndex(Indexes.ascending("orderId"), new IndexOptions().unique(true));
        orders.createIndex(Indexes.compoundIndex(Indexes.ascending("account"), Indexes.descending("transactTime")));
    }

    // ─────────────────────────── escritura (se llama desde el hilo del actor) ───────────────────────────

    /** Encola una ejecucion (fill). Solo arma el Document; el insert lo hace el hilo escritor. */
    public static void recordExecution(RoutingMessage.Order order) {
        if (!connected || order == null) return;
        offer(new PendingWrite(true, toExecutionDocument(order)));
    }

    /** Encola el estado final de una orden (FILLED / CANCELED / REJECTED). */
    public static void recordOrderTerminal(RoutingMessage.Order order) {
        if (!connected || order == null) return;
        offer(new PendingWrite(false, toOrderDocument(order)));
    }

    private static void offer(PendingWrite pending) {
        BlockingQueue<PendingWrite> q = queue;
        if (q == null || !q.offer(pending)) {
            long total = dropped.incrementAndGet();
            if (total == 1 || total % 1000 == 0) {
                log.warn("[History] cola llena, {} documentos descartados (el ruteo NO se frena)", total);
            }
        }
    }

    private static Document toExecutionDocument(RoutingMessage.Order o) {
        Document d = baseDocument(o);
        d.append("execId", o.getExecId())
                .append("orderId", o.getId())
                .append("lastPx", o.getLastPx())
                .append("lastQty", o.getLastQty())
                .append("cumQty", o.getCumQty())
                .append("avgPrice", o.getAvgPrice())
                .append("leaves", o.getLeaves())
                .append("execType", o.getExecType().name())
                .append("contraBroker", o.getContraBroker())
                .append("folio", o.getFolio());
        return d;
    }

    private static Document toOrderDocument(RoutingMessage.Order o) {
        Document d = baseDocument(o);
        d.append("orderId", o.getId())
                .append("clOrdId", o.getClOrdId())
                .append("orderQty", o.getOrderQty())
                .append("cumQty", o.getCumQty())
                .append("avgPrice", o.getAvgPrice())
                .append("ordType", o.getOrdType().name())
                .append("tif", o.getTif().name())
                .append("text", o.getText());
        return d;
    }

    /** Campos comunes. 'transactTime' es lo que indexa el calendario. */
    private static Document baseDocument(RoutingMessage.Order o) {
        return new Document()
                .append("account", o.getAccount())
                .append("symbol", o.getSymbol())
                .append("side", o.getSide().name())
                .append("price", o.getPrice())
                .append("currency", o.getCurrency().name())
                .append("operator", o.getOperator())
                .append("codeOperator", o.getCodeOperator())
                .append("ordStatus", o.getOrdStatus().name())
                .append("strategyOrder", o.getStrategyOrder().name())
                .append("securityExchange", o.getSecurityExchange().name())
                .append("settlType", o.getSettlType().name())
                .append("transactTime", Date.from(transactTime(o)));
    }

    /**
     * Fecha del evento. Si el sellside no mando 'time' se usa la hora de recepcion: sin esto el
     * documento quedaria fuera de cualquier rango del calendario y el usuario no lo veria nunca.
     */
    private static Instant transactTime(RoutingMessage.Order o) {
        if (o.hasTime()) {
            return Instant.ofEpochSecond(o.getTime().getSeconds(), o.getTime().getNanos());
        }
        return Instant.now();
    }

    private static void drainLoop() {
        List<PendingWrite> batch = new ArrayList<>(batchSize);
        List<Document> execDocs = new ArrayList<>(batchSize);
        List<Document> orderDocs = new ArrayList<>(batchSize);
        InsertManyOptions unordered = new InsertManyOptions().ordered(false);

        while (true) {
            try {
                PendingWrite first = queue.poll(1, TimeUnit.SECONDS);
                if (first == null) continue;
                batch.add(first);
                queue.drainTo(batch, batchSize - 1);

                for (PendingWrite w : batch) {
                    (w.execution() ? execDocs : orderDocs).add(w.doc());
                }
                flush(executions, execDocs, unordered, writtenExecutions, COL_EXECUTIONS);
                flush(orders, orderDocs, unordered, writtenOrders, COL_ORDERS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("[History] error en el hilo escritor: {}", e.toString());
            } finally {
                batch.clear();
                execDocs.clear();
                orderDocs.clear();
            }
        }
    }

    private static void flush(MongoCollection<Document> col, List<Document> docs,
                              InsertManyOptions options, AtomicLong counter, String name) {
        if (docs.isEmpty() || col == null) return;
        try {
            col.insertMany(docs, options);
            counter.addAndGet(docs.size());
        } catch (com.mongodb.MongoBulkWriteException e) {
            // Los duplicados por execId son esperados (reenvios del sellside): se cuentan los que si entraron.
            counter.addAndGet(e.getWriteResult().getInsertedCount());
            long dup = e.getWriteErrors().stream().filter(w -> w.getCode() == 11000).count();
            if (dup != e.getWriteErrors().size()) {
                log.error("[History] errores no-duplicado escribiendo {}: {}", name, e.getWriteErrors());
            }
        } catch (Exception e) {
            log.error("[History] fallo insertMany en {}: {}", name, e.toString());
        }
    }

    // ─────────────────────────── lectura ───────────────────────────

    /**
     * Ejecuciones de una cuenta en un rango de fechas (inclusive), mas recientes primero.
     * {@code symbol} y las fechas son opcionales (null = sin filtro).
     */
    public static List<Document> queryExecutions(String account, String symbol,
                                                 LocalDate from, LocalDate to, int skip, int limit) {
        return query(executions, account, symbol, from, to, skip, limit);
    }

    /** Ordenes en estado terminal de una cuenta en un rango de fechas. */
    public static List<Document> queryOrders(String account, String symbol,
                                             LocalDate from, LocalDate to, int skip, int limit) {
        return query(orders, account, symbol, from, to, skip, limit);
    }

    private static List<Document> query(MongoCollection<Document> col, String account, String symbol,
                                        LocalDate from, LocalDate to, int skip, int limit) {
        List<Document> out = new ArrayList<>();
        if (col == null || account == null || account.isBlank()) return out;

        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("account", account));
        if (symbol != null && !symbol.isBlank()) {
            filters.add(Filters.eq("symbol", symbol));
        }
        if (from != null) {
            filters.add(Filters.gte("transactTime", Date.from(from.atStartOfDay(MainApp.getZoneId()).toInstant())));
        }
        if (to != null) {
            filters.add(Filters.lt("transactTime", Date.from(to.plusDays(1).atStartOfDay(MainApp.getZoneId()).toInstant())));
        }
        col.find(Filters.and(filters))
                .sort(Sorts.descending("transactTime"))
                .skip(Math.max(0, skip))
                .limit(Math.max(1, limit))
                .into(out);
        return out;
    }

    // ─────────────────────────── admin ───────────────────────────

    /** Reconecta con la config actual. Lo usa el panel admin. */
    public static synchronized Map<String, Object> reconnectFromAdmin() {
        boolean wasConnected = connected;
        boolean ok = connect();
        if (ok && !wasConnected) {
            Thread writer = new Thread(MongoHistoryRepository::drainLoop, "mongo-history-writer");
            writer.setDaemon(true);
            writer.start();
        }
        return status();
    }

    public static Map<String, Object> status() {
        Properties p = MainApp.getProperties();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled", Boolean.parseBoolean(p.getProperty("history.enabled", "false")));
        m.put("connected", connected);
        m.put("db", currentDb);
        m.put("queueSize", queue == null ? -1 : queue.size());
        m.put("writtenExecutions", writtenExecutions.get());
        m.put("writtenOrders", writtenOrders.get());
        m.put("dropped", dropped.get());
        m.put("lastError", lastError);
        return m;
    }

    public static boolean isConnected() {
        return connected;
    }

    private static int readPositiveInt(Properties p, String key, int fallback) {
        try {
            int value = Integer.parseInt(p.getProperty(key, String.valueOf(fallback)).trim());
            return value > 0 ? value : fallback;
        } catch (Exception e) {
            return fallback;
        }
    }

    private static void closeQuietly() {
        MongoClient c = client;
        client = null;
        if (c == null) return;
        try {
            c.close();
        } catch (Exception e) {
            log.debug("[History] error cerrando cliente Mongo: {}", e.toString());
        }
    }
}
