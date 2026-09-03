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
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.google.protobuf.Timestamp;
import org.bson.BsonBinarySubType;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.Binary;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Historico multi-dia de ordenes y ejecuciones en Mongo. Lo que vive en Redis es estado intradia
 * (expira a las 17:59 y {@code purgeOrdersNotFromToday} lo borra al arrancar); esto es lo que
 * permite consultar meses atras a que precio se compro y se vendio.
 *
 * DISEÑO (rendimiento): {@link #recordFilledOrder} se llama desde el hilo del actor, asi que SOLO
 * arma dos documentos y los encola: el resumen de la orden y el fill individual. Un unico hilo
 * daemon drena la cola por lotes y escribe en Mongo. Si la cola se llena se descarta y se cuenta
 * (mejor perder historico que frenar el ruteo); {@link #status()} expone ese contador.
 *
 * Config (application.properties):
 *   history.enabled           (default false)
 *   history.mongo.connection  (default: reusa mongo.connection)
 *   history.mongo.db          (default vector_history)
 *   history.queue.size        (default 50000)
 *   history.batch.size        (default 500)
 *
 * Idempotencia: indice unico por cuenta + execId en 'historical_order_executions'. Los reenvios del
 * sellside con el mismo execId chocan contra el indice y se ignoran, sin duplicar el historico.
 * Las dos colecciones viven en 'vector_history'; nunca se escribe en 'close_prices'.
 */
@Slf4j
public class MongoHistoryRepository {

    private static final String COL_EXECUTIONS = "historical_order_executions";
    private static final String COL_ORDERS = "historical_order_summaries";

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

    private enum WriteKind { EXECUTION, SUMMARY }

    private record PendingWrite(WriteKind kind, Document doc) {
    }

    public record HistoricalOrderBundle(RoutingMessage.Order summary,
                                        List<RoutingMessage.Order> executions) {
    }

    public record HistoricalQueryResult(List<HistoricalOrderBundle> orders, boolean truncated) {
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
            log.info("[History] conectado a {}/[{}, {}]", dbName, COL_EXECUTIONS, COL_ORDERS);
            return true;
        } catch (Exception e) {
            lastError = e.getMessage();
            log.error("[History] no se pudo conectar: {}", e.toString());
            return false;
        }
    }

    private static void ensureIndexes() {
        executions.createIndex(Indexes.ascending("execKey"), new IndexOptions().unique(true));
        executions.createIndex(Indexes.compoundIndex(Indexes.ascending("account"), Indexes.descending("transactTime")));
        executions.createIndex(Indexes.compoundIndex(Indexes.ascending("account"),
                Indexes.ascending("orderId"), Indexes.ascending("transactTime")));
        orders.createIndex(Indexes.compoundIndex(Indexes.ascending("account"), Indexes.ascending("orderId")),
                new IndexOptions().unique(true));
        orders.createIndex(Indexes.compoundIndex(Indexes.ascending("account"), Indexes.descending("transactTime")));
        orders.createIndex(Indexes.compoundIndex(Indexes.ascending("account"),
                Indexes.ascending("symbol"), Indexes.descending("transactTime")));
    }

    // ─────────────────────────── escritura (se llama desde el hilo del actor) ───────────────────────────

    /** Encola el fill y actualiza el resumen de su orden, sin bloquear el actor de ruteo. */
    public static void recordFilledOrder(RoutingMessage.Order order) {
        if (!connected || !isPersistableFill(order)) return;
        offer(new PendingWrite(WriteKind.EXECUTION, toExecutionDocument(order)));
        offer(new PendingWrite(WriteKind.SUMMARY, toOrderDocument(order)));
    }

    /** Encola una ejecucion individual. Se conserva para recuperaciones y pruebas. */
    public static void recordExecution(RoutingMessage.Order order) {
        if (!connected || !isPersistableFill(order)) return;
        offer(new PendingWrite(WriteKind.EXECUTION, toExecutionDocument(order)));
    }

    /** Actualiza una fila resumen solo cuando la orden tiene ejecucion parcial o total. */
    public static void recordOrderSummary(RoutingMessage.Order order) {
        if (!connected || !isPersistableFill(order)) return;
        offer(new PendingWrite(WriteKind.SUMMARY, toOrderDocument(order)));
    }

    /**
     * Encola el estado FINAL de una orden: FILLED, CANCELED o REJECTED.
     *
     * <p>A diferencia de {@link #recordOrderSummary}, NO exige que la orden haya ejecutado. Una
     * cancelada o rechazada que nunca alcanzo un fill igual tiene que quedar en el historico: si
     * no, se pierde el rastro del id y el operador no puede reconstruir que paso con su orden.
     * Es el comportamiento de produccion (MongoHistoryRepository.recordOrderTerminal en PROD CORE).
     */
    public static void recordOrderTerminal(RoutingMessage.Order order) {
        if (!connected || order == null) return;
        offer(new PendingWrite(WriteKind.SUMMARY, toOrderDocument(order)));
    }

    static boolean isPersistableFill(RoutingMessage.Order order) {
        if (order == null || order.getExecType() != RoutingMessage.ExecutionType.EXEC_TRADE) return false;
        RoutingMessage.OrderStatus status = order.getOrdStatus();
        return (status == RoutingMessage.OrderStatus.FILLED
                || status == RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                && (order.getCumQty() > 0d || order.getLastQty() > 0d);
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
        d.append("execKey", executionKey(o))
                .append("execId", o.getExecId())
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
                .append("text", o.getText())
                .append("firstExecutionTime", Date.from(transactTime(o)));
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
                .append("orderIdFix", o.getOrderID())
                .append("ordStatus", o.getOrdStatus().name())
                .append("strategyOrder", o.getStrategyOrder().name())
                .append("securityExchange", o.getSecurityExchange().name())
                .append("settlType", o.getSettlType().name())
                .append("transactTime", Date.from(transactTime(o)))
                .append("orderProto", new Binary(BsonBinarySubType.BINARY, o.toByteArray()));
    }

    private static String executionKey(RoutingMessage.Order o) {
        String execId = o.getExecId();
        if (execId != null && !execId.isBlank()) {
            return o.getAccount() + "|" + execId;
        }
        String eventTime = o.hasTime()
                ? o.getTime().getSeconds() + ":" + o.getTime().getNanos()
                : "NO_TIME";
        return o.getAccount() + "|" + o.getId() + "|" + eventTime + "|"
                + o.getLastQty() + "|" + o.getLastPx() + "|" + o.getCumQty();
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
        List<Document> summaryDocs = new ArrayList<>(batchSize);
        InsertManyOptions unordered = new InsertManyOptions().ordered(false);

        while (true) {
            try {
                PendingWrite first = queue.poll(1, TimeUnit.SECONDS);
                if (first == null) continue;
                batch.add(first);
                queue.drainTo(batch, batchSize - 1);

                for (PendingWrite w : batch) {
                    (w.kind() == WriteKind.EXECUTION ? execDocs : summaryDocs).add(w.doc());
                }
                flush(executions, execDocs, unordered, writtenExecutions, COL_EXECUTIONS);
                upsertSummaries(summaryDocs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("[History] error en el hilo escritor: {}", e.toString());
            } finally {
                batch.clear();
                execDocs.clear();
                summaryDocs.clear();
            }
        }
    }

    private static void upsertSummaries(List<Document> docs) {
        if (docs.isEmpty() || orders == null) return;
        Map<String, Document> latestByOrder = new LinkedHashMap<>();
        for (Document candidate : docs) {
            String key = bundleKey(candidate.getString("account"), candidate.getString("orderId"));
            Document current = latestByOrder.get(key);
            if (current == null || summaryIsNewer(candidate, current)) {
                latestByOrder.put(key, candidate);
            }
        }

        List<WriteModel<Document>> writes = new ArrayList<>(latestByOrder.size());
        for (Document original : latestByOrder.values()) {
            Document current = new Document(original);
            Object firstExecutionTime = current.remove("firstExecutionTime");
            Document update = new Document("$set", current)
                    .append("$min", new Document("firstExecutionTime", firstExecutionTime));
            Bson filter = Filters.and(
                    Filters.eq("account", original.getString("account")),
                    Filters.eq("orderId", original.getString("orderId")));
            writes.add(new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true)));
        }
        try {
            var result = orders.bulkWrite(writes, new BulkWriteOptions().ordered(false));
            writtenOrders.addAndGet(result.getModifiedCount() + result.getUpserts().size());
        } catch (Exception e) {
            log.error("[History] fallo actualizando resumenes: {}", e.toString());
        }
    }

    private static boolean summaryIsNewer(Document candidate, Document current) {
        int quantity = Double.compare(doubleValue(candidate, "cumQty"), doubleValue(current, "cumQty"));
        if (quantity != 0) return quantity > 0;
        Date candidateTime = candidate.getDate("transactTime");
        Date currentTime = current.getDate("transactTime");
        return candidateTime != null && (currentTime == null || candidateTime.after(currentTime));
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

    /** Devuelve una fila resumen por orden y todos sus fills, ordenados cronologicamente. */
    public static HistoricalQueryResult queryHistoricalOrders(Collection<String> requestedAccounts,
                                                               String symbol, LocalDate from,
                                                               LocalDate to, int requestedLimit) {
        if (!connected || orders == null || executions == null || requestedAccounts == null) {
            return new HistoricalQueryResult(List.of(), false);
        }
        Set<String> accounts = new LinkedHashSet<>();
        requestedAccounts.stream()
                .filter(a -> a != null && !a.isBlank())
                .map(String::trim)
                .forEach(accounts::add);
        if (accounts.isEmpty()) return new HistoricalQueryResult(List.of(), false);

        // La tabla consulta 500; el margen mayor se reserva para exportaciones explícitas.
        int limit = Math.max(1, Math.min(requestedLimit <= 0 ? 500 : requestedLimit, 10_000));
        List<Bson> filters = dateAndAccountFilters(accounts, symbol, from, to);
        filters.add(Filters.in("ordStatus", List.of(
                RoutingMessage.OrderStatus.FILLED.name(),
                RoutingMessage.OrderStatus.PARTIALLY_FILLED.name())));
        filters.add(Filters.gt("cumQty", 0d));

        List<Document> summaryDocs = orders.find(Filters.and(filters))
                .sort(Sorts.descending("transactTime"))
                .limit(limit + 1)
                .into(new ArrayList<>());
        boolean truncated = summaryDocs.size() > limit;
        if (truncated) summaryDocs = new ArrayList<>(summaryDocs.subList(0, limit));
        if (summaryDocs.isEmpty()) return new HistoricalQueryResult(List.of(), truncated);

        Set<String> orderIds = new HashSet<>();
        summaryDocs.forEach(d -> orderIds.add(d.getString("orderId")));
        List<Bson> executionFilters = dateAndAccountFilters(accounts, symbol, from, to);
        executionFilters.add(Filters.in("orderId", orderIds));
        List<Document> executionDocs = executions.find(Filters.and(executionFilters))
                .sort(Sorts.ascending("transactTime"))
                .into(new ArrayList<>());

        Map<String, List<RoutingMessage.Order>> fillsByOrder = new HashMap<>();
        for (Document d : executionDocs) {
            RoutingMessage.Order fill = orderFromDocument(d);
            if (fill == null) continue;
            fillsByOrder.computeIfAbsent(bundleKey(fill.getAccount(), fill.getId()), ignored -> new ArrayList<>())
                    .add(fill);
        }

        List<HistoricalOrderBundle> result = new ArrayList<>(summaryDocs.size());
        for (Document d : summaryDocs) {
            RoutingMessage.Order summary = orderFromDocument(d);
            if (summary == null) continue;
            List<RoutingMessage.Order> fills = fillsByOrder.getOrDefault(
                    bundleKey(summary.getAccount(), summary.getId()), List.of());
            result.add(new HistoricalOrderBundle(executionSummary(summary, fills), List.copyOf(fills)));
        }
        return new HistoricalQueryResult(List.copyOf(result), truncated);
    }

    private static List<Bson> dateAndAccountFilters(Collection<String> accounts, String symbol,
                                                    LocalDate from, LocalDate to) {
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.in("account", accounts));
        if (symbol != null && !symbol.isBlank()) {
            filters.add(Filters.eq("symbol", symbol.trim().toUpperCase(java.util.Locale.ROOT)));
        }
        if (from != null) {
            filters.add(Filters.gte("transactTime",
                    Date.from(from.atStartOfDay(MainApp.getZoneId()).toInstant())));
        }
        if (to != null) {
            filters.add(Filters.lt("transactTime",
                    Date.from(to.plusDays(1).atStartOfDay(MainApp.getZoneId()).toInstant())));
        }
        return filters;
    }

    private static String bundleKey(String account, String orderId) {
        return account + "\u0000" + orderId;
    }

    private static RoutingMessage.Order executionSummary(RoutingMessage.Order persisted,
                                                         List<RoutingMessage.Order> fills) {
        if (fills.isEmpty()) return persisted;
        double detailQty = fills.stream().mapToDouble(RoutingMessage.Order::getLastQty).filter(q -> q > 0d).sum();
        double detailAmount = fills.stream()
                .filter(f -> f.getLastQty() > 0d && f.getLastPx() > 0d)
                .mapToDouble(f -> f.getLastQty() * f.getLastPx()).sum();
        double cumQty = Math.max(persisted.getCumQty(), detailQty);
        double reportedOrderQty = fills.stream()
                .mapToDouble(RoutingMessage.Order::getOrderQty)
                .filter(q -> q > 0d)
                .max()
                .orElse(0d);
        // Registros antiguos pueden venir del sell-side con OrderQty=0. En ese caso la
        // cantidad ejecutada es la mejor reconstruccion disponible para la vista historica.
        double orderQty = Math.max(Math.max(persisted.getOrderQty(), reportedOrderQty), cumQty);
        double avgPrice = persisted.getAvgPrice() > 0d
                ? persisted.getAvgPrice()
                : (detailQty > 0d ? detailAmount / detailQty : 0d);
        double executedAmount = avgPrice > 0d && cumQty > 0d ? avgPrice * cumQty : detailAmount;
        RoutingMessage.Order latest = fills.get(fills.size() - 1);
        return persisted.toBuilder()
                .setOrderQty(orderQty)
                .setCumQty(cumQty)
                .setAvgPrice(avgPrice)
                .setAmount(executedAmount)
                .setOrdStatus(latest.getOrdStatus())
                .setTime(latest.getTime())
                .build();
    }

    private static RoutingMessage.Order orderFromDocument(Document d) {
        try {
            Object raw = d.get("orderProto");
            if (raw instanceof Binary binary) {
                RoutingMessage.Order parsed = RoutingMessage.Order.parseFrom(binary.getData());
                if (!parsed.hasTime()) {
                    Date time = d.getDate("transactTime");
                    if (time != null) {
                        Instant instant = time.toInstant();
                        return parsed.toBuilder().setTime(Timestamp.newBuilder()
                                .setSeconds(instant.getEpochSecond())
                                .setNanos(instant.getNano())).build();
                    }
                }
                return parsed;
            }
            RoutingMessage.Order.Builder b = RoutingMessage.Order.newBuilder()
                    .setId(stringValue(d, "orderId"))
                    .setAccount(stringValue(d, "account"))
                    .setSymbol(stringValue(d, "symbol"))
                    .setPrice(doubleValue(d, "price"))
                    .setAvgPrice(doubleValue(d, "avgPrice"))
                    .setCumQty(doubleValue(d, "cumQty"))
                    .setLastPx(doubleValue(d, "lastPx"))
                    .setLastQty(doubleValue(d, "lastQty"))
                    .setOrderQty(doubleValue(d, "orderQty"))
                    .setLeaves(doubleValue(d, "leaves"))
                    .setAmount(doubleValue(d, "amount"))
                    .setExecId(stringValue(d, "execId"))
                    .setClOrdId(stringValue(d, "clOrdId"))
                    .setOrderID(stringValue(d, "orderIdFix"))
                    .setContraBroker(stringValue(d, "contraBroker"))
                    .setFolio(stringValue(d, "folio"));
            setEnum(d, "side", RoutingMessage.Side.class, b::setSide);
            setEnum(d, "ordStatus", RoutingMessage.OrderStatus.class, b::setOrdStatus);
            setEnum(d, "securityExchange", RoutingMessage.SecurityExchangeRouting.class, b::setSecurityExchange);
            setEnum(d, "settlType", RoutingMessage.SettlType.class, b::setSettlType);
            Date time = d.getDate("transactTime");
            if (time != null) {
                Instant instant = time.toInstant();
                b.setTime(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()));
            }
            return b.build();
        } catch (Exception e) {
            log.warn("[History] documento historico invalido orderId={}: {}", d.getString("orderId"), e.toString());
            return null;
        }
    }

    private static String stringValue(Document d, String key) {
        Object value = d.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    private static double doubleValue(Document d, String key) {
        Object value = d.get(key);
        return value instanceof Number number ? number.doubleValue() : 0d;
    }

    private static <E extends Enum<E>> void setEnum(Document d, String key, Class<E> type,
                                                     java.util.function.Consumer<E> setter) {
        String value = stringValue(d, key);
        if (value.isBlank()) return;
        try {
            setter.accept(Enum.valueOf(type, value));
        } catch (IllegalArgumentException ignored) {
        }
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
