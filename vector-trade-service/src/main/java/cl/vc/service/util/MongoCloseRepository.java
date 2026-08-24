package cl.vc.service.util;

import cl.vc.service.MainApp;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cierre del dia habil anterior (previous close) desde Mongo (coleccion close_prices), para calcular
 * la variacion % (el feed la manda con un cierre viejo). SOLO se usa para BCS.
 *
 * DISEÑO (rendimiento): Mongo se consulta UNA sola vez, en bloque, al conectar (precarga a cache en
 * un hilo background). getPreviousClose es siempre O(1) (solo lee la cache) -> nunca bloquea el
 * dispatcher de actores ni el camino de suscripcion al feed.
 *
 * Config (application.properties): mongo.isconnected / mongo.connection / mongo.db / collection.
 * Reconexion/estado en caliente desde el Admin: {@link #reconnectFromAdmin()} y {@link #status()}.
 *
 * OJO datos: 'date' viene NULL y 'createdAt' es Date (ISODate); se usa 'time' (string local) para
 * comparar contra "hoy". Doc: { symbol, securityExchange, time:"yyyy-MM-dd HH:mm...", protobufData }.
 */
@Slf4j
public class MongoCloseRepository {

    private static volatile MongoClient client;
    private static volatile MongoCollection<Document> collection;
    private static volatile boolean initTried = false;
    private static volatile boolean connected = false;
    /** true mientras se esta leyendo Mongo (precarga en curso). */
    private static volatile boolean reading = false;
    /** true cuando la precarga termino OK: recien ahi la var% de BCS es la calculada. */
    private static volatile boolean warmed = false;
    private static volatile String lastError = null;
    private static volatile String currentDb = null;
    private static volatile String currentCollection = null;
    private static final ConcurrentHashMap<String, Double> cache = new ConcurrentHashMap<>();

    /**
     * Arranca la conexion + precarga al inicio del servicio (background). Llamar desde MainApp para que
     * la cache este lista antes de que se conecte el primer cliente; si no, el primer Statistic de cada
     * simbolo saldria con la var% del feed.
     */
    public static void warmStart() {
        ensureStarted();
    }

    /** Dispara conexion + precarga en un hilo BACKGROUND, UNA sola vez. NO bloquea al que llama. */
    private static void ensureStarted() {
        if (initTried) return;
        synchronized (MongoCloseRepository.class) {
            if (initTried) return;
            initTried = true;
            Thread t = new Thread(MongoCloseRepository::connect, "mongo-prevclose-init");
            t.setDaemon(true);
            t.start();
        }
    }

    /** (Re)abre la conexion usando la config actual de properties y dispara la precarga. true si conecto. */
    private static synchronized boolean connect() {
        closeQuietly();
        collection = null;
        connected = false;
        lastError = null;
        try {
            Properties p = MainApp.getProperties();
            if (!Boolean.parseBoolean(p.getProperty("mongo.isconnected", "false"))) {
                lastError = "mongo.isconnected=false (feature apagado)";
                log.info("[Mongo] previous-close deshabilitado (mongo.isconnected=false)");
                return false;
            }
            String uri = p.getProperty("mongo.connection");
            if (uri == null || uri.isBlank()) {
                lastError = "falta mongo.connection";
                log.warn("[Mongo] mongo.isconnected=true pero falta mongo.connection; previous-close off");
                return false;
            }
            String dbName = p.getProperty("mongo.db", "close_prices");
            String colName = p.getProperty("collection", "close_prices");
            MongoClient c = MongoClients.create(uri);
            c.getDatabase(dbName).runCommand(new Document("ping", 1)); // valida conexion real
            client = c;
            collection = c.getDatabase(dbName).getCollection(colName);
            currentDb = dbName;
            currentCollection = colName;
            connected = true;
            cache.clear();
            log.info("[Mongo] previous-close conectado a {}/{} (precargando cache...)", dbName, colName);
            warmCache(); // corre en el hilo de conexion: background (ensureStarted) o admin (reconnect)
            return true;
        } catch (Exception e) {
            lastError = e.getMessage();
            log.error("[Mongo] no se pudo conectar previous-close: {}", e.toString());
            return false;
        }
    }

    // =========================================================================
    //  RECARGA EN CALIENTE POR LOTES (Admin -> seccion Mongo)
    //
    //  Para cuando el cierre llega a Mongo mas tarde de lo normal: relee sin
    //  reiniciar el core, en el orden de SecurityType que pida el operador
    //  (por defecto CS -> CFI -> ETF) y en lotes, para no saturar Mongo ni el
    //  dispatcher de actores. Modelado sobre el job de SqlRecoveryServlet
    //  (executor de 1 hilo daemon + estado volatile + rechazo 409 si ya corre).
    //
    //  DIFERENCIA IMPORTANTE CON connect(): esta recarga NO hace cache.clear().
    //  Escribe con put() incremental sobre la ConcurrentHashMap viva, asi la
    //  var% de BCS nunca cae al valor del feed mientras se repuebla. Un clear()
    //  en caliente degradaria a todos los clientes conectados durante minutos.
    // =========================================================================

    /** Orden por defecto que pidio la mesa. Son valores de SecurityType, NO de SettlType. */
    public static final List<String> DEFAULT_RELOAD_TYPES = List.of("CS", "CFI", "ETF");
    /** Simbolos que se leen ANTES que todo lo demas, pase lo que pase. */
    public static final List<String> DEFAULT_PRIORITY_SYMBOLS = List.of("CFMITNIPSA");
    /** Grupo para los simbolos que estan en Mongo pero sin SecurityType conocido: van al final. */
    static final String OTHERS_GROUP = "OTROS";
    static final String PRIORITY_GROUP = "PRIORIDAD";
    private static final int DEFAULT_BATCH_SIZE = 100;
    /** Ritmo objetivo: la mesa pidio 10 simbolos por segundo (16.000 -> ~27 min). */
    private static final double DEFAULT_RATE_PER_SECOND = 10d;
    /** Tope defensivo: evita que un batchSize enorme mande una query gigante a Mongo. */
    private static final int MAX_BATCH_SIZE = 1000;

    private static final ExecutorService reloadExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "mongo-close-reload");
        t.setDaemon(true);
        return t;
    });
    private static final Object reloadLock = new Object();
    private static volatile ReloadRun activeReload = null;
    private static volatile ReloadRun lastReload = null;

    /**
     * Estado de una recarga. Los flags son volatile y los contadores atomicos porque se escriben
     * en el hilo "mongo-close-reload" y se leen en el hilo de Jetty (GET /api/mongo).
     */
    private static final class ReloadRun {
        private final long startedAtMs = System.currentTimeMillis();
        private final List<String> types;
        private final List<String> prioritySymbols;
        private final int batchSize;
        private final double ratePerSecond;
        private final AtomicInteger symbolsTotal = new AtomicInteger();
        private final AtomicInteger symbolsProcessed = new AtomicInteger();
        private final AtomicInteger symbolsUpdated = new AtomicInteger();
        private final AtomicInteger batchesTotal = new AtomicInteger();
        private final AtomicInteger batchesDone = new AtomicInteger();
        private volatile String currentType = null;
        private volatile boolean running = true;
        private volatile boolean stopRequested = false;
        private volatile String error = null;
        private volatile long finishedAtMs = 0L;

        ReloadRun(List<String> types, List<String> prioritySymbols, int batchSize, double ratePerSecond) {
            this.types = types;
            this.prioritySymbols = prioritySymbols;
            this.batchSize = batchSize;
            this.ratePerSecond = ratePerSecond;
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            int total = symbolsTotal.get();
            int done = symbolsProcessed.get();
            m.put("running", running);
            m.put("types", types);
            m.put("prioritySymbols", prioritySymbols);
            m.put("batchSize", batchSize);
            m.put("ratePerSecond", ratePerSecond);
            m.put("currentType", currentType);
            m.put("symbolsTotal", total);
            m.put("symbolsProcessed", done);
            m.put("symbolsUpdated", symbolsUpdated.get());
            m.put("batchesTotal", batchesTotal.get());
            m.put("batchesDone", batchesDone.get());
            m.put("stopRequested", stopRequested);
            m.put("startedAt", startedAtMs);
            m.put("finishedAt", finishedAtMs);
            m.put("elapsedMs", (finishedAtMs > 0 ? finishedAtMs : System.currentTimeMillis()) - startedAtMs);
            // ETA derivada del ritmo pedido, que es lo que domina el tiempo total (no la query).
            m.put("etaSeconds", running && ratePerSecond > 0 && total > done
                    ? (long) Math.ceil((total - done) / ratePerSecond) : 0L);
            m.put("error", error);
            return m;
        }
    }

    /** Un grupo del plan de recarga: nombre visible + simbolos, ya deduplicados. */
    static final class PlanGroup {
        final String name;
        final List<String> symbols;

        PlanGroup(String name, List<String> symbols) {
            this.name = name;
            this.symbols = symbols;
        }
    }

    /**
     * Arma el orden de lectura. Funcion pura: es la pieza con mas reglas y la que hay que testear.
     *
     * Reglas:
     *  1. prioritySymbols van PRIMERO, en el orden dado, aunque no esten en el universo de Mongo ni
     *     en la SecurityList (la mesa los pide "si o si": si el papel no esta, la query simplemente
     *     no devuelve fila y queda registrado como no actualizado).
     *  2. Despues cada SecurityType en el orden pedido, con los simbolos del universo que tengan ese tipo.
     *  3. Al final OTROS: lo que esta en Mongo pero sin SecurityType conocido. Sin este grupo, con una
     *     SecurityList incompleta se dejarian de recargar miles de papeles en silencio.
     *  4. Cada simbolo aparece EXACTAMENTE una vez, en el primer grupo al que califica.
     *
     * @param universe    simbolos que realmente tienen cierre en Mongo (distinct sobre la ventana)
     * @param byType      SecurityType -> simbolos, desde la SecurityList del sellside
     * @param typeOrder   orden de tipos pedido (ej. CS, CFI, ETF)
     * @param prioritySymbols simbolos a leer antes que todo
     */
    static List<PlanGroup> buildReloadPlan(Collection<String> universe,
                                           Map<String, List<String>> byType,
                                           List<String> typeOrder,
                                           List<String> prioritySymbols) {
        List<PlanGroup> plan = new ArrayList<>();
        java.util.Set<String> used = new java.util.LinkedHashSet<>();

        // 1. Prioridad (forzada: no se filtra por universe)
        List<String> prio = new ArrayList<>();
        if (prioritySymbols != null) {
            for (String s : prioritySymbols) {
                if (s == null) continue;
                String sym = s.trim();
                if (!sym.isEmpty() && used.add(sym)) prio.add(sym);
            }
        }
        if (!prio.isEmpty()) plan.add(new PlanGroup(PRIORITY_GROUP, prio));

        java.util.Set<String> uni = new java.util.LinkedHashSet<>();
        if (universe != null) {
            for (String s : universe) {
                if (s != null && !s.trim().isEmpty()) uni.add(s.trim());
            }
        }

        // 2. Tipos en el orden pedido
        if (typeOrder != null) {
            for (String type : typeOrder) {
                if (type == null) continue;
                String t = type.trim().toUpperCase();
                if (t.isEmpty()) continue;
                List<String> candidates = byType == null ? List.of() : byType.getOrDefault(t, List.of());
                List<String> group = new ArrayList<>();
                for (String sym : candidates) {
                    if (uni.contains(sym) && used.add(sym)) group.add(sym);
                }
                if (!group.isEmpty()) plan.add(new PlanGroup(t, group));
            }
        }

        // 3. OTROS: resto del universo
        List<String> others = new ArrayList<>();
        for (String sym : uni) {
            if (used.add(sym)) others.add(sym);
        }
        if (!others.isEmpty()) plan.add(new PlanGroup(OTHERS_GROUP, others));

        return plan;
    }

    // --- Defaults configurables por properties -------------------------------
    // Se leen en cada arranque de job (no se cachean) para que /api/properties + el boton de la UI
    // los cambien en caliente. Si la clave no existe se usa la constante, asi que desplegar el jar
    // NO obliga a tocar el application.properties del servidor.

    private static List<String> csvProperty(String key, List<String> fallback) {
        try {
            String raw = MainApp.getProperties().getProperty(key);
            if (raw == null || raw.isBlank()) return fallback;
            List<String> out = new ArrayList<>();
            for (String part : raw.split(",")) {
                String v = part.trim().toUpperCase();
                if (!v.isEmpty()) out.add(v);
            }
            return out.isEmpty() ? fallback : out;
        } catch (Exception e) {
            log.warn("[Mongo/Reload] property {} invalida, se usa {}: {}", key, fallback, e.toString());
            return fallback;
        }
    }

    private static double doubleProperty(String key, double fallback) {
        try {
            String raw = MainApp.getProperties().getProperty(key);
            if (raw == null || raw.isBlank()) return fallback;
            double v = Double.parseDouble(raw.trim());
            return v > 0d ? v : fallback;
        } catch (Exception e) {
            log.warn("[Mongo/Reload] property {} invalida, se usa {}: {}", key, fallback, e.toString());
            return fallback;
        }
    }

    private static int intProperty(String key, int fallback) {
        try {
            String raw = MainApp.getProperties().getProperty(key);
            if (raw == null || raw.isBlank()) return fallback;
            return clamp(Integer.parseInt(raw.trim()), 1, MAX_BATCH_SIZE);
        } catch (Exception e) {
            log.warn("[Mongo/Reload] property {} invalida, se usa {}: {}", key, fallback, e.toString());
            return fallback;
        }
    }

    static List<String> configuredReloadTypes() {
        return csvProperty("mongo.close.reload.types", DEFAULT_RELOAD_TYPES);
    }

    static List<String> configuredPrioritySymbols() {
        return csvProperty("mongo.close.reload.priority.symbols", DEFAULT_PRIORITY_SYMBOLS);
    }

    static double configuredRatePerSecond() {
        return doubleProperty("mongo.close.reload.rate.per.second", DEFAULT_RATE_PER_SECOND);
    }

    static int configuredBatchSize() {
        return intProperty("mongo.close.reload.batch.size", DEFAULT_BATCH_SIZE);
    }

    /** true si hay una recarga por lotes en curso. */
    public static boolean isReloadRunning() {
        ReloadRun r = activeReload;
        return r != null && r.running;
    }

    /**
     * Dispara la recarga por lotes en background y devuelve de inmediato (el request HTTP no espera:
     * con ~5000 simbolos esto tarda minutos y el axios del admin corta a los 15 s).
     * Devuelve ok=false si ya hay una corriendo (el servlet lo traduce a 409).
     */
    public static Map<String, Object> startReloadFromAdmin(List<String> types, List<String> prioritySymbols,
                                                           Integer batchSize, Double ratePerSecond) {
        synchronized (reloadLock) {
            if (isReloadRunning()) {
                Map<String, Object> st = status();
                st.put("ok", false);
                st.put("message", "Ya hay una recarga de cierres corriendo.");
                return st;
            }
            if (collection == null) {
                Map<String, Object> st = status();
                st.put("ok", false);
                st.put("message", "Mongo no esta conectado; usa Reconectar antes de recargar.");
                return st;
            }
            // Precedencia: lo que manda la UI > application.properties > constante del codigo.
            List<String> t = (types == null || types.isEmpty()) ? configuredReloadTypes() : types;
            List<String> prio = (prioritySymbols == null) ? configuredPrioritySymbols() : prioritySymbols;
            int bs = clamp(batchSize == null ? configuredBatchSize() : batchSize, 1, MAX_BATCH_SIZE);
            double rate = (ratePerSecond == null || ratePerSecond <= 0d) ? configuredRatePerSecond() : ratePerSecond;
            ReloadRun run = new ReloadRun(t, prio, bs, rate);
            activeReload = run;
            reloadExecutor.submit(() -> runReload(run));
            Map<String, Object> st = status();
            st.put("ok", true);
            st.put("message", "Recarga iniciada a " + rate + " simbolos/s"
                    + (prio.isEmpty() ? "" : ", primero " + String.join(", ", prio))
                    + ", luego " + String.join(" -> ", t) + " y " + OTHERS_GROUP + ".");
            return st;
        }
    }

    /** Pide detener la recarga en curso (se corta al terminar el lote actual). */
    public static Map<String, Object> stopReloadFromAdmin() {
        ReloadRun r = activeReload;
        Map<String, Object> st;
        if (r == null || !r.running) {
            st = status();
            st.put("ok", false);
            st.put("message", "No hay recarga en curso.");
            return st;
        }
        r.stopRequested = true;
        st = status();
        st.put("ok", true);
        st.put("message", "Detencion solicitada; se corta al terminar el lote actual.");
        return st;
    }

    private static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    /** Cuerpo del job. Corre SIEMPRE en el hilo "mongo-close-reload". */
    private static void runReload(ReloadRun run) {
        long t0 = System.currentTimeMillis();
        try {
            // UNIVERSO: los simbolos que de verdad tienen cierre, sacados de Mongo. NO de la
            // SecurityList: si esa lista viene incompleta (o el sellside no la mando), tomarla como
            // universo dejaria miles de papeles sin recargar y en silencio.
            List<String> universe = distinctSymbolsInWindow();
            // CLASIFICACION: el SecurityType si sale del core, porque close_prices no lo trae
            // (sus documentos son { symbol, securityExchange, time, protobufData } y protobufData es
            // un STRING JSON que Mongo no puede filtrar ni indexar).
            Map<String, List<String>> byType = MainApp.snapshotBcsSymbolsBySecurityType();

            List<PlanGroup> plan = buildReloadPlan(universe, byType, run.types, run.prioritySymbols);

            int total = 0;
            for (PlanGroup g : plan) {
                total += g.symbols.size();
                run.batchesTotal.addAndGet((g.symbols.size() + run.batchSize - 1) / run.batchSize);
            }
            run.symbolsTotal.set(total);
            if (total == 0) {
                run.error = "No hay simbolos que recargar: Mongo no devolvio cierres en la ventana de "
                        + "14 dias y no se pidieron simbolos de prioridad.";
                log.warn("[Mongo/Reload] {}", run.error);
                return;
            }

            StringBuilder resumen = new StringBuilder();
            for (PlanGroup g : plan) resumen.append(g.name).append('(').append(g.symbols.size()).append(") ");
            log.info("[Mongo/Reload] INICIO {} simbolos a {}/s (~{} s) en {} lotes de {} | plan: {}",
                    total, run.ratePerSecond, (long) Math.ceil(total / run.ratePerSecond),
                    run.batchesTotal.get(), run.batchSize, resumen.toString().trim());

            for (PlanGroup group : plan) {
                if (run.stopRequested) break;
                run.currentType = group.name;
                int updatedInGroup = 0;
                for (int i = 0; i < group.symbols.size() && !run.stopRequested; i += run.batchSize) {
                    List<String> batch = group.symbols.subList(i, Math.min(group.symbols.size(), i + run.batchSize));
                    long batchStart = System.currentTimeMillis();
                    try {
                        Map<String, Double> found = loadClosesForSymbols(batch);
                        // put incremental: nunca clear(). Un simbolo sin cierre nuevo conserva el viejo.
                        found.forEach(cache::put);
                        run.symbolsUpdated.addAndGet(found.size());
                        updatedInGroup += found.size();
                    } catch (Exception e) {
                        run.error = e.getMessage();
                        log.warn("[Mongo/Reload] lote de {} fallo: {}", group.name, e.toString());
                    }
                    run.symbolsProcessed.addAndGet(batch.size());
                    run.batchesDone.incrementAndGet();
                    throttle(run, batch.size(), System.currentTimeMillis() - batchStart);
                }
                log.info("[Mongo/Reload] {} listo: {} cierres actualizados de {} simbolos",
                        group.name, updatedInGroup, group.symbols.size());
                // Avisa a los actores al cerrar cada grupo: los ya suscritos tienen previousDayClose
                // cacheado en el actor y solo lo releen si es 0, asi que sin este empujon el cliente
                // se queda con el valor viejo hasta el proximo tick (y si el papel no tickea, para siempre).
                // El grupo de PRIORIDAD se notifica por simbolo para que se vea de inmediato.
                if (PRIORITY_GROUP.equals(group.name)) {
                    for (String sym : group.symbols) MainApp.broadcastPreviousCloseRefresh(sym);
                } else {
                    MainApp.broadcastPreviousCloseRefresh(null);
                }
            }
            warmed = true;
            log.info("[Mongo/Reload] FIN{} {} de {} simbolos actualizados en {} ms",
                    run.stopRequested ? " (DETENIDO)" : "", run.symbolsUpdated.get(),
                    run.symbolsTotal.get(), System.currentTimeMillis() - t0);
        } catch (Exception e) {
            run.error = e.getMessage();
            log.error("[Mongo/Reload] FIN CON ERROR tras {} ms: {}", System.currentTimeMillis() - t0, e.toString(), e);
        } finally {
            // finally garantizado: si no, el admin muestra un job colgado para siempre.
            run.running = false;
            run.finishedAtMs = System.currentTimeMillis();
            run.currentType = null;
            lastReload = run;
            synchronized (reloadLock) {
                if (activeReload == run) activeReload = null;
            }
        }
    }

    /**
     * Duerme lo necesario para respetar el ritmo pedido (simbolos/segundo).
     *
     * Descuenta lo que tardo la query: asi el ritmo real es el pedido y no "el pedido mas la query".
     * Si un lote tarda mas que su presupuesto, no duerme nada (el ritmo cae solo, sin acumular deuda).
     */
    static long throttleMillis(int symbolsInBatch, double ratePerSecond, long batchElapsedMs) {
        if (ratePerSecond <= 0d || symbolsInBatch <= 0) return 0L;
        long budgetMs = (long) Math.ceil((symbolsInBatch / ratePerSecond) * 1000d);
        return Math.max(0L, budgetMs - batchElapsedMs);
    }

    private static void throttle(ReloadRun run, int symbolsInBatch, long batchElapsedMs) {
        long sleep = throttleMillis(symbolsInBatch, run.ratePerSecond, batchElapsedMs);
        if (sleep <= 0L) return;
        try {
            // En tramos: permite que "Detener" responda rapido en vez de esperar todo el presupuesto
            // del lote (con 100 simbolos a 10/s eso serian 10 s).
            long remaining = sleep;
            while (remaining > 0 && !run.stopRequested) {
                long chunk = Math.min(250L, remaining);
                Thread.sleep(chunk);
                remaining -= chunk;
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            run.stopRequested = true;
        }
    }

    /**
     * Simbolos BCS que tienen algun cierre en la ventana de 14 dias. Es el universo real de la
     * recarga; una sola query distinct al arrancar el job, no en el hot path.
     */
    private static List<String> distinctSymbolsInWindow() {
        MongoCollection<Document> col = collection;
        List<String> out = new ArrayList<>();
        if (col == null) return out;
        String today = LocalDate.now().toString();
        String from = LocalDate.now().minusDays(14).toString();
        Bson filter = Filters.and(
                Filters.eq("securityExchange", "BCS"),
                Filters.gte("time", from),
                Filters.lt("time", today));
        for (String s : col.distinct("symbol", filter, String.class)) {
            if (s != null && !s.isBlank()) out.add(s);
        }
        log.info("[Mongo/Reload] universo: {} simbolos BCS con cierre en {}..{}", out.size(), from, today);
        return out;
    }

    /**
     * Cierre del ultimo dia anterior a hoy para un conjunto acotado de simbolos.
     * Mismo pipeline que warmCache pero con `symbol IN (...)`, que si usa indice.
     */
    private static Map<String, Double> loadClosesForSymbols(Collection<String> symbols) {
        MongoCollection<Document> col = collection;
        Map<String, Double> out = new HashMap<>();
        if (col == null || symbols == null || symbols.isEmpty()) return out;
        String today = LocalDate.now().toString();
        String from = LocalDate.now().minusDays(14).toString();
        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.and(
                        Filters.eq("securityExchange", "BCS"),
                        Filters.in("symbol", symbols),
                        Filters.gte("time", from),
                        Filters.lt("time", today))),
                Aggregates.sort(Sorts.descending("time")),
                Aggregates.group("$symbol", Accumulators.first("pb", "$protobufData")));
        for (Document g : col.aggregate(pipeline).allowDiskUse(true)) {
            String sym = g.getString("_id");
            double close = parseClose(g.getString("pb"));
            if (sym != null && close > 0d) out.put(sym, close);
        }
        return out;
    }

    /**
     * Relee el cierre de UN simbolo y lo empuja a los actores suscritos. Es rapido (una query
     * acotada), asi que corre en el hilo del request sin riesgo de timeout.
     */
    public static Map<String, Object> refreshSymbolFromAdmin(String symbol) {
        Map<String, Object> out = new LinkedHashMap<>();
        String sym = symbol == null ? "" : symbol.trim();
        if (sym.isEmpty()) {
            out.put("ok", false);
            out.put("message", "Falta el simbolo.");
            return out;
        }
        if (collection == null) {
            out.put("ok", false);
            out.put("message", "Mongo no esta conectado; usa Reconectar primero.");
            return out;
        }
        try {
            Double before = cache.get(sym);
            Map<String, Double> found = loadClosesForSymbols(List.of(sym));
            Double after = found.get(sym);
            out.put("symbol", sym);
            out.put("previous", before);
            if (after == null) {
                out.put("ok", false);
                out.put("value", before);
                out.put("message", "Sin cierre en Mongo para " + sym
                        + " en la ventana de 14 dias (se mantiene " + (before == null ? "sin dato" : before) + ").");
                return out;
            }
            cache.put(sym, after);
            MainApp.broadcastPreviousCloseRefresh(sym);
            out.put("ok", true);
            out.put("value", after);
            out.put("changed", before == null || Double.compare(before, after) != 0);
            out.put("message", "Cierre de " + sym + " = " + after
                    + (before == null ? " (antes sin dato)" : " (antes " + before + ")") + "; actores notificados.");
            log.info("[Mongo/Reload] simbolo {} recargado: {} -> {}", sym, before, after);
            return out;
        } catch (Exception e) {
            log.error("[Mongo/Reload] error recargando simbolo {}: {}", sym, e.toString(), e);
            out.put("ok", false);
            out.put("message", "Error: " + e.getMessage());
            return out;
        }
    }

    /**
     * Precarga en UNA sola query el cierre del ultimo dia (anterior a hoy) de cada simbolo BCS.
     * Ventana de 14 dias para cubrir fines de semana/feriados sin escanear toda la coleccion.
     */
    private static void warmCache() {
        MongoCollection<Document> col = collection;
        if (col == null) return;
        long t0 = System.currentTimeMillis();
        reading = true;
        try {
            // OJO: 'createdAt' es un Date (ISODate) -> comparar con un string da 0. 'time' es string local
            // ("yyyy-MM-dd HH:mm..."), asi que con time < hoy (techo exclusivo) + orden desc queda el ultimo
            // cierre ANTERIOR a hoy de cada simbolo.
            String today = LocalDate.now().toString();               // yyyy-MM-dd (techo exclusivo)
            String from = LocalDate.now().minusDays(14).toString();  // piso: cubre feriados/fin de semana
            log.info("[Mongo] INICIO lectura de cierres (ventana {}..{}) — hasta que termine, la var% de BCS sale del feed", from, today);
            List<Bson> pipeline = Arrays.asList(
                    Aggregates.match(Filters.and(
                            Filters.eq("securityExchange", "BCS"),
                            Filters.gte("time", from),
                            Filters.lt("time", today))),
                    Aggregates.sort(Sorts.descending("time")),
                    Aggregates.group("$symbol", Accumulators.first("pb", "$protobufData")));
            int n = 0;
            for (Document g : col.aggregate(pipeline).allowDiskUse(true)) {
                String sym = g.getString("_id");
                double close = parseClose(g.getString("pb"));
                if (sym != null && close > 0d) {
                    cache.put(sym, close);
                    n++;
                }
            }
            warmed = true;
            log.info("[Mongo] FIN lectura: {} simbolos cargados en {} ms (ventana {}..{})",
                    n, System.currentTimeMillis() - t0, from, today);
        } catch (Exception e) {
            log.warn("[Mongo] FIN lectura CON ERROR tras {} ms: {}", System.currentTimeMillis() - t0, e.toString());
        } finally {
            reading = false;
        }
    }

    private static double parseClose(String pb) {
        if (pb == null || pb.isBlank()) return 0d;
        try {
            JSONObject o = new JSONObject(pb);
            double close = (o.opt("ohlcv") instanceof JSONObject)
                    ? o.getJSONObject("ohlcv").optDouble("close", 0d) : 0d;
            if (close <= 0d) close = o.optDouble("last", 0d);
            return close;
        } catch (Exception e) {
            return 0d;
        }
    }

    private static void closeQuietly() {
        if (client != null) {
            try { client.close(); } catch (Exception ignore) { /* no-op */ }
            client = null;
        }
    }

    /** Reconexion manual desde el Admin: relee properties, reconecta (y reprecarga) y devuelve el estado. */
    public static synchronized Map<String, Object> reconnectFromAdmin() {
        initTried = true;
        boolean ok = connect();
        Map<String, Object> st = status();
        st.put("ok", ok);
        st.put("message", ok
                ? "Mongo reconectado a " + currentDb + "/" + currentCollection + " (precargando cache)"
                : "No conecto: " + (lastError != null ? lastError : "revisa la configuracion"));
        return st;
    }

    /** Snapshot para el Admin (uri con password enmascarado; nunca se expone la clave). */
    public static Map<String, Object> status() {
        Properties p = MainApp.getProperties();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled",    Boolean.parseBoolean(p.getProperty("mongo.isconnected", "false")));
        m.put("connected",  connected);
        m.put("uri",        maskUri(p.getProperty("mongo.connection", "")));
        m.put("db",         p.getProperty("mongo.db", "close_prices"));
        m.put("collection", p.getProperty("collection", "close_prices"));
        m.put("cacheSize",  cache.size());
        m.put("reading",    reading);   // true = leyendo Mongo ahora (precarga completa)
        m.put("warmed",     warmed);    // true = cierres ya cargados
        m.put("lastError",  lastError);
        // Estado del job de recarga por lotes, aparte de reading/warmed: esos dos son globales del
        // repositorio y los usa el actor para distinguir "aun cargando" de "sin dato". Pisarlos con
        // el estado del job romperia ese diagnostico.
        ReloadRun active = activeReload;
        ReloadRun last = lastReload;
        m.put("reloadRunning", active != null && active.running);
        m.put("reload",        active != null ? active.toMap() : null);
        m.put("lastReload",    last != null ? last.toMap() : null);
        // Defaults efectivos (properties si estan, constante si no): la UI los usa para prellenar
        // el formulario en vez de tener los numeros duplicados en el JSX.
        m.put("reloadTypesDefault", configuredReloadTypes());
        m.put("reloadPriorityDefault", configuredPrioritySymbols());
        m.put("reloadRateDefault", configuredRatePerSecond());
        m.put("reloadBatchDefault", configuredBatchSize());
        return m;
    }

    private static String maskUri(String uri) {
        if (uri == null || uri.isBlank()) return "";
        return uri.replaceAll("://([^:/@]+):([^@]+)@", "://$1:****@");
    }

    /**
     * Cierre del dia habil anterior del simbolo. SIEMPRE O(1): solo lee la cache precargada.
     * La primera llamada dispara la conexion + precarga (async). Devuelve 0 si aun no esta en cache
     * (feature off, sin conexion, precarga en curso, o simbolo sin dato); el actor reintenta el lookup.
     */
    /** true mientras la precarga de cierres esta en curso (util para distinguir "aun cargando" de "sin dato"). */
    public static boolean isReading() {
        return reading;
    }

    /** true cuando la precarga ya termino OK. */
    public static boolean isWarmed() {
        return warmed;
    }

    public static double getPreviousClose(String symbol) {
        if (symbol == null || symbol.isEmpty()) return 0d;
        Double cached = cache.get(symbol);
        if (cached != null) return cached;
        ensureStarted(); // conexion + precarga en background; NO bloquea (devuelve 0 hasta que llegue el dato)
        Double v = cache.get(symbol);
        return v != null ? v : 0d;
    }
}
