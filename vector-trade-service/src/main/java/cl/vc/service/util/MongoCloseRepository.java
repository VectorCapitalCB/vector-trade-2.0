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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

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
        m.put("reading",    reading);   // true = leyendo Mongo ahora
        m.put("warmed",     warmed);    // true = cierres ya cargados
        m.put("lastError",  lastError);
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
