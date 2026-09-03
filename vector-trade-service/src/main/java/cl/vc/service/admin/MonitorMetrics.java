package cl.vc.service.admin;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Contadores del proceso para el monitor externo (MONITOR-VC): de dónde salió la custodia de cada
 * cuenta, cuánto tardó, y los rechazos recientes.
 *
 * <p>Se lee por HTTP en {@code /api/monitor/*}. Antes MONITOR-VC sacaba esto scrapeando los logs de
 * Docker con regex, lo que se rompe cada vez que cambia un mensaje y no sobrevive a que la carga de
 * cuentas dejara de ser secuencial.
 *
 * <p>Todo lock-free: se toca desde los actores de cuenta (varios hilos del dispatcher) y se lee
 * desde el hilo del servlet. Los contadores por cuenta/motivo tienen cardinalidad acotada; los
 * eventos de rechazo viven en un anillo con tope fijo.
 */
public final class MonitorMetrics {

    private MonitorMetrics() {}

    /** Tope del anillo de rechazos recientes: acota memoria sin perder la ventana útil. */
    private static final int MAX_REJECT_EVENTS = 2000;

    public static final String SOURCE_REDIS = "REDIS";
    public static final String SOURCE_SQL = "SQL";

    // ---- custodia ----
    private static final AtomicLong custodyFromRedis = new AtomicLong();
    private static final AtomicLong custodyFromSql = new AtomicLong();
    private static final AtomicLong custodyLoadCount = new AtomicLong();
    private static final AtomicLong custodyLoadTotalMs = new AtomicLong();
    private static final AtomicLong custodyLoadMaxMs = new AtomicLong();
    private static final AtomicLong custodySqlCount = new AtomicLong();
    private static final AtomicLong custodySqlTotalMs = new AtomicLong();
    private static final AtomicLong custodySqlMaxMs = new AtomicLong();
    private static final ConcurrentHashMap<String, CustodyLoad> lastLoadByAccount = new ConcurrentHashMap<>();

    // ---- rechazos ----
    private static final AtomicLong rejectsTotal = new AtomicLong();
    private static final ConcurrentHashMap<String, AtomicLong> rejectsByAccount = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, AtomicLong> rejectsByReason = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedDeque<RejectEvent> recentRejects = new ConcurrentLinkedDeque<>();
    // ConcurrentLinkedDeque.size() es O(n): el tamaño se lleva aparte para poder recortar barato.
    private static final AtomicInteger recentRejectsSize = new AtomicInteger();

    public record CustodyLoad(String account, String source, String reason, long durationMs,
                              int positions, double saldoDisponible, long atMillis) {}

    public record RejectEvent(String account, String reason, String symbol, String orderId,
                              String text, long atMillis) {}

    // ------------------------------------------------------------------
    // registro
    // ------------------------------------------------------------------

    public static void recordCustodyLoad(String account, String source, String reason, long durationMs,
                                         int positions, double saldoDisponible) {
        if (account == null || account.isBlank()) {
            return;
        }

        if (SOURCE_REDIS.equals(source)) {
            custodyFromRedis.incrementAndGet();
        } else if (SOURCE_SQL.equals(source)) {
            custodyFromSql.incrementAndGet();
            custodySqlCount.incrementAndGet();
            custodySqlTotalMs.addAndGet(durationMs);
            custodySqlMaxMs.accumulateAndGet(durationMs, Math::max);
        }

        custodyLoadCount.incrementAndGet();
        custodyLoadTotalMs.addAndGet(durationMs);
        custodyLoadMaxMs.accumulateAndGet(durationMs, Math::max);

        lastLoadByAccount.put(account, new CustodyLoad(account, source, reason, durationMs,
                positions, saldoDisponible, System.currentTimeMillis()));
    }

    public static void recordReject(String account, String symbol, String orderId, String text) {
        String reason = classifyReject(text);
        rejectsTotal.incrementAndGet();
        rejectsByAccount.computeIfAbsent(account == null ? "" : account, k -> new AtomicLong()).incrementAndGet();
        rejectsByReason.computeIfAbsent(reason, k -> new AtomicLong()).incrementAndGet();

        recentRejects.addLast(new RejectEvent(account, reason, symbol, orderId,
                text == null ? "" : text, System.currentTimeMillis()));
        if (recentRejectsSize.incrementAndGet() > MAX_REJECT_EVENTS && recentRejects.pollFirst() != null) {
            recentRejectsSize.decrementAndGet();
        }
    }

    /**
     * Motivo normalizado: el texto del rechazo es libre (viene de la bolsa o del riesgo propio) y
     * agruparlo tal cual dispararía la cardinalidad. Las categorías son las que se pueden accionar.
     */
    static String classifyReject(String text) {
        if (text == null || text.isBlank()) {
            return "SIN_TEXTO";
        }
        String t = text.toLowerCase();
        if (t.contains("sin custodia")) return "SIN_CUSTODIA";
        if (t.contains("símbolo bloqueado") || t.contains("simbolo bloqueado")) return "SIMBOLO_BLOQUEADO";
        if (t.contains("rechazada od") || t.contains("[od]")) return "PROTECCION_OD";
        if (t.contains("nocional") || t.contains("notional")) return "LIMITE_NOCIONAL";
        if (t.contains("riesgo")) return "RIESGO";
        if (t.contains("origclordid") || t.contains("missing from sequence")) return "ORIG_CL_ORD_ID";
        if (t.contains("saldo") || t.contains("cupo")) return "SALDO_INSUFICIENTE";
        return "EXTERNO";
    }

    // ------------------------------------------------------------------
    // lectura
    // ------------------------------------------------------------------

    /** Rechazos en los últimos {@code minutes} minutos. Es la métrica para "rechazos constantes". */
    public static long rejectsLastMinutes(int minutes) {
        long since = System.currentTimeMillis() - Math.max(1, minutes) * 60_000L;
        return recentRejects.stream().filter(r -> r.atMillis() >= since).count();
    }

    public static Map<String, Object> custodySnapshot() {
        long count = custodyLoadCount.get();
        long sqlCount = custodySqlCount.get();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("loadsTotal", count);
        out.put("loadsFromRedis", custodyFromRedis.get());
        out.put("loadsFromSql", custodyFromSql.get());
        out.put("cacheHitRatio", count == 0 ? 0d : round2((double) custodyFromRedis.get() / count));
        out.put("avgLoadMs", count == 0 ? 0L : custodyLoadTotalMs.get() / count);
        out.put("maxLoadMs", custodyLoadMaxMs.get());
        out.put("avgSqlLoadMs", sqlCount == 0 ? 0L : custodySqlTotalMs.get() / sqlCount);
        out.put("maxSqlLoadMs", custodySqlMaxMs.get());
        out.put("accountsTracked", lastLoadByAccount.size());
        return out;
    }

    public static Map<String, Object> rejectsSnapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("total", rejectsTotal.get());
        out.put("last1min", rejectsLastMinutes(1));
        out.put("last5min", rejectsLastMinutes(5));
        out.put("last15min", rejectsLastMinutes(15));
        out.put("last60min", rejectsLastMinutes(60));
        out.put("byReason", toPlainMap(rejectsByReason));
        out.put("topAccounts", topAccounts(rejectsByAccount, 20));
        return out;
    }

    /** Últimos rechazos, del más reciente al más antiguo. */
    public static List<RejectEvent> recentRejects(int limit) {
        List<RejectEvent> all = new ArrayList<>(recentRejects);
        all.sort(Comparator.comparingLong(RejectEvent::atMillis).reversed());
        return all.size() <= limit ? all : all.subList(0, limit);
    }

    public static List<CustodyLoad> custodyLoads() {
        List<CustodyLoad> all = new ArrayList<>(lastLoadByAccount.values());
        all.sort(Comparator.comparingLong(CustodyLoad::durationMs).reversed());
        return all;
    }

    public static CustodyLoad custodyLoad(String account) {
        return account == null ? null : lastLoadByAccount.get(account);
    }

    /** Se llama al invalidar la cache de una cuenta: el próximo load vuelve a medirse desde cero. */
    public static void forgetCustodyLoad(String account) {
        if (account != null) {
            lastLoadByAccount.remove(account);
        }
    }

    private static Map<String, Long> toPlainMap(Map<String, AtomicLong> source) {
        Map<String, Long> out = new LinkedHashMap<>();
        source.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                .forEach(e -> out.put(e.getKey(), e.getValue().get()));
        return out;
    }

    private static List<Map<String, Object>> topAccounts(Map<String, AtomicLong> source, int limit) {
        return source.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                .limit(limit)
                .map(e -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("account", e.getKey());
                    row.put("rejects", e.getValue().get());
                    return row;
                })
                .toList();
    }

    private static double round2(double value) {
        return Math.round(value * 100d) / 100d;
    }
}
