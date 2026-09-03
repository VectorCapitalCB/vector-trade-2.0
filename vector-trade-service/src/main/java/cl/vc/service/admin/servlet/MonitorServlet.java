package cl.vc.service.admin.servlet;

import cl.vc.service.MainApp;
import cl.vc.service.admin.AccountLoadTracker;
import cl.vc.service.admin.MonitorMetrics;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Telemetría del servicio para MONITOR-VC. Va detrás de AdminAuthFilter: requiere
 * {@code Authorization: Bearer <admin.token>}.
 *
 * <pre>
 * GET    /api/monitor/health              resumen para semáforo y reglas de alerta
 * GET    /api/monitor/custodias           una fila por cuenta: fuente, latencia, posiciones, saldo
 * GET    /api/monitor/custodias?account=X una sola cuenta
 * GET    /api/monitor/rejects             contadores + ventanas de 1/5/15/60 min
 * GET    /api/monitor/rejects/recent      últimos rechazos con motivo y texto
 * </pre>
 *
 * <p>NOTA 2.0: el endpoint DELETE /api/monitor/custodia de produccion no se porto: depende de la
 * cache de custodia (PatrimonioBase, generaciones y suspension de escrituras) que el 2.0 todavia
 * no tiene. El resto es identico a produccion.
 *
 * <p>Reemplaza el scraping de logs de Docker que hacía MONITOR-VC: los regex sobre
 * {@code [Actors] Usuario n/N ... inicializando cuenta} dejaron de servir para medir tiempos
 * cuando la carga de cuentas pasó a ser paralela.
 */
@Slf4j
public class MonitorServlet extends HttpServlet {

    /** Umbral por defecto para marcar "rechazos constantes" en el semáforo. */
    private static final int REJECT_WARN_LAST_5MIN = 20;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String path = req.getPathInfo() == null ? "/" : req.getPathInfo();

        try {
            switch (path) {
                case "/", "/health" -> write(res, health());
                case "/custodias" -> writeRaw(res, custodias(req.getParameter("account")));
                case "/rejects" -> write(res, MonitorMetrics.rejectsSnapshot());
                case "/rejects/recent" -> writeRaw(res, recentRejects(parseLimit(req.getParameter("limit"), 100)));
                default -> {
                    res.setStatus(404);
                    res.getWriter().write(new JSONObject()
                            .put("error", "Ruta no soportada")
                            .put("rutas", new JSONArray(new String[]{
                                    "/api/monitor/health", "/api/monitor/custodias",
                                    "/api/monitor/rejects", "/api/monitor/rejects/recent"}))
                            .toString());
                }
            }
        } catch (Exception e) {
            log.error("[Admin/Monitor] GET {} error: {}", path, e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", String.valueOf(e.getMessage())).toString());
        }
    }

    /** Invalida la custodia de una cuenta en Redis. Acción destructiva: exige cuenta explícita. */

    // ------------------------------------------------------------------
    // payloads
    // ------------------------------------------------------------------

    /** Todo lo que el semáforo y las reglas de alerta necesitan, en una sola llamada. */
    private Map<String, Object> health() {
        Map<String, Object> custody = MonitorMetrics.custodySnapshot();
        Map<String, Object> rejects = MonitorMetrics.rejectsSnapshot();
        Map<String, Object> redis = MainApp.getRedisStatusSnapshot();

        long rejects5min = ((Number) rejects.get("last5min")).longValue();
        boolean redisOk = Boolean.TRUE.equals(redis.get("connected"));
        boolean sqlOk = !MainApp.requiereCreasys || cl.vc.service.util.SQLServerConnection.connection != null;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("service", "vector-trade-service");
        out.put("environment", MainApp.getProperties().getProperty("ENVIRONMENT", ""));
        out.put("atMillis", System.currentTimeMillis());

        out.put("status", redisOk && sqlOk && rejects5min < REJECT_WARN_LAST_5MIN ? "OK" : "WARN");
        out.put("redisConnected", redisOk);
        out.put("sqlConnected", sqlOk);
        out.put("requiereCreasys", MainApp.requiereCreasys);
        out.put("redisPersistenceEnabled",
                Boolean.parseBoolean(MainApp.getProperties().getProperty("redis.enable.persistencia", "false")));

        out.put("accountActors", MainApp.getAccountGroupUser().size());
        out.put("custody", custody);
        out.put("rejects", rejects);
        out.put("redis", redis);
        out.put("accountLoadCycle", loadCycle());
        return out;
    }

    /** Estado del ciclo de carga de cuentas, que es lo que MONITOR-VC sacaba de los logs. */
    private Map<String, Object> loadCycle() {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            AccountLoadTracker.CycleSnapshot active = MainApp.getAccountLoadTracker().getActiveCycleSnapshot();
            out.put("active", active != null);
            if (active != null) {
                out.put("trigger", active.getTrigger());
                out.put("phase", active.getPhase());
                out.put("totalUsers", active.getTotalUsers());
                out.put("processedUsers", active.getProcessedUsers());
                out.put("declaredAccounts", active.getDeclaredAccounts());
                out.put("initializedAccounts", active.getInitializedAccounts());
                out.put("failedInitializations", active.getFailedAccountInitializations());
                out.put("startedAt", active.getStartedAt());
                out.put("etaMs", active.getEtaMs());
            }
        } catch (Exception e) {
            out.put("error", String.valueOf(e.getMessage()));
        }
        return out;
    }

    private JSONObject custodias(String account) {
        JSONArray rows = new JSONArray();

        if (account != null && !account.isBlank()) {
            MonitorMetrics.CustodyLoad load = MonitorMetrics.custodyLoad(account.trim());
            if (load != null) {
                rows.put(custodyRow(load));
            }
        } else {
            MonitorMetrics.custodyLoads().forEach(load -> rows.put(custodyRow(load)));
        }

        return new JSONObject()
                .put("count", rows.length())
                .put("summary", new JSONObject(MonitorMetrics.custodySnapshot()))
                .put("accounts", rows);
    }

    private JSONObject custodyRow(MonitorMetrics.CustodyLoad load) {
        double[] status = MainApp.getAccountCustodyStatus().get(load.account());
        return new JSONObject()
                .put("account", load.account())
                .put("source", load.source())
                .put("reason", load.reason())
                .put("durationMs", load.durationMs())
                .put("positions", load.positions())
                .put("saldoDisponible", load.saldoDisponible())
                .put("atMillis", load.atMillis())
                .put("ageSeconds", (System.currentTimeMillis() - load.atMillis()) / 1000L)
                // El estado publicado por el actor: sirve para cruzar contra lo que se midió.
                .put("custodyPositions", status == null ? -1 : (int) status[0])
                .put("custodyUpdatedAtMillis", status == null ? 0L : (long) status[2]);
    }

    private JSONObject recentRejects(int limit) {
        JSONArray rows = new JSONArray();
        MonitorMetrics.recentRejects(limit).forEach(r -> rows.put(new JSONObject()
                .put("account", r.account())
                .put("reason", r.reason())
                .put("symbol", r.symbol())
                .put("orderId", r.orderId())
                .put("text", r.text())
                .put("atMillis", r.atMillis())));
        return new JSONObject().put("count", rows.length()).put("rejects", rows);
    }

    private int parseLimit(String raw, int fallback) {
        try {
            return Math.max(1, Math.min(1000, Integer.parseInt(raw)));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private void write(HttpServletResponse res, Map<String, Object> payload) throws IOException {
        res.getWriter().write(new JSONObject(payload).toString());
    }

    private void writeRaw(HttpServletResponse res, JSONObject payload) throws IOException {
        res.getWriter().write(payload.toString());
    }
}
