package cl.vc.service.admin.servlet;

import cl.vc.service.util.MongoCloseRepository;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reconexion y estado de Mongo (previous-close) desde el Admin.
 *
 * GET  /api/mongo              -> estado (uri con password enmascarado) + estado del job de recarga
 * POST /api/mongo/reconnect    -> relee properties y reconecta en caliente
 * POST /api/mongo/reload       -> recarga de cierres por lotes, secuencial por SecurityType
 *                                 body opcional: { types:["CS","CFI","ETF"], batchSize:250, pauseMs:150 }
 *                                 responde 202 (job en background) o 409 si ya hay una corriendo
 * POST /api/mongo/reload/stop  -> pide detener la recarga en curso
 * POST /api/mongo/symbol       -> recarga UN simbolo: { "symbol":"SQM-B" }
 *
 * La edicion de la config (mongo.connection / mongo.db / collection / mongo.isconnected)
 * se hace por el endpoint existente /api/properties (persiste a disco); luego /reconnect la aplica.
 */
@Slf4j
public class MongoServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            writeJson(res, MongoCloseRepository.status());
        } catch (Exception e) {
            log.error("[Admin/Mongo] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();
        try {
            if (pathInfo == null || "/reconnect".equals(pathInfo) || "/".equals(pathInfo)) {
                writeJson(res, MongoCloseRepository.reconnectFromAdmin());
                return;
            }
            if ("/reload".equals(pathInfo)) {
                JSONObject body = readJson(req);
                List<String> types = readStringArray(body, "types", true);
                List<String> priority = readStringArray(body, "prioritySymbols", true);
                Integer batchSize = body.has("batchSize") ? body.optInt("batchSize") : null;
                Double rate = body.has("ratePerSecond") ? body.optDouble("ratePerSecond") : null;

                Map<String, Object> out =
                        MongoCloseRepository.startReloadFromAdmin(types, priority, batchSize, rate);
                // 409 si ya hay una corriendo, igual que SqlRecoveryServlet; 202 porque el trabajo
                // sigue en background despues de responder.
                if (!Boolean.TRUE.equals(out.get("ok"))) {
                    res.setStatus(MongoCloseRepository.isReloadRunning()
                            ? HttpServletResponse.SC_CONFLICT
                            : HttpServletResponse.SC_BAD_REQUEST);
                } else {
                    res.setStatus(HttpServletResponse.SC_ACCEPTED);
                }
                writeJson(res, out);
                return;
            }
            if ("/reload/stop".equals(pathInfo)) {
                writeJson(res, MongoCloseRepository.stopReloadFromAdmin());
                return;
            }
            if ("/symbol".equals(pathInfo)) {
                String symbol = readJson(req).optString("symbol", "").trim();
                if (symbol.isEmpty()) {
                    res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    res.getWriter().write(new JSONObject().put("error", "Falta 'symbol'.").toString());
                    return;
                }
                writeJson(res, MongoCloseRepository.refreshSymbolFromAdmin(symbol));
                return;
            }
            res.setStatus(404);
            res.getWriter().write(new JSONObject()
                    .put("error", "Ruta no soportada. Usa POST /api/mongo/reconnect, /reload, /reload/stop o /symbol")
                    .toString());
        } catch (Exception e) {
            log.error("[Admin/Mongo] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private void writeJson(HttpServletResponse res, Map<String, Object> payload) throws IOException {
        res.getWriter().write(new JSONObject(payload).toString());
    }

    /**
     * Lee un array de strings del body. Devuelve null si la clave no vino (para que el repositorio
     * aplique su default) y lista vacia si vino explicitamente vacia (para poder desactivar la prioridad).
     */
    private List<String> readStringArray(JSONObject body, String key, boolean upper) {
        JSONArray arr = body.optJSONArray(key);
        if (arr == null) return null;
        List<String> out = new ArrayList<>();
        for (int i = 0; i < arr.length(); i++) {
            String v = arr.optString(i, "").trim();
            if (upper) v = v.toUpperCase();
            if (!v.isEmpty()) out.add(v);
        }
        return out;
    }

    /** Body JSON del request; objeto vacio si no vino cuerpo (mismo criterio que SqlRecoveryServlet). */
    private JSONObject readJson(HttpServletRequest req) throws IOException {
        String body = req.getReader().lines().collect(Collectors.joining());
        return (body == null || body.isBlank()) ? new JSONObject() : new JSONObject(body);
    }
}
