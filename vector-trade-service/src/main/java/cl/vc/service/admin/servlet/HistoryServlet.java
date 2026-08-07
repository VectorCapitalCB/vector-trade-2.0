package cl.vc.service.admin.servlet;

import cl.vc.service.util.MongoHistoryRepository;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

/**
 * Historico multi-dia de ordenes y ejecuciones desde el Admin.
 *
 * GET  /api/history                 -> estado del repositorio (cola, escritos, descartados)
 * GET  /api/history/executions      -> ?account=X[&symbol=Y][&from=yyyy-MM-dd][&to=yyyy-MM-dd][&skip=0][&limit=200]
 * GET  /api/history/orders          -> mismos parametros
 * POST /api/history/reconnect       -> relee properties y reconecta en caliente
 *
 * Existe para validar por HTTP que la escritura quedo bien antes de construir la vista del blotter,
 * que necesita mensajes protobuf nuevos en principal-module.
 */
@Slf4j
public class HistoryServlet extends HttpServlet {

    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT = 5000;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();
        try {
            if (pathInfo == null || "/".equals(pathInfo)) {
                res.getWriter().write(new JSONObject(MongoHistoryRepository.status()).toString());
                return;
            }

            boolean isExecutions = "/executions".equals(pathInfo);
            if (!isExecutions && !"/orders".equals(pathInfo)) {
                res.setStatus(404);
                res.getWriter().write(new JSONObject()
                        .put("error", "Ruta no soportada. Usa /api/history, /api/history/executions o /api/history/orders")
                        .toString());
                return;
            }

            String account = req.getParameter("account");
            if (account == null || account.isBlank()) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "falta el parametro 'account'").toString());
                return;
            }

            LocalDate from = parseDate(req.getParameter("from"));
            LocalDate to = parseDate(req.getParameter("to"));
            int skip = parseInt(req.getParameter("skip"), 0);
            int limit = Math.min(parseInt(req.getParameter("limit"), DEFAULT_LIMIT), MAX_LIMIT);
            String symbol = req.getParameter("symbol");

            List<Document> rows = isExecutions
                    ? MongoHistoryRepository.queryExecutions(account, symbol, from, to, skip, limit)
                    : MongoHistoryRepository.queryOrders(account, symbol, from, to, skip, limit);

            JSONArray items = new JSONArray();
            rows.forEach(d -> items.put(new JSONObject(d.toJson())));

            res.getWriter().write(new JSONObject()
                    .put("account", account)
                    .put("count", items.length())
                    .put("skip", skip)
                    .put("limit", limit)
                    .put("items", items)
                    .toString());

        } catch (IllegalArgumentException e) {
            res.setStatus(400);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        } catch (Exception e) {
            log.error("[Admin/History] GET error: {}", e.getMessage(), e);
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
                res.getWriter().write(new JSONObject(MongoHistoryRepository.reconnectFromAdmin()).toString());
                return;
            }
            res.setStatus(404);
            res.getWriter().write(new JSONObject()
                    .put("error", "Ruta no soportada. Usa POST /api/history/reconnect")
                    .toString());
        } catch (Exception e) {
            log.error("[Admin/History] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private static LocalDate parseDate(String raw) {
        if (raw == null || raw.isBlank()) return null;
        try {
            return LocalDate.parse(raw.trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("fecha invalida '" + raw + "', se espera yyyy-MM-dd");
        }
    }

    private static int parseInt(String raw, int fallback) {
        if (raw == null || raw.isBlank()) return fallback;
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
