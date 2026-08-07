package cl.vc.service.admin.servlet;

import cl.vc.service.util.MongoCloseRepository;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Reconexion y estado de Mongo (previous-close) desde el Admin.
 *
 * GET  /api/mongo            -> estado (uri con password enmascarado)
 * POST /api/mongo/reconnect  -> relee properties y reconecta en caliente
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
            res.setStatus(404);
            res.getWriter().write(new JSONObject()
                    .put("error", "Ruta no soportada. Usa POST /api/mongo/reconnect")
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
}
