package cl.vc.service.admin.servlet;

import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Recuperación controlada de Redis desde el Admin.
 *
 * GET  /api/redis
 * POST /api/redis/reconnect
 */
@Slf4j
public class RedisServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            writeJson(res, MainApp.getRedisStatusSnapshot());
        } catch (Exception e) {
            log.error("[Admin/Redis] GET error: {}", e.getMessage(), e);
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
                writeJson(res, MainApp.reconnectRedisFromAdmin());
                return;
            }

            res.setStatus(404);
            res.getWriter().write(new JSONObject()
                    .put("error", "Ruta no soportada. Usa POST /api/redis/reconnect")
                    .toString());
        } catch (Exception e) {
            log.error("[Admin/Redis] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private void writeJson(HttpServletResponse res, Map<String, Object> payload) throws IOException {
        res.getWriter().write(new JSONObject(payload).toString());
    }
}
