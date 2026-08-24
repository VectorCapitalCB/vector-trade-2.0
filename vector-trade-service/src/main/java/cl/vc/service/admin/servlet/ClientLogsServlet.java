package cl.vc.service.admin.servlet;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.service.MainApp;
import cl.vc.service.admin.ClientLogDiagnosticsStore;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/** Admin API for requesting and reviewing bounded client-side logs. */
@Slf4j
public class ClientLogsServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setCharacterEncoding(StandardCharsets.UTF_8.name());
        String path = normalizePath(req.getPathInfo());
        try {
            if (path.isEmpty()) {
                res.setContentType("application/json;charset=UTF-8");
                JSONArray items = new JSONArray();
                ClientLogDiagnosticsStore.list().forEach(entry -> items.put(entry.toJson()));
                res.getWriter().write(items.toString());
                return;
            }

            String[] parts = path.split("/");
            String requestId = parts[0];
            ClientLogDiagnosticsStore.Entry entry = ClientLogDiagnosticsStore.find(requestId);
            if (entry == null) {
                writeError(res, 404, "Diagnostico no encontrado");
                return;
            }

            String content = ClientLogDiagnosticsStore.readContent(requestId);
            if (parts.length == 2 && "download".equals(parts[1])) {
                res.setContentType("text/plain;charset=UTF-8");
                String filename = (entry.getUsername() + "-" + requestId + ".log")
                        .replaceAll("[^A-Za-z0-9._-]", "_");
                res.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
                res.getWriter().write("===== DIAGNOSTICO VECTOR TRADE =====\n"
                        + "Usuario: " + entry.getUsername() + "\n"
                        + "Equipo: " + entry.getDeviceId() + "\n"
                        + "Version: " + entry.getAppVersion() + "\n"
                        + "Sistema: " + entry.getOs() + "\n"
                        + "Hardware: " + entry.getHardware() + "\n"
                        + "=====================================\n\n"
                        + content);
                return;
            }

            res.setContentType("application/json;charset=UTF-8");
            JSONObject payload = entry.toJson().put("content", content);
            res.getWriter().write(payload.toString());
        } catch (Exception e) {
            log.error("[Admin/ClientLogs] GET error: {}", e.getMessage(), e);
            writeError(res, 500, "No se pudo leer el diagnostico");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String path = normalizePath(req.getPathInfo());
        if (!"request".equals(path)) {
            writeError(res, 404, "Ruta no soportada. Usa POST /api/client-logs/request");
            return;
        }

        try {
            JSONObject body = new JSONObject(req.getReader().lines().collect(Collectors.joining()));
            String username = body.optString("username").trim();
            int minutes = Math.min(Math.max(body.optInt("minutes", 30), 5), 240);
            if (username.isEmpty()) {
                writeError(res, 400, "Selecciona un usuario conectado");
                return;
            }

            ActorRef actor = MainApp.getUserSessionActorsMap().get(username);
            Session session = MainApp.getUserActiveSessionsMap().get(username);
            if (actor == null || session == null || !session.isOpen()) {
                writeError(res, 409, "El usuario ya no esta conectado");
                return;
            }

            ClientLogDiagnosticsStore.Entry entry = ClientLogDiagnosticsStore.createPending(
                    username, minutes, ClientLogDiagnosticsStore.DEFAULT_MAX_BYTES);
            BlotterMessage.ClientLogRequest request = BlotterMessage.ClientLogRequest.newBuilder()
                    .setRequestId(entry.getRequestId())
                    .setUsername(username)
                    .setMinutes(minutes)
                    .setMaxBytes(entry.getMaxBytes())
                    .setRequestedAt(entry.getRequestedAt())
                    .build();
            actor.tell(request, ActorRef.noSender());

            log.info("[Admin/ClientLogs] Diagnostico solicitado usuario={} minutos={} request={}",
                    username, minutes, entry.getRequestId());
            res.setStatus(HttpServletResponse.SC_ACCEPTED);
            res.getWriter().write(entry.toJson().toString());
        } catch (Exception e) {
            log.error("[Admin/ClientLogs] POST error: {}", e.getMessage(), e);
            writeError(res, 500, "No se pudo solicitar el diagnostico: " + e.getMessage());
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String path = normalizePath(req.getPathInfo());
        try {
            if (path.isEmpty()) {
                int deleted = ClientLogDiagnosticsStore.deleteAll();
                res.getWriter().write(new JSONObject().put("deleted", deleted).toString());
                return;
            }

            if (path.contains("/")) {
                writeError(res, 404, "Ruta de eliminacion no soportada");
                return;
            }
            if (!ClientLogDiagnosticsStore.delete(path)) {
                writeError(res, 404, "Diagnostico no encontrado");
                return;
            }
            res.getWriter().write(new JSONObject().put("deleted", 1).toString());
        } catch (Exception e) {
            log.error("[Admin/ClientLogs] DELETE error: {}", e.getMessage(), e);
            writeError(res, 500, "No se pudo eliminar el diagnostico");
        }
    }

    private static String normalizePath(String path) {
        if (path == null || path.isBlank() || "/".equals(path)) return "";
        String value = path.startsWith("/") ? path.substring(1) : path;
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static void writeError(HttpServletResponse res, int status, String message) throws IOException {
        res.setStatus(status);
        res.setContentType("application/json;charset=UTF-8");
        res.getWriter().write(new JSONObject().put("error", message).toString());
    }
}
