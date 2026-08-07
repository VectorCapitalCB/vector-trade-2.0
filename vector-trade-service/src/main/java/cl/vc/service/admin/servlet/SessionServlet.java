package cl.vc.service.admin.servlet;

import cl.vc.service.MainApp;
import cl.vc.service.admin.SessionDisconnectService;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;

/**
 * Gestión de sesiones WebSocket activas y bloqueo de usuarios.
 *
 * GET  /api/sessions               → lista usuarios conectados con IP y hora de conexión
 * GET  /api/sessions/blocked       → lista usuarios bloqueados
 * POST /api/sessions/disconnect    → desconecta un usuario  { "username": "jperez" }
 * POST /api/sessions/block         → bloquea (y desconecta si en línea) { "username": "jperez" }
 * POST /api/sessions/unblock       → desbloquea un usuario  { "username": "jperez" }
 */
@Slf4j
public class SessionServlet extends HttpServlet {

    private static final DateTimeFormatter FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();

        try {
            if ("/blocked".equals(pathInfo)) {
                // Lista de usuarios bloqueados
                JSONArray arr = new JSONArray();
                MainApp.getBlockedUsers().forEach(arr::put);
                res.getWriter().write(arr.toString());
                return;
            }

            // Lista de sesiones activas (path null, "/" o cualquier otro)
            JSONArray arr = new JSONArray();
            MainApp.getUserActiveSessionsMap().forEach((username, session) -> {
                JSONObject obj = new JSONObject();
                obj.put("username", username);
                obj.put("open", session.isOpen());
                obj.put("remote", session.getRemoteAddress() != null
                        ? session.getRemoteAddress().toString() : "");
                Long connAt = MainApp.getUserSessionConnectedAt().get(username);
                obj.put("connectedAt", connAt != null ? FMT.format(Instant.ofEpochMilli(connAt)) : "");
                obj.put("blocked", MainApp.isUserBlocked(username));
                arr.put(obj);
            });
            res.getWriter().write(arr.toString());
        } catch (Exception e) {
            log.error("[Admin/Sessions] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();

        try {
            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = new JSONObject(body);
            String username = json.getString("username").trim();

            if (username.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"username no puede estar vacío\"}");
                return;
            }

            if ("/disconnect".equals(pathInfo)) {
                try {
                    String msg = SessionDisconnectService.disconnect(username);
                    res.getWriter().write(new JSONObject()
                            .put("ok", true)
                            .put("username", username)
                            .put("message", msg)
                            .toString());
                } catch (SessionDisconnectService.UserNotConnectedException e) {
                    res.setStatus(404);
                    res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
                }
            } else if ("/block".equals(pathInfo)) {
                String msg = SessionDisconnectService.blockUser(username);
                res.getWriter().write(new JSONObject()
                        .put("ok", true)
                        .put("username", username)
                        .put("message", msg)
                        .toString());
            } else if ("/unblock".equals(pathInfo)) {
                SessionDisconnectService.unblockUser(username);
                res.getWriter().write(new JSONObject()
                        .put("ok", true)
                        .put("username", username)
                        .put("message", "Usuario '" + username + "' desbloqueado")
                        .toString());
            } else {
                res.setStatus(404);
                res.getWriter().write("{\"error\":\"Path no reconocido. Use /disconnect, /block o /unblock\"}");
            }

        } catch (Exception e) {
            log.error("[Admin/Sessions] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }
}

