package cl.vc.service.admin.servlet;

import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * API REST para gestión de seguridad por IP.
 *
 * GET  /api/security/status          → estado del sistema (modo protección, contadores)
 * GET  /api/security/blocked-ips     → lista IPs bloqueadas
 * POST /api/security/block-ip        → bloquear IP manualmente  { "ip": "1.2.3.4" }
 * POST /api/security/unblock-ip      → desbloquear IP           { "ip": "1.2.3.4" }
 * POST /api/security/reset-protection → resetear modo protección
 */
@Slf4j
public class IpSecurityServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();
        try {
            if ("/status".equals(pathInfo)) {
                JSONObject obj = new JSONObject();
                obj.put("protectionMode",     MainApp.getIpRateLimiter().isProtectionModeActive());
                obj.put("autoBlockEvents",    MainApp.getIpRateLimiter().getAutoBlockEventCount());
                obj.put("trackedIps",         MainApp.getIpRateLimiter().getTrackedIpCount());
                obj.put("blockedIpsCount",    MainApp.getBlockedIps().size());
                obj.put("maxMessagesPerSec",  MainApp.getIpRateLimiter().getMaxMessagesPerSecond());
                obj.put("maxOrdersPerSec",    MainApp.getIpRateLimiter().getMaxOrdersPerSecond());
                obj.put("autoBlockThreshold", MainApp.getIpRateLimiter().getAutoBlockThreshold());
                res.getWriter().write(obj.toString());
                return;
            }
            // Default → blocked-ips
            JSONArray arr = new JSONArray();
            MainApp.getBlockedIps().stream().sorted().forEach(arr::put);
            res.getWriter().write(arr.toString());
        } catch (Exception e) {
            log.error("[Admin/IpSecurity] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();
        try {
            if ("/reset-protection".equals(pathInfo)) {
                MainApp.getIpRateLimiter().resetProtectionMode();
                log.info("[Admin/IpSecurity] Modo protección reseteado por admin");
                res.getWriter().write("{\"ok\":true,\"action\":\"reset-protection\"}");
                return;
            }

            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = new JSONObject(body);
            String ip = json.getString("ip").trim();

            if (ip.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"ip no puede estar vacía\"}");
                return;
            }

            if ("/block-ip".equals(pathInfo)) {
                MainApp.blockIp(ip);
                log.info("[Admin/IpSecurity] IP bloqueada manualmente por admin: {}", ip);
                res.getWriter().write(new JSONObject()
                        .put("ok", true).put("action", "blocked").put("ip", ip).toString());

            } else if ("/unblock-ip".equals(pathInfo)) {
                MainApp.unblockIp(ip);
                log.info("[Admin/IpSecurity] IP desbloqueada por admin: {}", ip);
                res.getWriter().write(new JSONObject()
                        .put("ok", true).put("action", "unblocked").put("ip", ip).toString());

            } else {
                res.setStatus(404);
                res.getWriter().write("{\"error\":\"Path no reconocido\"}");
            }

        } catch (Exception e) {
            log.error("[Admin/IpSecurity] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }
}
