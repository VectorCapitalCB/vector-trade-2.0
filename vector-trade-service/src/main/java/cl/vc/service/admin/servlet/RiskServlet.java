package cl.vc.service.admin.servlet;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
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
 * Gestión de controles de riesgo.
 *
 * GET  /api/risk/blocked-symbols               → lista símbolos bloqueados
 * POST /api/risk/blocked-symbols/block         → bloquea un símbolo  { "symbol": "SQM-B" }
 * POST /api/risk/blocked-symbols/unblock       → desbloquea un símbolo
 *
 * GET  /api/risk/notional-limits               → lista límites de monto nominal por destino
 * POST /api/risk/notional-limits/set           → actualiza un límite { "exchange": "XSGO", "limit": 25000000 }
 */
@Slf4j
public class RiskServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();
        try {
            if ("/notional-limits".equals(pathInfo)) {
                JSONArray arr = new JSONArray();
                MainApp.getNotionalLimits().forEach((exchange, limit) -> {
                    JSONObject obj = new JSONObject();
                    obj.put("exchange", exchange);
                    obj.put("limit", limit);
                    arr.put(obj);
                });
                res.getWriter().write(arr.toString());
                return;
            }
            // Default → blocked-symbols
            JSONArray arr = new JSONArray();
            MainApp.getBlockedSymbols().stream().sorted().forEach(arr::put);
            res.getWriter().write(arr.toString());
        } catch (Exception e) {
            log.error("[Admin/Risk] GET error: {}", e.getMessage(), e);
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

            if ("/notional-limits/set".equals(pathInfo)) {
                String exchangeName = json.getString("exchange").trim().toUpperCase();
                double limit = json.getDouble("limit");
                if (limit <= 0) {
                    res.setStatus(400);
                    res.getWriter().write("{\"error\":\"limit debe ser mayor a 0\"}");
                    return;
                }
                RoutingMessage.SecurityExchangeRouting exchange =
                        RoutingMessage.SecurityExchangeRouting.valueOf(exchangeName);
                MainApp.setNotionalLimit(exchange, limit);
                log.info("[Admin/Risk] Límite nominal actualizado por admin: {} → {}", exchangeName, limit);
                res.getWriter().write(new JSONObject()
                        .put("ok", true)
                        .put("exchange", exchangeName)
                        .put("limit", limit)
                        .toString());
                return;
            }

            String symbol = json.getString("symbol").trim().toUpperCase();
            if (symbol.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"symbol no puede estar vacío\"}");
                return;
            }

            if ("/block".equals(pathInfo) || "/blocked-symbols/block".equals(pathInfo)) {
                MainApp.blockSymbol(symbol);
                log.info("[Admin/Risk] Símbolo bloqueado por admin: {}", symbol);
                res.getWriter().write(new JSONObject()
                        .put("ok", true).put("action", "blocked").put("symbol", symbol).toString());

            } else if ("/unblock".equals(pathInfo) || "/blocked-symbols/unblock".equals(pathInfo)) {
                MainApp.unblockSymbol(symbol);
                log.info("[Admin/Risk] Símbolo desbloqueado por admin: {}", symbol);
                res.getWriter().write(new JSONObject()
                        .put("ok", true).put("action", "unblocked").put("symbol", symbol).toString());

            } else {
                res.setStatus(404);
                res.getWriter().write("{\"error\":\"Path no reconocido\"}");
            }

        } catch (Exception e) {
            log.error("[Admin/Risk] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }
}
