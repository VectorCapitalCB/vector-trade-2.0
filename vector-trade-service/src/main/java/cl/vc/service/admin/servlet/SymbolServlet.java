package cl.vc.service.admin.servlet;

import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
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
 * Feature 1 — Gestión de suscripciones de Market Data.
 *
 * GET  /api/symbols              → todos los símbolos conocidos por exchange
 * GET  /api/symbols/subscriptions → suscripciones activas con estado de snapshot
 * POST /api/symbols/subscribe    → fuerza re-suscripción de un símbolo
 * POST /api/symbols/unsubscribe  → elimina la suscripción activa
 */
@Slf4j
public class SymbolServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();

        try {
            if ("/subscriptions".equals(pathInfo)) {
                JSONArray arr = new JSONArray();
                MainApp.getIdSymbolsSubscrib().forEach((id, sub) -> {
                    JSONObject obj = new JSONObject();
                    obj.put("id",          id);
                    obj.put("symbol",      sub.getSymbol());
                    obj.put("exchange",    sub.getSecurityExchange().name());
                    obj.put("settlType",   sub.getSettlType().name());
                    obj.put("securityType", sub.getSecurityType().name());
                    obj.put("hasSnapshot", MainApp.getSnapshotHashMap().containsKey(id));
                    arr.put(obj);
                });
                res.getWriter().write(arr.toString());
            } else {
                // Lista de todos los símbolos disponibles (SecurityList por exchange)
                JSONArray arr = new JSONArray();

                // Desde el mapa de símbolos individuales
                MainApp.getSecurityExchangeSymbolsMaps().forEach((symbol, security) -> {
                    JSONObject obj = new JSONObject();
                    obj.put("symbol",       security.getSymbol());
                    obj.put("exchange",     security.getSecurityExchange().name());
                    obj.put("currency",     security.getCurrency());
                    obj.put("securityType", security.getSecurityType());
                    arr.put(obj);
                });

                // Desde los SecurityList por exchange (pueden tener símbolos no en el mapa individual)
                MainApp.getSecurityExchangeMaps().forEach((exch, list) ->
                    list.getListSecuritiesList().forEach(s -> {
                        if (!MainApp.getSecurityExchangeSymbolsMaps().containsKey(s.getSymbol())) {
                            JSONObject obj = new JSONObject();
                            obj.put("symbol",       s.getSymbol());
                            obj.put("exchange",     s.getSecurityExchange().name());
                            obj.put("currency",     s.getCurrency());
                            obj.put("securityType", s.getSecurityType());
                            arr.put(obj);
                        }
                    })
                );

                res.getWriter().write(arr.toString());
            }
        } catch (Exception e) {
            log.error("[Admin/Symbols] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo(); // /subscribe o /unsubscribe

        try {
            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = new JSONObject(body);

            String symbol      = json.getString("symbol").trim().toUpperCase();
            String exchangeStr = json.getString("exchange").trim().toUpperCase();
            RoutingMessage.SettlType settlType = RoutingMessage.SettlType.valueOf(
                    json.optString("settlType", "T2").trim().toUpperCase());
            RoutingMessage.SecurityType securityType = RoutingMessage.SecurityType.valueOf(
                    json.optString("securityType", "CS").trim().toUpperCase());

            MarketDataMessage.SecurityExchangeMarketData exchange =
                    MarketDataMessage.SecurityExchangeMarketData.valueOf(exchangeStr);

            String id = json.optString("id", "").trim();
            if (id.isEmpty()) {
                id = TopicGenerator.getTopicMKD(symbol, exchange, settlType, securityType);
            }

            if ("/unsubscribe".equals(pathInfo)) {
                MainApp.getIdSymbolsSubscrib().remove(id);
                MainApp.getSnapshotHashMap().remove(id);
                log.info("[Admin/Symbols] Desuscripción de {} en {} {}", symbol, exchangeStr, settlType);
                res.getWriter().write("{\"ok\":true,\"action\":\"unsubscribed\",\"id\":\"" + id + "\"}");

            } else {
                // /subscribe (o cualquier otro path) → fuerza re-suscripción
                // Remover del mapa para que subscribeSymbol lo reenvíe aunque ya existiera
                MainApp.getIdSymbolsSubscrib().remove(id);
                MainApp.getSnapshotHashMap().remove(id);

                MarketDataMessage.Subscribe subscribe = MarketDataMessage.Subscribe.newBuilder()
                        .setSymbol(symbol)
                        .setSecurityExchange(exchange)
                        .setSettlType(settlType)
                        .setSecurityType(securityType)
                        .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                        .setBook(true)
                        .setStatistic(true)
                        .setTrade(true)
                        .build();

                id = TopicGenerator.getTopicMKD(subscribe);
                subscribe = subscribe.toBuilder().setId(id).build();
                MainApp.getIdSymbolsSubscrib().remove(id);
                MainApp.getSnapshotHashMap().remove(id);

                MainApp.subscribeSymbol(subscribe, id);
                log.info("[Admin/Symbols] Re-suscripción forzada de {} en {} {}", symbol, exchangeStr, settlType);
                res.getWriter().write("{\"ok\":true,\"action\":\"subscribed\",\"id\":\"" + id + "\"}");
            }

        } catch (Exception e) {
            log.error("[Admin/Symbols] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }
}
