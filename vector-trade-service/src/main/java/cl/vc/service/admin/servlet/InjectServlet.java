package cl.vc.service.admin.servlet;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Feature 5 — Inyección de mensajes del sellside.
 *
 * POST /api/inject
 * Body:
 * {
 *   "type": "ORDER" | "CANCEL_REJECT",
 *   "payload": { ... campos del mensaje Protobuf ... }
 * }
 *
 * El mensaje se publica en el MessageEventBus exactamente igual que si llegara
 * del sellside real, disparando los handlers en ActorGroupPerAccount / ActorPerSession.
 *
 * Tipos soportados:
 *   ORDER        → RoutingMessage.Order  (campos: id, symbol, account, quantity, price,
 *                                         side, status, exchange, executedQty, leavesQty,
 *                                         text, strategyOrder, ordType)
 *   CANCEL_REJECT → RoutingMessage.OrderCancelReject (campos: id, account, text)
 */
@Slf4j
public class InjectServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");

        try {
            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = new JSONObject(body);

            String     type    = json.getString("type").toUpperCase();
            JSONObject payload = json.getJSONObject("payload");

            switch (type) {
                case "ORDER"        -> injectOrder(payload, res);
                case "ORDER_BURST_TEST" -> injectOrderBurst(payload, res);
                case "CANCEL_REJECT"-> injectCancelReject(payload, res);
                default -> {
                    res.setStatus(400);
                    res.getWriter().write(
                            "{\"error\":\"Tipo no soportado: " + type
                            + ". Tipos válidos: ORDER, ORDER_BURST_TEST, CANCEL_REJECT\"}"
                    );
                }
            }

        } catch (Exception e) {
            log.error("[Admin/Inject] Error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────

    private void injectOrder(JSONObject p, HttpServletResponse res) throws IOException {
        RoutingMessage.Order.Builder builder = RoutingMessage.Order.newBuilder();

        if (p.has("id"))          builder.setId(p.getString("id"));
        if (p.has("symbol"))      builder.setSymbol(p.getString("symbol"));
        if (p.has("account"))     builder.setAccount(p.getString("account"));
        if (p.has("orderQty"))    builder.setOrderQty(p.getDouble("orderQty"));
        if (p.has("quantity"))    builder.setOrderQty(p.getDouble("quantity"));
        if (p.has("price"))       builder.setPrice(p.getDouble("price"));
        if (p.has("cumQty"))      builder.setCumQty(p.getDouble("cumQty"));
        if (p.has("executedQty")) builder.setCumQty(p.getDouble("executedQty"));
        if (p.has("leaves"))      builder.setLeaves(p.getDouble("leaves"));
        if (p.has("leavesQty"))   builder.setLeaves(p.getDouble("leavesQty"));
        if (p.has("text"))        builder.setText(p.getString("text"));
        if (p.has("execId"))      builder.setExecId(p.getString("execId"));
        if (p.has("lastPx"))      builder.setLastPx(p.getDouble("lastPx"));
        if (p.has("lastQty"))     builder.setLastQty(p.getDouble("lastQty"));
        if (p.has("avgPrice"))    builder.setAvgPrice(p.getDouble("avgPrice"));

        // Sin execType=EXEC_TRADE el actor no lo trata como fill y no llega al historico de ejecuciones.
        if (p.has("execType"))
            builder.setExecType(RoutingMessage.ExecutionType.valueOf(p.getString("execType").toUpperCase()));

        // 'time' define la fecha con la que se indexa el historico; por defecto, ahora.
        java.time.Instant when = p.has("time")
                ? java.time.Instant.parse(p.getString("time"))
                : java.time.Instant.now();
        builder.setTime(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(when.getEpochSecond())
                .setNanos(when.getNano())
                .build());

        if (p.has("side"))
            builder.setSide(RoutingMessage.Side.valueOf(p.getString("side").toUpperCase()));
        if (p.has("status"))
            builder.setOrdStatus(RoutingMessage.OrderStatus.valueOf(p.getString("status").toUpperCase()));
        if (p.has("ordStatus"))
            builder.setOrdStatus(RoutingMessage.OrderStatus.valueOf(p.getString("ordStatus").toUpperCase()));
        if (p.has("exchange"))
            builder.setSecurityExchange(
                    RoutingMessage.SecurityExchangeRouting.valueOf(p.getString("exchange").toUpperCase()));
        if (p.has("strategyOrder"))
            builder.setStrategyOrder(
                    RoutingMessage.StrategyOrder.valueOf(p.getString("strategyOrder").toUpperCase()));
        if (p.has("ordType"))
            builder.setOrdType(RoutingMessage.OrdType.valueOf(p.getString("ordType").toUpperCase()));

        RoutingMessage.Order order = builder.build();
        MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));

        log.info("[Admin/Inject] ORDER inyectada: id={} symbol={} account={} status={}",
                order.getId(), order.getSymbol(), order.getAccount(), order.getOrdStatus().name());

        res.getWriter().write(
                "{\"ok\":true,\"type\":\"ORDER\",\"id\":\"" + order.getId() + "\"}"
        );
    }

    private void injectCancelReject(JSONObject p, HttpServletResponse res) throws IOException {
        RoutingMessage.OrderCancelReject.Builder builder = RoutingMessage.OrderCancelReject.newBuilder();

        if (p.has("id"))      builder.setId(p.getString("id"));
        if (p.has("text"))    builder.setText(p.getString("text"));

        RoutingMessage.OrderCancelReject reject = builder.build();
        MainApp.getMessageEventBus().publish(new Envelope(reject.getId(), reject));

        log.info("[Admin/Inject] CANCEL_REJECT inyectado: id={}", reject.getId());

        res.getWriter().write(
                "{\"ok\":true,\"type\":\"CANCEL_REJECT\",\"id\":\"" + reject.getId() + "\"}"
        );
    }

    private void injectOrderBurst(JSONObject p, HttpServletResponse res) throws IOException {
        if (!Boolean.parseBoolean(MainApp.getProperties()
                .getProperty("admin.inject.order-burst.enabled", "false"))) {
            res.setStatus(403);
            res.getWriter().write("{\"error\":\"ORDER_BURST_TEST deshabilitado por properties\"}");
            return;
        }

        String account = p.getString("account").trim();
        String symbol = p.optString("symbol", "CENCOSUD").trim().toUpperCase();
        int count = Math.max(2, Math.min(p.optInt("count", 200), 1_000));
        int intervalMs = Math.max(0, Math.min(p.optInt("intervalMs", 25), 5_000));
        double qtyPerFill = p.optDouble("qtyPerFill", 1_000d);
        double price = p.optDouble("price", 1_959.6d);

        if (account.isEmpty() || symbol.isEmpty() || qtyPerFill <= 0d || price <= 0d) {
            res.setStatus(400);
            res.getWriter().write("{\"error\":\"account, symbol, qtyPerFill y price deben ser válidos\"}");
            return;
        }

        ActorRef accountActor = MainApp.getAccountGroupUser().get(account);
        if (accountActor == null) {
            res.setStatus(404);
            res.getWriter().write("{\"error\":\"La cuenta no tiene actor inicializado en el core\"}");
            return;
        }

        String orderId = "SIM-BURST-" + System.currentTimeMillis();
        double totalQty = count * qtyPerFill;
        RoutingMessage.Order base = RoutingMessage.Order.newBuilder()
                .setId(orderId)
                .setOrderID(orderId)
                .setClOrdId(orderId + "-R")
                .setAccount(account)
                .setSymbol(symbol)
                .setSecurityExchange(RoutingMessage.SecurityExchangeRouting.XSGO)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSide(RoutingMessage.Side.SELL)
                .setOrdType(RoutingMessage.OrdType.LIMIT)
                .setStrategyOrder(RoutingMessage.StrategyOrder.NONE_STRATEGY)
                .setOrderQty(totalQty)
                .setPrice(price)
                .build();

        Thread playback = new Thread(() -> playOrderBurst(accountActor, base, count, qtyPerFill,
                price, intervalMs), "admin-order-burst-" + orderId);
        playback.setDaemon(true);
        playback.start();

        log.warn("[Admin/Inject][NO_ROUTING_BURST] inicio cuenta={} symbol={} orderId={} fills={} qtyTotal={} intervalMs={}",
                account, symbol, orderId, count, totalQty, intervalMs);
        res.getWriter().write("{\"ok\":true,\"type\":\"ORDER_BURST_TEST\",\"id\":\""
                + orderId + "\",\"fills\":" + count + ",\"qtyTotal\":" + totalQty + "}");
    }

    private void playOrderBurst(ActorRef accountActor,
                                RoutingMessage.Order base,
                                int count,
                                double qtyPerFill,
                                double price,
                                int intervalMs) {
        try {
            double totalQty = count * qtyPerFill;
            RoutingMessage.Order malformedReplace = base.toBuilder()
                    .setExecId(base.getId() + "-REPLACE")
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED)
                    .setOrdStatus(RoutingMessage.OrderStatus.FILLED)
                    .setCumQty(0d)
                    .setLeaves(totalQty)
                    .build();
            accountActor.tell(malformedReplace, ActorRef.noSender());

            int half = count / 2;
            for (int arrival = 1; arrival <= count; arrival++) {
                int cumulativeIndex;
                if (arrival <= half) {
                    cumulativeIndex = arrival;
                } else if (arrival == half + 1) {
                    cumulativeIndex = count;
                } else {
                    cumulativeIndex = arrival - 1;
                }

                double cumQty = cumulativeIndex * qtyPerFill;
                RoutingMessage.OrderStatus status = cumulativeIndex == count
                        ? RoutingMessage.OrderStatus.FILLED
                        : RoutingMessage.OrderStatus.PARTIALLY_FILLED;
                RoutingMessage.Order fill = base.toBuilder()
                        .setExecId(base.getId() + "-F" + String.format("%04d", arrival))
                        .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                        .setOrdStatus(status)
                        .setLastQty(qtyPerFill)
                        .setLastPx(price)
                        .setCumQty(cumQty)
                        .setLeaves(Math.max(0d, totalQty - cumQty))
                        .build();
                accountActor.tell(fill, ActorRef.noSender());

                if (intervalMs > 0) {
                    Thread.sleep(intervalMs);
                }
            }
            log.warn("[Admin/Inject][NO_ROUTING_BURST] completado cuenta={} symbol={} orderId={} fills={} qtyTotal={}",
                    base.getAccount(), base.getSymbol(), base.getId(), count, totalQty);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[Admin/Inject][NO_ROUTING_BURST] interrumpido orderId={}", base.getId());
        } catch (Exception e) {
            log.error("[Admin/Inject][NO_ROUTING_BURST] error orderId={}", base.getId(), e);
        }
    }
}
