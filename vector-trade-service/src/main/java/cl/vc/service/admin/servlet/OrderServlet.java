package cl.vc.service.admin.servlet;

import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Feature 4 — Reinyección de órdenes al frontend.
 *
 * GET  /api/orders?account={acct}  → lista órdenes del Redis para una cuenta
 * GET  /api/orders                 → lista todas las órdenes (todas las cuentas)
 * POST /api/orders/resend          → re-publica desde Redis → MessageEventBus
 *   body: { "account": "12345/6", "orderIds": ["id1","id2"] }
 * POST /api/orders/resend-log      → parsea líneas de log y reenvía
 *   body: { "lines": "[2026-05-06 18:31:44,596] Order : {...}\n[...] Order : {...}" }
 */
@Slf4j
public class OrderServlet extends HttpServlet {

    /** Prefijo de timestamp de las líneas de log: [2026-06-04 10:40:27,711] (zona local). */
    private static final Pattern LOG_TS = Pattern.compile("^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})]");

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String account = req.getParameter("account");

        try {
            JSONArray arr = new JSONArray();

            if (account != null && !account.isBlank()) {
                HashMap<String, RoutingMessage.Order> orders = MainApp.getOrdersMapRedis().get(account);
                if (orders != null) {
                    for (Map.Entry<String, RoutingMessage.Order> e : orders.entrySet()) {
                        arr.put(orderToJson(e.getValue()));
                    }
                }
            } else {
                MainApp.getOrdersMapRedis().forEach((acct, orders) -> {
                    if (orders != null) {
                        for (Map.Entry<String, RoutingMessage.Order> e : orders.entrySet()) {
                            arr.put(orderToJson(e.getValue()));
                        }
                    }
                });
            }

            res.getWriter().write(arr.toString());

        } catch (Exception e) {
            log.error("[Admin/Orders] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");

        String pathInfo = req.getPathInfo() == null ? "" : req.getPathInfo();

        try {
            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = new JSONObject(body);

            if ("/resend-log".equals(pathInfo)) {
                // ── Parsear líneas de log y reenviar (buyside + sellside) ─────
                String lines = json.getString("lines");
                // Matches: NewOrderRequest, OrderReplaceRequest, OrderCancelRequest, Order
                Pattern p = Pattern.compile("(NewOrderRequest|OrderReplaceRequest|OrderCancelRequest|Order)\\s*:\\s*(\\{.+\\})");
                List<String> sent   = new ArrayList<>();
                List<String> errors = new ArrayList<>();

                for (String line : lines.split("\\r?\\n")) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    Matcher m = p.matcher(line);
                    if (!m.find()) { errors.add("Línea no reconocida: " + line.substring(0, Math.min(60, line.length()))); continue; }
                    String msgType = m.group(1);
                    // Timestamp del prefijo de la línea de log (respaldo si falta el 'time' del mensaje).
                    Matcher tsm = LOG_TS.matcher(line);
                    String logTs = tsm.find() ? tsm.group(1) : null;
                    try {
                        JSONObject payload = new JSONObject(m.group(2));
                        switch (msgType) {
                            case "Order": {
                                RoutingMessage.Order order = buildOrderFromJson(payload, logTs);
                                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));
                                sent.add("[Sellside/Order] " + order.getId());
                                log.info("[Admin/Orders] Sellside-resend → id={} account={} symbol={} status={}",
                                        order.getId(), order.getAccount(), order.getSymbol(), order.getOrdStatus());
                                break;
                            }
                            case "NewOrderRequest": {
                                JSONObject orderJson = payload.getJSONObject("order");
                                RoutingMessage.Order order = buildOrderFromJson(orderJson, logTs);
                                RoutingMessage.NewOrderRequest newOrderReq = RoutingMessage.NewOrderRequest.newBuilder()
                                        .setOrder(order).build();
                                akka.actor.ActorRef actor = MainApp.getAccountGroupUser().get(order.getAccount());
                                if (actor == null) throw new IllegalStateException("No actor para cuenta " + order.getAccount());
                                actor.tell(newOrderReq, akka.actor.ActorRef.noSender());
                                sent.add("[Buyside/NewOrder] " + order.getId() + " acct=" + order.getAccount());
                                log.info("[Admin/Orders] Buyside NewOrderRequest → id={} account={} symbol={}",
                                        order.getId(), order.getAccount(), order.getSymbol());
                                break;
                            }
                            case "OrderReplaceRequest": {
                                String id = payload.getString("id");
                                akka.actor.ActorRef actor = findActorByOrderId(id);
                                if (actor == null) throw new IllegalStateException("No se encontró cuenta para order id=" + id);
                                RoutingMessage.OrderReplaceRequest replaceReq = RoutingMessage.OrderReplaceRequest.newBuilder()
                                        .setId(id)
                                        .setQuantity(payload.optDouble("quantity", 0))
                                        .setPrice(payload.optDouble("price", 0))
                                        .setLimit(payload.optDouble("limit", 0))
                                        .setSpread(payload.optDouble("spread", 0))
                                        .setMaxFloor(payload.optDouble("maxFloor", 0))
                                        .setIcebergPercentage(payload.optString("icebergPercentage", ""))
                                        .setAmount(payload.optDouble("amount", 0))
                                        .build();
                                actor.tell(replaceReq, akka.actor.ActorRef.noSender());
                                sent.add("[Buyside/Replace] " + id);
                                log.info("[Admin/Orders] Buyside OrderReplaceRequest → id={}", id);
                                break;
                            }
                            case "OrderCancelRequest": {
                                String id = payload.getString("id");
                                akka.actor.ActorRef actor = findActorByOrderId(id);
                                if (actor == null) throw new IllegalStateException("No se encontró cuenta para order id=" + id);
                                RoutingMessage.OrderCancelRequest cancelReq = RoutingMessage.OrderCancelRequest.newBuilder()
                                        .setId(id).build();
                                actor.tell(cancelReq, akka.actor.ActorRef.noSender());
                                sent.add("[Buyside/Cancel] " + id);
                                log.info("[Admin/Orders] Buyside OrderCancelRequest → id={}", id);
                                break;
                            }
                            default:
                                errors.add("Tipo desconocido: " + msgType);
                        }
                    } catch (Exception e2) {
                        errors.add("[" + msgType + "] Error: " + e2.getMessage());
                        log.warn("[Admin/Orders] resend-log error en {}: {}", msgType, e2.getMessage());
                    }
                }

                res.getWriter().write(new JSONObject()
                        .put("ok",     true)
                        .put("sent",   sent.size())
                        .put("errors", errors.size())
                        .put("message", sent.size() + " mensaje(s) inyectado(s)" + (errors.isEmpty() ? "" : ", " + errors.size() + " error(es)"))
                        .put("ids",    new JSONArray(sent))
                        .put("errorList", new JSONArray(errors))
                        .toString());

            } else {
                // ── Reenvío desde Redis ───────────────────────────────────────
                String account = json.getString("account");
                HashMap<String, RoutingMessage.Order> orders = MainApp.getOrdersMapRedis().get(account);
                if (orders == null || orders.isEmpty()) {
                    res.getWriter().write("{\"ok\":true,\"resent\":0,\"message\":\"Sin órdenes en Redis para " + account + "\"}");
                    return;
                }
                JSONArray requestedIds = json.optJSONArray("orderIds");
                int count = 0;
                for (Map.Entry<String, RoutingMessage.Order> e : orders.entrySet()) {
                    if ((requestedIds == null || requestedIds.length() == 0) || containsId(requestedIds, e.getKey())) {
                        MainApp.getMessageEventBus().publish(new Envelope(e.getValue().getId(), e.getValue()));
                        count++;
                    }
                }
                log.info("[Admin/Orders] {} órdenes reenviadas al front para cuenta {}", count, account);
                res.getWriter().write("{\"ok\":true,\"resent\":" + count + ",\"message\":\"" + count + " órdenes reenviadas\",\"account\":\"" + account + "\"}");
            }

        } catch (Exception e) {
            log.error("[Admin/Orders] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()).toString());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────

    /** Busca el ActorGroupPerAccount que tiene la orden con ese id (buscando en Redis). */
    private akka.actor.ActorRef findActorByOrderId(String orderId) {
        for (Map.Entry<String, HashMap<String, RoutingMessage.Order>> entry : MainApp.getOrdersMapRedis().entrySet()) {
            if (entry.getValue() != null && entry.getValue().containsKey(orderId)) {
                return MainApp.getAccountGroupUser().get(entry.getKey());
            }
        }
        // fallback: buscar por el campo id dentro de las órdenes (Redis guarda por clOrdId a veces)
        for (Map.Entry<String, HashMap<String, RoutingMessage.Order>> entry : MainApp.getOrdersMapRedis().entrySet()) {
            if (entry.getValue() == null) continue;
            for (RoutingMessage.Order o : entry.getValue().values()) {
                if (orderId.equals(o.getId())) return MainApp.getAccountGroupUser().get(entry.getKey());
            }
        }
        return null;
    }

    /**
     * Construye un RoutingMessage.Order desde el JSON de una línea de log.
     * @param logTs timestamp del prefijo de la línea de log (zona local), usado como respaldo de la hora.
     */
    private RoutingMessage.Order buildOrderFromJson(JSONObject p, String logTs) {
        RoutingMessage.Order.Builder b = RoutingMessage.Order.newBuilder();

        if (p.has("id"))            b.setId(p.getString("id"));
        if (p.has("symbol"))        b.setSymbol(p.getString("symbol"));
        if (p.has("account"))       b.setAccount(p.getString("account"));
        if (p.has("price"))         b.setPrice(p.getDouble("price"));
        if (p.has("avgPrice"))      b.setAvgPrice(p.getDouble("avgPrice"));
        if (p.has("orderQty"))      b.setOrderQty(p.getDouble("orderQty"));
        if (p.has("cumQty"))        b.setCumQty(p.getDouble("cumQty"));
        if (p.has("leaves"))        b.setLeaves(p.getDouble("leaves"));
        if (p.has("lastPx"))        b.setLastPx(p.getDouble("lastPx"));
        if (p.has("lastQty"))       b.setLastQty(p.getDouble("lastQty"));
        if (p.has("text"))          b.setText(p.getString("text"));
        if (p.has("clOrdId"))       b.setClOrdId(p.getString("clOrdId"));
        if (p.has("orderID"))       b.setOrderID(p.getString("orderID"));
        if (p.has("origClOrdID"))   b.setOrigClOrdID(p.getString("origClOrdID"));
        if (p.has("execId"))        b.setExecId(p.getString("execId"));
        if (p.has("operator"))      b.setOperator(p.getString("operator"));
        try { if (p.has("broker"))          b.setBroker(RoutingMessage.ExecBroker.valueOf(p.getString("broker"))); } catch (Exception ignored) {}
        if (p.has("prefixID"))      b.setPrefixID(p.getString("prefixID"));
        try { if (p.has("tif"))             b.setTif(RoutingMessage.Tif.valueOf(p.getString("tif"))); } catch (Exception ignored) {}
        // Hora de la orden: usar el 'time' del mensaje (UTC ISO, p.ej. 2026-06-04T14:40:39.304Z);
        // si falta o no parsea, caer al timestamp del prefijo del log (zona local).
        // (Antes se seteaba un Timestamp vacío = epoch 0 (1970) -> el front mostraba ~21 h.)
        try { com.google.protobuf.Timestamp ts = orderTime(p.optString("time", null), logTs); if (ts != null) b.setTime(ts); } catch (Exception ignored) {}
        try { if (p.has("expireTime"))    { com.google.protobuf.Timestamp e = isoToProto(p.optString("expireTime", null));    if (e != null) b.setExpireTime(e); } } catch (Exception ignored) {}
        try { if (p.has("effectiveTime")) { com.google.protobuf.Timestamp e = isoToProto(p.optString("effectiveTime", null)); if (e != null) b.setEffectiveTime(e); } } catch (Exception ignored) {}
        try { if (p.has("settlDate"))     b.setSettlDate(p.optString("settlDate", "")); } catch (Exception ignored) {}
        try { if (p.has("currency"))      b.setCurrency(RoutingMessage.Currency.valueOf(p.getString("currency"))); } catch (Exception ignored) {}
        if (p.has("folio"))         b.setFolio(p.getString("folio"));
        if (p.has("contraTrader"))  b.setContraTrader(p.getString("contraTrader"));
        if (p.has("contraBroker"))  b.setContraBroker(p.getString("contraBroker"));
        if (p.has("clOrdLinkID"))   b.setClOrdLinkID(p.getString("clOrdLinkID"));
        if (p.has("maxFloor"))      b.setMaxFloor(p.getDouble("maxFloor"));
        if (p.has("spread"))        b.setSpread(p.getDouble("spread"));

        try { if (p.has("side"))            b.setSide(RoutingMessage.Side.valueOf(p.getString("side"))); } catch (Exception ignored) {}
        try { if (p.has("ordStatus"))       b.setOrdStatus(RoutingMessage.OrderStatus.valueOf(p.getString("ordStatus"))); } catch (Exception ignored) {}
        try { if (p.has("execType"))        b.setExecType(RoutingMessage.ExecutionType.valueOf(p.getString("execType"))); } catch (Exception ignored) {}
        try { if (p.has("ordType"))         b.setOrdType(RoutingMessage.OrdType.valueOf(p.getString("ordType"))); } catch (Exception ignored) {}
        try { if (p.has("securityExchange"))b.setSecurityExchange(RoutingMessage.SecurityExchangeRouting.valueOf(p.getString("securityExchange"))); } catch (Exception ignored) {}
        try { if (p.has("strategyOrder"))   b.setStrategyOrder(RoutingMessage.StrategyOrder.valueOf(p.getString("strategyOrder"))); } catch (Exception ignored) {}
        try { if (p.has("securityType"))    b.setSecurityType(RoutingMessage.SecurityType.valueOf(p.getString("securityType"))); } catch (Exception ignored) {}
        try { if (p.has("handlInst"))       b.setHandlInst(RoutingMessage.HandlInst.valueOf(p.getString("handlInst"))); } catch (Exception ignored) {}

        return b.build();
    }

    /** ISO-8601 instant en UTC (con 'Z'), p.ej. 2026-06-04T14:40:39.304Z → protobuf Timestamp. null si no parsea. */
    private static com.google.protobuf.Timestamp isoToProto(String iso) {
        if (iso == null || iso.isBlank()) return null;
        try {
            java.time.Instant inst = java.time.Instant.parse(iso.trim());
            return com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(inst.getEpochSecond())
                    .setNanos(inst.getNano())
                    .build();
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Hora de la orden a partir del 'time' del mensaje (UTC); si falta o no parsea,
     * usa el timestamp del prefijo del log [yyyy-MM-dd HH:mm:ss,SSS] interpretado en la zona local.
     */
    private static com.google.protobuf.Timestamp orderTime(String isoTime, String logTs) {
        com.google.protobuf.Timestamp t = isoToProto(isoTime);
        if (t != null) return t;
        if (logTs != null && !logTs.isBlank()) {
            try {
                java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(
                        logTs.trim(),
                        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS"));
                java.time.ZoneId zone = MainApp.getZoneId() != null
                        ? MainApp.getZoneId() : java.time.ZoneId.systemDefault();
                java.time.Instant inst = ldt.atZone(zone).toInstant();
                return com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(inst.getEpochSecond())
                        .setNanos(inst.getNano())
                        .build();
            } catch (Exception ex) {
                return null;
            }
        }
        return null;
    }

    private boolean containsId(JSONArray arr, String id) {
        for (int i = 0; i < arr.length(); i++) {
            if (id.equals(arr.optString(i))) return true;
        }
        return false;
    }

    private JSONObject orderToJson(RoutingMessage.Order order) {
        JSONObject obj = new JSONObject();
        obj.put("id",       order.getId());
        obj.put("symbol",   order.getSymbol());
        obj.put("account",  order.getAccount());
        obj.put("quantity", order.getOrderQty());
        obj.put("price",    order.getPrice());

        try { obj.put("side",       order.getSide().name());             } catch (Exception ignored) {}
        try { obj.put("status",     order.getOrdStatus().name());        } catch (Exception ignored) {}
        try { obj.put("exchange",   order.getSecurityExchange().name()); } catch (Exception ignored) {}
        try { obj.put("strategy",   order.getStrategyOrder().name());    } catch (Exception ignored) {}
        try { obj.put("cumQty",     order.getCumQty());                  } catch (Exception ignored) {}
        try { obj.put("leaves",     order.getLeaves());                  } catch (Exception ignored) {}

        return obj;
    }
}
