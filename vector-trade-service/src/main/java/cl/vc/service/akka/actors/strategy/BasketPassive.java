package cl.vc.service.akka.actors.strategy;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;
import cl.vc.service.util.OrderStateSupport;

import java.math.BigDecimal;

public class BasketPassive implements StrategyI {

    private final Logger log;
    private final ActorRef actorGroupPerOrder;
    private MarketDataMessage.Statistic statistic;
    private RoutingMessage.Order order;
    private Double maxfloor;
    private String icebergperc = "";
    private Boolean blockOrders = false;
    private int blockrejected = 0;
    private Double limit_calculate_buy = 0d;
    private Double limit_calculate_sell = 0d;
    private ActorRef actorStrategy;
    private BigDecimal ticks;
    /** true apenas se ve un libro USABLE (punta del lado > 0). Si tras el timeout sigue false,
     *  ActorStrategy rechaza la orden hacia el front (en vez de dejarla en el limbo). */
    private volatile boolean hadUsableMarketData = false;

    public BasketPassive(RoutingMessage.Order order, String idSubscribe, Logger fileLog, ActorRef actorGroupPerOrder, ActorRef actorStrategy) {

        this.order = order;



        maxfloor = order.getMaxFloor();

        this.actorStrategy = actorStrategy;
        icebergperc = order.getIcebergPercentage();
        this.log = fileLog;
        this.actorGroupPerOrder = actorGroupPerOrder;
    }

    /**
     * No cruzar el spread (la estrategia debe quedarse PASIVA, no tomar liquidez).
     *  - BUY : el precio no debe alcanzar el ask  -> tope = ask - 1 tick.
     *  - SELL: el precio no debe alcanzar el bid  -> piso = bid + 1 tick.
     * Si no hay punta contraria (<= 0) se respeta el precio deseado.
     * Esto evita el "sube y sube": al toparse en ask-1/bid+1 deja de escalar
     * y nunca cruza para ejecutar de forma agresiva.
     */
    /**
     * Precio de REFERENCIA para calcular la banda de límite (limit%).
     *
     * Antes la banda se calculaba SOLO con statistic.getLast(). Cuando el papel aún no
     * ha transado en el día (pre-apertura / ilíquido) last=0 -> la banda colapsa a 0:
     *   - BUY : limit_calculate_buy=0, el guard (limBuy >= bid) nunca se cumple -> la orden
     *           queda en PENDING_NEW y NUNCA sale al mercado.
     *   - SELL: limit_calculate_sell=0 <= ask siempre true -> vendería SIN protección de límite.
     *
     * Fallback en cascada: last -> cierre previo -> mid (bid+ask)/2 -> la punta del lado
     * que aplica -> cualquier punta disponible. Devuelve 0 sólo si no hay NADA usable.
     */
    private double referencePrice(MarketDataMessage.Statistic s, boolean sideBuy) {
        double ref = s.getLast();
        if (ref > 0) return ref;
        ref = s.getPreviusClose();
        if (ref > 0) return ref;
        double bid = s.getBidPx();
        double ask = s.getAskPx();
        if (bid > 0 && ask > 0) return (bid + ask) / 2d;
        double sidePx = sideBuy ? bid : ask;
        if (sidePx > 0) return sidePx;
        return Math.max(bid, ask);
    }

    private double noCrossPrice(double desiredPx, double bidPx, double askPx, double tickValue) {
        double px = desiredPx;
        if (order.getSide().equals(RoutingMessage.Side.BUY)) {
            if (askPx > 0) {
                double maxPassive = askPx - tickValue;   // tope BUY: no alcanzar el ask
                if (px > maxPassive) {
                    px = maxPassive;
                }
            }
        } else {
            if (bidPx > 0) {
                double minPassive = bidPx + tickValue;   // piso SELL: no alcanzar el bid
                if (px < minPassive) {
                    px = minPassive;
                }
            }
        }
        return px;
    }

    /** Cantidad visible positiva del iceberg, acotada al saldo vivo. */
    private double visibleFloor() {
        return StrategyReplaceSupport.maxFloorForReplace(maxfloor, order);
    }

    /**
     * Log detallado y a prueba de fallos al archivo de la estrategia (fileLog).
     * Registra el contexto completo de cada decisión para poder reconstruir qué pasó:
     * lado, estado, blockOrders, precio de la orden, leaves, tick, bid/ask/last y los límites.
     * NUNCA lanza excepción (el logging jamás debe afectar el ruteo).
     */
    private void slog(String detail) {
        try {
            if (log == null) return;
            MarketDataMessage.Statistic s = this.statistic;
            log.info("{} | side={} status={} block={} orderPx={} leaves={} tick={} bid={} ask={} last={} limBuy={} limSell={}",
                    detail,
                    order.getSide(), order.getOrdStatus(), blockOrders,
                    order.getPrice(), order.getLeaves(),
                    (ticks != null ? ticks.doubleValue() : null),
                    (s != null ? s.getBidPx() : null),
                    (s != null ? s.getAskPx() : null),
                    (s != null ? s.getLast() : null),
                    limit_calculate_buy, limit_calculate_sell);
        } catch (Throwable ignore) {
            // jamás romper la estrategia por un problema de logging
        }
    }

    /** Loguea la MKD (estadística / top-of-book + stats) que va llegando. Nunca lanza. */
    private void logMkdStat(MarketDataMessage.Statistic s, String src) {
        try {
            if (log == null || s == null) return;
            log.info("MKD[{}] bidPx={} bidQty={} askPx={} askQty={} last={} vwap={} vol={} imbalance={} delta={} prevClose={} tickDir={}",
                    src, s.getBidPx(), s.getBidQty(), s.getAskPx(), s.getAskQty(), s.getLast(),
                    s.getVwap(), s.getTradeVolume(), s.getImbalance(), s.getDelta(), s.getPreviusClose(), s.getTickDirecion());
        } catch (Throwable ignore) {
        }
    }

    /** Formatea los primeros N niveles de una punta como "precio@cantidad". */
    private String levels(java.util.List<MarketDataMessage.DataBook> side, int n) {
        StringBuilder sb = new StringBuilder("[");
        try {
            if (side != null) {
                int lim = Math.min(n, side.size());
                for (int i = 0; i < lim; i++) {
                    if (i > 0) sb.append(',');
                    sb.append(side.get(i).getPrice()).append('@').append(side.get(i).getSize());
                }
            }
        } catch (Throwable ignore) {
        }
        return sb.append(']').toString();
    }

    /** Loguea la profundidad del libro (top 5 por lado) y los trades del snapshot. Nunca lanza. */
    private void logMkdBook(BookSnapshot snap) {
        try {
            if (log == null || snap == null) return;
            int trades = snap.getTradesList() != null ? snap.getTradesList().size() : 0;
            log.info("MKD_BOOK bids(top5)={} asks(top5)={} trades={}",
                    levels(snap.getBid(), 5), levels(snap.getAsk(), 5), trades);
        } catch (Throwable ignore) {
        }
    }

    @Override
    public void onStatistic(MarketDataMessage.Statistic statistic) {

        try {

            String id = TopicGenerator.getTopicMKD(statistic);
            BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(id);

            this.statistic = bookSnapshot.getStatistic();
            // FIX "No price": el parámetro 'statistic' que llega del feed puede venir parcial/vacío
            // (bid=0/ask=0). El libro confiable es el acumulado del mapa (this.statistic). Usamos
            // SIEMPRE ese para toda la lógica (antes el cálculo leía el parámetro -> px=0).
            statistic = this.statistic;

            logMkdStat(this.statistic, "stat");
            slog("STAT_IN");

            if (blockOrders) {
                slog("SKIP (block=true: esperando ack del exchange)");
                return;
            }

            if (order.getLimit() <= 0) {
                blockOrders = true;
                slog("REJECT->front motivo=LIMIT_CERO (limit must not be zero)");
                RoutingMessage.Order order1 = order.toBuilder().setText("Basket Passive!!!! Limit must not be Zero")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED).setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setTime(TimeGenerator.getTimeProto())
                        .setExecId(IDGenerator.getID()).build();

                MainApp.getMessageEventBus().publish(new Envelope(order1.getId(), order1));
                return;
            }
            if (order.getOrderQty() <= 0) {
                blockOrders = true;
                slog("REJECT->front motivo=QTY_CERO (qty must not be zero)");
                RoutingMessage.Order order1 = order.toBuilder()
                        .setText("Basket Passive!!!! qty must not be Zero")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setTime(TimeGenerator.getTimeProto())
                        .setExecId(IDGenerator.getID())
                        .build();
                MainApp.getMessageEventBus().publish(new Envelope(order1.getId(), order1));
                return;
            }

            // Defensa dura: jamás cotizar contra un libro sin punta usable -> evita enviar SIN PRECIO.
            boolean sideBuy = order.getSide().equals(RoutingMessage.Side.BUY);
            if ((sideBuy && statistic.getBidPx() <= 0) || (!sideBuy && statistic.getAskPx() <= 0)) {
                slog("SKIP libro sin punta usable (bid=" + statistic.getBidPx() + " ask=" + statistic.getAskPx() + ") -> no se cotiza");
                return;
            }
            hadUsableMarketData = true;

            // Banda de límite contra un precio de referencia robusto (NO solo last): si el papel
            // aún no transó (last=0) la banda colapsaba a 0 y la orden nunca salía. Se cachea una
            // sola vez (al primer tick con referencia usable) para no perseguir el precio.
            double ref = referencePrice(statistic, sideBuy);
            if (limit_calculate_buy <= 0 && ref > 0) {
                limit_calculate_buy = ref * (1 + order.getLimit() / 100d);
            }
            if (limit_calculate_sell <= 0 && ref > 0) {
                limit_calculate_sell = ref * (1 - order.getLimit() / 100d);
            }
            slog("REF_PRICE ref=" + ref + " limBuy=" + limit_calculate_buy + " limSell=" + limit_calculate_sell);

            if (order.getSide().equals(RoutingMessage.Side.BUY)) {

                if (!blockOrders && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

                    if (limit_calculate_buy >= statistic.getBidPx()) {
                        blockOrders = true;
                        ticks = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(statistic.getBidPx()));
                        double px = statistic.getBidPx() + ticks.doubleValue();
                        double pxRaw = px;
                        px = noCrossPrice(px, statistic.getBidPx(), statistic.getAskPx(), ticks.doubleValue());
                        px = Ticks.applyRulePrice(order.getSecurityExchange(), px);
                        order = order.toBuilder().setPrice(px).build();
                        RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder()
                                .setOrder(order).build();
                        slog("NEW_BUY -> exchange px=" + px + " (bid+tick=" + pxRaw + ", noCross/ask=" + statistic.getAskPx() + ")");
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                    } else {
                        slog("NEW_BUY skip: bid(" + statistic.getBidPx() + ") > limitBuy(" + limit_calculate_buy + ")");
                    }

                } else if (!blockOrders && (order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED))) {

                    // FIX desync: leemos bid/ask UNA sola vez (del mismo snapshot) y los usamos
                    // tanto en el guard como en el cálculo -> evita la auto-persecución (antes el
                    // guard miraba this.statistic y el cálculo re-leía bookSnapshot.getStatistic()).
                    double bid = statistic.getBidPx();
                    double ask = statistic.getAskPx();

                    if (order.getPrice() == bid) {
                        slog("BUY no-requote: orderPx==bid (ya es la mejor punta)");
                        return;
                    }

                    if (limit_calculate_buy >= bid) {

                        // FIX NPE: ticks puede ser null si la estrategia se (re)creó con la orden ya
                        // viva (NEW/PARTIALLY_FILLED) sin pasar por el alta. Se recalcula siempre.
                        ticks = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(bid));
                        double pxRaw = bid + ticks.doubleValue();
                        double px = noCrossPrice(pxRaw, bid, ask, ticks.doubleValue());
                        px = Ticks.applyRulePrice(order.getSecurityExchange(), px);

                        if (order.getPrice() == px) {
                            slog("BUY no-requote: px sin cambio px=" + px);
                            return;
                        }

                        blockOrders = true;

                        order = order.toBuilder().setPrice(px).build();
                        RoutingMessage.OrderReplaceRequest orderReplaceRequest = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(px)
                                .setLimit(order.getLimit())
                                .setMaxFloor(visibleFloor())
                                .setQuantity(order.getOrderQty()).build();
                        slog("REPLACE_BUY -> exchange px=" + px + " (bid+tick=" + pxRaw + ", ask=" + ask + ") maxFloor=" + visibleFloor());
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderReplaceRequest);
                    } else {
                        slog("BUY no-requote: bid(" + bid + ") > limitBuy(" + limit_calculate_buy + ")");
                    }

                }

            } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {

                if (!blockOrders && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

                    if (limit_calculate_sell <= statistic.getAskPx()) {
                        blockOrders = true;
                        ticks = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(statistic.getAskPx()));
                        double px = statistic.getAskPx() - ticks.doubleValue();
                        double pxRaw = px;
                        px = noCrossPrice(px, statistic.getBidPx(), statistic.getAskPx(), ticks.doubleValue());
                        px = Ticks.applyRulePrice(order.getSecurityExchange(), px);
                        order = order.toBuilder().setPrice(px).build();
                        RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest
                                .newBuilder()
                                .setOrder(order)
                                .build();
                        slog("NEW_SELL -> exchange px=" + px + " (ask-tick=" + pxRaw + ", noCross/bid=" + statistic.getBidPx() + ")");
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                    } else {
                        slog("NEW_SELL skip: ask(" + statistic.getAskPx() + ") < limitSell(" + limit_calculate_sell + ")");
                    }

                } else if (!blockOrders && (order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED))) {

                    double bid = statistic.getBidPx();
                    double ask = statistic.getAskPx();

                    if (order.getPrice() == ask) {
                        slog("SELL no-requote: orderPx==ask (ya es la mejor punta)");
                        return;
                    }

                    if (limit_calculate_sell >= ask) {

                        // FIX NPE: recalcular ticks (puede ser null tras (re)crear la estrategia viva).
                        ticks = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(ask));
                        double pxRaw = ask - ticks.doubleValue();
                        double px = noCrossPrice(pxRaw, bid, ask, ticks.doubleValue());
                        px = Ticks.applyRulePrice(order.getSecurityExchange(), px);

                        if (order.getPrice() == px) {
                            slog("SELL no-requote: px sin cambio px=" + px);
                            return;
                        }

                        blockOrders = true;

                        order = order.toBuilder().setPrice(px).build();

                        RoutingMessage.OrderReplaceRequest orderReplaceRequest = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(px)
                                .setLimit(order.getLimit())
                                .setMaxFloor(visibleFloor())
                                .setQuantity(order.getOrderQty()).build();
                        slog("REPLACE_SELL -> exchange px=" + px + " (ask-tick=" + pxRaw + ", bid=" + bid + ") maxFloor=" + visibleFloor());
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderReplaceRequest);
                    } else {
                        slog("SELL no-requote: ask(" + ask + ") < limitSell(" + limit_calculate_sell + ")");
                    }
                }
            }

        } catch (Exception e) {
            slog("ERROR " + e);
            log.error(e.getMessage(), e);
        }


    }

    @Override
    public void onOrders(RoutingMessage.Order order) {

        try {


        if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE) ||
                order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL)) {
            return;
        }

        this.order = order;

        slog("EXEC IN execType=" + order.getExecType() + " status=" + order.getOrdStatus()
                + " px=" + order.getPrice() + " cumQty=" + order.getCumQty()
                + " leaves=" + order.getLeaves() + " lastPx=" + order.getLastPx() + " lastQty=" + order.getLastQty());

        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {

            blockOrders = false;
            blockrejected = 0;
            slog("ACK vivo (block=false) status=" + order.getOrdStatus());


        } else if (OrderStateSupport.isConclusiveStrategyTerminal(order)) {

            blockOrders = true;
            slog("TERMINAL status=" + order.getOrdStatus() + " -> PoisonPill (fin de la estrategia)");
            actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());

        }

        this.actorGroupPerOrder.tell(this.order, ActorRef.noSender());

        } catch (Exception e) {
            slog("ERROR " + e);
            log.error(e.getMessage(), e);
        }

    }

    @Override
    public void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {
        try {
            if (log != null && incrementalBook != null) {
                log.info("MKD_INCR recibido (symbol={})", incrementalBook.getSymbol());
            }
        } catch (Throwable ignore) {
        }
    }

    @Override
    public void onSnapshot(BookSnapshot snapshot) {
        logMkdBook(snapshot);
        onStatistic(snapshot.getStatistic());
    }

    @Override
    public void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest) {

        slog("ON_REPLACE (operador) newQty=" + orderReplaceRequest.getQuantity()
                + " newLimit=" + orderReplaceRequest.getLimit() + " newMaxFloor=" + orderReplaceRequest.getMaxFloor());

        if (orderReplaceRequest.getQuantity() <= 0d) {

            slog("ON_REPLACE rechazado: cantidad cero");
            RoutingMessage.OrderCancelReject orderCancelReject = RoutingMessage.OrderCancelReject.newBuilder()
                    .setId(order.getId())
                    .setText("Basket Passive Strategy!!!! Quantity is zero.").build();
            actorGroupPerOrder.tell(orderCancelReject, ActorRef.noSender());
            return;
        }

        if(orderReplaceRequest.getLimit() != order.getLimit() ){
            double ref = referencePrice(statistic, order.getSide().equals(RoutingMessage.Side.BUY));
            limit_calculate_buy = ref * ( 1 + orderReplaceRequest.getLimit()/100d);
            limit_calculate_sell = ref * ( 1 - orderReplaceRequest.getLimit()/100d);
        }


        icebergperc = orderReplaceRequest.getIcebergPercentage();

        if (orderReplaceRequest.getMaxFloor() > 0d) {
            maxfloor = orderReplaceRequest.getMaxFloor();
        }

        order = order.toBuilder()
                .setOrderQty(orderReplaceRequest.getQuantity())
                .setMaxFloor(maxfloor)
                .setLimit(orderReplaceRequest.getLimit())
                .build();

        onStatistic(this.statistic);

    }

    @Override
    public void onCancelRequest(RoutingMessage.OrderCancelRequest orderCancelRequest) {
        blockOrders = true;
        slog("ON_CANCEL_REQUEST -> exchange id=" + orderCancelRequest.getId());
        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);
    }

    @Override
    public void onRejected(RoutingMessage.OrderCancelReject rejected) {

        blockOrders = false;
        blockrejected++;

        slog("REJECT recibido del exchange: '" + rejected.getText() + "' blockrejected=" + blockrejected);
        log.info("received rejected {}", rejected.toString());

        if (blockrejected >= 5) {

            blockOrders = true;

            RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder().setId(order.getId()).build();
            slog("CANCEL por 5 rechazos consecutivos id=" + order.getId());
            log.info("order cancel by rejected {} {}", blockrejected, order.getId());
            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);

        }

    }

    @Override
    public boolean awaitingFirstMarketData() {
        // Sigue esperando si: nunca vio un libro usable, la orden no salió al mercado
        // (PENDING_NEW) y no está ya bloqueada por otro rechazo/terminal.
        return !hadUsableMarketData
                && !blockOrders
                && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW);
    }

    @Override
    public void rejectNoMarketData() {
        try {
            if (!awaitingFirstMarketData()) {
                return; // llegó data / ya salió / ya terminó entre el timeout y este llamado
            }
            blockOrders = true;
            String motivo = "Basket Passive: SIN MARKET DATA para " + order.getSymbol()
                    + " (libro vacio o sin suscripcion). Orden rechazada para no dejarla pendiente.";
            slog("REJECT->front motivo=SIN_MARKET_DATA (timeout esperando primer libro usable)");
            RoutingMessage.Order rejected = order.toBuilder()
                    .setText(motivo)
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                    .setTime(TimeGenerator.getTimeProto())
                    .setExecId(IDGenerator.getID())
                    .build();
            // El propio ActorStrategy está suscrito al id de la orden: este publish vuelve por
            // onOrders -> TERMINAL -> PoisonPill, y le llega al front/blotter por el bus.
            MainApp.getMessageEventBus().publish(new Envelope(rejected.getId(), rejected));
        } catch (Exception e) {
            slog("ERROR rejectNoMarketData " + e);
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean isTemporarilyBlocked() {
        return blockOrders;
    }

    @Override
    public void resumeAfterTemporaryBlock() {
        blockOrders = false;
    }

    @Override
    public void resetRejectRecovery() {
        blockrejected = 0;
    }

    @Override
    public boolean isAtConfiguredLimit() {
        if (statistic == null) {
            return false;
        }
        if (order.getSide().equals(RoutingMessage.Side.BUY)) {
            return limit_calculate_buy > 0d && statistic.getBidPx() > limit_calculate_buy;
        }
        return limit_calculate_sell > 0d && statistic.getAskPx() < limit_calculate_sell;
    }

}
