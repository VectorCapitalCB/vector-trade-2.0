package cl.vc.service.akka.actors.strategy;

import akka.actor.ActorRef;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;
import cl.vc.service.util.OrderStateSupport;

import java.math.BigDecimal;

public class Best implements StrategyI {

    private final Logger log;
    private final ActorRef actorGroupPerOrder;
    private RoutingMessage.Order order;
    private BookSnapshot snapshot;
    private Double limit = 0d;
    private Double spread = 0d;
    private Double maxfloor = 0d;
    private String icebergperc = "";
    private Double blockQty = 0d;
    private Boolean blockOrders = false;
    private int blockrejected = 0;
    private double targetQty;
    private double maxCumQty;
    private Double pendingTargetQty;
    private boolean replacePending;

    public Best(RoutingMessage.Order order, String idSubscribe, Logger fileLog, ActorRef actorGroupPerOrder) {

        this.order = order;
        this.targetQty = order.getOrderQty();
        this.maxCumQty = Math.max(0d, order.getCumQty());

        maxfloor = order.getMaxFloor();

        icebergperc = order.getIcebergPercentage();
        limit = order.getLimit();
        spread = order.getSpread();
        this.log = fileLog;
        this.actorGroupPerOrder = actorGroupPerOrder;
        blog("INIT orden recibida en la estrategia BEST");
    }

    /**
     * Log exhaustivo y A PRUEBA DE FALLOS al archivo de la estrategia (fileLog).
     * Registra el contexto completo —incluido el LÍMITE en cada paso— para poder
     * determinar 100% si la orden/replace llegó del front SIN límite o si el core
     * lo perdió/sobrescribió. NUNCA lanza excepción (el logging no debe afectar el ruteo).
     */
    private void blog(String detail) {
        try {
            if (log == null || order == null) return;
            log.info("{} | side={} status={} block={} replacePending={} orderLimit={} orderPrice={} instLimit={} spread={} maxFloor={} iceberg={} qty={} targetQty={} cumQty={} leaves={}",
                    detail, order.getSide(), order.getOrdStatus(), blockOrders,
                    replacePending,
                    order.getLimit(), order.getPrice(), limit, spread, maxfloor, icebergperc,
                    order.getOrderQty(), targetQty, maxCumQty, order.getLeaves());
        } catch (Throwable ignore) {
        }
    }

    /** Mejor bid/ask del snapshot, a prueba de índices vacíos (para dejar el motivo explícito en el log). */
    private String bookStr() {
        try {
            String bid = (snapshot != null && snapshot.getBid() != null && !snapshot.getBid().isEmpty())
                    ? String.valueOf(snapshot.getBid().get(0).getPrice()) : "?";
            String ask = (snapshot != null && snapshot.getAsk() != null && !snapshot.getAsk().isEmpty())
                    ? String.valueOf(snapshot.getAsk().get(0).getPrice()) : "?";
            return "bestBid=" + bid + " bestAsk=" + ask;
        } catch (Throwable t) {
            return "bestBid=? bestAsk=?";
        }
    }

    /** Enruta el mensaje al exchange y loguea EXACTAMENTE lo que sale (precio, límite, etc.). */
    private void send(com.google.protobuf.Message m) {
        if (m instanceof RoutingMessage.OrderReplaceRequest) {
            if (replacePending) {
                blog("SKIP replace: ya existe un replace pendiente");
                return;
            }
            replacePending = true;
            blockOrders = true;
        } else if (m instanceof RoutingMessage.OrderCancelRequest) {
            blockOrders = true;
        }

        try {
            if (log != null && m != null) {
                if (m instanceof RoutingMessage.NewOrderRequest) {
                    RoutingMessage.Order o = ((RoutingMessage.NewOrderRequest) m).getOrder();
                    blog("OUT NewOrderRequest -> exchange px=" + o.getPrice() + " limit=" + o.getLimit()
                            + " qty=" + o.getOrderQty() + " maxFloor=" + o.getMaxFloor());
                } else if (m instanceof RoutingMessage.OrderReplaceRequest) {
                    RoutingMessage.OrderReplaceRequest r = (RoutingMessage.OrderReplaceRequest) m;
                    blog("OUT OrderReplaceRequest -> exchange px=" + r.getPrice() + " limit(enviado)=" + r.getLimit()
                            + " qty=" + r.getQuantity() + " maxFloor=" + r.getMaxFloor());
                } else if (m instanceof RoutingMessage.OrderCancelRequest) {
                    blog("OUT OrderCancelRequest -> exchange id=" + ((RoutingMessage.OrderCancelRequest) m).getId());
                }
            }
        } catch (Throwable ignore) {
        }
        var client = MainApp.getConnections().get(order.getSecurityExchange());
        client.sendMessage(m);
    }

    private double replaceTargetQty() {
        return pendingTargetQty != null ? pendingTargetQty : targetQty;
    }

    private double remainingQty(double requestedTargetQty) {
        return Math.max(0d, requestedTargetQty - maxCumQty);
    }

    private double visibleMaxFloor(double requestedTargetQty) {
        return StrategyReplaceSupport.maxFloorForReplace(
                maxfloor,
                remainingQty(requestedTargetQty));
    }

    private void rejectReplaceLocally(String reason) {
        replacePending = false;
        blockOrders = false;
        pendingTargetQty = null;
        blog("REPLACE bloqueado localmente: " + reason);
        notifyReplaceRejected(reason);
    }

    private void notifyReplaceRejected(String reason) {
        RoutingMessage.OrderCancelReject reject = RoutingMessage.OrderCancelReject.newBuilder()
                .setId(order.getId())
                .setExecId(IDGenerator.getID())
                .setText(reason)
                .build();
        actorGroupPerOrder.tell(reject, ActorRef.noSender());
    }

    private void sendReplace(double price) {
        double requestedTargetQty = replaceTargetQty();
        if (requestedTargetQty <= 0d) {
            rejectReplaceLocally("BEST: totalQty debe ser mayor que cero");
            return;
        }
        if (requestedTargetQty < maxCumQty) {
            rejectReplaceLocally("BEST: totalQty " + requestedTargetQty
                    + " es menor que cumQty " + maxCumQty);
            return;
        }

        double leavesQty = remainingQty(requestedTargetQty);
        double displayQty = visibleMaxFloor(requestedTargetQty);
        if (displayQty > leavesQty) {
            rejectReplaceLocally("BEST: maxFloor " + displayQty
                    + " supera leavesQty " + leavesQty);
            return;
        }

        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                .setId(order.getId())
                .setPrice(price)
                .setMaxFloor(displayQty)
                .setIcebergPercentage(icebergperc)
                .setQuantity(requestedTargetQty)
                .build();
        send(replace);
    }


    @Override
    public void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {

        try {

            BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(incrementalBook.getId());
            onSnapshot(bookSnapshot);

        } catch (Exception e) {
            blog("ERROR onIncrementalBook " + e);
            log.error(e.getMessage(), e);
        }

    }


    @Override
    public void onSnapshot(BookSnapshot snapshot) {

        if (blockOrders) {
            return;
        }

        this.snapshot = snapshot;

        blog("SNAP_IN " + bookStr());

        if (order.getLimit() <= 0) {

            blockOrders = true;
            blog("❌ RECHAZO->front: order.getLimit()=" + order.getLimit() + " <= 0  ('Best Strategy!!!! Limit must not be Zero')"
                    + " -- AQUÍ se pierde el límite: revisar el último 'IN onReplace' (req.limit) y 'onOrders' previos");
            RoutingMessage.Order order1 = order.toBuilder().setText("Best Strategy!!!! Limit must not be Zero")
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED).setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                    .setTime(TimeGenerator.getTimeProto())
                    .setExecId(IDGenerator.getID()).build();
            MainApp.getMessageEventBus().publish(new Envelope(order1.getId(), order1));
            return;
        }

        if (order.getSide().equals(RoutingMessage.Side.BUY)) {

            MarketDataMessage.DataBook dataBook = snapshot.getBid().get(0);

            if (!blockOrders && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

                if (dataBook.getPrice() >= order.getLimit()) {
                    blockOrders = true;

                    order = order.toBuilder().setPrice(order.getLimit()).build();

                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                    send(newOrderRequest);
                } else {
                    blockOrders = true;
                    BigDecimal tick = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBook.getPrice()));
                    double newPrice = BigDecimal.valueOf(dataBook.getPrice()).add(tick).doubleValue();
                    order = order.toBuilder().setPrice(newPrice).build();
                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                    send(newOrderRequest);
                }

            } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)) {

                if (dataBook.getPrice() <= order.getLimit()) {

                    BigDecimal tick = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBook.getPrice()));
                    double newPrice = BigDecimal.valueOf(dataBook.getPrice()).add(tick).doubleValue();

                    if (newPrice != order.getPrice() && order.getPrice() != dataBook.getPrice() && order.getLimit() >= newPrice && !blockOrders) {
                        sendReplace(newPrice);

                    } else if (order.getPrice() == dataBook.getPrice()) {

                        MarketDataMessage.DataBook dataBookSecond = snapshot.getBid().get(1);

                        BigDecimal tick2 = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBookSecond.getPrice()));
                        double newPrice2 = BigDecimal.valueOf(dataBookSecond.getPrice()).add(tick2).doubleValue();

                        if (order.getPrice() != newPrice2 && order.getLimit() >= newPrice2 && !blockOrders) {
                            sendReplace(newPrice2);
                        }
                    } else if (dataBook.getPrice() >= order.getLimit() && !blockOrders) {
                        sendReplace(order.getLimit());
                    }

                    // FIX simétrico (BUY): cuando el bid sube > límite (p.ej. el operador BAJA el techo
                    // por debajo del mercado) hay que mover la orden HASTA el límite, aunque sea la mejor
                    // punta (order.price == bid). Antes el guard 'order.getPrice() != bid' lo impedía.
                } else if (order.getPrice() != order.getLimit() && !blockOrders) {

                    if (order.getLimit() != order.getPrice()) {
                        blog("REPLACE_BUY al límite (bajada de techo): px=" + order.getLimit() + " (bid=" + dataBook.getPrice() + " > limit)");
                        sendReplace(order.getLimit());
                    }

                }
            }


        } else if (order.getSide().equals(RoutingMessage.Side.SELL) || order.getSide().equals(RoutingMessage.Side.SELL_SHORT)) {

            MarketDataMessage.DataBook dataBook = snapshot.getAsk().get(0);

            if (!blockOrders && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

                if (dataBook.getPrice() <= order.getLimit()) {
                    blockOrders = true;
                    order = order.toBuilder().setPrice(order.getLimit()).build();
                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                    send(newOrderRequest);
                } else {
                    blockOrders = true;
                    BigDecimal tick = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBook.getPrice()));
                    double newPrice = BigDecimal.valueOf(dataBook.getPrice()).subtract(tick).doubleValue();
                    order = order.toBuilder().setPrice(newPrice).build();
                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                    send(newOrderRequest);
                }

            } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                    | order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)) {

                if (dataBook.getPrice() >= order.getLimit() && !blockOrders) {

                    BigDecimal tick = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBook.getPrice()));
                    double newPrice = BigDecimal.valueOf(dataBook.getPrice()).subtract(tick).doubleValue();

                    if (newPrice != order.getPrice() && order.getPrice() != dataBook.getPrice() && newPrice >= order.getLimit() && !blockOrders) {
                        sendReplace(newPrice);

                    } else if (order.getPrice() == dataBook.getPrice() && !blockOrders) {

                        MarketDataMessage.DataBook dataBookSecond = snapshot.getAsk().get(1);

                        BigDecimal tick2 = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBookSecond.getPrice()));
                        double newPrice2 = BigDecimal.valueOf(dataBookSecond.getPrice()).subtract(tick2).doubleValue();

                        if (order.getPrice() != newPrice2 && order.getLimit() <= newPrice2 && !blockOrders) {
                            sendReplace(newPrice2);
                        }

                    } else if (dataBook.getPrice() >= order.getLimit() && !blockOrders) {
                        sendReplace(order.getLimit());
                    }

                    // FIX subida (SELL): cuando el ask cae <= límite (p.ej. el operador SUBE el piso
                    // por encima del mercado) hay que mover la orden HASTA el límite, AUNQUE la orden
                    // sea la mejor punta (order.price == ask). El guard anterior 'order.getPrice() != ask'
                    // lo impedía -> "subir no funcionaba". Ahora se re-cotiza si el precio != límite.
                } else if (dataBook.getPrice() <= order.getLimit() && order.getPrice() != order.getLimit() && !blockOrders) {
                    if (order.getLimit() != order.getPrice()) {
                        blog("REPLACE_SELL al límite (subida): px=" + order.getLimit() + " (ask=" + dataBook.getPrice() + " <= limit)");
                        sendReplace(order.getLimit());
                    }
                }
            }
        }

    }

    @Override
    public void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest) {

        // ===== LOG CLAVE: lo que llega EXACTAMENTE del front, y el límite ANTES de tocarlo =====
        // Si req.limit==0 -> el front lo mandó SIN límite. Si req.limit>0 pero luego se rechaza
        // por limit<=0 -> el core lo perdió en alguna transición posterior.
        blog("IN onReplace (del front) req.price=" + orderReplaceRequest.getPrice()
                + " req.limit=" + orderReplaceRequest.getLimit()
                + " req.spread=" + orderReplaceRequest.getSpread()
                + " req.maxFloor=" + orderReplaceRequest.getMaxFloor()
                + " req.qty=" + orderReplaceRequest.getQuantity()
                + " req.iceberg=" + orderReplaceRequest.getIcebergPercentage()
                + "  [ANTES: order.limit=" + order.getLimit() + " instLimit=" + limit + "]");

        if (replacePending) {
            notifyReplaceRejected("BEST: ya existe un replace pendiente");
            return;
        }

        double price = order.getPrice();
        double requestedTargetQty = orderReplaceRequest.getQuantity();
        if (requestedTargetQty <= 0d || requestedTargetQty < maxCumQty) {
            rejectReplaceLocally("BEST: totalQty " + requestedTargetQty
                    + " no puede ser menor que cumQty " + maxCumQty);
            return;
        }

        pendingTargetQty = requestedTargetQty;
        limit = orderReplaceRequest.getLimit();
        spread = orderReplaceRequest.getSpread();
        icebergperc = orderReplaceRequest.getIcebergPercentage();

        if (orderReplaceRequest.getMaxFloor() > 0d) {
            maxfloor = orderReplaceRequest.getMaxFloor();
        }

        if ((requestedTargetQty != targetQty || orderReplaceRequest.getMaxFloor() != order.getMaxFloor())
                && orderReplaceRequest.getSpread() == order.getSpread() && orderReplaceRequest.getLimit() == order.getLimit()) {
            sendReplace(order.getLimit());
            return;
        }


        order = order.toBuilder()
                .setPrice(price)
                .setMaxFloor(maxfloor)
                .setIcebergPercentage(icebergperc)
                .setSpread(orderReplaceRequest.getSpread())
                .setLimit(orderReplaceRequest.getLimit()).build();

        blog("onReplace APLICADO [DESPUÉS: order.limit=" + order.getLimit() + " instLimit=" + limit + "]"
                + (order.getLimit() <= 0 ? "  ⚠️ quedó con límite 0 -> el próximo tick lo rechazará" : ""));

        blockOrders = false;
        onSnapshot(snapshot);

    }

    @Override
    public void onCancelRequest(RoutingMessage.OrderCancelRequest orderCancelRequest) {
        blog("IN onCancelRequest (del front)");
        blockOrders = true;
        send(orderCancelRequest);
    }

    @Override
    public void onOrders(RoutingMessage.Order order) {


        if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE) ||
                order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL)) {
            return;
        }

        boolean replaceAcknowledged =
                order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_REPLACED);
        if (replaceAcknowledged) {
            if (pendingTargetQty != null) {
                targetQty = pendingTargetQty;
                pendingTargetQty = null;
            }
            // NUAM conserva PARTIALLY_FILLED después de confirmar un replace.
            // El execType es el ACK confiable para volver a seguir la punta.
            replacePending = false;
        }

        double incomingCumQty = Math.max(0d, order.getCumQty());
        boolean cumQtyRegression = incomingCumQty < maxCumQty
                && order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE);
        if (cumQtyRegression) {
            blockOrders = true;
            blog("CUM_QTY_REGRESSION incoming=" + incomingCumQty + " previous=" + maxCumQty);
        }
        maxCumQty = Math.max(maxCumQty, incomingCumQty);
        if (OrderStateSupport.isConclusiveFilled(order)) {
            maxCumQty = Math.max(maxCumQty, targetQty);
        }

        double normalizedLeaves = remainingQty(targetQty);

        // 'order' aquí es el PARÁMETRO (lo que devuelve el exchange). 'limit' es el de la estrategia.
        blog("IN onOrders exec=" + order.getExecType() + " status=" + order.getOrdStatus()
                + " incoming.limit=" + order.getLimit() + " incoming.price=" + order.getPrice()
                + " tif=" + order.getTif() + "  [instLimit=" + limit + "]");

        if (!order.getTif().equals(RoutingMessage.Tif.FILL_OR_KILL)) {
            this.order = order.toBuilder()
                    .setOrderQty(targetQty)
                    .setCumQty(maxCumQty)
                    .setLeaves(normalizedLeaves)
                    .setLimit(limit)
                    .setSpread(spread)
                    .setMaxFloor(maxfloor)
                    .setIcebergPercentage(icebergperc)
                    .build();
            blog("onOrders RECONSTRUYE this.order con instLimit -> this.order.limit=" + this.order.getLimit()
                    + (this.order.getLimit() <= 0 ? "  ⚠️ quedó en 0 (el próximo tick rechazará)" : ""));
        }


        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)) {
            replacePending = false;
            blockOrders = false;
            blockrejected = 0;
        } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {
            blockOrders = replacePending || cumQtyRegression;
            blockrejected = 0;
        } else if (OrderStateSupport.isConclusiveStrategyTerminal(order)) {
            replacePending = false;
            blockOrders = true;

        } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.DONE_FOR_DAY)) {
            blockQty = blockQty - order.getOrderQty();
        }

        this.actorGroupPerOrder.tell(this.order, ActorRef.noSender());

    }

    @Override
    public void onRejected(RoutingMessage.OrderCancelReject rejected) {
        replacePending = false;
        pendingTargetQty = null;
        blockOrders = false;
        blockrejected++;

        blog("IN onRejected (rechazo del exchange): '" + rejected.getText() + "' blockrejected=" + blockrejected);
        log.info("received rejected {}", rejected.toString());

        if (blockrejected >= 5) {

            blockOrders = true;

            RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder().setId(order.getId()).build();
            log.info("order cancel by rejected {} {}", blockrejected, order.getId());
            send(orderCancelRequest);

        }

    }


    @Override
    public void onStatistic(MarketDataMessage.Statistic statistic) {

    }

    @Override
    public boolean isTemporarilyBlocked() {
        return blockOrders || replacePending;
    }

    @Override
    public void resumeAfterTemporaryBlock() {
        blockOrders = false;
        replacePending = false;
        pendingTargetQty = null;
    }

    @Override
    public void resetRejectRecovery() {
        blockrejected = 0;
    }

    @Override
    public boolean isAtConfiguredLimit() {
        return limit > 0d && Math.abs(order.getPrice() - limit) < 0.0000001d;
    }

}
