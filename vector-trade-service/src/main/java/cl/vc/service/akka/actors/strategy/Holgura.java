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
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;
import com.google.protobuf.util.JsonFormat;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Holgura implements StrategyI {

    private final JsonFormat.Printer printer;
    private final Logger log;
    private final ActorRef actorStrategy;
    private final String idSubscribe;
    private final Runnable task;
    private final ActorRef actorGroupPerOrder;
    private final Object lock = new Object();
    int blockrejected = 0;
    private volatile RoutingMessage.Order.Builder order;
    private RoutingMessage.OrderReplaceRequest orderPendingReplace;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private Double precioOriginal;
    private Double maxfloor;
    private Double spreadPx;
    private MarketDataMessage.Statistic statistic;
    private volatile Boolean isReplace = false;
    private volatile Boolean blockOrder = false;

    public Holgura(RoutingMessage.Order orders, String idSubscribe, Logger fileLog, ActorRef actorStrategy, ActorRef actorGroupPerOrder) {

        this.order = orders.toBuilder();

        if (orders.getMaxFloor() <= 0d) {
            maxfloor = orders.getOrderQty();
        } else {
            maxfloor = orders.getMaxFloor();
        }


        this.precioOriginal = orders.getPrice();
        this.spreadPx = orders.getSpread();
        this.actorStrategy = actorStrategy;
        this.idSubscribe = idSubscribe;
        this.log = fileLog;
        this.printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();
        this.actorGroupPerOrder = actorGroupPerOrder;

        task = () -> {

            synchronized (lock) {

                try {

                    if (!blockOrder && order.getPrice() != precioOriginal) {

                        blockOrder = true;
                        RoutingMessage.OrderReplaceRequest orderReplaceRequest = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(precioOriginal)
                                .setMaxFloor(maxfloor)
                                .setSpread(spreadPx)
                                .setQuantity(order.getOrderQty())
                                .build();

                        log.info("se envia replace spread {}",orderReplaceRequest.getSpread());

                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderReplaceRequest);
                        scheduler.shutdown();
                        log.info("se envia replace por task {} {} {}", order.getOrderQty(), order.getSymbol(), blockOrder);
                    }

                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                }
            }
        };

        riskOrder();

    }

    @Override
    public synchronized void onStatistic(MarketDataMessage.Statistic statistic) {

        try {


            String id = TopicGenerator.getTopicMKD(statistic);
            BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(id);

            this.statistic = bookSnapshot.getStatistic();

             synchronized (blockOrder) {

                if (order.getSide().equals(RoutingMessage.Side.BUY)) {

                    double px = statistic.getAskPx();
                    double spread = precioOriginal + spreadPx;
                    spread = Ticks.applyRulePrice(order.getSecurityExchange(), spread);

                    if (px > precioOriginal && px <= spread && !blockOrder && spread != order.getPrice()) {

                        blockOrder = true;
                        RoutingMessage.OrderReplaceRequest orderReplaceRequest = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(spread)
                                .setQuantity(order.getOrderQty())
                                .setMaxFloor(maxfloor)
                                .build();


                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderReplaceRequest);

                        scheduler = Executors.newScheduledThreadPool(1);
                        scheduler.scheduleAtFixedRate(task, 5, 5, TimeUnit.SECONDS);

                    }

                } else if (order.getSide().equals(RoutingMessage.Side.SELL) || order.getSide().equals(RoutingMessage.Side.SELL_SHORT)) {

                    double px = statistic.getBidPx();
                    double spread = precioOriginal - spreadPx;
                    spread = Ticks.applyRulePrice(order.getSecurityExchange(), spread);

                    if (px < precioOriginal && px >= spread && !blockOrder && spread != order.getPrice()) {

                        blockOrder = true;

                        RoutingMessage.OrderReplaceRequest orderReplaceRequest = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(spread)
                                .setQuantity(order.getOrderQty())
                                .build();

                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderReplaceRequest);

                        scheduler = Executors.newScheduledThreadPool(1);
                        scheduler.scheduleAtFixedRate(task, 5, 5, TimeUnit.SECONDS);

                        log.info("se envia replace por mkd SELL {} {} {}", order.getId() + " " + blockOrder, orderReplaceRequest.getPrice(), order.getSymbol());

                    }
                }

            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }


    }

    @Override
    public synchronized void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {
    }

    @Override
    public synchronized void onSnapshot(BookSnapshot snapshot) {
        onStatistic(snapshot.getStatistic());
    }

    private synchronized boolean riskOrder() {

        try {


            double riskPercentage = (spreadPx / order.getPrice()) * 100;

            if (order.getPrice() <= 0) {

                blockOrder = true;
                RoutingMessage.Order order1 = order.clone().setText("Holgura Strategy!!!! Price must not be Zero")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED).setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setTime(TimeGenerator.getTimeProto())
                        .setExecId(IDGenerator.getID()).build();
                this.actorGroupPerOrder.tell(order1, ActorRef.noSender());
                MainApp.getMessageEventBus().unsubscribe(actorStrategy, idSubscribe);
                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                log.info("el precio no puede ser cero {}", printer.print(order1));
                return true;

            } else if (spreadPx <= 0 || order.getOrderQty() <= 0) {

                blockOrder = true;
                RoutingMessage.Order order1 = order.clone().setText("Holgura Strategy!!!! Spread must not be Zero")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED).setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setTime(TimeGenerator.getTimeProto())
                        .setExecId(IDGenerator.getID()).build();
                this.actorGroupPerOrder.tell(order1, ActorRef.noSender());
                MainApp.getMessageEventBus().unsubscribe(actorStrategy, idSubscribe);
                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                log.info("la cantidad no puede ser cero {}", printer.print(order1));
                return true;

            } else if (riskPercentage > 1) {

                blockOrder = true;
                RoutingMessage.Order order1 = order.clone().setText("Holgura Strategy!!!! Spread sobre el rango de riesgo")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setTime(TimeGenerator.getTimeProto())
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setExecId(IDGenerator.getID()).build();
                this.actorGroupPerOrder.tell(order1, ActorRef.noSender());
                MainApp.getMessageEventBus().unsubscribe(actorStrategy, idSubscribe);
                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                log.info("la cantidad no puede ser cero {}", printer.print(order1));
                return true;

            } else if (!blockOrder && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {
                blockOrder = true;
                order = order.setOrdType(RoutingMessage.OrdType.LIMIT);
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                log.info("Comienza estrategia!!! se envia nueva orden {}", printer.print(newOrderRequest));
                return true;
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    @Override
    public synchronized void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest) {

        try {

            orderPendingReplace = orderReplaceRequest;

            double riskPercentage = (orderReplaceRequest.getSpread() / orderReplaceRequest.getPrice()) * 100;

            if (riskPercentage > 1) {
                RoutingMessage.OrderCancelReject orderCancelReject = RoutingMessage.OrderCancelReject.newBuilder()
                        .setId(order.getId())
                        .setText("Holgura Strategy!!!! Spread sobre el rango de riesgo").build();
                //MainApp.getGroupActors().forEach((key, value) -> value.tell(orderCancelReject, ActorRef.noSender()));
                actorStrategy.tell(orderCancelReject, ActorRef.noSender());

                return;
            }


            if (order.getOrderQty() > orderReplaceRequest.getQuantity() && order.getLeaves() < orderReplaceRequest.getQuantity()) {

                RoutingMessage.OrderCancelReject orderCancelReject = RoutingMessage.OrderCancelReject.newBuilder()
                        .setId(order.getId())
                        .setText("la Qty es menos que la cantidad viva").build();

                MainApp.getMessageEventBus().publish(new Envelope(orderCancelReject.getId(), orderCancelReject));

                return;
            }


            if (orderReplaceRequest.getPrice() != order.getPrice() || orderReplaceRequest.getQuantity() != order.getOrderQty() ||
                    orderReplaceRequest.getSpread() != spreadPx || !orderReplaceRequest.getIcebergPercentage().equals(order.getIcebergPercentage())) {

                if (orderReplaceRequest.getMaxFloor() <= 0d) {
                    maxfloor = orderReplaceRequest.getQuantity();
                } else {
                    maxfloor = orderReplaceRequest.getMaxFloor();
                }

                if (blockOrder) {
                    isReplace = true;

                } else {
                    blockOrder = true;

                    System.out.println("se envia replace spread " + orderReplaceRequest.getSpread());

                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderReplaceRequest);
                }

            } else {

                spreadPx = orderReplaceRequest.getSpread();
                this.order = this.order.setSpread(orderReplaceRequest.getSpread());
                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order.build()));
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public synchronized void onCancelRequest(RoutingMessage.OrderCancelRequest orderCancelRequest) {
        log.info("se envia cancel enviada");
        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);
    }

    @Override
    public synchronized void onOrders(RoutingMessage.Order order) {

        try {

            synchronized (this.order) {

                if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE) ||
                        order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL)) {
                    return;
                }


                blockrejected = 0;

                log.info("llega confirmacion {} -> {} ", blockOrder, order.getOrderQty());

                if (isReplace) {
                    blockOrder = true;
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderPendingReplace);
                    log.info("enviamos replace pendiente {}", printer.print(orderPendingReplace));
                    return;
                }

                if (orderPendingReplace != null) {

                    this.order.setPrice(orderPendingReplace.getPrice());
                    this.order.setOrderQty(orderPendingReplace.getQuantity());
                    this.order.setMaxFloor(orderPendingReplace.getMaxFloor());
                    this.order.setSpread(orderPendingReplace.getSpread());

                    log.info("actualizamos el precio original por el nuevo {} -> {}", precioOriginal, order.getPrice());
                    log.info("la Qty es de  {} -> {}", order.getOrderQty(), this.order.getOrderQty());

                    precioOriginal = orderPendingReplace.getPrice();
                    spreadPx = orderPendingReplace.getSpread();
                    maxfloor = orderPendingReplace.getMaxFloor();
                    orderPendingReplace = null;

                } else {
                    this.order = order.toBuilder();
                }

                if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED) || order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED) ||
                        order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)) {
                    blockOrder = true;
                    actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                }


                if (MainApp.getSnapshotHashMap().containsKey(idSubscribe)) {
                    BookSnapshot snapshot = MainApp.getSnapshotHashMap().get(idSubscribe);
                    onSnapshot(snapshot);

                }

                blockOrder = false;
                onStatistic(statistic);
                this.actorGroupPerOrder.tell(order, ActorRef.noSender());

            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    @Override
    public void onRejected(RoutingMessage.OrderCancelReject rejected) {

        try {

            log.info("received rejected {} {} ", rejected.toString(), blockrejected);

            blockrejected++;
            blockOrder = false;
            orderPendingReplace = null;

            if (blockrejected >= 5) {

                blockOrder = true;
                MainApp.getMessageEventBus().unsubscribe(actorStrategy, idSubscribe);

                RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder()
                        .setId(order.getId()).build();

                log.info("order cancel by rejected {} {}", blockrejected, order.getId());
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);
                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }


}
