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

import java.math.BigDecimal;

public class BasketLast implements StrategyI {

    private final Logger log;
    private final ActorRef actorGroupPerOrder;
    private MarketDataMessage.Statistic statistic;
    private RoutingMessage.Order order;
    private Double maxfloor = 0d;
    private String icebergperc = "";
    private Boolean blockOrders = false;
    private int blockrejected = 0;
    private Double limit_calculate_buy = 0d;
    private Double limit_calculate_sell = 0d;
    private ActorRef actorStrategy;
    private BigDecimal ticks;
    private BookSnapshot snapshot;
    private Double limit = 0d;


    public BasketLast(RoutingMessage.Order order, String idSubscribe, Logger fileLog, ActorRef actorGroupPerOrder, ActorRef actorStrategy) {

        this.order = order;

        if (order.getMaxFloor() <= 0d) {
            maxfloor = order.getOrderQty();
        } else {
            maxfloor = order.getMaxFloor();
        }

        this.actorStrategy = actorStrategy;
        icebergperc = order.getIcebergPercentage();
        this.log = fileLog;
        this.actorGroupPerOrder = actorGroupPerOrder;
    }


    @Override
    public void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {

    }

    @Override
    public void onSnapshot(BookSnapshot snapshot) {
        onStatistic(snapshot.getStatistic());
    }

    @Override
    public void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest) {

        if (orderReplaceRequest.getQuantity() <= 0d) {


            RoutingMessage.OrderCancelReject orderCancelReject = RoutingMessage.OrderCancelReject.newBuilder()
                    .setId(order.getId())
                    .setText("Basket Passive Strategy!!!! Quantity is zero.").build();
            actorGroupPerOrder.tell(orderCancelReject, ActorRef.noSender());
            return;
        }

        if(orderReplaceRequest.getLimit() != order.getLimit() ){
            limit_calculate_buy = statistic.getLast() * ( 1 + orderReplaceRequest.getLimit() /100d);
            limit_calculate_sell = statistic.getLast() * ( 1 - orderReplaceRequest.getLimit() /100d);
        }

        limit = orderReplaceRequest.getLimit();
        icebergperc = orderReplaceRequest.getIcebergPercentage();

        if (orderReplaceRequest.getMaxFloor() <= 0d) {
            maxfloor = orderReplaceRequest.getQuantity();
        } else {
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
        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);
    }

    @Override
    public void onOrders(RoutingMessage.Order order) {


        try {


            if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE) ||
                    order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL)) {
                return;
            }

            this.order = order;

            if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {

                blockOrders = false;
                blockrejected = 0;


            } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)) {

                blockOrders = true;
                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());

            }

            this.actorGroupPerOrder.tell(this.order, ActorRef.noSender());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    @Override
    public void onRejected(RoutingMessage.OrderCancelReject rejected) {

        blockOrders = false;
        blockrejected++;

        log.info("received rejected {}", rejected.toString());

        if (blockrejected >= 5) {
            blockOrders = true;
            RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder().setId(order.getId()).build();
            log.info("order cancel by rejected {} {}", blockrejected, order.getId());
            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);

        }

    }

    @Override
    public void onStatistic(MarketDataMessage.Statistic statisti) {

        String id = TopicGenerator.getTopicMKD(statisti);
        BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(id);

        this.statistic = bookSnapshot.getStatistic();

        if (blockOrders) {
            return;
        }
        if (order.getLimit() <= 0) {
            blockOrders = true;
            RoutingMessage.Order order1 = order.toBuilder()
                    .setText("Basket Last!!!! Limit must not be Zero")
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                    .setTime(TimeGenerator.getTimeProto())
                    .setExecId(IDGenerator.getID())
                    .build();
            MainApp.getMessageEventBus().publish(new Envelope(order1.getId(), order1));
            return;
        }
        if (order.getOrderQty() <= 0) {
            blockOrders = true;
            RoutingMessage.Order order1 = order.toBuilder()
                    .setText("Basket Last!!!! qty must not be Zero")
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                    .setTime(TimeGenerator.getTimeProto())
                    .setExecId(IDGenerator.getID())
                    .build();
            MainApp.getMessageEventBus().publish(new Envelope(order1.getId(), order1));
            return;
        }
        if (statistic.getLast() <= 0) {
            log.info("el last es cero {} {}", order.getId(), order.getSymbol());
            return;
        }


        if (limit_calculate_buy <= 0) {
            limit_calculate_buy = statisti.getLast() * (1 + order.getLimit() / 100d);
        }
        if (limit_calculate_sell <= 0) {
            limit_calculate_sell = statisti.getLast() * (1 - order.getLimit() / 100d);
        }


        if (order.getSide().equals(RoutingMessage.Side.BUY)) {


            if (!blockOrders && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

                if (limit_calculate_buy >= statisti.getLast()) {
                    blockOrders = true;



                    ticks = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(statisti.getLast()));
                    double px = statisti.getLast() + ticks.doubleValue();
                    px = Ticks.applyRulePrice(order.getSecurityExchange(), px);

                    order = order.toBuilder().setPrice(px).build();

                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder()
                            .setOrder(order).build();
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                }


            } else if (!blockOrders && (
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                            || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)
                            || order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED))) {

                if (order.getPrice() == statisti.getLast()) {
                    return;
                }

                if (limit_calculate_buy >= statisti.getLast()) {


                    double px = bookSnapshot.getStatistic().getLast() + ticks.doubleValue();
                    px = Ticks.applyRulePrice(order.getSecurityExchange(), px);

                    if (order.getPrice() == px) {
                        return;
                    }

                    blockOrders = true;

                    order = order.toBuilder().setPrice(px).build();
                    RoutingMessage.OrderReplaceRequest orderReplaceRequest = RoutingMessage.OrderReplaceRequest
                            .newBuilder()
                            .setId(order.getId())
                            .setPrice(px)
                            .setLimit(order.getLimit())
                            .setMaxFloor(maxfloor)
                            .setQuantity(order.getOrderQty())
                            .build();
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderReplaceRequest);
                }

            } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)) {

                blockOrders = true;
            }

        } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {


            if (!blockOrders && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {


                if (limit_calculate_sell <= statisti.getLast()) {
                    blockOrders = true;
                    ticks = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(statisti.getLast()));
                    double px = statisti.getLast() - ticks.doubleValue();
                    px = Ticks.applyRulePrice(order.getSecurityExchange(), px);

                    order = order.toBuilder().setPrice(px).build();

                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest
                            .newBuilder()
                            .setOrder(order)
                            .build();
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);

                }

            } else if (!blockOrders && (
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                            || order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                            || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED))) {

                if (order.getPrice() == statisti.getLast()) {
                    return;
                }

                if (limit_calculate_sell >= statisti.getLast()) {

                    double px = bookSnapshot.getStatistic().getLast() - ticks.doubleValue();
                    px = Ticks.applyRulePrice(order.getSecurityExchange(), px);

                    if (order.getPrice() == px) {
                        return;
                    }

                    blockOrders = true;
                    RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest
                            .newBuilder()
                            .setId(order.getId())
                            .setPrice(px)
                            .setLimit(order.getLimit())
                            .setIcebergPercentage(icebergperc)
                            .setMaxFloor(maxfloor)
                            .setQuantity(order.getOrderQty())
                            .build();
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);
                    blockOrders = true;
                }

            } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)) {

                blockOrders = true;
            }
        }
    }

}




