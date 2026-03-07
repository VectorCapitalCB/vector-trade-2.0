package cl.vc.service.akka.actors.strategy;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;

import java.math.BigDecimal;

public class Trailing implements StrategyI {

    private final Logger log;
    private final ActorRef actorStrategy;
    private final String idSubscribe;
    private final ActorRef actorGroupPerOrder;
    private RoutingMessage.Order order;
    private Double blockQty = 0d;
    private Boolean blockOrders = false;
    private int blockrejected = 0;
    private MarketDataMessage.Statistic statistic;
    private BigDecimal stopLoss = BigDecimal.ZERO;
    private BigDecimal maxPriceBid = BigDecimal.ZERO;
    private BigDecimal minPriceSell = new BigDecimal(Double.MAX_VALUE);

    public Trailing(RoutingMessage.Order order, ActorRef actorStrategy, String idSubscribe, Logger fileLog, ActorRef actorGroupPerOrder) {
        this.order = order;
        this.actorStrategy = actorStrategy;
        this.idSubscribe = idSubscribe;
        this.log = fileLog;
        this.actorGroupPerOrder = actorGroupPerOrder;

        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

            if (order.getLimit() <= 0) {
                order = order.toBuilder().setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setPrice(order.getSpread())
                        .setText("el limite tiene que ser mayor que cero")
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED).build();
                onOrders(order);
                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));
                return;
            }

            this.order = order.toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                    .setPrice(0d)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_NEW).build();
            MainApp.getMessageEventBus().publish(new Envelope(order.getId(), this.order));
        }
    }


    @Override
    public void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {
    }


    @Override
    public void onSnapshot(BookSnapshot snapshot) {
        onStatistic(snapshot.getStatistic());
    }

    @Override
    public void onStatistic(MarketDataMessage.Statistic statistic) {

        this.statistic = statistic;

        if (BigDecimal.valueOf(statistic.getBidPx()).compareTo(maxPriceBid) >= 0) {
            maxPriceBid = BigDecimal.valueOf(statistic.getBidPx());
        }

        if (BigDecimal.valueOf(statistic.getAskPx()).compareTo(minPriceSell) <= 0) {
            minPriceSell = BigDecimal.valueOf(statistic.getAskPx());
        }

        if (order.getSide().equals(RoutingMessage.Side.BUY)) {

            stopLoss = minPriceSell.add(BigDecimal.valueOf(order.getLimit()));

            if (this.order.getPrice() != stopLoss.doubleValue()) {
                log.info("calculamos nuevo stop lost {} pxSellMKD {} minSell alcanzado {}", stopLoss, statistic.getAskPx(), minPriceSell);
                this.order = order.toBuilder().setPrice(stopLoss.doubleValue()).setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED).build();
                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), this.order));
            }


        } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {

            stopLoss = maxPriceBid.add(BigDecimal.valueOf(order.getLimit()));

            if (this.order.getPrice() != stopLoss.doubleValue()) {
                log.info("calculamos nuevo stop lost {} pxBuyMKD {}", stopLoss, statistic.getBidPx());
                this.order = order.toBuilder().setPrice(stopLoss.doubleValue()).setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED).build();
                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), this.order));
            }
        }

        if (order.getSide().equals(RoutingMessage.Side.BUY) && BigDecimal.valueOf(statistic.getAskPx()).compareTo(stopLoss) >= 0 && !blockOrders) {
            blockOrders = true;
            this.order = this.order.toBuilder().setPrice(stopLoss.doubleValue()).build();
            RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);

        } else if (order.getSide().equals(RoutingMessage.Side.SELL) && BigDecimal.valueOf(statistic.getBidPx()).compareTo(stopLoss) <= 0 && !blockOrders) {
            blockOrders = true;
            this.order = this.order.toBuilder().setPrice(stopLoss.doubleValue()).build();
            RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
        }


    }

    @Override
    public void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest) {

        if (order.getSide().equals(RoutingMessage.Side.BUY)) {
            stopLoss = minPriceSell.subtract(BigDecimal.valueOf(orderReplaceRequest.getLimit()));

        } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {
            stopLoss = maxPriceBid.add(BigDecimal.valueOf(orderReplaceRequest.getLimit()));
        }

        order = order.toBuilder()
                .setOrderQty(orderReplaceRequest.getQuantity())
                .setPrice(stopLoss.doubleValue())
                .setSpread(orderReplaceRequest.getSpread())
                .setLimit(orderReplaceRequest.getLimit()).build();

        onStatistic(this.statistic);
        MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));

    }

    @Override
    public void onCancelRequest(RoutingMessage.OrderCancelRequest orderCancelRequest) {

        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {
            this.order = order.toBuilder().setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED).setOrdStatus(RoutingMessage.OrderStatus.CANCELED).build();
            MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));

        } else {
            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);
        }
    }

    @Override
    public void onOrders(RoutingMessage.Order order) {

        this.order = order;
        blockOrders = true;

        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {

            blockrejected = 0;


        } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)) {

            MainApp.getMessageEventBus().unsubscribe(actorStrategy, idSubscribe);
            MainApp.getMessageEventBus().unsubscribe(actorStrategy, order.getId());
            actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());


        } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.DONE_FOR_DAY)) {
            blockQty = blockQty - order.getOrderQty();
        }

        this.actorGroupPerOrder.tell(this.order, ActorRef.noSender());

    }

    @Override
    public void onRejected(RoutingMessage.OrderCancelReject rejected) {

        blockOrders = false;
        blockrejected++;

        log.info("received rejected {}", rejected.toString());

        if (blockrejected >= 5) {

            blockOrders = true;
            MainApp.getMessageEventBus().unsubscribe(actorStrategy, idSubscribe);

            RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder()
                    .setId(order.getId()).build();

            log.info("order cancel by rejected {} {}", blockrejected, order.getId());
            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);
            actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

    }

}
