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
import com.google.protobuf.util.JsonFormat;

public class Oco implements StrategyI {
    private final JsonFormat.Printer printer;
    private Logger log;
    private RoutingMessage.Order order;
    private Double blockQty = 0d;
    private Boolean blockOrders = false;
    private ActorRef actorStrategy;
    private String idSubscribe;
    private int blockrejected = 0;
    private ActorRef actorPerSession;
    private ActorRef actorGroupPerOrder;

    private MarketDataMessage.Statistic statistic;

    public Oco(RoutingMessage.Order order, ActorRef actorStrategy, String idSubscribe, ActorRef actorPerSession, Logger fileLog, ActorRef actorGroupPerOrder) {
        this.order = order;
        this.actorStrategy = actorStrategy;
        this.idSubscribe = idSubscribe;
        this.actorPerSession = actorPerSession;
        this.log = fileLog;
        this.printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();
        this.actorGroupPerOrder = actorGroupPerOrder;

        log.info(order.toString());

        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

            if (order.getSpread() > order.getLimit() && order.getSide().equals(RoutingMessage.Side.BUY) && order.getSpread() > 0 && order.getLimit() > 0) {
                order = order.toBuilder().setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setPrice(order.getSpread())
                        .setText("el take profit es menos que el stop loss")
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED).build();
                onOrders(order);
                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));
                return;
            }

            if (order.getSpread() < order.getLimit() && order.getSide().equals(RoutingMessage.Side.SELL) && order.getSpread() > 0 && order.getLimit() > 0) {
                order = order.toBuilder().setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setPrice(order.getSpread())
                        .setText("el take profit es mayor que el stop loss")
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED).build();
                onOrders(order);
                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));
                return;
            }

            this.order = order.toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                    .setPrice(0d)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_NEW).build();
            MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));
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

        if (order.getSide().equals(RoutingMessage.Side.BUY)) {
            if (statistic.getAskPx() >= order.getLimit() && order.getLimit() > 0 && blockOrders == false) {
                blockOrders = true;
                this.order = this.order.toBuilder().setPrice(statistic.getAskPx()).setText("StopLoss :(").build();
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                log.info("{} / {}", statistic.getBidPx(), statistic.getAskPx());
                log.info("se envia orden stoplost {}", order.toBuilder());

            } else if (statistic.getAskPx() <= order.getSpread() && order.getSpread() > 0 && blockOrders == false) {
                log.info("{} / {}", statistic.getBidPx(), statistic.getAskPx());
                blockOrders = true;
                this.order = this.order.toBuilder().setPrice(statistic.getAskPx()).setText("TakeProfit :D").build();
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                log.info("{} / {}", statistic.getBidPx(), statistic.getAskPx());
                log.info("se envia orden takeporfot {}", order.toBuilder());

            }

        } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {

            if (statistic.getBidPx() <= order.getLimit() && order.getLimit() > 0 && blockOrders == false) {
                blockOrders = true;
                this.order = this.order.toBuilder().setPrice(statistic.getBidPx()).setText("StopLoss :(").build();
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                log.info("{} / {}", statistic.getBidPx(), statistic.getAskPx());
                log.info("se envia orden stoplost {}", order.toBuilder());

            } else if (statistic.getBidPx() >= order.getSpread() && order.getSpread() > 0 && blockOrders == false) {
                blockOrders = true;
                this.order = this.order.toBuilder().setPrice(statistic.getBidPx()).setText("TakeProfit :D").build();
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                log.info("{} / {}", statistic.getBidPx(), statistic.getAskPx());
                log.info("se envia orden takeporfot {}", order.toBuilder());
            }
        }

    }


    @Override
    public void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest) {
        Double price = order.getPrice();

        if (orderReplaceRequest.getSpread() > orderReplaceRequest.getLimit() && order.getSide().equals(RoutingMessage.Side.BUY) && orderReplaceRequest.getSpread() > 0 && orderReplaceRequest.getLimit() > 0) {
            RoutingMessage.OrderCancelReject reject = RoutingMessage.OrderCancelReject.newBuilder()
                    .setId(order.getId()).setText("el take profit es menos que el stop loss").build();
            MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));
            return;
        }

        if (orderReplaceRequest.getSpread() < orderReplaceRequest.getLimit() && order.getSide().equals(RoutingMessage.Side.SELL) && orderReplaceRequest.getSpread() > 0 && orderReplaceRequest.getLimit() > 0) {

            RoutingMessage.OrderCancelReject reject = RoutingMessage.OrderCancelReject.newBuilder()
                    .setId(order.getId()).setText("el take profit es mayor que el stop loss").build();
            MainApp.getMessageEventBus().publish(new Envelope(order.getId(), reject));
            return;
        }


        order = order.toBuilder()
                .setPrice(price)
                .setOrderQty(orderReplaceRequest.getQuantity())
                .setSpread(orderReplaceRequest.getSpread())
                .setLimit(orderReplaceRequest.getLimit()).build();

        onStatistic(this.statistic);

        MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));

        log.info("orden replace {}", orderReplaceRequest.toString());
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
