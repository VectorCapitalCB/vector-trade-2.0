package cl.vc.service.akka.actors.routing;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.LogGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.strategy.*;
import cl.vc.service.util.BookSnapshot;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashMap;

@Slf4j
public class ActorStrategy extends AbstractActor {

    private static final Object lock = new Object();
    private final RoutingMessage.Order order;
    private final ActorRef actorGroupPerOrder;
    private StrategyI strategy;
    private Logger fileLog;
    private MarketDataMessage.Subscribe subscribe;

    private String idSubscribe;
    private HashMap<String, ActorRef> strategyActors;


    private ActorStrategy(RoutingMessage.NewOrderRequest msg, ActorRef actorGroupPerOrder, HashMap<String, ActorRef> strategyActors ) {
        this.actorGroupPerOrder = actorGroupPerOrder;
        this.order = msg.getOrder().toBuilder().setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW).build();
        this. strategyActors = strategyActors;
    }

    private ActorStrategy(RoutingMessage.Order msg, ActorRef actorGroupPerOrder, HashMap<String, ActorRef> strategyActors) {
        this.actorGroupPerOrder = actorGroupPerOrder;
        this. strategyActors = strategyActors;
        this.order = msg;
    }


    public static Props props(RoutingMessage.Order msg, ActorRef actorGroupPerOrder, HashMap<String, ActorRef> strategyActors) {
        return Props.create(ActorStrategy.class, msg, actorGroupPerOrder, strategyActors);
    }

    public static Props props(RoutingMessage.NewOrderRequest msg, ActorRef actorGroupPerOrder, HashMap<String, ActorRef> strategyActors) {
        return Props.create(ActorStrategy.class, msg, actorGroupPerOrder, strategyActors);
    }

    @Override
    public void preStart() {
        try {


            synchronized (lock) {
                try {

                    String path = MainApp.getProperties().getProperty("path.logs") + File.separator + order.getStrategyOrder().name() + File.separator;
                    String name = order.getSymbol() + "_" + order.getId();
                    fileLog = LogGenerator.start(path, name);
                    fileLog.info("##########################################");
                    fileLog.info("Se inicia strategia {} {}", order.getSymbol(), order.getId());
                    fileLog.info("##########################################");

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.HOLGURA)) {
                strategy = new Holgura(order, idSubscribe, fileLog, getSelf(), actorGroupPerOrder);
            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BEST)) {
                strategy = new Best(order, idSubscribe, fileLog, actorGroupPerOrder);
            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_LAST)) {
                strategy = new BasketLast(order, idSubscribe, fileLog, actorGroupPerOrder, getSelf());
            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_PASSIVE)) {
                strategy = new BasketPassive(order, idSubscribe, fileLog, actorGroupPerOrder, getSelf());
            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_AGGRESSIVE)) {
                strategy = new BasketAggressive(order, idSubscribe, fileLog, actorGroupPerOrder, getSelf());
            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.OCO)) {
                strategy = new Oco(order, getSelf(), idSubscribe, actorGroupPerOrder, fileLog, actorGroupPerOrder);
            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.TRAILING)) {
                strategy = new Trailing(order, getSelf(), idSubscribe, fileLog, actorGroupPerOrder);
            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.VWAP)) {
                strategy = new Vwap(order, fileLog, actorGroupPerOrder,  getSelf(), strategyActors);
            }

             subscribe = MarketDataMessage.Subscribe.newBuilder()
                    .setId(IDGenerator.getID())
                    .setBook(true)
                    .setStatistic(true)
                    .setTrade(false)
                    .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                    .setSymbol(order.getSymbol())
                    .setSecurityExchange(IDGenerator.conversorExdestination(order.getSecurityExchange()))
                    .setSettlType(order.getSettlType())
                    .setSecurityType(order.getSecurityType())
                    .build();

            MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));
            subscribcion();

        } catch (Exception e) {
            fileLog.error(e.getMessage(), e);
        }
    }

    public void subscribcion(){

        if (order.getSecurityType().equals(RoutingMessage.SecurityType.CFI)) {
            subscribe = subscribe.toBuilder().setSecurityType(RoutingMessage.SecurityType.CS).build();
        }

        idSubscribe = TopicGenerator.getTopicMKD(subscribe);

        MainApp.getMessageEventBus().subscribe(getSelf(), idSubscribe);
        MainApp.getMessageEventBus().subscribe(getSelf(), order.getId());


        if (MainApp.getSnapshotHashMap().containsKey(idSubscribe)) {
            BookSnapshot snapshot = MainApp.getSnapshotHashMap().get(idSubscribe);
            getSelf().tell(snapshot, ActorRef.noSender());
        } else {
            subscribe =  subscribe.toBuilder().setId(idSubscribe).build();
            MainApp.subscribeSymbol(subscribe, idSubscribe);
            MainApp.getConnections_mkd().get(subscribe.getSecurityExchange()).sendMessage(subscribe);
        }


    }



    @Override
    public void postStop() {
        MainApp.getMessageEventBus().unsubscribe(getSelf(), idSubscribe);
        MainApp.getMessageEventBus().unsubscribe(getSelf(), order.getId());


        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
        log.info("se elimina actor estrategia {}", order.getId());

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(BookSnapshot.class, this::onSnapshot)
                .match(MarketDataMessage.IncrementalBook.class, this::onIncrementalBook)
                .match(MarketDataMessage.Statistic.class, this::onStatistic)
                .match(RoutingMessage.Order.class, this::onOrders)
                .match(RoutingMessage.OrderReplaceRequest.class, this::onReplaceRequest)
                .match(RoutingMessage.OrderCancelRequest.class, this::onCancelRequest)
                .match(RoutingMessage.OrderCancelReject.class, this::onRejected)
                .match(SessionsMessage.Disconnect.class, this::onDisconect)
                .match(SessionsMessage.Connect.class, this::onConect)
                .build();
    }

    public void onDisconect(SessionsMessage.Disconnect disconnect) {

    }

    public void onConect(SessionsMessage.Connect onconect) {

        try {

            if(onconect.getDestination().equals("XSGO")){
                RoutingMessage.SecurityExchangeRouting routingdestination = RoutingMessage.SecurityExchangeRouting.XSGO;
                if(order.getSecurityExchange().equals(routingdestination)) {
                    subscribcion();
                }
            }

            if(onconect.getDestination().equals("BCS")){
                subscribcion();
            }


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onReplaceRequest(RoutingMessage.OrderReplaceRequest msg) {
        strategy.onReplace(msg);
    }

    public void onCancelRequest(RoutingMessage.OrderCancelRequest msg) {
        strategy.onCancelRequest(msg);
    }

    public void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {
        strategy.onIncrementalBook(incrementalBook);
    }

    public void onSnapshot(BookSnapshot snapshot) {
        strategy.onSnapshot(snapshot);
    }

    public void onStatistic(MarketDataMessage.Statistic statistic) {
        strategy.onStatistic(statistic);
    }

    private void onRejected(RoutingMessage.OrderCancelReject rejected) {
        strategy.onRejected(rejected);
        actorGroupPerOrder.tell(rejected, ActorRef.noSender());
    }

    private void onOrders(RoutingMessage.Order order) {

        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)
                || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)
                || order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)) {


            if(!order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.VWAP)){
                MainApp.getMessageEventBus().unsubscribe(getSelf(), idSubscribe);
                MainApp.getMessageEventBus().unsubscribe(getSelf(), order.getId());
            }


        }

        strategy.onOrders(order);
    }

}
