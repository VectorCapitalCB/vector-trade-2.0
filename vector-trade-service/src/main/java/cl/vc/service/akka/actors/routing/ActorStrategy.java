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
import cl.vc.service.util.OrigClOrdIdRecoverySupport;
import cl.vc.service.util.OrderStateSupport;
import cl.vc.service.util.StrategyRecoverySupport;
import cl.vc.service.util.StrategyRecoveryState;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashMap;

@Slf4j
public class ActorStrategy extends AbstractActor {

    /** Mensaje interno: chequear si la estrategia sigue SIN market data tras el timeout. */
    private static final class MkdTimeoutCheck {
        static final MkdTimeoutCheck INSTANCE = new MkdTimeoutCheck();
    }

    private static final class ResumeAfterRateLimit {
        private final long generation;

        private ResumeAfterRateLimit(long generation) {
            this.generation = generation;
        }
    }

    private static final class BlockedStrategyCheck {
        static final BlockedStrategyCheck INSTANCE = new BlockedStrategyCheck();
    }

    private static final Object lock = new Object();
    private final RoutingMessage.Order order;
    private final ActorRef actorGroupPerOrder;
    private StrategyI strategy;
    private Logger fileLog;
    private MarketDataMessage.Subscribe subscribe;

    private String idSubscribe;
    private HashMap<String, ActorRef> strategyActors;
    private int missingOrigClOrdIdRejects;
    private boolean limitReasonActive;
    private long blockedSinceMillis = -1L;
    private RoutingMessage.Order latestOrder;
    private BookSnapshot latestSnapshot;
    private final StrategyRecoveryState recoveryState = new StrategyRecoveryState();

    public static final class StrategyStatusReason {
        private final String orderId;
        private final String reason;

        public StrategyStatusReason(String orderId, String reason) {
            this.orderId = orderId;
            this.reason = reason;
        }

        public String getOrderId() {
            return orderId;
        }

        public String getReason() {
            return reason;
        }
    }

    public static final class ExternalOrderUnavailable {
        private final String orderId;
        private final RoutingMessage.OrderCancelReject rejected;

        public ExternalOrderUnavailable(String orderId, RoutingMessage.OrderCancelReject rejected) {
            this.orderId = orderId;
            this.rejected = rejected;
        }

        public String getOrderId() {
            return orderId;
        }

        public RoutingMessage.OrderCancelReject getRejected() {
            return rejected;
        }
    }


    private ActorStrategy(RoutingMessage.NewOrderRequest msg, ActorRef actorGroupPerOrder, HashMap<String, ActorRef> strategyActors ) {
        this.actorGroupPerOrder = actorGroupPerOrder;
        this.order = msg.getOrder().toBuilder().setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW).build();
        this.latestOrder = this.order;
        this. strategyActors = strategyActors;
    }

    private ActorStrategy(RoutingMessage.Order msg, ActorRef actorGroupPerOrder, HashMap<String, ActorRef> strategyActors) {
        this.actorGroupPerOrder = actorGroupPerOrder;
        this. strategyActors = strategyActors;
        this.order = msg;
        this.latestOrder = msg;
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

            MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));


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


            subscribcion();

            // Si tras N segundos la estrategia sigue SIN un libro usable (sin market data),
            // se rechaza la orden hacia el front: jamás dejarla en PENDING_NEW invisible.
            int mkdRejectSeconds = 20;
            try {
                mkdRejectSeconds = Integer.parseInt(
                        MainApp.getProperties().getProperty("strategy.mkd.reject.seconds", "20").trim());
            } catch (Exception ignore) {
            }
            if (mkdRejectSeconds > 0) {
                getContext().getSystem().scheduler().scheduleOnce(
                        java.time.Duration.ofSeconds(mkdRejectSeconds),
                        getSelf(), MkdTimeoutCheck.INSTANCE,
                        getContext().getDispatcher(), ActorRef.noSender());
            }

            scheduleBlockedStrategyCheck();

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


        BookSnapshot snapshot = MainApp.getSnapshotHashMap().get(idSubscribe);
        if (snapshot != null) {
            getSelf().tell(snapshot, ActorRef.noSender());
        }

        // Trades o estadisticas pueden crear un snapshot sin profundidad. En ese caso
        // el snapshot no reemplaza la suscripcion al libro que necesita la estrategia.
        if (!hasBookDepth(snapshot)) {
            log.warn("[MKD/Strategy] Snapshot sin profundidad para {} ({}); solicitando FULL_BOOK.",
                    order.getSymbol(), idSubscribe);
            subscribe = subscribe.toBuilder().setId(idSubscribe).build();
            MainApp.subscribeSymbol(subscribe, idSubscribe);
        }


    }

    static boolean hasBookDepth(BookSnapshot snapshot) {
        return snapshot != null
                && ((!snapshot.getBid().isEmpty()) || (!snapshot.getAsk().isEmpty()));
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
                .match(MkdTimeoutCheck.class, this::onMkdTimeoutCheck)
                .match(ResumeAfterRateLimit.class, this::onResumeAfterRateLimit)
                .match(BlockedStrategyCheck.class, this::onBlockedStrategyCheck)
                .build();
    }

    public void onDisconect(SessionsMessage.Disconnect disconnect) {

    }

    /** Timeout sin market data: si la estrategia sigue esperando el primer libro usable,
     *  rechaza la orden hacia el front (no dejarla en el limbo PENDING_NEW). */
    private void onMkdTimeoutCheck(MkdTimeoutCheck check) {
        try {
            if (strategy != null && strategy.awaitingFirstMarketData()) {
                log.warn("orden {} {} SIN market data tras timeout -> se rechaza hacia el front",
                        order.getSymbol(), order.getId());
                if (fileLog != null) {
                    fileLog.warn("TIMEOUT sin market data ({}) -> REJECT hacia el front", idSubscribe);
                }
                strategy.rejectNoMarketData();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
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
        if (recoveryState.isRateLimitPaused()) {
            latestSnapshot = MainApp.getSnapshotHashMap().get(idSubscribe);
            return;
        }
        strategy.onIncrementalBook(incrementalBook);
        publishLimitReasonIfChanged(false);
    }

    public void onSnapshot(BookSnapshot snapshot) {
        latestSnapshot = snapshot;
        if (recoveryState.isRateLimitPaused()) {
            return;
        }
        strategy.onSnapshot(snapshot);
        publishLimitReasonIfChanged(false);
    }

    public void onStatistic(MarketDataMessage.Statistic statistic) {
        if (recoveryState.isRateLimitPaused()) {
            latestSnapshot = MainApp.getSnapshotHashMap().get(idSubscribe);
            return;
        }
        strategy.onStatistic(statistic);
        publishLimitReasonIfChanged(false);
    }

    private void onRejected(RoutingMessage.OrderCancelReject rejected) {
        if (OrigClOrdIdRecoverySupport.isMissingFromSequence(rejected.getText())) {
            missingOrigClOrdIdRejects++;
            RoutingMessage.OrderCancelReject operatorReject =
                    OrigClOrdIdRecoverySupport.withOperatorReason(rejected);

            log.warn("[OrderRecovery][ORIG_CL_ORD_ID_MISSING] orderId={} strategy={} attempt={}/{} exchangeReason={}",
                    order.getId(), order.getStrategyOrder(), missingOrigClOrdIdRejects,
                    OrigClOrdIdRecoverySupport.MAX_REJECTS, rejected.getText());

            if (missingOrigClOrdIdRejects >= OrigClOrdIdRecoverySupport.MAX_REJECTS) {
                actorGroupPerOrder.tell(
                        new ExternalOrderUnavailable(order.getId(), operatorReject),
                        ActorRef.noSender());
                getContext().stop(getSelf());
                return;
            }

            strategy.onRejected(operatorReject);
            actorGroupPerOrder.tell(operatorReject, ActorRef.noSender());
            return;
        }

        if (StrategyRecoverySupport.isOrderRateLimit(rejected.getText())) {
            missingOrigClOrdIdRejects = 0;
            pauseStrategyForRateLimit(rejected);
            actorGroupPerOrder.tell(rejected, ActorRef.noSender());
            return;
        }

        missingOrigClOrdIdRejects = 0;

        StrategyRecoveryState.RejectAction rejectAction = recoveryState.registerReject(rejectThreshold());
        if (rejectAction == StrategyRecoveryState.RejectAction.CANCEL_REJECTED_RESUME) {
            strategy.resumeAfterTemporaryBlock();
            strategy.resetRejectRecovery();
            blockedSinceMillis = -1L;
            log.warn("[StrategyRecovery][CANCEL_REJECTED_RESUME] orderId={} strategy={} reason={}",
                    order.getId(), order.getStrategyOrder(), rejected.getText());
            replayLatestMarketData();
            actorGroupPerOrder.tell(rejected, ActorRef.noSender());
            return;
        }

        int rejectThreshold = rejectThreshold();
        if (rejectAction == StrategyRecoveryState.RejectAction.CANCEL_LIVE_ORDER) {
            strategy.resetRejectRecovery();
            strategy.cancelAfterConsecutiveRejects(order.getId());
            blockedSinceMillis = System.currentTimeMillis();
            log.warn("[StrategyRecovery][CANCEL_AFTER_REJECTS] orderId={} strategy={} rejects={}/{} reason={}",
                    order.getId(), order.getStrategyOrder(), recoveryState.getConsecutiveRejects(), rejectThreshold,
                    rejected.getText());
        } else {
            strategy.onRejected(rejected);
            strategy.resetRejectRecovery();
        }
        actorGroupPerOrder.tell(rejected, ActorRef.noSender());
    }

    private void onOrders(RoutingMessage.Order incomingOrder) {
        if (!incomingOrder.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE)
                && !incomingOrder.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL)) {
            missingOrigClOrdIdRejects = 0;
        }
        if (OrderStateSupport.isInconsistentFilled(incomingOrder)) {
            log.warn("[OrderState][STRATEGY_FILLED_NORMALIZED] strategy={} orderId={} execId={} execType={} orderQty={} cumQty={} leaves={}",
                    incomingOrder.getStrategyOrder(),
                    incomingOrder.getId(),
                    incomingOrder.getExecId(),
                    incomingOrder.getExecType(),
                    incomingOrder.getOrderQty(),
                    incomingOrder.getCumQty(),
                    incomingOrder.getLeaves());
        }
        RoutingMessage.Order order = OrderStateSupport.normalizeInconsistentFilled(incomingOrder);
        latestOrder = order;

        if (!order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE)
                && !order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL)) {
            recoveryState.successfulNonPendingUpdate();
            blockedSinceMillis = -1L;
            strategy.resetRejectRecovery();
        }

        if (OrderStateSupport.isConclusiveStrategyTerminal(order)) {


            if(!order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.VWAP)){
                MainApp.getMessageEventBus().unsubscribe(getSelf(), idSubscribe);
                MainApp.getMessageEventBus().unsubscribe(getSelf(), order.getId());
            }


        }

        strategy.onOrders(order);
        publishLimitReasonIfChanged(true);
    }

    private void pauseStrategyForRateLimit(RoutingMessage.OrderCancelReject rejected) {
        long generation = recoveryState.pauseForRateLimit();
        int pauseSeconds = propertyInt("strategy.rate.limit.pause.seconds", 3, 1, 30);
        log.warn("[StrategyRecovery][RATE_LIMIT_PAUSE] orderId={} strategy={} seconds={} reason={}",
                order.getId(), order.getStrategyOrder(), pauseSeconds, rejected.getText());
        getContext().getSystem().scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(pauseSeconds),
                getSelf(), new ResumeAfterRateLimit(generation),
                getContext().getDispatcher(), ActorRef.noSender());
    }

    private void onResumeAfterRateLimit(ResumeAfterRateLimit resume) {
        if (!recoveryState.resumeAfterRateLimit(resume.generation)) {
            return;
        }
        strategy.resumeAfterTemporaryBlock();
        strategy.resetRejectRecovery();
        blockedSinceMillis = -1L;
        log.info("[StrategyRecovery][RATE_LIMIT_RESUME] orderId={} strategy={}",
                order.getId(), order.getStrategyOrder());
        replayLatestMarketData();
    }

    private void scheduleBlockedStrategyCheck() {
        getContext().getSystem().scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(1),
                getSelf(), BlockedStrategyCheck.INSTANCE,
                getContext().getDispatcher(), ActorRef.noSender());
    }

    private void onBlockedStrategyCheck(BlockedStrategyCheck ignored) {
        try {
            if (!recoveryState.isRateLimitPaused() && isActiveOrder(latestOrder)
                    && (recoveryState.isCancelPending() || strategy.isTemporarilyBlocked())) {
                long now = System.currentTimeMillis();
                if (blockedSinceMillis < 0L) {
                    blockedSinceMillis = now;
                }
                int resumeSeconds = propertyInt("strategy.blocked.resume.seconds", 10, 1, 120);
                if (now - blockedSinceMillis >= resumeSeconds * 1000L) {
                    log.warn("[StrategyRecovery][STALE_BLOCK_RESUME] orderId={} strategy={} seconds={}",
                            order.getId(), order.getStrategyOrder(), resumeSeconds);
                    recoveryState.releaseStaleBlock();
                    strategy.resumeAfterTemporaryBlock();
                    strategy.resetRejectRecovery();
                    blockedSinceMillis = -1L;
                    replayLatestMarketData();
                }
            } else if (!strategy.isTemporarilyBlocked()) {
                blockedSinceMillis = -1L;
            }
        } finally {
            scheduleBlockedStrategyCheck();
        }
    }

    private void replayLatestMarketData() {
        if (!recoveryState.isRateLimitPaused() && latestSnapshot != null && isActiveOrder(latestOrder)) {
            strategy.onSnapshot(latestSnapshot);
            publishLimitReasonIfChanged(false);
        }
    }

    private void publishLimitReasonIfChanged(boolean refreshAtLimit) {
        if (!isActiveOrder(latestOrder)) {
            limitReasonActive = false;
            return;
        }
        boolean atLimit = strategy.isAtConfiguredLimit();
        String reason = StrategyRecoverySupport.limitStatusReason(
                limitReasonActive, atLimit, refreshAtLimit);
        if (reason == null) {
            return;
        }
        limitReasonActive = atLimit;
        actorGroupPerOrder.tell(new StrategyStatusReason(order.getId(), reason), ActorRef.noSender());
        log.info("[StrategyRecovery][LIMIT_STATUS] orderId={} strategy={} atLimit={} reason={}",
                order.getId(), order.getStrategyOrder(), atLimit, reason);
    }

    private int rejectThreshold() {
        String configured = MainApp.getProperties().getProperty("strategy.reject.cancel.threshold", "5");
        return StrategyRecoverySupport.rejectThreshold(configured);
    }

    private int propertyInt(String key, int defaultValue, int min, int max) {
        int value = defaultValue;
        try {
            value = Integer.parseInt(MainApp.getProperties().getProperty(key, String.valueOf(defaultValue)).trim());
        } catch (Exception ignore) {
        }
        return Math.max(min, Math.min(max, value));
    }

    private boolean isActiveOrder(RoutingMessage.Order current) {
        return current != null
                && StrategyRecoverySupport.isExchangeRecognizedActive(current.getOrdStatus());
    }

}
