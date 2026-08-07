package cl.vc.service.akka.actors.mkd;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;
import cl.vc.service.util.MongoCloseRepository;
import com.google.protobuf.Timestamp;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ActorPerSubscriptionMkd extends AbstractActor {

    private final MarketDataMessage.Subscribe subscribe;
    private final ActorRef actorRef;
    private String id;
    /**
     * Cierre del dia habil anterior (Mongo). Base para calcular la variacion %. 0 = sin dato.
     */
    private double previousDayClose = 0d;
    private boolean varPctOkLogged = false;
    private boolean varPctFallbackLogged = false;
    /**
     * true solo si el destino es BCS: unico exchange cuya var% calculamos desde Mongo.
     */
    private boolean isBcs = false;
    private MarketDataMessage.Statistic.Builder statistics;
    private MarketDataMessage.Trade.Builder trade;
    private static final int MAX_ENTRIES = 40;

    private ActorPerSubscriptionMkd(ActorRef actorRef, MarketDataMessage.Subscribe subscribe) {
        this.subscribe = subscribe;
        this.actorRef = actorRef;
    }

    public static Props props(ActorRef actorRef, MarketDataMessage.Subscribe subscribe) {
        return Props.create(ActorPerSubscriptionMkd.class, actorRef, subscribe);
    }

    @Override
    public void preStart() {

        try {

            id = TopicGenerator.getTopicMKD(subscribe);
            isBcs = subscribe.getSecurityExchange() == MarketDataMessage.SecurityExchangeMarketData.BCS;

            MainApp.getMessageEventBus().subscribe(getSelf(), id);

            statistics = MarketDataMessage.Statistic.newBuilder();
            statistics.setId(subscribe.getId());
            statistics.setSecurityExchange(subscribe.getSecurityExchange());
            statistics.setSymbol(subscribe.getSymbol());
            statistics.setSecurityType(subscribe.getSecurityType());
            statistics.setSettlType(subscribe.getSettlType());

            trade = MarketDataMessage.Trade.newBuilder();
            trade.setId(subscribe.getId());
            trade.setSecurityExchange(subscribe.getSecurityExchange());
            trade.setSymbol(subscribe.getSymbol());
            trade.setSecurityType(subscribe.getSecurityType());
            trade.setSettlType(subscribe.getSettlType());

            if (MainApp.getSendsecuritylist()) {
                if (MainApp.getSecurityExchangeMaps().containsKey(subscribe.getSecurityExchange())) {
                    MainApp.getSecurityExchangeMaps().get(subscribe.getSecurityExchange()).getListSecuritiesList().forEach(s -> {
                        if (s.getSymbol().equals(subscribe.getSymbol())) {
                            try {
                                statistics.setPreviusClose(Double.parseDouble(s.getText()));
                            } catch (NumberFormatException e) {
                            }
                        }
                    });
                }
            }

            // Suscripcion / entrega de snapshot PRIMERO: es el camino critico del feed, no debe esperar a Mongo.
            if (MainApp.getSnapshotHashMap().containsKey(id)) {
                BookSnapshot snapshot1 = MainApp.getSnapshotHashMap().get(id);

                getSelf().tell(snapshot1, ActorRef.noSender());

            } else {
                MainApp.subscribeSymbol(subscribe, id);
            }

            // Cierre de AYER (Mongo) para la variacion %, SOLO BCS. Va DESPUES de suscribir para no demorar
            // el feed. getPreviousClose es O(1) (cache precargada en bloque); si aun no esta, onStatistic reintenta.
            if (isBcs) {
                previousDayClose = MongoCloseRepository.getPreviousClose(subscribe.getSymbol());
            }

        } catch (Exception e) {
            Instant instant = Instant.now();
            NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                    .setSecurityExchange(subscribe.getSecurityExchange().name())
                    .setTime(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build())
                    .setComponent(NotificationMessage.Component.VECTOR_SCREEN_SERVICE)
                    .setMessage("Service unavailable " + subscribe.getSecurityExchange().name())
                    .setLevel(NotificationMessage.Level.ERROR)
                    .setComments("This service is down or may not exist")
                    .setTitle("Not connected")
                    .setTypeState(NotificationMessage.TypeState.DISCONNECTION)
                    .build();
            MainApp.getNotificationConectionMap().put(notification.getSecurityExchange(), notification);
            MainApp.getNotificationMap().add(notification);
            this.actorRef.tell(notification, ActorRef.noSender());

        }

    }

    @Override
    public void postStop() {
        MainApp.getMessageEventBus().unsubscribe(getSelf(), id);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(BookSnapshot.class, this::onSnapshot)
                .match(MarketDataMessage.Trade.class, this::onTrade)
                .match(MarketDataMessage.IncrementalBook.class, this::onIncrementalBook)
                .match(MarketDataMessage.Statistic.class, this::onStatistic)
                .match(MarketDataMessage.Subscribe.class, this::onSubscribe)
                .match(Resubscribe.class, this::onResubscribe)
                .match(SendRejected.class, this::onSendRejected)
                .match(MarketDataMessage.Rejected.class, this::onRejected)
                .build();
    }

    private void onSendRejected(SendRejected conn) {
        try {

            MarketDataMessage.Rejected rejected = MarketDataMessage.Rejected.newBuilder()
                    .setId(conn.getMsg().getId())
                    .setText(conn.getMessageRejected()).build();
            actorRef.tell(rejected, ActorRef.noSender());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onRejected(MarketDataMessage.Rejected conn) {
        try {

            MarketDataMessage.Rejected.Builder rejected = conn.toBuilder();
            rejected.setId(subscribe.getId());
            actorRef.tell(rejected, ActorRef.noSender());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Pisa la variacion % de una Statistic CRUDA del feed (viene con un cierre de ayer viejo) usando el
     * cierre de Mongo. Se aplica a la Statistic que va dentro del Snapshot: si no, el cliente recibe el
     * valor malo del feed y, si el simbolo no vuelve a tickear, se queda pegado con ese valor.
     * Solo BCS; si no hay cierre o precio, devuelve la original sin copiar.
     */
    private MarketDataMessage.Statistic fixVarPct(MarketDataMessage.Statistic raw) {
        if (raw == null || !isBcs) return raw;
        if (previousDayClose <= 0d) {
            previousDayClose = MongoCloseRepository.getPreviousClose(subscribe.getSymbol());
        }
        if (previousDayClose <= 0d) return raw;
        double px = raw.getLast();
        if (px <= 0d) px = raw.getOhlcv().getClose();
        if (px <= 0d) return raw;
        return raw.toBuilder()
                .setPreviusClose(previousDayClose)
                .setImbalance((px - previousDayClose) / previousDayClose * 100d)
                .build();
    }

    private void onSubscribe(MarketDataMessage.Subscribe conn) {
        try {

            if (MainApp.getSnapshotHashMap().containsKey(id)) {

                BookSnapshot snapshot1 = MainApp.getSnapshotHashMap().get(id);

                if (snapshot1.getStatistic() != null) {
                    getSelf().tell(snapshot1.getStatistic(), ActorRef.noSender());
                }


                MarketDataMessage.Snapshot snapshot2 = MarketDataMessage.Snapshot.newBuilder()
                        .setId(subscribe.getId())
                        .setStatistic(fixVarPct(snapshot1.getStatistic()))
                        .addAllBids(snapshot1.getBid())
                        .addAllAsks(snapshot1.getAsk())
                        .addAllTrades(snapshot1.getTradesList())
                        .setSymbol(snapshot1.getSymbol())
                        .setSettlType(snapshot1.getSettlType())
                        .setSecurityType(snapshot1.getSecurityType())
                        .build();

                actorRef.tell(snapshot2, ActorRef.noSender());

            } else {
                MainApp.subscribeSymbol(subscribe, id);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onResubscribe(Resubscribe conn) {
        try {

            if (subscribe.getSecurityExchange().name().equals(conn.getMsg().getSecurityExchange())) {
                MainApp.getConnections_mkd().get(subscribe.getSecurityExchange()).sendMessage(subscribe);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void onTrade(MarketDataMessage.Trade trade) {

        try {

            if (!subscribe.getTrade()) {
                return;
            }

            String id = TopicGenerator.getTopicMKD(trade);
            BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(id);

            if (bookSnapshot == null) {
                log.error("BookSnapshot not found {}", trade.getId());
                return;
            }

            //MarketDataMessage.Trade tradeLast = bookSnapshot.getTradesList().getLast();
            this.trade.setIdGenerico(trade.getIdGenerico());
            if (trade.getIdGenerico().isEmpty()) {
                this.trade.setIdGenerico(IDGenerator.getID());
            }

            this.trade.setT(trade.getT());
            this.trade.setAmount(trade.getAmount());
            this.trade.setBuyer(trade.getBuyer());
            this.trade.setSeller(trade.getSeller());
            this.trade.setPrice(trade.getPrice());
            this.trade.setQty(trade.getQty());


            actorRef.tell(this.trade.build(), ActorRef.noSender());


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onStatistic(MarketDataMessage.Statistic statistic) {

        try {

            if (!subscribe.getStatistic()) {
                return;
            }

            if (statistic.getId().isEmpty()) {
                return;
            }


            String id = TopicGenerator.getTopicMKD(statistic);
            BookSnapshot bookSnapshot;

            if (MainApp.getSnapshotHashMap().containsKey(statistic.getId())) {
                bookSnapshot = MainApp.getSnapshotHashMap().get(statistic.getId());
            } else if (MainApp.getSnapshotHashMap().containsKey(id)) {
                bookSnapshot = MainApp.getSnapshotHashMap().get(id);
            } else {
                return;
            }

              this.statistics.setAmount(bookSnapshot.getStatistic().getAmount());
            this.statistics.setBidQty(bookSnapshot.getStatistic().getBidQty());
            this.statistics.setBidPx(bookSnapshot.getStatistic().getBidPx());
            this.statistics.setAskQty(bookSnapshot.getStatistic().getAskQty());
            this.statistics.setAskPx(bookSnapshot.getStatistic().getAskPx());
            this.statistics.setTradeVolume(bookSnapshot.getStatistic().getTradeVolume());
            this.statistics.setOhlcv(bookSnapshot.getStatistic().getOhlcv());
            this.statistics.setAmountTheoric(bookSnapshot.getStatistic().getAmountTheoric());
            this.statistics.setPriceTheoric(bookSnapshot.getStatistic().getPriceTheoric());
            // this.statistics.setPreviusClose(bookSnapshot.getStatistic().getPreviusClose());
            // this.statistics.setDelta(bookSnapshot.getStatistic().getDelta());
            this.statistics.setRatio(bookSnapshot.getStatistic().getRatio());
            this.statistics.setLast(bookSnapshot.getStatistic().getLast());
            this.statistics.setClose(bookSnapshot.getStatistic().getClose());
            this.statistics.setMedio((bookSnapshot.getStatistic().getBidPx() + bookSnapshot.getStatistic().getAskPx()) / 2);
            this.statistics.setDesbalTheoric(bookSnapshot.getStatistic().getDesbalTheoric());
            this.statistics.setOwnDemand(bookSnapshot.getStatistic().getOwnDemand());
            this.statistics.setTickDirecion(bookSnapshot.getStatistic().getTickDirecion());
            this.statistics.setReferencialPrice(bookSnapshot.getStatistic().getReferencialPrice());
            this.statistics.setVwap(bookSnapshot.getStatistic().getVwap());

            // Si aun no tenemos el cierre (cache precargando al arrancar), reintenta O(1) desde la cache.
            if (isBcs && previousDayClose <= 0d) {
                previousDayClose = MongoCloseRepository.getPreviousClose(subscribe.getSymbol());
            }

            // Variacion %: SOLO BCS la calculamos y PISAMOS la del feed (trae un cierre de ayer viejo/malo).
            // Alpaca/otros destinos, o BCS sin cierre en Mongo: se usa la var% del feed tal cual.
            if (isBcs && previousDayClose > 0d) {
                // "el que te esta cayendo" = ultimo precio transado (columna Ultimo). Fallback al close del ohlcv.
                double px = this.statistics.getLast();
                if (px <= 0d) px = this.statistics.getOhlcv().getClose();
                if (px > 0d) {
                    double varPct = (px - previousDayClose) / previousDayClose * 100d;
                    varPct = Math.round(varPct * 100.0) / 100.0;

                    this.statistics.setPreviusClose(previousDayClose);
                    this.statistics.setImbalance(varPct);
                    if (!varPctOkLogged) {
                        log.info("[VarPct] {} OK var%={} (precio={}, cierreAyer={})",
                                subscribe.getSymbol(), String.format("%.2f", varPct), px, previousDayClose);
                        varPctOkLogged = true;
                    }
                }
            } else {
                this.statistics.setImbalance(bookSnapshot.getStatistic().getImbalance());
                this.statistics.setPreviusClose(bookSnapshot.getStatistic().getPreviusClose());
                this.statistics.setDelta(bookSnapshot.getStatistic().getDelta());
                // Deja constancia (1 vez por simbolo) de POR QUE no se calculo: leyendo Mongo vs sin dato.
                if (isBcs && !varPctFallbackLogged) {
                    log.warn("[VarPct] {} usa var% del FEED: {}", subscribe.getSymbol(),
                            MongoCloseRepository.isReading() ? "leyendo cierres de Mongo (precarga en curso)"
                                    : MongoCloseRepository.isWarmed() ? "sin cierre de ayer en Mongo para este simbolo"
                                    : "Mongo no cargo (revisar mongo.isconnected/conexion)");
                    varPctFallbackLogged = true;
                }
            }

            actorRef.tell(statistics.build(), ActorRef.noSender());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onIncrementalBook(MarketDataMessage.IncrementalBook msg) {

        try {

            if (!subscribe.getBook()) {
                return;
            }

            if (!MainApp.getSnapshotHashMap().containsKey(msg.getId())) {
                return;
            }


            BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(msg.getId());


            MarketDataMessage.IncrementalBook.Builder incremental = MarketDataMessage.IncrementalBook.newBuilder();
            incremental.setId(subscribe.getId());
            incremental.setSymbol(subscribe.getSymbol());
            incremental.setSecurityExchange(subscribe.getSecurityExchange());
            incremental.setSettlType(subscribe.getSettlType());


            if (subscribe.getDepth().equals(MarketDataMessage.Depth.FULL_BOOK)) {

                for (int i = 0; i < Math.min(bookSnapshot.getBid().size(), MAX_ENTRIES); i++) {
                    incremental.addBids(bookSnapshot.getBid().get(i));
                }

                for (int i = 0; i < Math.min(bookSnapshot.getAsk().size(), MAX_ENTRIES); i++) {
                    incremental.addAsks(bookSnapshot.getAsk().get(i));
                }

            } else {

                if (!bookSnapshot.getBid().isEmpty()) {
                    incremental.addBids(bookSnapshot.getBid().get(0));
                }

                if (!bookSnapshot.getAsk().isEmpty()) {
                    incremental.addAsks(bookSnapshot.getAsk().get(0));
                }

            }

            actorRef.tell(incremental.build(), ActorRef.noSender());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onSnapshot(BookSnapshot msg) {
        try {


            BookSnapshot snapshot1 = MainApp.getSnapshotHashMap().get(id);

            List<MarketDataMessage.DataBook> bid = new ArrayList<>();
            List<MarketDataMessage.DataBook> ask = new ArrayList<>();


            for (int i = 0; i < Math.min(snapshot1.getBid().size(), MAX_ENTRIES); i++) {
                bid.add(snapshot1.getBid().get(i));
            }

            for (int i = 0; i < Math.min(snapshot1.getAsk().size(), MAX_ENTRIES); i++) {
                ask.add(snapshot1.getAsk().get(i));
            }

            MarketDataMessage.Snapshot snapshot2 = MarketDataMessage.Snapshot.newBuilder()
                    .setId(subscribe.getId())
                    .setStatistic(fixVarPct(snapshot1.getStatistic()))
                    .addAllBids(bid)
                    .addAllAsks(ask)
                    .addAllTrades(snapshot1.getTradesList())
                    .setSymbol(snapshot1.getSymbol())
                    .setSettlType(snapshot1.getSettlType())
                    .setSecurityType(snapshot1.getSecurityType())
                    .setSecurityExchange(snapshot1.getSecurityExchangeMarketData())
                    .build();

            actorRef.tell(snapshot2, ActorRef.noSender());
            getSelf().tell(snapshot1.getStatistic(), ActorRef.noSender());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    @Value
    public static class SendRejected {
        private MarketDataMessage.Subscribe msg;
        private String messageRejected;
    }

    @Value
    public static class Resubscribe {
        private NotificationMessage.Notification msg;
    }

}
