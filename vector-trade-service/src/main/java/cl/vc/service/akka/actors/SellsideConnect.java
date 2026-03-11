package cl.vc.service.akka.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;

@Slf4j
public class SellsideConnect extends AbstractActor {

    public static Props props() {
        return Props.create(SellsideConnect.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransportingObjects.class, this::onMessages)
                .match(MarketDataMessage.Snapshot.class, this::onSnapshot)
                .match(MarketDataMessage.Trade.class, this::onTrade)
                .match(MarketDataMessage.IncrementalBook.class, this::onIncrementalBook)
                .match(MarketDataMessage.Statistic.class, this::onStatistic)
                .build();
    }

    private void onMessages(TransportingObjects conn) {

        try {

            if (conn.getMessage() instanceof SessionsMessage.Connect) {
                onConnect(conn);

            } else if (conn.getMessage() instanceof SessionsMessage.Disconnect) {
                onDisconnect(conn);

            } else if (conn.getMessage() instanceof NotificationMessage.Notification notification) {

                BuySideConnect.getClientSesionId().forEach((key, value) -> value.channel().writeAndFlush(notification));
                MainApp.getNotificationMap().add(notification);

            } else if (conn.getMessage() instanceof RoutingMessage.OrderCancelReject order) {

                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));


            } else if (conn.getMessage() instanceof RoutingMessage.Order order) {
                MainApp.getMessageEventBus().publish(new Envelope(order.getId(), order));
                if (!order.getAccount().isEmpty()) {
                    MainApp.getMessageEventBus().publish(new Envelope(order.getAccount(), order));
                }

            } else if (conn.getMessage() instanceof MarketDataMessage.Snapshot snapshot) {
                getSelf().tell(snapshot, ActorRef.noSender());

            } else if (conn.getMessage() instanceof MarketDataMessage.Trade trade) {
                getSelf().tell(trade, ActorRef.noSender());
            } else if (conn.getMessage() instanceof MarketDataMessage.IncrementalBook incrementalBook) {
                getSelf().tell(incrementalBook, ActorRef.noSender());

            } else if (conn.getMessage() instanceof MarketDataMessage.Statistic incremental) {

                getSelf().tell(incremental, ActorRef.noSender());

            } else if (conn.getMessage() instanceof MarketDataMessage.SecurityList securityList) {

                if (MainApp.getSecurityExchangeMaps().containsKey(securityList.getSecurityExchange())) {

                    MarketDataMessage.SecurityList.Builder securityListBuild = MainApp.getSecurityExchangeMaps().get(securityList.getSecurityExchange()).toBuilder();

                    securityList.getListSecuritiesList().forEach(s -> {
                        securityListBuild.addListSecurities(s);
                        String id = s.getSymbol() + s.getSecurityExchange().name();
                        MainApp.getSecurityExchangeSymbolsMaps().put(id, s);
                    });

                    MainApp.getSecurityExchangeMaps().put(securityList.getSecurityExchange(), securityListBuild.build());

                    if (MainApp.isSubscribeSecuritylist_bcs() && securityList.getSecurityExchange().equals(MarketDataMessage.SecurityExchangeMarketData.BCS)) {

                        securityList.getListSecuritiesList().forEach(s -> {

                            MarketDataMessage.Subscribe.Builder subscribe = MarketDataMessage.Subscribe.newBuilder()
                                    .setSettlType(RoutingMessage.SettlType.T2)
                                    .setSecurityType(RoutingMessage.SecurityType.valueOf(s.getSecurityType()))
                                    .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                                    .setBook(true)
                                    .setStatistic(true)
                                    .setTrade(true)
                                    .setSymbol(s.getSymbol())
                                    .setId(IDGenerator.getID())
                                    .setSecurityExchange(s.getSecurityExchange());
                            String id = TopicGenerator.getTopicMKD(subscribe);

                            MainApp.subscribeSymbol(subscribe.build(), id);

                            MarketDataMessage.Subscribe.Builder subscribeT1 = MarketDataMessage.Subscribe.newBuilder()
                                    .setSettlType(RoutingMessage.SettlType.NEXT_DAY)
                                    .setSecurityType(RoutingMessage.SecurityType.valueOf(s.getSecurityType()))
                                    .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                                    .setBook(true)
                                    .setStatistic(true)
                                    .setTrade(true)
                                    .setSymbol(s.getSymbol())
                                    .setId(IDGenerator.getID())
                                    .setSecurityExchange(s.getSecurityExchange());
                            String id_t1 = TopicGenerator.getTopicMKD(subscribeT1);

                            MainApp.subscribeSymbol(subscribeT1.build(), id_t1);

                            MarketDataMessage.Subscribe.Builder subscribeT0 = MarketDataMessage.Subscribe.newBuilder()
                                    .setSettlType(RoutingMessage.SettlType.CASH)
                                    .setSecurityType(RoutingMessage.SecurityType.valueOf(s.getSecurityType()))
                                    .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                                    .setBook(true)
                                    .setStatistic(true)
                                    .setTrade(true)
                                    .setSymbol(s.getSymbol())
                                    .setId(IDGenerator.getID())
                                    .setSecurityExchange(s.getSecurityExchange());
                            String id_t0 = TopicGenerator.getTopicMKD(subscribeT0);

                            MainApp.subscribeSymbol(subscribeT0.build(), id_t0);
                        });
                    }

                } else {
                    MainApp.getSecurityExchangeMaps().put(securityList.getSecurityExchange(), securityList);
                }


            } else if (conn.getMessage() instanceof MarketDataMessage.SnapshotNews news) {
                MainApp.getListNews().addAll(news.getNewsList());

            } else if (conn.getMessage() instanceof MarketDataMessage.News) {
                MarketDataMessage.News.Builder news = (MarketDataMessage.News.Builder) conn.getMessage().toBuilder();
                news.setT(TimeGenerator.getTimeProto());
                MainApp.getListNews().add(news.build());
                MainApp.getMessageEventBus().publish(new Envelope("news", news.build()));

            } else if (conn.getMessage() instanceof MarketDataMessage.Rejected rejected) {
                MainApp.getMessageEventBus().publish(new Envelope(rejected.getId(), rejected));
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void onConnect(TransportingObjects conn) {
        try {

            SessionsMessage.Connect connect = (SessionsMessage.Connect) conn.getMessage();

            log.info("connected, {} {}", connect.getDestination(), connect.getComponent());

            NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                    .setTitle("Connect")
                    .setTypeState(NotificationMessage.TypeState.CONNECTION)
                    .setLevel(NotificationMessage.Level.SUCCESS)
                    .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                    .setComponent(NotificationMessage.Component.VECTOR_TRADE_SERVICES)
                    .setComments(NotificationMessage.Component.VECTOR_TRADE_SERVICES.name() + "->" + connect.getComponent().name())
                    .setSecurityExchange(connect.getDestination())
                    .setMessage("Connected session: " + connect.getDestination()).build();


            MainApp.putConnectionNotification(notification);
            MainApp.getNotificationMap().add(notification);
            BuySideConnect.getClientSesionId().forEach((key1, value1) -> value1.channel().writeAndFlush(notification));


            if (connect.getComponent().equals(NotificationMessage.Component.ORB)) {

                MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData = MarketDataMessage.SecurityExchangeMarketData.valueOf(connect.getDestination());

                MainApp.getIdSymbolsSubscrib().forEach((key, value) -> {
                    if (value.getSecurityExchange().equals(securityExchangeMarketData)) {
                        MainApp.getConnections_mkd().get(value.getSecurityExchange()).sendMessage(value);
                    }
                });


                BuySideConnect.getActorPerSessionMaps().forEach((key, value) -> value.tell(notification, ActorRef.noSender()));

                MarketDataMessage.SecurityExchangeMarketData securityExchange = MarketDataMessage.SecurityExchangeMarketData.valueOf(connect.getDestination());
                MarketDataMessage.SecurityListRequest securityListRequest = MarketDataMessage.SecurityListRequest.newBuilder().setId(IDGenerator.getID()).build();
                MainApp.getConnections_mkd().get(securityExchange).sendMessage(securityListRequest);
            }

            if (connect.getDestination().contains(MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL.name())) {

                MarketDataMessage.Subscribe.Builder subscribe = MarketDataMessage.Subscribe.newBuilder()
                        .setId(IDGenerator.getID())
                        .setSymbol("USD/CLP")
                        .setStatistic(true)
                        .setBook(true)
                        .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL)
                        .setSecurityType(RoutingMessage.SecurityType.CS)
                        .setSettlType(RoutingMessage.SettlType.T2);

                String id = TopicGenerator.getTopicMKD(subscribe.build());
                MainApp.getIdSymbolsSubscrib().put(id, subscribe.build());
                MainApp.getConnections_mkd().get(subscribe.getSecurityExchange()).sendMessage(subscribe.build());
            }

            MainApp.getAccountGroupUser().forEach((key, value) -> value.tell(connect, ActorRef.noSender()));


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onDisconnect(TransportingObjects conn) {
        try {

            SessionsMessage.Disconnect disconnect = (SessionsMessage.Disconnect) conn.getMessage();

            log.info("disconnected, {} {}", disconnect.getDestination(), disconnect.getComponent());

            NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                    .setTitle("onDisconnect")
                    .setLevel(NotificationMessage.Level.ERROR)
                    .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                    .setComponent(disconnect.getComponent())
                    .setComments(NotificationMessage.Component.VECTOR_TRADE_SERVICES.name() + " ->" + disconnect.getComponent().name())
                    .setSecurityExchange(disconnect.getDestination())
                    .setMessage("The session disconnected: " + disconnect.getDestination()).build();

            BuySideConnect.getClientSesionId().forEach((key, value) -> value.channel().writeAndFlush(notification));
            MainApp.putConnectionNotification(notification);
            MainApp.getNotificationMap().add(notification);

            MainApp.getAccountGroupUser().forEach((key, value) -> value.tell(disconnect, ActorRef.noSender()));


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onSnapshot(MarketDataMessage.Snapshot snapshot) {
        try {

            BookSnapshot bookSnapshot = new BookSnapshot(snapshot);
            snapshot.getTradesList().forEach(bookSnapshot::updateTrades);
            bookSnapshot.setStatistic(snapshot.getStatistic());
            bookSnapshot.setBid(snapshot.getBidsList());
            bookSnapshot.setAsk(snapshot.getAsksList());

            MainApp.getSnapshotHashMap().put(bookSnapshot.getId(), bookSnapshot);
            MainApp.getMessageEventBus().publish(new Envelope(bookSnapshot.getId(), bookSnapshot));

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onTrade(MarketDataMessage.Trade trade) {
        try {

            try {

                String id = TopicGenerator.getTopicMKD(trade);
                BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(id);

                if (bookSnapshot == null) {
                    bookSnapshot = new BookSnapshot(trade, id);
                    MainApp.getSnapshotHashMap().put(id, bookSnapshot);
                }

                bookSnapshot.updateTrades(trade);

                MainApp.getMessageEventBus().publish(new Envelope(id, trade));

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }


            try {

                byte[] tradeBytes = trade.toByteArray();
                MarketDataMessage.TradeGeneral tradeGeneral = MarketDataMessage.TradeGeneral.parseFrom(tradeBytes);

                synchronized (MainApp.getTradeGeneral()) {
                    if (tradeGeneral != null) {
                        MainApp.getTradeGeneral().addLast(tradeGeneral);
                        MainApp.getMessageEventBus().publish(new Envelope("TradeGeneral", tradeGeneral));
                        if (MainApp.getTradeGeneral().size() > 200) {
                            MainApp.getTradeGeneral().removeFirst();
                        }
                    }
                }

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public synchronized void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {

        try {


            String id = TopicGenerator.getTopicMKD(incrementalBook);
            BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(id);

            if(bookSnapshot == null){
                bookSnapshot = new BookSnapshot(incrementalBook, id);
            }

            bookSnapshot.setAsk(incrementalBook.getAsksList());
            bookSnapshot.setBid(incrementalBook.getBidsList());

            MainApp.getSnapshotHashMap().put(id, bookSnapshot);
            MainApp.getMessageEventBus().publish(new Envelope(id, bookSnapshot.getIncrementalBookEmpty()));

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onStatistic(MarketDataMessage.Statistic statistic) {
        try {

            if(statistic.getSymbol().isEmpty()){
                return;
            }

            String id = TopicGenerator.getTopicMKD(statistic);
            BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(id);

            if (bookSnapshot == null) {
                bookSnapshot = new BookSnapshot(statistic, id);
            }

            bookSnapshot.setStatistic(statistic);
            MainApp.getSnapshotHashMap().put(id, bookSnapshot);
            MainApp.getMessageEventBus().publish(new Envelope(id, bookSnapshot.getStatisticBookEmpty()));


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

}
