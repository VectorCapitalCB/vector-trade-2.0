package cl.vc.service.akka.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import cl.vc.service.MainApp;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;


@Slf4j
public class BuySideConnect extends AbstractActor {

    @Getter
    private static final BiMap<String, ActorRef> actorPerSessionMaps = HashBiMap.create();

    @Getter
    private static final BiMap<String, ChannelHandlerContext> clientSesionId = HashBiMap.create();

    public static Props props() {
        return Props.create(BuySideConnect.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransportingObjects.class, this::onMessages)
                .match(SessionsMessage.Connect.class, this::onConnect)
                .build();
    }

    private void onMessages(TransportingObjects conn) {

        try {

            if (conn.getMessage() instanceof SessionsMessage.Connect) {
                onConnect(conn);

            } else if (conn.getMessage() instanceof SessionsMessage.Disconnect) {
                onDisconnect(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.NewOrderRequest) {
                onNewOrderRequest(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.OrderReplaceRequest) {
                onOrderReplaceRequest(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.OrderCancelRequest) {
                onCancelOrderRequest(conn);

            } else if (conn.getMessage() instanceof MarketDataMessage.Subscribe) {
                onSubscribe(conn);

            } else if (conn.getMessage() instanceof MarketDataMessage.Unsubscribe) {
                onUnsubscribe(conn);

            } else if (conn.getMessage() instanceof NotificationMessage.NotificationRequest) {
                onNotificationRequest(conn);

                //BLOTTER
            } else if (conn.getMessage() instanceof BlotterMessage.PortfolioRequest) {
                onPortfolioRequest(conn);

            } else if (conn.getMessage() instanceof BlotterMessage.PreselectRequest) {
                onPreselect(conn);

            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void onSnapshotBasket(TransportingObjects conn) {
        try {
            
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onPreselect(TransportingObjects conn) {
        try {

            BlotterMessage.PreselectRequest request = (BlotterMessage.PreselectRequest) conn.getMessage();

            BlotterMessage.PreselectResponse.Builder response = BlotterMessage.PreselectResponse.newBuilder();
            response.setUsername(request.getUsername());
            response.setStatusPreselect(BlotterMessage.StatusPreselect.SNAPSHOT_PRESELECT);

            List<RoutingMessage.Order> orderList;

            if (request.getStatusPreselect().equals(BlotterMessage.StatusPreselect.SNAPSHOT_PRESELECT)) {
                orderList = MainApp.getPreSelectordersMap().get(request.getUsername());
                if (orderList == null) {
                    orderList = new ArrayList<>();
                    MainApp.getPreSelectordersMap().put(request.getUsername(), orderList);
                }
                response.addAllOrders(orderList);

            } else if (request.getStatusPreselect().equals(BlotterMessage.StatusPreselect.ADD_PRESELECT)) {
                orderList = MainApp.getPreSelectordersMap().get(request.getUsername());
                orderList.add(request.getOrders());
                MainApp.getPreSelectordersMap().put(request.getUsername(), orderList);
                response.addAllOrders(orderList);

            } else if (request.getStatusPreselect().equals(BlotterMessage.StatusPreselect.REMOVE_PRESELECT)) {

                orderList = MainApp.getPreSelectordersMap().get(request.getUsername());

                Predicate<RoutingMessage.Order> predicate = objeto -> objeto.getId().equals(request.getOrders().getId());
                orderList.removeIf(predicate);

                MainApp.getPreSelectordersMap().put(request.getUsername(), orderList);
                response.addAllOrders(orderList);

            }

            conn.getCtx().channel().writeAndFlush(response.build());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onNotificationRequest(TransportingObjects conn) {
        try {

            NotificationMessage.NotificationRequest request = (NotificationMessage.NotificationRequest) conn.getMessage();

            if (request.getNotificationRequestType().equals(NotificationMessage.NotificationRequestType.CONNECTION_REQUEST)) {

                NotificationMessage.NotificationResponse response = NotificationMessage.NotificationResponse
                        .newBuilder()
                        .setId(request.getId())
                        .addAllNotificationlist(MainApp.getNotificationConectionMap().values()).build();

                conn.getCtx().channel().writeAndFlush(response);

            } else if (request.getNotificationRequestType().equals(NotificationMessage.NotificationRequestType.MESSAGES_REQUEST)) {

                NotificationMessage.NotificationResponse response = NotificationMessage.NotificationResponse
                        .newBuilder()
                        .setId(request.getId())
                        .addAllNotificationlist(MainApp.getNotificationMap()).build();

                conn.getCtx().channel().writeAndFlush(response);
            }


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onUnsubscribe(TransportingObjects conn) {
        try {
            actorPerSessionMaps.get(conn.getCtx().channel().id().toString()).tell(conn.getMessage(), ActorRef.noSender());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onSubscribe(TransportingObjects conn) {
        try {
            actorPerSessionMaps.get(conn.getCtx().channel().id().toString()).tell(conn.getMessage(), ActorRef.noSender());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onPortfolioRequest(TransportingObjects conn) {
        try {
            actorPerSessionMaps.get(conn.getCtx().channel().id().toString()).tell(conn.getMessage(), ActorRef.noSender());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onConnect(TransportingObjects conn) {
        try {

            if (!actorPerSessionMaps.containsKey(conn.getCtx().channel().id().toString())) {

                //ActorRef client = getContext().actorOf(new RoundRobinPool(1).props(ActorPerSession.props(conn.getCtx())));
                //actorPerSessionMaps.put(conn.getCtx().channel().id().toString(), client);
                //clientSesionId.put(conn.getCtx().channel().id().toString(), conn.getCtx());

            } else {

                actorPerSessionMaps.get(conn.getCtx().channel().id().toString()).tell(conn.getMessage(), ActorRef.noSender());
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onConnect(SessionsMessage.Connect conn) {
        try {

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onDisconnect(TransportingObjects conn) {
        try {

            if (actorPerSessionMaps.containsKey(conn.getCtx().channel().id().toString())) {
                ActorRef actorToDelete = actorPerSessionMaps.get(conn.getCtx().channel().id().toString());
                actorToDelete.tell(PoisonPill.getInstance(), ActorRef.noSender());
                actorPerSessionMaps.remove(conn.getCtx().channel().id().toString());
                clientSesionId.remove(conn.getCtx().channel().id().toString());
                log.error("Actor eliminado id {}", conn.getCtx().channel().id());
            } else {
                log.error("Actor no fue encontrado para eliminar id {}", conn.getCtx().channel().id());
            }


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onNewOrderRequest(TransportingObjects conn) {
        try {

            RoutingMessage.NewOrderRequest routingMessage = (RoutingMessage.NewOrderRequest) conn.getMessage();
            actorPerSessionMaps.get(conn.getCtx().channel().id().toString()).tell(routingMessage, ActorRef.noSender());


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onOrderReplaceRequest(TransportingObjects conn) {
        try {

            RoutingMessage.OrderReplaceRequest routingMessage = (RoutingMessage.OrderReplaceRequest) conn.getMessage();
            actorPerSessionMaps.get(conn.getCtx().channel().id().toString()).tell(routingMessage, ActorRef.noSender());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onCancelOrderRequest(TransportingObjects conn) {
        try {

            RoutingMessage.OrderCancelRequest cancelRequest = (RoutingMessage.OrderCancelRequest) conn.getMessage();
            actorPerSessionMaps.get(conn.getCtx().channel().id().toString()).tell(cancelRequest, ActorRef.noSender());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

}
