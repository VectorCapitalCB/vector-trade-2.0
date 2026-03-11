package cl.vc.module.protocolbuff.tcp;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.algos.adr.proto.PairsStrategyProtos;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generalstrategy.GeneralStrategy;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;


@Slf4j
public class ParseMessageActor extends AbstractActor {


    private ActorRef client;

    private ParseMessageActor(ActorRef client) {
        this.client = client;
    }

    public static Props props(ActorRef client) {
        return Props.create(ParseMessageActor.class, client);
    }

    @Override
    public Receive createReceive() {

        return receiveBuilder()
                .match(String.class, this::onTransportingObjects)
                .build();

    }

    private void onTransportingObjects(String message) {

        try {

            JSONObject jsonMessage = new JSONObject(message.replace("\n", ""));

            String topic = jsonMessage.getString("topic").toString();
            String messageCast = jsonMessage.get("payload").toString();


            switch (topic) {

                case "trade":
                case "tradegeneral":
                    MarketDataMessage.Trade.Builder trde = MarketDataMessage.Trade.newBuilder();
                    JsonFormat.parser().merge(messageCast, trde);
                    MarketDataMessage.Trade trade = trde.build();
                    client.tell(trade, ActorRef.noSender());
                    break;


                case "order":
                    RoutingMessage.Order.Builder order = RoutingMessage.Order.newBuilder();
                    JsonFormat.parser().merge(messageCast, order);
                    client.tell(order.build(), ActorRef.noSender());
                    break;

                case "news":
                    MarketDataMessage.News.Builder orderBuilder = MarketDataMessage.News.newBuilder();
                    JsonFormat.parser().merge(messageCast, orderBuilder);
                    MarketDataMessage.News sub = orderBuilder.build();
                    client.tell(sub, ActorRef.noSender());
                    break;

                case "subscribe":
                    MarketDataMessage.Subscribe.Builder subscribe = MarketDataMessage.Subscribe.newBuilder();
                    JsonFormat.parser().merge(messageCast, subscribe);
                    client.tell(subscribe.build(), ActorRef.noSender());
                    break;

                case "unsubscribe":
                    MarketDataMessage.Unsubscribe.Builder orderBuilders = MarketDataMessage.Unsubscribe.newBuilder();
                    JsonFormat.parser().merge(messageCast, orderBuilders);
                    MarketDataMessage.Unsubscribe unSub = orderBuilders.build();
                    client.tell(unSub, ActorRef.noSender());
                    break;

                case "neworderrequest":
                    RoutingMessage.Order.Builder newOrderRequest = RoutingMessage.Order.newBuilder();
                    JsonFormat.parser().merge(messageCast, newOrderRequest);
                    client.tell(newOrderRequest.build(), ActorRef.noSender());

                    break;

                case "orderreplacerequest":
                    RoutingMessage.OrderReplaceRequest.Builder orderReplaceBuilders = RoutingMessage.OrderReplaceRequest.newBuilder();
                    JsonFormat.parser().merge(messageCast, orderReplaceBuilders);
                    RoutingMessage.OrderReplaceRequest replace = orderReplaceBuilders.build();
                    client.tell(replace, ActorRef.noSender());
                    break;

                case "ordercancelrequest":
                    RoutingMessage.OrderCancelRequest.Builder orderBuildersCancel = RoutingMessage.OrderCancelRequest.newBuilder();
                    JsonFormat.parser().merge(messageCast, orderBuildersCancel);
                    RoutingMessage.OrderCancelRequest cancel = orderBuildersCancel.build();
                    client.tell(cancel, ActorRef.noSender());
                    break;

                case "ping":
                    SessionsMessage.Ping.Builder ping = SessionsMessage.Ping.newBuilder();
                    JsonFormat.parser().merge(messageCast, ping);
                    SessionsMessage.Ping pings = ping.build();
                    client.tell(pings, ActorRef.noSender());
                    break;

                case "snapshotnews":
                    MarketDataMessage.SnapshotNews.Builder sn = MarketDataMessage.SnapshotNews.newBuilder();
                    JsonFormat.parser().merge(messageCast, sn);
                    client.tell(sn.build(), ActorRef.noSender());
                    break;

                case "securitylist":
                    MarketDataMessage.SecurityList.Builder sl = MarketDataMessage.SecurityList.newBuilder();
                    JsonFormat.parser().merge(messageCast, sl);
                    client.tell(sl.build(), ActorRef.noSender());
                    break;

                case "snapshotpositions":
                    BlotterMessage.SnapshotPositions.Builder blotter = BlotterMessage.SnapshotPositions.newBuilder();
                    JsonFormat.parser().merge(messageCast, blotter);
                    client.tell(blotter.build(), ActorRef.noSender());
                    break;

                case "portfolioresponse":
                    BlotterMessage.PortfolioResponse.Builder portfolioResponse = BlotterMessage.PortfolioResponse.newBuilder();
                    JsonFormat.parser().merge(messageCast, portfolioResponse);
                    client.tell(portfolioResponse.build(), ActorRef.noSender());
                    break;

                case "statistic":
                    MarketDataMessage.Statistic.Builder statistic = MarketDataMessage.Statistic.newBuilder();
                    JsonFormat.parser().merge(messageCast, statistic);
                    client.tell(statistic.build(), ActorRef.noSender());
                    break;
                case "bolsastats":
                case "bolsaStats":
                    MarketDataMessage.BolsaStats.Builder bolsaStats = MarketDataMessage.BolsaStats.newBuilder();
                    JsonFormat.parser().merge(messageCast, bolsaStats);
                    client.tell(bolsaStats.build(), ActorRef.noSender());
                    break;

                case "snapshot":
                    MarketDataMessage.Snapshot.Builder snapshot = MarketDataMessage.Snapshot.newBuilder();
                    JsonFormat.parser().merge(messageCast, snapshot);
                    client.tell(snapshot.build(), ActorRef.noSender());
                    break;

                case "notification":
                    NotificationMessage.Notification.Builder nf = NotificationMessage.Notification.newBuilder();
                    JsonFormat.parser().merge(messageCast, nf);
                    client.tell(nf.build(), ActorRef.noSender());
                    break;

                case "incrementalbook":
                    MarketDataMessage.IncrementalBook.Builder incrementalBook = MarketDataMessage.IncrementalBook.newBuilder();
                    JsonFormat.parser().merge(messageCast, incrementalBook);
                    client.tell(incrementalBook.build(), ActorRef.noSender());
                    break;

                case "rejected":
                    MarketDataMessage.Rejected.Builder reject = MarketDataMessage.Rejected.newBuilder();
                    JsonFormat.parser().merge(messageCast, reject);
                    client.tell(reject.build(), ActorRef.noSender());
                    break;

                case "preselectrequest":
                    BlotterMessage.PreselectRequest.Builder preselect = BlotterMessage.PreselectRequest.newBuilder();
                    JsonFormat.parser().merge(messageCast, preselect);
                    client.tell(preselect.build(), ActorRef.noSender());
                    break;

                case "preselectresponse":
                    BlotterMessage.PreselectResponse.Builder preselectres = BlotterMessage.PreselectResponse.newBuilder();
                    JsonFormat.parser().merge(messageCast, preselectres);
                    client.tell(preselectres.build(), ActorRef.noSender());
                    break;

                case "dollarriskofpairs":
                    GeneralStrategy.DollarRiskOfPairs.Builder dr = GeneralStrategy.DollarRiskOfPairs.newBuilder();
                    JsonFormat.parser().merge(messageCast, dr);
                    client.tell(dr.build(), ActorRef.noSender());
                    break;

                case "pairsstrategy":
                    PairsStrategyProtos.PairsStrategy.Builder ps = PairsStrategyProtos.PairsStrategy.newBuilder();
                    JsonFormat.parser().merge(messageCast, ps);
                    client.tell(ps.build(), ActorRef.noSender());
                    break;

                case "operationscontrol":
                    GeneralStrategy.OperationsControl.Builder oc = GeneralStrategy.OperationsControl.newBuilder();
                    JsonFormat.parser().merge(messageCast, oc);
                    client.tell(oc.build(), ActorRef.noSender());
                    break;

                case "disconnect":
                    SessionsMessage.Disconnect.Builder dis = SessionsMessage.Disconnect.newBuilder();
                    JsonFormat.parser().merge(messageCast, dis);
                    client.tell(dis.build(), ActorRef.noSender());
                    break;

                case "connect":
                    SessionsMessage.Connect.Builder con = SessionsMessage.Connect.newBuilder();
                    JsonFormat.parser().merge(messageCast, con);
                    client.tell(con.build(), ActorRef.noSender());
                    break;

                case "preconnect":
                    SessionsMessage.PreConnect.Builder precon = SessionsMessage.PreConnect.newBuilder();
                    JsonFormat.parser().merge(messageCast, precon);
                    client.tell(precon.build(), ActorRef.noSender());
                    break;

                case "patrimonio":
                    BlotterMessage.Patrimonio.Builder patrimonio = BlotterMessage.Patrimonio.newBuilder();
                    JsonFormat.parser().merge(messageCast, patrimonio);
                    client.tell(patrimonio.build(), ActorRef.noSender());
                    break;


                default:
                    log.error("Tópico desconocido: " + topic);
                    break;
            }



        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }



}
