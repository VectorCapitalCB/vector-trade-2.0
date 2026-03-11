package cl.vc.module.protocolbuff.ws.vectortrade;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import java.nio.ByteBuffer;

@Slf4j
public class MessageUtilVT {

    /**
     * El mensaje que recibe en un mensaje generico proto y devuelve el mensaje que hay que enviar por websocket
     *
     * @param message the Protocol Buffers message to be sent
     */
    public static ByteBuffer serializeMessageByteBuffer(Message message) {
        try {
            TopicIdentifierVT topicIdentifier = TopicIdentifierVT.valueOf(message.getDescriptorForType().getName());
            byte[] messageBytes = message.toByteArray();
            byte[] data = new byte[messageBytes.length + 1];
            data[0] = topicIdentifier.getId();
            System.arraycopy(messageBytes, 0, data, 1, messageBytes.length);
            return ByteBuffer.wrap(data);
        } catch (IllegalArgumentException e) {
            log.error("error al procesar el mensaje no esta mapeado {}", message.getDescriptorForType().getName());
        }
        return null;
    }


    private static Message deserializeMessage(byte[] data, Message.Builder builder) throws InvalidProtocolBufferException {
        byte[] messageBytes = new byte[data.length - 1];
        System.arraycopy(data, 1, messageBytes, 0, data.length - 1);
        builder.mergeFrom(messageBytes);
        return builder.build();
    }


    /**
     * Recibe el mensaje de websocket byte y lo transforma en Proto
     *
     * @param byteBuffer mensake websocket
     */

    public static Message onDeserializeMessage(ByteBuffer byteBuffer) {

        try {

            byte[] data = new byte[byteBuffer.remaining()];
            byteBuffer.get(data);
            TopicIdentifierVT topic = getTopic(data);


            Message.Builder builder;
            switch (topic) {
                case PortfolioRequest:
                    builder = BlotterMessage.PortfolioRequest.newBuilder();
                    break;

                case NewOrderRequest:
                    builder = RoutingMessage.NewOrderRequest.newBuilder();
                    break;

                case UserList:
                    builder = BlotterMessage.UserList.newBuilder();
                    break;
                case Pong:
                    builder = SessionsMessage.Pong.newBuilder();
                    break;
                case Ping:
                    builder = SessionsMessage.Ping.newBuilder();
                    break;
                case OrderCancelReject:
                    builder = RoutingMessage.OrderCancelReject.newBuilder();
                    break;

                case Rejected:
                    builder = MarketDataMessage.Rejected.newBuilder();
                    break;

                case Statistic:
                    builder = MarketDataMessage.Statistic.newBuilder();
                    break;

                case Trade:
                    builder = MarketDataMessage.Trade.newBuilder();
                    break;

                case News:
                    builder = MarketDataMessage.News.newBuilder();
                    break;

                case TradeGeneral:
                    builder = MarketDataMessage.TradeGeneral.newBuilder();
                    break;

                case SnapshotTradeGeneral:
                    builder = MarketDataMessage.SnapshotTradeGeneral.newBuilder();
                    break;

                case Subscribe:
                    builder = MarketDataMessage.Subscribe.newBuilder();
                    break;

                case Unsubscribe:
                    builder = MarketDataMessage.Unsubscribe.newBuilder();
                    break;

                case SnapshotNews:
                    builder = MarketDataMessage.SnapshotNews.newBuilder();
                    break;

                case SecurityList:
                    builder = MarketDataMessage.SecurityList.newBuilder();
                    break;

                case Snapshot:
                    builder = MarketDataMessage.Snapshot.newBuilder();
                    break;

                case IncrementalBook:
                    builder = MarketDataMessage.IncrementalBook.newBuilder();
                    break;

                case Order:
                    builder = RoutingMessage.Order.newBuilder();
                    break;

                case OrderReplaceRequest:
                    builder = RoutingMessage.OrderReplaceRequest.newBuilder();
                    break;

                case OrderCancelRequest:
                    builder = RoutingMessage.OrderCancelRequest.newBuilder();
                    break;

                case SnapshotPositions:
                    builder = BlotterMessage.SnapshotPositions.newBuilder();
                    break;

                case PortfolioResponse:
                    builder = BlotterMessage.PortfolioResponse.newBuilder();
                    break;

                case Notification:
                    builder = NotificationMessage.Notification.newBuilder();
                    break;

                case PreselectRequest:
                    builder = BlotterMessage.PreselectRequest.newBuilder();
                    break;

                case PreselectResponse:
                    builder = BlotterMessage.PreselectResponse.newBuilder();
                    break;

                case Disconnect:
                    builder = SessionsMessage.Disconnect.newBuilder();
                    break;

                case Connect:
                    builder = SessionsMessage.Connect.newBuilder();
                    break;

                case Balance:
                    builder = BlotterMessage.Balance.newBuilder();
                    break;

                case Patrimonio:
                    builder = BlotterMessage.Patrimonio.newBuilder();
                    break;

                case SnapshotPositionHistory:
                    builder = BlotterMessage.SnapshotPositionHistory.newBuilder();
                    break;

                case User:
                    builder = BlotterMessage.User.newBuilder();
                    break;

                case Multibook:
                    builder = BlotterMessage.Multibook.newBuilder();
                    break;

                case Simultaneas:
                    builder = BlotterMessage.Simultaneas.newBuilder();
                    break;

                case SnapshotSimultaneas:
                    builder = BlotterMessage.SnapshotSimultaneas.newBuilder();
                    break;
                case PositionHistory:
                    builder = BlotterMessage.PositionHistory.newBuilder();
                    break;
                case SnapshotPrestamos:
                    builder = BlotterMessage.SnapshotPrestamos.newBuilder();
                    break;
                case BolsaStats:
                    builder = MarketDataMessage.BolsaStats.newBuilder();
                    break;
                default:
                    log.error("Unknown topic: " + topic);
                    return null;
            }

            return MessageUtilVT.deserializeMessage(data, builder);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


        return null;

    }

    public static TopicIdentifierVT getTopic(byte[] data) {
        return TopicIdentifierVT.fromId(data[0]);
    }
}