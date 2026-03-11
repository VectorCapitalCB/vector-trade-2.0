package cl.vc.module.protocolbuff.ws.algopair;

import cl.vc.algos.adr.proto.PairsStrategyProtos;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generalstrategy.GeneralStrategy;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class MessageUtil {

    /**
     * El mensaje que recibe en un mensaje generico proto y devuelve el mensaje que hay que enviar por websocket
     *
     * @param message the Protocol Buffers message to be sent
     */
    public static ByteBuffer serializeMessageByteBuffer(Message message) throws Exception {
        TopicIdentifier topicIdentifier = TopicIdentifier.valueOf(message.getDescriptorForType().getName());
        byte[] messageBytes = message.toByteArray();
        byte[] data = new byte[messageBytes.length + 1];
        data[0] = topicIdentifier.getId();
        System.arraycopy(messageBytes, 0, data, 1, messageBytes.length);
        return ByteBuffer.wrap(data);
    }


    private static Message deserializeMessage(byte[] data, Message.Builder builder) throws Exception {
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

    public static Message onDeserializeMessage(ByteBuffer byteBuffer) throws Exception {

        byte[] data = new byte[byteBuffer.remaining()];
        byteBuffer.get(data);
        TopicIdentifier topic = getTopic(data);

        Message.Builder builder;
        switch (topic) {
            case FxDolar:
                builder = PairsStrategyProtos.FxDolar.newBuilder();
                break;
            case Order:
                builder = RoutingMessage.Order.newBuilder();
                break;
            case Subscribe:
                builder = MarketDataMessage.Subscribe.newBuilder();
                break;
            case Unsubscribe:
                builder = MarketDataMessage.Unsubscribe.newBuilder();
                break;
            case Ping:
                builder = SessionsMessage.Ping.newBuilder();
                break;
            case Statistic:
                builder = MarketDataMessage.Statistic.newBuilder();
                break;
            case Snapshot:
                builder = MarketDataMessage.Snapshot.newBuilder();
                break;
            case Notification:
                builder = NotificationMessage.Notification.newBuilder();
                break;
            case Rejected:
                builder = MarketDataMessage.Rejected.newBuilder();
                break;
            case DollarRiskOfPairs:
                builder = GeneralStrategy.DollarRiskOfPairs.newBuilder();
                break;
            case PairsStrategy:
                builder = PairsStrategyProtos.PairsStrategy.newBuilder();
                break;
            case OperationsControl:
                builder = GeneralStrategy.OperationsControl.newBuilder();
                break;
            case Disconnect:
                builder = SessionsMessage.Disconnect.newBuilder();
                break;
            case Connect:
                builder = SessionsMessage.Connect.newBuilder();
                break;
            case PreConnect:
                builder = SessionsMessage.PreConnect.newBuilder();
                break;
            case OrderCancelReject:
                builder = RoutingMessage.OrderCancelReject.newBuilder();
                break;
            case Pong:
                builder = SessionsMessage.Pong.newBuilder();
                break;
            case PairsSnapshot:
                builder = PairsStrategyProtos.PairsSnapshot.newBuilder();
                break;
            case LogMessageResponse:
                builder = PairsStrategyProtos.LogMessageResponse.newBuilder();
                break;
            case LogMessageRequest:
                builder = PairsStrategyProtos.LogMessageRequest.newBuilder();
                break;
            case News:
                builder = MarketDataMessage.News.newBuilder();
                break;
            case IncrementalBook:
                builder = MarketDataMessage.IncrementalBook.newBuilder();
                break;


            default:
                log.error("Unknown topic: " + topic);
                return null;
        }

        return MessageUtil.deserializeMessage(data, builder);

    }

    public static TopicIdentifier getTopic(byte[] data) throws Exception {
        return TopicIdentifier.fromId(data[0]);
    }
}