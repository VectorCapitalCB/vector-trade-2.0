package cl.vc.module.protocolbuff.ws.trademonitor;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import java.nio.ByteBuffer;

@Slf4j
public class MessageUtilTM {

    /**
     * El mensaje que recibe en un mensaje generico proto y devuelve el mensaje que hay que enviar por websocket
     *
     * @param message the Protocol Buffers message to be sent
     */
    public static ByteBuffer serializeMessageByteBuffer(Message message) {
        try {
            TopicIdentifierTM topicIdentifier = TopicIdentifierTM.valueOf(message.getDescriptorForType().getName());
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
            TopicIdentifierTM topic = getTopic(data);

            Message.Builder builder;
            switch (topic) {
                case Order:
                    builder = RoutingMessage.Order.newBuilder();
                    break;
                case OrderCancelRequest:
                    builder = RoutingMessage.OrderCancelRequest.newBuilder();
                    break;
                case Connect:
                    builder = SessionsMessage.Connect.newBuilder();
                    break;
                case Disconnect:
                    builder = SessionsMessage.Disconnect.newBuilder();
                    break;
                case Ping:
                    builder = SessionsMessage.Ping.newBuilder();
                case Pong:
                    builder = SessionsMessage.Pong.newBuilder();
                    break;
                case OrderCancelReject:
                    builder = RoutingMessage.OrderCancelReject.newBuilder();
                    break;
                case SnapshotOrders:
                    builder = BlotterMessage.SnapshotOrders.newBuilder();
                    break;
                case SnapshotTrades:
                    builder = BlotterMessage.SnapshotTrades.newBuilder();
                    break;
                case SnapshotPositions:
                    builder = BlotterMessage.SnapshotPositions.newBuilder();
                    break;
                case User:
                    builder = BlotterMessage.User.newBuilder();
                    break;
                default:
                    log.error("Unknown topic: " + topic);
                    return null;
            }

            return MessageUtilTM.deserializeMessage(data, builder);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


        return null;

    }

    public static TopicIdentifierTM getTopic(byte[] data) {
        return TopicIdentifierTM.fromId(data[0]);
    }
}