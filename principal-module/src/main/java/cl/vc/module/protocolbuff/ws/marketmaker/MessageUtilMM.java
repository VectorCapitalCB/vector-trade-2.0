package cl.vc.module.protocolbuff.ws.marketmaker;

import cl.vc.algos.mm.proto.MarketMakerProtos;
import cl.vc.module.protocolbuff.generalstrategy.GeneralStrategy;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * Utilidades de serialización/deserialización para el protocolo WebSocket
 * del Market Maker.
 *
 * Wire format:
 *   [ topicByte (1 byte) | protobuf payload (N bytes) ]
 */
@Slf4j
public class MessageUtilMM {

    // ── Serialización ─────────────────────────────────────────────────────────

    /**
     * Serializa un mensaje Proto anteponiendo el byte de tópico.
     *
     * @param message mensaje Proto a enviar
     * @return ByteBuffer listo para enviar por WebSocket binario
     */
    // Proto class name → TopicIdentifierMM (for cases where names don't match)
    private static final java.util.Map<String, TopicIdentifierMM> PROTO_TO_TOPIC =
            java.util.Map.of(
                "UfConfig",   TopicIdentifierMM.UfUpdate,
                "MakerAlert", TopicIdentifierMM.MakerAlert
            );

    public static ByteBuffer serializeMessageByteBuffer(Message message) {
        try {
            String protoName = message.getDescriptorForType().getName();
            TopicIdentifierMM topic = PROTO_TO_TOPIC.containsKey(protoName)
                    ? PROTO_TO_TOPIC.get(protoName)
                    : TopicIdentifierMM.valueOf(protoName);
            byte[] msgBytes = message.toByteArray();
            byte[] data = new byte[msgBytes.length + 1];
            data[0] = topic.getId();
            System.arraycopy(msgBytes, 0, data, 1, msgBytes.length);
            return ByteBuffer.wrap(data);
        } catch (IllegalArgumentException e) {
            log.error("[MM] Mensaje no mapeado en TopicIdentifierMM: {}",
                    message.getDescriptorForType().getName());
        }
        return null;
    }

    // ── Deserialización ───────────────────────────────────────────────────────

    /**
     * Deserializa un frame WebSocket binario al mensaje Proto correspondiente.
     *
     * @param byteBuffer frame recibido por WebSocket
     * @return mensaje Proto, o null si el tópico es desconocido/error
     */
    public static Message onDeserializeMessage(ByteBuffer byteBuffer) {
        try {
            byte[] data = new byte[byteBuffer.remaining()];
            byteBuffer.get(data);
            TopicIdentifierMM topic = getTopic(data);

            Message.Builder builder = switch (topic) {

                // ── Sesión ──────────────────────────────────────────────────
                case Connect        -> SessionsMessage.Connect.newBuilder();
                case Disconnect     -> SessionsMessage.Disconnect.newBuilder();
                case Ping           -> SessionsMessage.Ping.newBuilder();
                case Pong           -> SessionsMessage.Pong.newBuilder();

                // ── Market Data ─────────────────────────────────────────────
                case Subscribe      -> MarketDataMessage.Subscribe.newBuilder();
                case Unsubscribe    -> MarketDataMessage.Unsubscribe.newBuilder();
                case Statistic      -> MarketDataMessage.Statistic.newBuilder();
                case IncrementalBook-> MarketDataMessage.IncrementalBook.newBuilder();
                case Snapshot       -> MarketDataMessage.Snapshot.newBuilder();
                case News           -> MarketDataMessage.News.newBuilder();

                // ── Routing ─────────────────────────────────────────────────
                case Order              -> RoutingMessage.Order.newBuilder();
                case OrderCancelReject  -> RoutingMessage.OrderCancelReject.newBuilder();

                // ── Notificación ─────────────────────────────────────────────
                case Notification -> NotificationMessage.Notification.newBuilder();

                // ── Market Maker (propio) ────────────────────────────────────
                case MarketMakerStrategy   -> MarketMakerProtos.MarketMakerStrategy.newBuilder();
                case MarketMakerSnapshot   -> MarketMakerProtos.MarketMakerSnapshot.newBuilder();
                case MakerStatus           -> MarketMakerProtos.MakerStatus.newBuilder();
                case MakerOperationsControl-> MarketMakerProtos.MakerOperationsControl.newBuilder();
                case MakerLogRequest       -> MarketMakerProtos.MakerLogRequest.newBuilder();
                case MakerLogResponse      -> MarketMakerProtos.MakerLogResponse.newBuilder();
                case UfUpdate              -> MarketMakerProtos.UfConfig.newBuilder();
                case MakerAlert            -> MarketMakerProtos.MakerAlert.newBuilder();
            };

            return deserializeMessage(data, builder);

        } catch (Exception e) {
            log.error("[MM] Error deserializando mensaje: {}", e.getMessage(), e);
        }
        return null;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    public static TopicIdentifierMM getTopic(byte[] data) {
        return TopicIdentifierMM.fromId(data[0]);
    }

    private static Message deserializeMessage(byte[] data, Message.Builder builder)
            throws InvalidProtocolBufferException {
        byte[] msgBytes = new byte[data.length - 1];
        System.arraycopy(data, 1, msgBytes, 0, data.length - 1);
        builder.mergeFrom(msgBytes);
        return builder.build();
    }
}
