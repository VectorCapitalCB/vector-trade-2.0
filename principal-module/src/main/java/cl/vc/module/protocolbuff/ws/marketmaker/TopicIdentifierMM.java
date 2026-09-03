package cl.vc.module.protocolbuff.ws.marketmaker;

import lombok.extern.slf4j.Slf4j;

/**
 * Identificadores de tópico para el protocolo WebSocket del Market Maker.
 * Cada byte identifica el tipo de mensaje Proto en el wire format:
 *   [ topicByte (1 byte) | protobuf payload (N bytes) ]
 */
@Slf4j
public enum TopicIdentifierMM {

    Connect              ((byte)  0),
    Disconnect           ((byte)  1),
    Ping                 ((byte)  2),
    Pong                 ((byte)  3),
    Subscribe            ((byte)  4),
    Statistic            ((byte)  5),   // ← debe coincidir con front proto-handler.js
    Notification         ((byte)  6),
    Order                ((byte)  7),
    OrderCancelReject    ((byte)  8),
    Unsubscribe          ((byte)  9),   // ← debe coincidir con front proto-handler.js
    MarketMakerStrategy  ((byte) 10),   // ← debe coincidir con front proto-handler.js
    Snapshot             ((byte) 11),
    IncrementalBook      ((byte) 12),   // ← debe coincidir con front proto-handler.js
    MarketMakerSnapshot  ((byte) 13),
    MakerStatus          ((byte) 14),
    MakerOperationsControl((byte) 15),
    MakerLogRequest      ((byte) 16),
    MakerLogResponse     ((byte) 17),
    News                 ((byte) 18),
    UfUpdate             ((byte) 19),
    MakerAlert           ((byte) 20);

    private final byte id;

    TopicIdentifierMM(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static TopicIdentifierMM fromId(byte id) {
        for (TopicIdentifierMM topic : values()) {
            if (topic.id == id) return topic;
        }
        throw new IllegalArgumentException("TopicIdentifierMM desconocido: " + id);
    }
}
