package cl.vc.module.protocolbuff.ws.trademonitor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum TopicIdentifierTM {
    OrderCancelRequest((byte) 0),
    Connect((byte) 1),
    Disconnect((byte) 2),
    Ping((byte) 3),
    Order((byte) 4),
    OrderCancelReject((byte) 5),
    SnapshotOrders((byte) 6),
    SnapshotTrades((byte) 7),
    SnapshotPositions((byte) 8),
    User((byte) 9),
    Pong((byte) 10);


    private final byte id;

    TopicIdentifierTM(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static TopicIdentifierTM fromId(byte id) {
        for (TopicIdentifierTM topic : values()) {
            if (topic.id == id) {
                return topic;
            }
        }
        throw new IllegalArgumentException("Unknown topic id: " + id);
    }

}