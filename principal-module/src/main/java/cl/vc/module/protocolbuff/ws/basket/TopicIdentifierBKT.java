package cl.vc.module.protocolbuff.ws.basket;

import lombok.extern.slf4j.Slf4j;



@Slf4j
public enum TopicIdentifierBKT {

    DeleteBasket((byte) 0),
    SnapshotBasket((byte) 1),
    BktSymbolList((byte) 2),
    EtfConfig((byte) 3),
    Subscribe((byte) 4),
    Unsubscribe((byte) 5),
    Disconnect((byte) 6),
    Connect((byte) 7),
    Pong((byte) 8),
    Ping((byte) 9),
    User((byte) 10),
    SnapshotBasketRequest((byte) 11),
    BktSymbol((byte) 12),
    OperationsControl((byte) 13);

    private final byte id;

    TopicIdentifierBKT(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static TopicIdentifierBKT fromId(byte id) {
        for (TopicIdentifierBKT topic : values()) {
            if (topic.id == id) {
                return topic;
            }
        }
        throw new IllegalArgumentException("Unknown topic id: " + id);
    }

}