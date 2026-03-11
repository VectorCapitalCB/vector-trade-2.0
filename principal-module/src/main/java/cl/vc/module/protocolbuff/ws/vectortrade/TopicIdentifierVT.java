package cl.vc.module.protocolbuff.ws.vectortrade;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum TopicIdentifierVT {
    Trade((byte) 0),
    TradeGeneral((byte) 1),
    SnapshotTradeGeneral((byte) 2),
    Order((byte) 3),
    News((byte) 4),
    Subscribe((byte) 5),
    Unsubscribe((byte) 6),
    OrderReplaceRequest((byte) 7),
    OrderCancelRequest((byte) 8),
    Ping((byte) 9),
    SnapshotNews((byte) 10),
    SecurityList((byte) 11),
    SnapshotPositions((byte) 12),
    PortfolioResponse((byte) 13),
    Statistic((byte) 14),
    Snapshot((byte) 15),
    Notification((byte) 16),
    Rejected((byte) 17),
    PreselectRequest((byte) 18),
    PreselectResponse((byte) 19),
    Disconnect((byte) 20),
    Connect((byte) 21),
    PreConnect((byte) 22),
    SnapshotPositionHistory((byte) 23),
    Patrimonio((byte) 24),
    Balance((byte) 25),
    OrderCancelReject((byte) 26),
    Pong((byte) 27),
    UserList((byte) 28),
    User((byte) 29),
    NewOrderRequest((byte) 30),
    PortfolioRequest((byte) 31),
    IncrementalBook((byte) 32),
    Multibook((byte) 33),
    Simultaneas((byte) 34),
    SnapshotSimultaneas(((byte) 35)),
    SnapshotBasket(((byte) 36)),
    Basket(((byte) 37)),
    PositionHistory((byte) 38),
    SnapshotPrestamos((byte) 39),
    BolsaStats((byte) 40);



    private final byte id;

    TopicIdentifierVT(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static TopicIdentifierVT fromId(byte id) {
        for (TopicIdentifierVT topic : values()) {
            if (topic.id == id) {
                return topic;
            }
        }
        throw new IllegalArgumentException("Unknown topic id: " + id);
    }

}