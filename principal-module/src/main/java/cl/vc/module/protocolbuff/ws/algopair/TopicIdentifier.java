package cl.vc.module.protocolbuff.ws.algopair;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum TopicIdentifier {
    FxDolar((byte) 0),
    Order((byte) 1),
    Subscribe((byte) 2),
    Unsubscribe((byte) 3),
    Ping((byte) 4),
    Statistic((byte) 5),
    Snapshot((byte) 6),
    Notification((byte) 7),
    Rejected((byte) 8),
    DollarRiskOfPairs((byte) 9),
    PairsStrategy((byte) 10),
    OperationsControl((byte) 11),
    Disconnect((byte) 12),
    Connect((byte) 13),
    PreConnect((byte) 14),
    OrderCancelReject((byte) 15),
    Pong((byte) 16),
    PairsSnapshot((byte) 17),
    LogMessageResponse((byte) 18),
    LogMessageRequest((byte) 19),
    News((byte) 20),
    IncrementalBook((byte) 21);


    private final byte id;

    TopicIdentifier(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static TopicIdentifier fromId(byte id) throws Exception {
        for (TopicIdentifier topic : values()) {
            if (topic.id == id) {
                return topic;
            }
        }
        return null;
    }

}