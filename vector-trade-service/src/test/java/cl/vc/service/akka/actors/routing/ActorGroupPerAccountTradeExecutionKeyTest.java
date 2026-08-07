package cl.vc.service.akka.actors.routing;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ActorGroupPerAccountTradeExecutionKeyTest {

    @Test
    void acceptsEveryPartialFillWhenGatewayReusesExecId() {
        double[] cumulativeQuantities = {114_673, 140_413, 247_302, 647_302, 981_302, 1_000_000};
        Set<String> keys = new HashSet<>();

        for (double cumulativeQuantity : cumulativeQuantities) {
            keys.add(ActorGroupPerAccount.tradeExecutionKey(order(
                    "p18t6hmad3cf", "-1pvfl6unog35", cumulativeQuantity)));
        }

        assertEquals(6, keys.size());
    }

    @Test
    void rejectsAnIdenticalRetransmission() {
        RoutingMessage.Order fill = order("p18t6hmad3cf", "-1pvfl6unog35", 647_302);
        RoutingMessage.Order retransmission = order("p18t6hmad3cf", "-1pvfl6unog35", 647_302);

        assertEquals(
                ActorGroupPerAccount.tradeExecutionKey(fill),
                ActorGroupPerAccount.tradeExecutionKey(retransmission));
    }

    @Test
    void doesNotCollideAcrossOrders() {
        assertNotEquals(
                ActorGroupPerAccount.tradeExecutionKey(order("order-a", "shared-exec", 1_000_000)),
                ActorGroupPerAccount.tradeExecutionKey(order("order-b", "shared-exec", 1_000_000)));
    }

    private static RoutingMessage.Order order(String orderId, String execId, double cumulativeQuantity) {
        return RoutingMessage.Order.newBuilder()
                .setId(orderId)
                .setExecId(execId)
                .setCumQty(cumulativeQuantity)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .build();
    }
}
