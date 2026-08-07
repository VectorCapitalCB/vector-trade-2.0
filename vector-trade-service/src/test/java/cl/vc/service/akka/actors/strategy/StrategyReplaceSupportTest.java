package cl.vc.service.akka.actors.strategy;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class StrategyReplaceSupportTest {

    @Test
    void keepsMaxFloorStrictlyBelowLiveQuantity() {
        RoutingMessage.Order order = order(100_000d, 40_000d, 60_000d);

        assertEquals(10_000d,
                StrategyReplaceSupport.maxFloorForReplace(10_000d, order),
                1e-9);
    }

    @Test
    void clampsMaxFloorToFinalRemainder() {
        RoutingMessage.Order order = order(100_000d, 90_000d, 10_000d);

        assertEquals(10_000d,
                StrategyReplaceSupport.maxFloorForReplace(10_000d, order),
                1e-9);
        assertEquals(10_000d,
                StrategyReplaceSupport.maxFloorForReplace(12_000d, order),
                1e-9);
    }

    @Test
    void usesLeavesWhenReplaceAckResetsCumQty() {
        RoutingMessage.Order order = order(100_000d, 0d, 8_000d);

        assertEquals(8_000d,
                StrategyReplaceSupport.maxFloorForReplace(10_000d, order),
                1e-9);
    }

    @Test
    void normalizesOperatorReplaceWithoutChangingQuantityOrPrice() {
        RoutingMessage.Order order = order(100_000d, 95_000d, 5_000d);
        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                .setId("order-1")
                .setQuantity(100_000d)
                .setPrice(127.49d)
                .setMaxFloor(10_000d)
                .build();

        RoutingMessage.OrderReplaceRequest normalized =
                StrategyReplaceSupport.normalize(order, replace);

        assertEquals(5_000d, normalized.getMaxFloor(), 1e-9);
        assertEquals(100_000d, normalized.getQuantity(), 1e-9);
        assertEquals(127.49d, normalized.getPrice(), 1e-9);
    }

    @Test
    void preservesExistingIcebergWhenManualReplaceSendsZero() {
        RoutingMessage.Order order = order(100_000d, 95_000d, 5_000d).toBuilder()
                .setMaxFloor(10_000d)
                .build();
        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                .setId("order-1")
                .setQuantity(100_000d)
                .setPrice(127.49d)
                .setMaxFloor(0d)
                .build();

        RoutingMessage.OrderReplaceRequest normalized =
                StrategyReplaceSupport.normalize(order, replace);

        assertEquals(5_000d, normalized.getMaxFloor(), 1e-9);
        assertEquals(100_000d, normalized.getQuantity(), 1e-9);
    }

    @Test
    void newOrderStillOmitsIcebergWhenDisplayReachesTotal() {
        assertEquals(0d,
                StrategyReplaceSupport.maxFloorForNewOrder(10_000d, 10_000d),
                1e-9);
        assertEquals(2_000d,
                StrategyReplaceSupport.maxFloorForNewOrder(2_000d, 10_000d),
                1e-9);
    }

    @Test
    void leavesValidOperatorReplaceUntouched() {
        RoutingMessage.Order order = order(100_000d, 40_000d, 60_000d);
        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                .setId("order-1")
                .setQuantity(100_000d)
                .setMaxFloor(10_000d)
                .build();

        assertSame(replace, StrategyReplaceSupport.normalize(order, replace));
    }

    private static RoutingMessage.Order order(
            double quantity,
            double cumulativeQuantity,
            double leavesQuantity) {
        return RoutingMessage.Order.newBuilder()
                .setId("order-1")
                .setOrderQty(quantity)
                .setCumQty(cumulativeQuantity)
                .setLeaves(leavesQuantity)
                .build();
    }
}
