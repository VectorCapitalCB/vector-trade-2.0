package cl.vc.service.akka.actors.routing;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ActorGroupPerAccountReplaceTest {

    @Test
    void clampsIcebergWhenNoneStrategyMaxFloorReachesRemainingQuantity() {
        RoutingMessage.Order order = order(
                RoutingMessage.StrategyOrder.NONE_STRATEGY,
                123_123d,
                117_904d);
        RoutingMessage.OrderReplaceRequest replace = replace(123_123d, 12_313d);

        RoutingMessage.OrderReplaceRequest normalized =
                ActorGroupPerAccount.normalizeNoneStrategyReplace(order, replace);

        assertEquals(5_219d, normalized.getMaxFloor(), 1e-9);
        assertEquals(123_123d, normalized.getQuantity(), 1e-9);
        assertEquals(84.54d, normalized.getPrice(), 1e-9);
        assertEquals("10", normalized.getIcebergPercentage());
    }

    @Test
    void keepsValidNoneStrategyMaxFloor() {
        RoutingMessage.Order order = order(
                RoutingMessage.StrategyOrder.NONE_STRATEGY,
                123_123d,
                100_000d);
        RoutingMessage.OrderReplaceRequest replace = replace(123_123d, 12_313d);

        assertSame(replace, ActorGroupPerAccount.normalizeNoneStrategyReplace(order, replace));
    }

    @Test
    void preservesExistingIcebergWhenNoneStrategyReplaceSendsZero() {
        RoutingMessage.Order order = order(
                RoutingMessage.StrategyOrder.NONE_STRATEGY,
                123_123d,
                117_904d);
        RoutingMessage.OrderReplaceRequest replace = replace(123_123d, 0d);

        RoutingMessage.OrderReplaceRequest normalized =
                ActorGroupPerAccount.normalizeNoneStrategyReplace(order, replace);

        assertEquals(5_219d, normalized.getMaxFloor(), 1e-9);
        assertEquals(123_123d, normalized.getQuantity(), 1e-9);
    }

    @Test
    void leavesNonIcebergNoneStrategyReplaceUntouched() {
        RoutingMessage.Order order = order(
                RoutingMessage.StrategyOrder.NONE_STRATEGY,
                123_123d,
                100_000d).toBuilder()
                .setMaxFloor(0d)
                .build();
        RoutingMessage.OrderReplaceRequest replace = replace(123_123d, 0d);

        assertSame(replace, ActorGroupPerAccount.normalizeNoneStrategyReplace(order, replace));
    }

    @Test
    void doesNotChangeStrategyManagedReplaces() {
        RoutingMessage.Order order = order(
                RoutingMessage.StrategyOrder.BEST,
                123_123d,
                117_904d);
        RoutingMessage.OrderReplaceRequest replace = replace(123_123d, 12_313d);

        assertSame(replace, ActorGroupPerAccount.normalizeNoneStrategyReplace(order, replace));
    }

    private static RoutingMessage.Order order(
            RoutingMessage.StrategyOrder strategy,
            double orderQuantity,
            double cumulativeQuantity) {
        return RoutingMessage.Order.newBuilder()
                .setId("39flmubpo2wu")
                .setOrderQty(orderQuantity)
                .setCumQty(cumulativeQuantity)
                .setMaxFloor(12_313d)
                .setStrategyOrder(strategy)
                .build();
    }

    private static RoutingMessage.OrderReplaceRequest replace(double quantity, double maxFloor) {
        return RoutingMessage.OrderReplaceRequest.newBuilder()
                .setId("39flmubpo2wu")
                .setQuantity(quantity)
                .setPrice(84.54d)
                .setMaxFloor(maxFloor)
                .setIcebergPercentage("10")
                .build();
    }
}
