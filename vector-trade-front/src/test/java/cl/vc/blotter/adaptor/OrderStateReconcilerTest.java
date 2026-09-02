package cl.vc.blotter.adaptor;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class OrderStateReconcilerTest {

    @Test
    void terminalStateCannotReturnToPartial() {
        RoutingMessage.Order filled = order(RoutingMessage.OrderStatus.FILLED, 300d);
        RoutingMessage.Order delayedPartial = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 100d);

        assertSame(filled, OrderStateReconciler.latest(filled, delayedPartial));
    }

    @Test
    void cumulativeQuantityCannotGoBackwards() {
        RoutingMessage.Order current = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 200d);
        RoutingMessage.Order delayed = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 100d);

        assertSame(current, OrderStateReconciler.latest(current, delayed));
    }

    @Test
    void replacedAfterPartialKeepsIncomingPriceAndPreservesExecutionProgress() {
        RoutingMessage.Order current = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 6d).toBuilder()
                .setOrderQty(2_000d)
                .setLeaves(1_994d)
                .setPrice(6_338.2d)
                .setAvgPrice(6_338.2d)
                .setLastPx(6_338.2d)
                .setLastQty(6d)
                .setClOrdId("old-clordid")
                .build();
        RoutingMessage.Order replaced = order(RoutingMessage.OrderStatus.REPLACED, 0d).toBuilder()
                .setOrderQty(1_994d)
                .setLeaves(0d)
                .setPrice(6_338.1d)
                .setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED)
                .setClOrdId("new-clordid")
                .build();

        RoutingMessage.Order latest = OrderStateReconciler.latest(current, replaced);

        assertEquals(RoutingMessage.OrderStatus.REPLACED, latest.getOrdStatus());
        assertEquals(RoutingMessage.ExecutionType.EXEC_REPLACED, latest.getExecType());
        assertEquals(6_338.1d, latest.getPrice());
        assertEquals("new-clordid", latest.getClOrdId());
        assertEquals(2_000d, latest.getOrderQty());
        assertEquals(6d, latest.getCumQty());
        assertEquals(1_994d, latest.getLeaves());
        assertEquals(6_338.2d, latest.getAvgPrice());
        assertEquals(6_338.2d, latest.getLastPx());
        assertEquals(6d, latest.getLastQty());
    }

    @Test
    void replacedAfterPartialAcceptsQuantityChangesThatAreNotPreviousLeaves() {
        RoutingMessage.Order current = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 6d).toBuilder()
                .setOrderQty(2_000d)
                .setLeaves(1_994d)
                .build();
        RoutingMessage.Order replaced = order(RoutingMessage.OrderStatus.REPLACED, 0d).toBuilder()
                .setOrderQty(1_500d)
                .setLeaves(0d)
                .setPrice(6_338.1d)
                .setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED)
                .build();

        RoutingMessage.Order latest = OrderStateReconciler.latest(current, replaced);

        assertEquals(1_500d, latest.getOrderQty());
        assertEquals(6d, latest.getCumQty());
        assertEquals(1_494d, latest.getLeaves());
    }

    @Test
    void terminalEventIsAcceptedButKeepsMaximumCumulativeQuantity() {
        RoutingMessage.Order current = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 200d);
        RoutingMessage.Order canceled = order(RoutingMessage.OrderStatus.CANCELED, 100d);

        RoutingMessage.Order latest = OrderStateReconciler.latest(current, canceled);

        assertEquals(RoutingMessage.OrderStatus.CANCELED, latest.getOrdStatus());
        assertEquals(200d, latest.getCumQty());
    }

    @Test
    void filledWinsOverCanceledAtSameQuantity() {
        RoutingMessage.Order canceled = order(RoutingMessage.OrderStatus.CANCELED, 300d);
        RoutingMessage.Order filled = order(RoutingMessage.OrderStatus.FILLED, 300d);

        assertSame(filled, OrderStateReconciler.latest(canceled, filled));
    }

    private RoutingMessage.Order order(RoutingMessage.OrderStatus status, double cumQty) {
        return RoutingMessage.Order.newBuilder()
                .setId("order-1")
                .setOrderQty(300d)
                .setCumQty(cumQty)
                .setOrdStatus(status)
                .build();
    }
}
