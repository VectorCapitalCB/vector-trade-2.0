package cl.vc.blotter.adaptor;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderPendingQuantityTrackerTest {

    @Test
    void everyExecutionShowsTheCurrentRemainingQuantity() {
        OrderPendingQuantityTracker tracker = new OrderPendingQuantityTracker();
        RoutingMessage.Order earlyFill = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 9_214d);
        RoutingMessage.Order latestFill = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 162_323d);

        tracker.accept(earlyFill);
        tracker.accept(latestFill);

        assertEquals(37_677d, tracker.pendingQuantity(earlyFill));
        assertEquals(37_677d, tracker.pendingQuantity(latestFill));
    }

    @Test
    void delayedFillCannotIncreaseThePendingQuantity() {
        OrderPendingQuantityTracker tracker = new OrderPendingQuantityTracker();
        RoutingMessage.Order latestFill = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 162_323d);

        tracker.accept(latestFill);
        tracker.accept(order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 9_214d));

        assertEquals(37_677d, tracker.pendingQuantity(latestFill));
    }

    @Test
    void terminalOrderHasNoPendingQuantity() {
        OrderPendingQuantityTracker tracker = new OrderPendingQuantityTracker();
        RoutingMessage.Order fill = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 162_323d);

        tracker.accept(fill);
        tracker.accept(order(RoutingMessage.OrderStatus.CANCELED, 162_323d));

        assertEquals(0d, tracker.pendingQuantity(fill));
    }

    @Test
    void clearRemovesStateFromPreviousConnection() {
        OrderPendingQuantityTracker tracker = new OrderPendingQuantityTracker();
        RoutingMessage.Order earlyFill = order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 9_214d);

        tracker.accept(order(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 162_323d));
        tracker.clear();

        assertEquals(190_786d, tracker.pendingQuantity(earlyFill));
    }

    private RoutingMessage.Order order(RoutingMessage.OrderStatus status, double cumulativeQuantity) {
        return RoutingMessage.Order.newBuilder()
                .setId("-17bhv-order")
                .setOrderQty(200_000d)
                .setCumQty(cumulativeQuantity)
                .setLeaves(200_000d - cumulativeQuantity)
                .setOrdStatus(status)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .build();
    }
}
