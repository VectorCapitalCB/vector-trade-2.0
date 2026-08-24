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
