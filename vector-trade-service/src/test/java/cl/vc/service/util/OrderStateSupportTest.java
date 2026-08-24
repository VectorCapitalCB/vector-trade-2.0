package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderStateSupportTest {

    @Test
    void keepsStrategyAliveForContradictoryFilledReplaceAndAcceptsFinalFills() {
        RoutingMessage.Order contradictoryReplace = order(
                "7270", RoutingMessage.ExecutionType.EXEC_REPLACED,
                RoutingMessage.OrderStatus.FILLED, 25_861d, 4_717d, 0d);

        RoutingMessage.Order normalized =
                OrderStateSupport.normalizeInconsistentFilled(contradictoryReplace);

        assertEquals(RoutingMessage.OrderStatus.PARTIALLY_FILLED, normalized.getOrdStatus());
        assertFalse(OrderStateSupport.isConclusiveStrategyTerminal(normalized));

        RoutingMessage.Order penultimateFill = order(
                "7271", RoutingMessage.ExecutionType.EXEC_TRADE,
                RoutingMessage.OrderStatus.PARTIALLY_FILLED, 27_945d, 2_633d, 2_084d);
        assertFalse(OrderStateSupport.isConclusiveStrategyTerminal(penultimateFill));

        RoutingMessage.Order finalFill = order(
                "7272", RoutingMessage.ExecutionType.EXEC_TRADE,
                RoutingMessage.OrderStatus.FILLED, 30_578d, 0d, 2_633d);
        assertTrue(OrderStateSupport.isConclusiveStrategyTerminal(finalFill));
        assertSame(finalFill, OrderStateSupport.normalizeInconsistentFilled(finalFill));
    }

    @Test
    void preservesCanceledAndRejectedAsConclusiveStates() {
        assertTrue(OrderStateSupport.isConclusiveStrategyTerminal(
                order("cancel", RoutingMessage.ExecutionType.EXEC_CANCELED,
                        RoutingMessage.OrderStatus.CANCELED, 0d, 30_578d, 0d)));
        assertTrue(OrderStateSupport.isConclusiveStrategyTerminal(
                order("reject", RoutingMessage.ExecutionType.EXEC_REJECTED,
                        RoutingMessage.OrderStatus.REJECTED, 0d, 30_578d, 0d)));
    }

    private static RoutingMessage.Order order(
            String execId,
            RoutingMessage.ExecutionType execType,
            RoutingMessage.OrderStatus status,
            double cumulativeQuantity,
            double leavesQuantity,
            double lastQuantity) {
        return RoutingMessage.Order.newBuilder()
                .setId("818219724047114093")
                .setExecId(execId)
                .setStrategyOrder(RoutingMessage.StrategyOrder.HOLGURA)
                .setOrderQty(30_578d)
                .setCumQty(cumulativeQuantity)
                .setLeaves(leavesQuantity)
                .setLastQty(lastQuantity)
                .setExecType(execType)
                .setOrdStatus(status)
                .build();
    }
}
