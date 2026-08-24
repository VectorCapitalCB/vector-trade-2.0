package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrigClOrdIdRecoverySupportTest {

    @Test
    void detectsMissingOrigClOrdIdSequenceRejects() {
        assertTrue(OrigClOrdIdRecoverySupport.isMissingFromSequence(
                "REJ- Order with OrigClOrdID not found in sequence"));
        assertTrue(OrigClOrdIdRecoverySupport.isMissingFromSequence(
                "OrigClOrdID not last id in sequence"));
        assertFalse(OrigClOrdIdRecoverySupport.isMissingFromSequence(
                "User has breached order rate limit"));
        assertFalse(OrigClOrdIdRecoverySupport.isMissingFromSequence(null));
    }

    @Test
    void replacesExchangeDetailWithOperatorReason() {
        RoutingMessage.OrderCancelReject rejected = RoutingMessage.OrderCancelReject.newBuilder()
                .setId("order-1")
                .setText("REJ- Order with OrigClOrdID not found in sequence")
                .build();

        RoutingMessage.OrderCancelReject normalized =
                OrigClOrdIdRecoverySupport.withOperatorReason(rejected);

        assertEquals("order-1", normalized.getId());
        assertEquals("Posible % FILLED/CANCEL", normalized.getText());
    }

    @Test
    void persistedReasonMarksOrderAsBlocked() {
        RoutingMessage.Order marked = RoutingMessage.Order.newBuilder()
                .setId("order-1")
                .setText(OrigClOrdIdRecoverySupport.POSSIBLE_FILLED_OR_CANCEL_REASON)
                .build();

        assertTrue(OrigClOrdIdRecoverySupport.isMarked(marked));
        assertFalse(OrigClOrdIdRecoverySupport.isMarked(
                marked.toBuilder().setText("otro motivo").build()));
    }
}
