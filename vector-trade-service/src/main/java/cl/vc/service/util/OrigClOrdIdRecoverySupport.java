package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;

import java.util.Locale;

/** Identifica ordenes que la bolsa ya no puede resolver por su cadena de OrigClOrdID. */
public final class OrigClOrdIdRecoverySupport {

    public static final int MAX_REJECTS = 3;
    public static final String POSSIBLE_FILLED_OR_CANCEL_REASON = "Posible % FILLED/CANCEL";

    private OrigClOrdIdRecoverySupport() {
    }

    public static boolean isMissingFromSequence(String text) {
        if (text == null || text.isBlank()) {
            return false;
        }

        String normalized = text.toLowerCase(Locale.ROOT);
        return normalized.contains("origclordid")
                && (normalized.contains("not found")
                || normalized.contains("not last")
                || normalized.contains("sequence"));
    }

    public static boolean isMarked(RoutingMessage.Order order) {
        return order != null && POSSIBLE_FILLED_OR_CANCEL_REASON.equals(order.getText());
    }

    public static RoutingMessage.OrderCancelReject withOperatorReason(
            RoutingMessage.OrderCancelReject rejected) {
        return rejected.toBuilder()
                .setText(POSSIBLE_FILLED_OR_CANCEL_REASON)
                .build();
    }
}
