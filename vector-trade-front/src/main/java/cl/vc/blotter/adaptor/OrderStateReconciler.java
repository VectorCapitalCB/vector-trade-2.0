package cl.vc.blotter.adaptor;

import cl.vc.module.protocolbuff.routing.RoutingMessage;

public final class OrderStateReconciler {

    private static final double QUANTITY_EPSILON = 0.000001d;

    private OrderStateReconciler() {
    }

    public static RoutingMessage.Order latest(RoutingMessage.Order current, RoutingMessage.Order incoming) {
        if (current == null) {
            return incoming;
        }

        boolean currentTerminal = isTerminal(current.getOrdStatus());
        boolean incomingTerminal = isTerminal(incoming.getOrdStatus());

        if (currentTerminal && !incomingTerminal) {
            return current;
        }

        if (!incomingTerminal && incoming.getCumQty() + QUANTITY_EPSILON < current.getCumQty()) {
            return current;
        }

        if (currentTerminal && incomingTerminal
                && incoming.getCumQty() <= current.getCumQty() + QUANTITY_EPSILON
                && terminalRank(incoming.getOrdStatus()) <= terminalRank(current.getOrdStatus())) {
            return current;
        }

        if (incoming.getCumQty() + QUANTITY_EPSILON < current.getCumQty()) {
            return incoming.toBuilder().setCumQty(current.getCumQty()).build();
        }

        return incoming;
    }

    public static boolean isTerminal(RoutingMessage.OrderStatus status) {
        return status == RoutingMessage.OrderStatus.FILLED
                || status == RoutingMessage.OrderStatus.CANCELED
                || status == RoutingMessage.OrderStatus.REJECTED
                || status == RoutingMessage.OrderStatus.DONE_FOR_DAY
                || status == RoutingMessage.OrderStatus.STOPPED
                || status == RoutingMessage.OrderStatus.EXPIRED
                || status == RoutingMessage.OrderStatus.ABORTED
                || status == RoutingMessage.OrderStatus.CALCULATED;
    }

    private static int terminalRank(RoutingMessage.OrderStatus status) {
        if (status == RoutingMessage.OrderStatus.FILLED) {
            return 3;
        }
        if (status == RoutingMessage.OrderStatus.CANCELED) {
            return 2;
        }
        return isTerminal(status) ? 1 : 0;
    }
}
