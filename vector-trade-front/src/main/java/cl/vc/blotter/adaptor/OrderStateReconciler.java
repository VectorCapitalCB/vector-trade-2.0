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
        boolean cumulativeQuantityRegressed = incoming.getCumQty() + QUANTITY_EPSILON < current.getCumQty();

        if (currentTerminal && !incomingTerminal) {
            return current;
        }

        if (currentTerminal && incomingTerminal
                && incoming.getCumQty() <= current.getCumQty() + QUANTITY_EPSILON
                && terminalRank(incoming.getOrdStatus()) <= terminalRank(current.getOrdStatus())) {
            return current;
        }

        if (cumulativeQuantityRegressed) {
            if (incomingTerminal || isLifecycleUpdate(incoming)) {
                return preserveExecutionProgress(current, incoming);
            }
            return current;
        }

        if (current.getCumQty() > QUANTITY_EPSILON && isLifecycleUpdate(incoming)) {
            return preserveExecutionProgress(current, incoming);
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

    private static boolean isLifecycleUpdate(RoutingMessage.Order order) {
        return order.getOrdStatus() == RoutingMessage.OrderStatus.REPLACED
                || order.getOrdStatus() == RoutingMessage.OrderStatus.PENDING_REPLACE
                || order.getOrdStatus() == RoutingMessage.OrderStatus.PENDING_CANCEL
                || order.getExecType() == RoutingMessage.ExecutionType.EXEC_REPLACED
                || order.getExecType() == RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE
                || order.getExecType() == RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL
                || order.getExecType() == RoutingMessage.ExecutionType.EXEC_CANCELED;
    }

    private static RoutingMessage.Order preserveExecutionProgress(RoutingMessage.Order current, RoutingMessage.Order incoming) {
        double cumulativeQuantity = Math.max(current.getCumQty(), incoming.getCumQty());
        double orderQuantity = resolvedOrderQuantity(current, incoming);

        RoutingMessage.Order.Builder builder = incoming.toBuilder()
                .setCumQty(cumulativeQuantity);

        if (orderQuantity > QUANTITY_EPSILON) {
            builder.setOrderQty(orderQuantity);
            if (isTerminal(incoming.getOrdStatus())) {
                builder.setLeaves(0d);
            } else {
                builder.setLeaves(Math.max(0d, orderQuantity - cumulativeQuantity));
            }
        } else if (incoming.getLeaves() + QUANTITY_EPSILON < current.getLeaves()) {
            builder.setLeaves(current.getLeaves());
        }

        if (incoming.getAvgPrice() <= QUANTITY_EPSILON && current.getAvgPrice() > QUANTITY_EPSILON) {
            builder.setAvgPrice(current.getAvgPrice());
        }
        if (incoming.getLastPx() <= QUANTITY_EPSILON && current.getLastPx() > QUANTITY_EPSILON) {
            builder.setLastPx(current.getLastPx());
        }
        if (incoming.getLastQty() <= QUANTITY_EPSILON && current.getLastQty() > QUANTITY_EPSILON) {
            builder.setLastQty(current.getLastQty());
        }

        return builder.build();
    }

    private static double resolvedOrderQuantity(RoutingMessage.Order current, RoutingMessage.Order incoming) {
        double incomingOrderQuantity = incoming.getOrderQty();
        double currentOrderQuantity = current.getOrderQty();

        if (incomingOrderQuantity <= QUANTITY_EPSILON) {
            return currentOrderQuantity;
        }
        if (currentOrderQuantity <= QUANTITY_EPSILON || current.getCumQty() <= QUANTITY_EPSILON) {
            return incomingOrderQuantity;
        }

        double previousRemainingQuantity = Math.max(0d, currentOrderQuantity - current.getCumQty());
        if (Math.abs(incomingOrderQuantity - previousRemainingQuantity) <= QUANTITY_EPSILON) {
            return currentOrderQuantity;
        }

        return incomingOrderQuantity;
    }
}
