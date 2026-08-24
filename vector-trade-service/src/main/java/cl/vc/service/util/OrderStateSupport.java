package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;

public final class OrderStateSupport {

    private OrderStateSupport() {
    }

    public static boolean isInconsistentFilled(RoutingMessage.Order order) {
        return order != null
                && order.getOrdStatus() == RoutingMessage.OrderStatus.FILLED
                && order.getOrderQty() > 0d
                && order.getCumQty() < order.getOrderQty()
                && order.getLeaves() > 0d;
    }

    public static RoutingMessage.Order normalizeInconsistentFilled(RoutingMessage.Order order) {
        if (!isInconsistentFilled(order)) {
            return order;
        }
        return order.toBuilder()
                .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                .build();
    }

    public static boolean isConclusiveFilled(RoutingMessage.Order order) {
        if (order == null || order.getOrdStatus() != RoutingMessage.OrderStatus.FILLED) {
            return false;
        }
        return order.getOrderQty() <= 0d
                || order.getCumQty() >= order.getOrderQty()
                || order.getLeaves() <= 0d;
    }

    public static boolean isConclusiveStrategyTerminal(RoutingMessage.Order order) {
        if (order == null) {
            return false;
        }
        return switch (order.getOrdStatus()) {
            case FILLED -> isConclusiveFilled(order);
            case CANCELED, REJECTED -> true;
            default -> false;
        };
    }

    public static boolean isConclusiveFinalState(RoutingMessage.Order order) {
        if (order == null) {
            return false;
        }
        return switch (order.getOrdStatus()) {
            case FILLED -> isConclusiveFilled(order);
            case CANCELED, REJECTED, STOPPED, DONE_FOR_DAY -> true;
            default -> false;
        };
    }
}
