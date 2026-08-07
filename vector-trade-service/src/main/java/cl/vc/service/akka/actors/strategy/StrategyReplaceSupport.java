package cl.vc.service.akka.actors.strategy;

import cl.vc.module.protocolbuff.routing.RoutingMessage;

final class StrategyReplaceSupport {

    private StrategyReplaceSupport() {
    }

    static double remainingQuantity(RoutingMessage.Order order) {
        return remainingQuantity(order, order.getOrderQty());
    }

    static double remainingQuantity(RoutingMessage.Order order, double targetQuantity) {
        if (targetQuantity <= 0d) {
            return 0d;
        }

        double cumulativeQuantity = Math.max(0d, order.getCumQty());
        double calculatedRemaining = Math.max(0d, targetQuantity - cumulativeQuantity);

        // Some replace acknowledgements reset CumQty but preserve LeavesQty.
        double reportedRemaining = order.getLeaves();
        if (reportedRemaining > 0d && targetQuantity == order.getOrderQty()) {
            return Math.min(calculatedRemaining, reportedRemaining);
        }
        return calculatedRemaining;
    }

    static double maxFloorForReplace(double configuredMaxFloor, RoutingMessage.Order order) {
        return maxFloorForReplace(configuredMaxFloor, remainingQuantity(order));
    }

    static double maxFloorForReplace(
            double configuredMaxFloor,
            RoutingMessage.Order order,
            double targetQuantity) {
        return maxFloorForReplace(
                configuredMaxFloor,
                remainingQuantity(order, targetQuantity));
    }

    static double maxFloorForReplace(double configuredMaxFloor, double remainingQuantity) {
        if (configuredMaxFloor <= 0d || remainingQuantity <= 0d) {
            return 0d;
        }

        // On a NUAM replace, zero means "unchanged" for an existing iceberg.
        // Keep a positive display quantity and expose the full final remainder.
        return Math.min(configuredMaxFloor, remainingQuantity);
    }

    static double maxFloorForNewOrder(double configuredMaxFloor, double orderQuantity) {
        if (configuredMaxFloor <= 0d || orderQuantity <= 0d
                || configuredMaxFloor >= orderQuantity) {
            return 0d;
        }
        return configuredMaxFloor;
    }

    static RoutingMessage.OrderReplaceRequest normalize(
            RoutingMessage.Order order,
            RoutingMessage.OrderReplaceRequest replace) {
        double targetQuantity = replace.getQuantity() > 0d
                ? replace.getQuantity()
                : order.getOrderQty();
        double configuredMaxFloor = replace.getMaxFloor() > 0d
                ? replace.getMaxFloor()
                : order.getMaxFloor();
        double normalizedMaxFloor =
                maxFloorForReplace(configuredMaxFloor, order, targetQuantity);

        if (normalizedMaxFloor == replace.getMaxFloor()) {
            return replace;
        }
        return replace.toBuilder()
                .setMaxFloor(normalizedMaxFloor)
                .build();
    }
}
