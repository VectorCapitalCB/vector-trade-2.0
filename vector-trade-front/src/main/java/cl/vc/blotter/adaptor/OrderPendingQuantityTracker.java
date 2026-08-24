package cl.vc.blotter.adaptor;

import cl.vc.module.protocolbuff.routing.RoutingMessage;

import java.util.HashMap;
import java.util.Map;

public final class OrderPendingQuantityTracker {

    private final Map<String, RoutingMessage.Order> latestOrdersById = new HashMap<>();

    public void accept(RoutingMessage.Order incoming) {
        if (incoming == null || incoming.getId().isBlank()) {
            return;
        }

        RoutingMessage.Order current = latestOrdersById.get(incoming.getId());
        latestOrdersById.put(incoming.getId(), OrderStateReconciler.latest(current, incoming));
    }

    public double pendingQuantity(RoutingMessage.Order execution) {
        if (execution == null) {
            return 0d;
        }

        RoutingMessage.Order latest = latestOrdersById.getOrDefault(execution.getId(), execution);
        if (OrderStateReconciler.isTerminal(latest.getOrdStatus())) {
            return 0d;
        }

        double orderQuantity = latest.getOrderQty() > 0d
                ? latest.getOrderQty()
                : execution.getOrderQty();
        if (orderQuantity <= 0d) {
            return Math.max(0d, latest.getLeaves());
        }

        return Math.max(0d, orderQuantity - Math.max(0d, latest.getCumQty()));
    }

    public void clear() {
        latestOrdersById.clear();
    }
}
