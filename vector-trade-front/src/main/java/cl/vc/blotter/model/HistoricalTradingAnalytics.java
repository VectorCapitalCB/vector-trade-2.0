package cl.vc.blotter.model;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class HistoricalTradingAnalytics {

    private HistoricalTradingAnalytics() {
    }

    public record Snapshot(int orders,
                           int fills,
                           double buyAmount,
                           double sellAmount,
                           double buyAveragePrice,
                           double sellAveragePrice,
                           double realizedPnl,
                           Map<String, Double> pnlByOrder,
                           Map<String, Double> amountBySymbol) {

        public double realizedPnl(BlotterMessage.HistoricalOrderGroup group) {
            return pnlByOrder.getOrDefault(orderKey(group.getSummary()), 0d);
        }
    }

    public static Snapshot calculate(Collection<BlotterMessage.HistoricalOrderGroup> groups) {
        if (groups == null || groups.isEmpty()) {
            return new Snapshot(0, 0, 0d, 0d, 0d, 0d, 0d, Map.of(), Map.of());
        }

        List<Fill> fills = new ArrayList<>();
        int fillCount = 0;
        for (BlotterMessage.HistoricalOrderGroup group : groups) {
            RoutingMessage.Order summary = group.getSummary();
            fillCount += group.getExecutionsCount();
            if (group.getExecutionsCount() == 0) {
                if (summary.getCumQty() > 0d && summary.getAvgPrice() > 0d) {
                    fills.add(Fill.from(summary, summary.getCumQty(), summary.getAvgPrice()));
                }
                continue;
            }
            for (RoutingMessage.Order execution : group.getExecutionsList()) {
                if (execution.getLastQty() > 0d && execution.getLastPx() > 0d) {
                    fills.add(Fill.from(execution, execution.getLastQty(), execution.getLastPx()));
                }
            }
        }
        fills.sort(Comparator.comparingLong(Fill::seconds).thenComparingInt(Fill::nanos));

        Map<String, PositionCost> positions = new LinkedHashMap<>();
        Map<String, Double> pnlByOrder = new LinkedHashMap<>();
        Map<String, Double> amountBySymbol = new LinkedHashMap<>();
        double buyAmount = 0d;
        double sellAmount = 0d;
        double buyQuantity = 0d;
        double sellQuantity = 0d;
        double realizedPnl = 0d;

        for (Fill fill : fills) {
            double amount = fill.quantity() * fill.price();
            amountBySymbol.merge(fill.symbol(), amount, Double::sum);
            if (fill.side() == RoutingMessage.Side.BUY) {
                buyAmount += amount;
                buyQuantity += fill.quantity();
            }
            if (fill.side() == RoutingMessage.Side.SELL) {
                sellAmount += amount;
                sellQuantity += fill.quantity();
            }

            PositionCost position = positions.computeIfAbsent(fill.positionKey(), ignored -> new PositionCost());
            double pnl = position.apply(fill.side(), fill.quantity(), fill.price());
            if (pnl != 0d) {
                realizedPnl += pnl;
                pnlByOrder.merge(fill.orderKey(), pnl, Double::sum);
            }
        }

        double buyAveragePrice = buyQuantity > 0d ? buyAmount / buyQuantity : 0d;
        double sellAveragePrice = sellQuantity > 0d ? sellAmount / sellQuantity : 0d;
        return new Snapshot(groups.size(), fillCount, buyAmount, sellAmount,
                buyAveragePrice, sellAveragePrice, realizedPnl,
                Map.copyOf(pnlByOrder), Map.copyOf(amountBySymbol));
    }

    public static String orderKey(RoutingMessage.Order order) {
        return order.getAccount() + "\u0000" + order.getId();
    }

    private record Fill(String account, String symbol, String orderId, RoutingMessage.Side side,
                        double quantity, double price, long seconds, int nanos) {
        private static Fill from(RoutingMessage.Order order, double quantity, double price) {
            return new Fill(order.getAccount(), order.getSymbol(), order.getId(), order.getSide(),
                    quantity, price,
                    order.hasTime() ? order.getTime().getSeconds() : 0L,
                    order.hasTime() ? order.getTime().getNanos() : 0);
        }

        private String positionKey() {
            return account + "\u0000" + symbol;
        }

        private String orderKey() {
            return account + "\u0000" + orderId;
        }
    }

    private static final class PositionCost {
        private double netQuantity;
        private double averageCost;

        private double apply(RoutingMessage.Side side, double quantity, double price) {
            if (side == RoutingMessage.Side.BUY) return buy(quantity, price);
            if (side == RoutingMessage.Side.SELL) return sell(quantity, price);
            return 0d;
        }

        private double buy(double quantity, double price) {
            if (netQuantity >= 0d) {
                averageCost = weightedAverage(netQuantity, averageCost, quantity, price);
                netQuantity += quantity;
                return 0d;
            }

            double closed = Math.min(quantity, -netQuantity);
            double pnl = (averageCost - price) * closed;
            netQuantity += quantity;
            if (netQuantity > 0d) averageCost = price;
            else if (netQuantity == 0d) averageCost = 0d;
            return pnl;
        }

        private double sell(double quantity, double price) {
            if (netQuantity <= 0d) {
                averageCost = weightedAverage(-netQuantity, averageCost, quantity, price);
                netQuantity -= quantity;
                return 0d;
            }

            double closed = Math.min(quantity, netQuantity);
            double pnl = (price - averageCost) * closed;
            netQuantity -= quantity;
            if (netQuantity < 0d) averageCost = price;
            else if (netQuantity == 0d) averageCost = 0d;
            return pnl;
        }

        private static double weightedAverage(double currentQuantity, double currentPrice,
                                              double addedQuantity, double addedPrice) {
            double total = currentQuantity + addedQuantity;
            return total == 0d ? 0d
                    : ((currentQuantity * currentPrice) + (addedQuantity * addedPrice)) / total;
        }
    }
}
