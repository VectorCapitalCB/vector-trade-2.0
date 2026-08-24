package cl.vc.blotter.model;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HistoricalTradingAnalyticsTest {

    @Test
    void calculaPnlRealizadoConCostoPromedioMovil() {
        BlotterMessage.HistoricalOrderGroup buys = group("BUY-1", RoutingMessage.Side.BUY,
                fill("BUY-1", "B1", RoutingMessage.Side.BUY, 100, 10, 100, 1),
                fill("BUY-1", "B2", RoutingMessage.Side.BUY, 50, 12, 150, 2));
        BlotterMessage.HistoricalOrderGroup sells = group("SELL-1", RoutingMessage.Side.SELL,
                fill("SELL-1", "S1", RoutingMessage.Side.SELL, 120, 15, 120, 3));

        HistoricalTradingAnalytics.Snapshot result =
                HistoricalTradingAnalytics.calculate(List.of(sells, buys));

        assertEquals(2, result.orders());
        assertEquals(3, result.fills());
        assertEquals(1_600d, result.buyAmount(), 0.0001d);
        assertEquals(1_800d, result.sellAmount(), 0.0001d);
        assertEquals(10.6666667d, result.buyAveragePrice(), 0.0001d);
        assertEquals(15d, result.sellAveragePrice(), 0.0001d);
        assertEquals(520d, result.realizedPnl(), 0.0001d);
        assertEquals(520d, result.realizedPnl(sells), 0.0001d);
        assertEquals(0d, result.realizedPnl(buys), 0.0001d);
        assertEquals(3_400d, result.amountBySymbol().get("LTM"), 0.0001d);
    }

    @Test
    void calculaPnlAlCerrarUnaPosicionCorta() {
        BlotterMessage.HistoricalOrderGroup sells = group("SHORT-1", RoutingMessage.Side.SELL,
                fill("SHORT-1", "S1", RoutingMessage.Side.SELL, 100, 20, 100, 1));
        BlotterMessage.HistoricalOrderGroup buys = group("COVER-1", RoutingMessage.Side.BUY,
                fill("COVER-1", "B1", RoutingMessage.Side.BUY, 40, 17, 40, 2));

        HistoricalTradingAnalytics.Snapshot result =
                HistoricalTradingAnalytics.calculate(List.of(buys, sells));

        assertEquals(120d, result.realizedPnl(), 0.0001d);
        assertEquals(17d, result.buyAveragePrice(), 0.0001d);
        assertEquals(20d, result.sellAveragePrice(), 0.0001d);
        assertEquals(120d, result.realizedPnl(buys), 0.0001d);
    }

    private static BlotterMessage.HistoricalOrderGroup group(String orderId, RoutingMessage.Side side,
                                                              RoutingMessage.Order... fills) {
        double qty = java.util.Arrays.stream(fills).mapToDouble(RoutingMessage.Order::getLastQty).sum();
        double amount = java.util.Arrays.stream(fills)
                .mapToDouble(fill -> fill.getLastQty() * fill.getLastPx()).sum();
        RoutingMessage.Order summary = fills[fills.length - 1].toBuilder()
                .setId(orderId)
                .setSide(side)
                .setOrderQty(qty)
                .setCumQty(qty)
                .setAvgPrice(amount / qty)
                .build();
        return BlotterMessage.HistoricalOrderGroup.newBuilder()
                .setSummary(summary)
                .addAllExecutions(List.of(fills))
                .build();
    }

    private static RoutingMessage.Order fill(String orderId, String execId, RoutingMessage.Side side,
                                              double qty, double price, double cumQty, long seconds) {
        return RoutingMessage.Order.newBuilder()
                .setId(orderId)
                .setExecId(execId)
                .setAccount("18415523/0")
                .setSymbol("LTM")
                .setSide(side)
                .setLastQty(qty)
                .setLastPx(price)
                .setCumQty(cumQty)
                .setAvgPrice(price)
                .setOrderQty(cumQty)
                .setTime(Timestamp.newBuilder().setSeconds(seconds))
                .build();
    }
}
