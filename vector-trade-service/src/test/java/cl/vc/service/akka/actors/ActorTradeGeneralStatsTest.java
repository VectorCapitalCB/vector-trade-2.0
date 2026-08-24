package cl.vc.service.akka.actors;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ActorTradeGeneralStatsTest {

    @Test
    void acumulaRankingSinConservarTodosLosTrades() {
        ActorTradeGeneralStats.TradeAccumulator accumulator = new ActorTradeGeneralStats.TradeAccumulator();
        accumulator.add(trade(100.0, 10.0));
        accumulator.add(trade(101.0, 20.0));
        accumulator.add(trade(99.0, 5.0));
        MarketDataMessage.TradeGeneral lastTrade = trade(102.0, 15.0);
        accumulator.add(lastTrade);

        MarketDataMessage.RankinSymbol rank = accumulator.toRankin("LTMBCST2CS", lastTrade);

        assertEquals(4, accumulator.count());
        assertEquals(4, accumulator.retainedPriceCount());
        assertEquals(102.0, rank.getPrecioUltimo());
        assertEquals(102.0, rank.getPrecioMaximo());
        assertEquals(99.0, rank.getPrecioMinimo());
        assertEquals(100.5, rank.getPrecioPromedio());
        assertEquals(50.0, rank.getVolumen());
        assertEquals(5_045.0, rank.getMonto());
        assertEquals(100.9, rank.getVwap(), 0.000001);
        assertEquals(2.0, rank.getVariacionPct(), 0.000001);
        assertEquals(66.666666, rank.getRsi(), 0.0001);
        assertTrue(accumulator.realizedVolatility() > 0.0);
    }

    @Test
    void limitaLaVentanaDePreciosAunqueCrezcaElNumeroDeTrades() {
        ActorTradeGeneralStats.TradeAccumulator accumulator = new ActorTradeGeneralStats.TradeAccumulator();
        MarketDataMessage.TradeGeneral lastTrade = null;
        for (int i = 0; i < 100_000; i++) {
            lastTrade = trade(100.0 + (i % 17), 1.0);
            accumulator.add(lastTrade);
        }

        MarketDataMessage.RankinSymbol rank = accumulator.toRankin("LTMBCST2CS", lastTrade);

        assertEquals(100_000, accumulator.count());
        assertEquals(26, accumulator.retainedPriceCount());
        assertEquals(100_000.0, rank.getVolumen());
    }

    @Test
    void conservaMaMacdRsiYVolatilidadDelCalculoAnterior() {
        ActorTradeGeneralStats.TradeAccumulator accumulator = new ActorTradeGeneralStats.TradeAccumulator();
        List<Double> prices = new ArrayList<>();
        MarketDataMessage.TradeGeneral lastTrade = null;
        for (int i = 0; i < 80; i++) {
            double price = 90.0 + ((i * 7) % 19) + (i * 0.05);
            prices.add(price);
            lastTrade = trade(price, i + 1.0);
            accumulator.add(lastTrade);
        }

        MarketDataMessage.RankinSymbol rank = accumulator.toRankin("LTMBCST2CS", lastTrade);

        assertEquals(averageOfLast(prices, 10), rank.getMa(), 0.000000001);
        assertEquals(emaOfLast(prices, 12) - emaOfLast(prices, 26), rank.getMacd(), 0.000000001);
        assertEquals(legacyRsi(prices), rank.getRsi(), 0.000000001);
        assertEquals(legacyVolatility(prices), accumulator.realizedVolatility(), 0.000000001);
    }

    private MarketDataMessage.TradeGeneral trade(double price, double quantity) {
        return MarketDataMessage.TradeGeneral.newBuilder()
                .setSymbol("LTM")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setPrice(price)
                .setQty(quantity)
                .setAmount(price * quantity)
                .build();
    }

    private double averageOfLast(List<Double> prices, int period) {
        return prices.stream()
                .skip(Math.max(0, prices.size() - period))
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }

    private double emaOfLast(List<Double> prices, int period) {
        if (prices.size() < period) {
            return 0.0;
        }
        int first = prices.size() - period;
        double multiplier = 2.0 / (period + 1.0);
        double ema = prices.get(first);
        for (int i = first + 1; i < prices.size(); i++) {
            ema = (prices.get(i) - ema) * multiplier + ema;
        }
        return ema;
    }

    private double legacyRsi(List<Double> prices) {
        double gains = 0.0;
        double losses = 0.0;
        for (int i = 1; i < prices.size(); i++) {
            double difference = prices.get(i) - prices.get(i - 1);
            if (difference > 0) {
                gains += difference;
            } else {
                losses += -difference;
            }
        }
        if (gains == 0.0 && losses == 0.0) {
            return 50.0;
        }
        return 100.0 - (100.0 / (1.0 + (gains / losses)));
    }

    private double legacyVolatility(List<Double> prices) {
        double sum = 0.0;
        double sumSquared = 0.0;
        for (int i = 1; i < prices.size(); i++) {
            double logReturn = Math.log(prices.get(i) / prices.get(i - 1));
            sum += logReturn;
            sumSquared += logReturn * logReturn;
        }
        int count = prices.size() - 1;
        double mean = sum / count;
        return Math.sqrt(Math.max((sumSquared / count) - (mean * mean), 0.0));
    }
}
