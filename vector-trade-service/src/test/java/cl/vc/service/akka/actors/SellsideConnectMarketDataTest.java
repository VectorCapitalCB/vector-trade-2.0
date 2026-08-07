package cl.vc.service.akka.actors;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SellsideConnectMarketDataTest {

    private static final String TOPIC = "CFINASDAQBCST2ETF";

    @AfterEach
    void cleanSubscription() {
        MainApp.getIdSymbolsSubscrib().remove(TOPIC);
    }

    @Test
    void normalizesItchSnapshotToSubscribedEtfType() {
        MainApp.getIdSymbolsSubscrib().put(TOPIC, MarketDataMessage.Subscribe.newBuilder()
                .setId(TOPIC)
                .setSymbol("CFINASDAQ")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.ETF)
                .build());
        MarketDataMessage.Snapshot source = MarketDataMessage.Snapshot.newBuilder()
                .setId(TOPIC)
                .setSymbol("CFINASDAQ")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setStatistic(MarketDataMessage.Statistic.newBuilder()
                        .setSecurityType(RoutingMessage.SecurityType.CS))
                .addTrades(MarketDataMessage.Trade.newBuilder()
                        .setId(TOPIC)
                        .setIdGenerico("trade-1")
                        .setSecurityType(RoutingMessage.SecurityType.CS))
                .build();

        MarketDataMessage.Snapshot normalized = SellsideConnect.normalizeSnapshot(source);

        assertEquals(TOPIC, normalized.getId());
        assertEquals(RoutingMessage.SecurityType.ETF, normalized.getSecurityType());
        assertEquals(RoutingMessage.SecurityType.ETF, normalized.getStatistic().getSecurityType());
        assertEquals(RoutingMessage.SecurityType.ETF, normalized.getTrades(0).getSecurityType());
        assertEquals("trade-1", normalized.getTrades(0).getIdGenerico());
    }

    @Test
    void normalizesLiveMessagesToSubscribedEtfTopic() {
        MainApp.getIdSymbolsSubscrib().put(TOPIC, MarketDataMessage.Subscribe.newBuilder()
                .setId(TOPIC)
                .setSymbol("CFINASDAQ")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.ETF)
                .build());
        MarketDataMessage.IncrementalBook source = MarketDataMessage.IncrementalBook.newBuilder()
                .setId(TOPIC)
                .setSymbol("CFINASDAQ")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .build();

        MarketDataMessage.IncrementalBook normalized = SellsideConnect.normalizeIncrementalBook(source);

        assertEquals(TOPIC, normalized.getId());
        assertEquals(RoutingMessage.SecurityType.ETF, normalized.getSecurityType());
    }
}
