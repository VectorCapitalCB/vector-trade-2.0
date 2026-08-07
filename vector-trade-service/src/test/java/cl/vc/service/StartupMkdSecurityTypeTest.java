package cl.vc.service;

import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StartupMkdSecurityTypeTest {

    private static final String SYMBOL = "CFI_TEST_ETF";
    private static final MarketDataMessage.SecurityExchangeMarketData EXCHANGE =
            MarketDataMessage.SecurityExchangeMarketData.BCS;

    @AfterEach
    void cleanSecurity() {
        MainApp.getSecurityExchangeSymbolsMaps().remove(SYMBOL + EXCHANGE.name());
    }

    @Test
    void resolvesSecurityTypeFromSecurityListEntry() {
        MarketDataMessage.Security security = MarketDataMessage.Security.newBuilder()
                .setSymbol(SYMBOL)
                .setSecurityExchange(EXCHANGE)
                .setSecurityType(RoutingMessage.SecurityType.ETF.name())
                .build();
        MainApp.getSecurityExchangeSymbolsMaps().put(SYMBOL + EXCHANGE.name(), security);

        assertEquals(
                RoutingMessage.SecurityType.ETF,
                MainApp.resolveMkdSecurityType(SYMBOL, EXCHANGE, RoutingMessage.SecurityType.CFI)
        );
    }

    @Test
    void usesFallbackWhenSecurityIsUnknown() {
        assertEquals(
                RoutingMessage.SecurityType.CFI,
                MainApp.resolveMkdSecurityType(SYMBOL, EXCHANGE, RoutingMessage.SecurityType.CFI)
        );
    }

    @Test
    void generatesItchCompatibleEtfTopic() {
        MarketDataMessage.Subscribe subscribe = MarketDataMessage.Subscribe.newBuilder()
                .setSymbol("CFINASDAQ")
                .setSecurityExchange(EXCHANGE)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.ETF)
                .build();

        assertEquals("CFINASDAQBCST2ETF", TopicGenerator.getTopicMKD(subscribe));
    }
}
