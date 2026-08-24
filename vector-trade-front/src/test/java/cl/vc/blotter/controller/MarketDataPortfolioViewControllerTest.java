package cl.vc.blotter.controller;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MarketDataPortfolioViewControllerTest {

    @Test
    void recognizesSymbolFromLiveMarketDataSubscription() {
        MarketDataMessage.Subscribe aguas = MarketDataMessage.Subscribe.newBuilder()
                .setSymbol("AGUAS-A")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .build();

        MarketDataMessage.Security security =
                MarketDataPortfolioViewController.findSubscribedSecurity(
                        " aguas-a ",
                        MarketDataMessage.SecurityExchangeMarketData.BCS,
                        List.of(aguas));

        assertEquals("AGUAS-A", security.getSymbol());
        assertEquals("CS", security.getSecurityType());
        assertEquals(MarketDataMessage.SecurityExchangeMarketData.BCS,
                security.getSecurityExchange());
    }

    @Test
    void doesNotAcceptSubscriptionFromAnotherMarket() {
        MarketDataMessage.Subscribe aguas = MarketDataMessage.Subscribe.newBuilder()
                .setSymbol("AGUAS-A")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .build();

        assertNull(MarketDataPortfolioViewController.findSubscribedSecurity(
                "AGUAS-A",
                MarketDataMessage.SecurityExchangeMarketData.FH_IBKR,
                List.of(aguas)));
    }

    @Test
    void keepsUnknownSymbolsRejected() {
        assertNull(MarketDataPortfolioViewController.findSubscribedSecurity(
                "NO-EXISTE",
                MarketDataMessage.SecurityExchangeMarketData.BCS,
                List.of()));
    }
}
