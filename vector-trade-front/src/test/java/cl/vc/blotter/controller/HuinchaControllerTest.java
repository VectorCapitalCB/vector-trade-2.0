package cl.vc.blotter.controller;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HuinchaControllerTest {

    @Test
    void usaLaClaveTecnicaDeCandleAunqueElRankingSeaNacional() {
        MarketDataMessage.RankinSymbol rank = MarketDataMessage.RankinSymbol.newBuilder()
                .setId("COPECBCSNATIONALCS")
                .setSymbol("COPEC")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .build();

        assertEquals("COPECBCST2CS", HuinchaController.topic(rank));
    }
}
