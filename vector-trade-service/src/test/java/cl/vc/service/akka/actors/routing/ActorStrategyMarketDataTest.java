package cl.vc.service.akka.actors.routing;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.util.BookSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ActorStrategyMarketDataTest {

    @Test
    void requestsBookWhenSnapshotDoesNotExist() {
        assertFalse(ActorStrategy.hasBookDepth(null));
    }

    @Test
    void requestsBookWhenSnapshotWasCreatedOnlyByTradesOrStatistics() {
        assertFalse(ActorStrategy.hasBookDepth(snapshot()));
    }

    @Test
    void reusesSnapshotWhenAtLeastOneBookSideHasDepth() {
        BookSnapshot bidSnapshot = snapshot();
        bidSnapshot.setBid(List.of(level(MarketDataMessage.TypeBook.BID)));

        BookSnapshot askSnapshot = snapshot();
        askSnapshot.setAsk(List.of(level(MarketDataMessage.TypeBook.ASK)));

        assertTrue(ActorStrategy.hasBookDepth(bidSnapshot));
        assertTrue(ActorStrategy.hasBookDepth(askSnapshot));
    }

    private static BookSnapshot snapshot() {
        MarketDataMessage.Subscribe subscribe = MarketDataMessage.Subscribe.newBuilder()
                .setId("SALFACORPBCST2CS")
                .setSymbol("SALFACORP")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setBook(true)
                .build();
        return new BookSnapshot("SALFACORPBCST2CS", subscribe);
    }

    private static MarketDataMessage.DataBook level(MarketDataMessage.TypeBook type) {
        return MarketDataMessage.DataBook.newBuilder()
                .setPrice(1_250d)
                .setSize(100d)
                .setTypeBook(type)
                .build();
    }
}
