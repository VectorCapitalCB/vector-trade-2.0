package cl.vc.blotter;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryLiveOrderTest {

    @AfterEach
    void clearIndex() {
        Repository.clearLiveOrderLevels();
    }

    @Test
    void matchesOwnLiveOrderByBookLevelAndRemovesTerminalOrder() {
        RoutingMessage.Order live = order(RoutingMessage.OrderStatus.NEW);
        Repository.updateLiveOrderLevel(live);

        assertTrue(Repository.tieneOrdenVivaEn("ltm", RoutingMessage.Side.BUY, 25.10001,
                RoutingMessage.SecurityExchangeRouting.XSGO, RoutingMessage.SettlType.T2));
        assertFalse(Repository.tieneOrdenVivaEn("LTM", RoutingMessage.Side.SELL, 25.10,
                RoutingMessage.SecurityExchangeRouting.XSGO, RoutingMessage.SettlType.T2));
        assertFalse(Repository.tieneOrdenVivaEn("LTM", RoutingMessage.Side.BUY, 25.10,
                RoutingMessage.SecurityExchangeRouting.IB_SMART, RoutingMessage.SettlType.T2));
        assertFalse(Repository.tieneOrdenVivaEn("LTM", RoutingMessage.Side.BUY, 25.10,
                RoutingMessage.SecurityExchangeRouting.XSGO, RoutingMessage.SettlType.T3));

        Repository.updateLiveOrderLevel(live.toBuilder()
                .setOrdStatus(RoutingMessage.OrderStatus.CANCELED)
                .build());

        assertFalse(Repository.tieneOrdenVivaEn("LTM", RoutingMessage.Side.BUY, 25.10));
    }

    @Test
    void keepsPendingCancelMarkedUntilTheMarketConfirmsCancellation() {
        Repository.updateLiveOrderLevel(order(RoutingMessage.OrderStatus.PENDING_CANCEL));

        assertTrue(Repository.tieneOrdenVivaEn("LTM", RoutingMessage.Side.BUY, 25.10));
    }

    @Test
    void disaggregatedBookMarksOnlyTheMatchingPositionAtTheSamePrice() {
        Repository.updateLiveOrderLevel(order(RoutingMessage.OrderStatus.NEW).toBuilder()
                .setAccount("18415523/0")
                .setOperator("daedo")
                .setOrderQty(1d)
                .setLeaves(1d)
                .build());

        assertFalse(Repository.tienePosturaVivaEn(
                "LTM", RoutingMessage.Side.BUY, 25.10, 500d, "", "",
                RoutingMessage.SecurityExchangeRouting.XSGO, RoutingMessage.SettlType.T2));
        assertTrue(Repository.tienePosturaVivaEn(
                "LTM", RoutingMessage.Side.BUY, 25.10, 1d, "", "",
                RoutingMessage.SecurityExchangeRouting.XSGO, RoutingMessage.SettlType.T2));
        assertTrue(Repository.tienePosturaVivaEn(
                "LTM", RoutingMessage.Side.BUY, 25.10, 500d, "18415523/0", "",
                RoutingMessage.SecurityExchangeRouting.XSGO, RoutingMessage.SettlType.T2));
    }

    private static RoutingMessage.Order order(RoutingMessage.OrderStatus status) {
        return RoutingMessage.Order.newBuilder()
                .setId("order-1")
                .setSymbol("LTM")
                .setSide(RoutingMessage.Side.BUY)
                .setPrice(25.10)
                .setOrdStatus(status)
                .setSecurityExchange(RoutingMessage.SecurityExchangeRouting.XSGO)
                .setSettlType(RoutingMessage.SettlType.T2)
                .build();
    }
}
