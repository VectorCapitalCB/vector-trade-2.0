package cl.vc.blotter.controller;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LibroEmergenteControllerTest {

    @Test
    void hidesSettlementForBcs() {
        assertFalse(LibroEmergenteController.shouldShowSettlement(
                MarketDataMessage.SecurityExchangeMarketData.BCS));
    }

    @Test
    void showsSettlementForOtherMarkets() {
        assertTrue(LibroEmergenteController.shouldShowSettlement(
                MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL));
        assertFalse(LibroEmergenteController.shouldShowSettlement(null));
    }

    @Test
    void expandsTicketOnlyWhenBcsReleasesSettlementSpace() {
        assertEquals(LibroEmergenteController.BCS_TICKET_WIDTH,
                LibroEmergenteController.ticketWidthForMarket(
                        MarketDataMessage.SecurityExchangeMarketData.BCS));
        assertEquals(LibroEmergenteController.COMPACT_TICKET_WIDTH,
                LibroEmergenteController.ticketWidthForMarket(
                        MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL));
    }

    @Test
    void depthControlsTheExactVisibleTableHeight() {
        assertEquals(3, LibroEmergenteController.normalizeVisibleDepth(3));
        assertEquals(5, LibroEmergenteController.normalizeVisibleDepth(5));
        assertEquals(10, LibroEmergenteController.normalizeVisibleDepth(10));
        assertEquals(15, LibroEmergenteController.normalizeVisibleDepth(15));
        assertEquals(5, LibroEmergenteController.normalizeVisibleDepth(6));
        assertEquals(92, LibroEmergenteController.tableHeightForDepth(3));
        assertEquals(136, LibroEmergenteController.tableHeightForDepth(5));
    }

    @Test
    void bookLauncherUsesSubscriptionSymbolAndClassWhenLevelHasNoSymbol() {
        LibroEmergenteController controller = new LibroEmergenteController();
        controller.setSubscribe(MarketDataMessage.Subscribe.newBuilder()
                .setSymbol("cfispETF")
                .setSecurityType(cl.vc.module.protocolbuff.routing.RoutingMessage.SecurityType.ETF)
                .build());

        assertEquals("CFISPETF", controller.resolveLauncherSymbol(""));
        assertEquals(cl.vc.module.protocolbuff.routing.RoutingMessage.SecurityType.ETF,
                controller.resolveLauncherSecurityType("CFISPETF"));
    }
}
