package cl.vc.blotter.controller;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanzadorControllerTest {

    @Test
    void usesFiftyPercentVisibleByDefaultFromMultibook() {
        assertEquals("50", LanzadorController.MULTIBOOK_DEFAULT_VISIBLE_PERCENTAGE);
    }

    @Test
    void usesHolguraStrategyByDefaultFromMultibook() {
        assertEquals(RoutingMessage.StrategyOrder.HOLGURA, LanzadorController.MULTIBOOK_DEFAULT_STRATEGY);
    }

    @Test
    void preservesExplicitZeroVisible() {
        assertEquals(0, LanzadorController.calculateVisibleMaxFloor(100_000d, 0d));
    }

    @Test
    void keepsExistingTenPercentMinimumForPositiveVisible() {
        assertEquals(10_000, LanzadorController.calculateVisibleMaxFloor(100_000d, 5d));
        assertEquals(25_000, LanzadorController.calculateVisibleMaxFloor(100_000d, 25d));
    }

    @Test
    void usesSqmBOnlyAsInitialInstrument() {
        assertEquals("SQM-B", LanzadorController.INITIAL_INSTRUMENT);
    }

    @Test
    void exposesOnlyEnabledOrderTypes() {
        assertEquals(List.of(
                        RoutingMessage.OrdType.MARKET,
                        RoutingMessage.OrdType.LIMIT),
                LanzadorController.ALLOWED_ORDER_TYPES);
        assertFalse(LanzadorController.ALLOWED_ORDER_TYPES.contains(RoutingMessage.OrdType.MARKET_CLOSE));
    }

    @Test
    void exposesOnlyEnabledSecurityTypes() {
        assertEquals(List.of(
                        RoutingMessage.SecurityType.CS,
                        RoutingMessage.SecurityType.CFI,
                        RoutingMessage.SecurityType.MON,
                        RoutingMessage.SecurityType.ETF,
                        RoutingMessage.SecurityType.CORP),
                LanzadorController.ALLOWED_SECURITY_TYPES);
    }

    @Test
    void showsLiquidationOnlyForIbSmartInAdvancedMode() {
        assertTrue(LanzadorController.shouldShowAdvancedLiquidation(
                false, RoutingMessage.SecurityExchangeRouting.IB_SMART));
        assertFalse(LanzadorController.shouldShowAdvancedLiquidation(
                false, RoutingMessage.SecurityExchangeRouting.XSGO));
        assertFalse(LanzadorController.shouldShowAdvancedLiquidation(
                false, null));
    }

    @Test
    void leavesLiquidationVisibilityToLightModeLayout() {
        assertFalse(LanzadorController.shouldShowAdvancedLiquidation(
                true, RoutingMessage.SecurityExchangeRouting.IB_SMART));
    }
}
