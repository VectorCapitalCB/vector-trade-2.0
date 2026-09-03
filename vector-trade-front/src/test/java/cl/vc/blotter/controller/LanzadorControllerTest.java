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

    /**
     * Paridad con producción en el iceberg del lanzador: el campo se recorta y sólo cuenta
     * como iceberg cuando trae un porcentaje positivo. Cubre los formatos que llegan del
     * operador ("20", "20%", "020") y los casos que antes marcaban iceberg igual (vacío con
     * espacios, "0", texto no numérico) y la bolsa rechazaba.
     */
    @Test
    void parsesIcebergPercentageLikeProduction() {
        assertEquals(20d, LanzadorController.parseIcebergPercentage("20"));
        assertEquals(20d, LanzadorController.parseIcebergPercentage("20%"));
        assertEquals(20d, LanzadorController.parseIcebergPercentage("020"));
        assertEquals(10d, LanzadorController.parseIcebergPercentage("  10 % "));
    }

    /**
     * Rareza heredada de producción que se fija a propósito: el punto y la coma se ELIMINAN,
     * no se interpretan como decimal, así que "50,0" o "50.0" se leen como 500. No corrompe
     * la orden porque aguas abajo StrategyReplaceSupport.maxFloorForNewOrder descarta un
     * maxFloor que alcance la cantidad total. Si algún día se decide tratar la coma como
     * decimal, este test debe cambiar conscientemente, no por accidente.
     */
    @Test
    void separatorsInsideIcebergAreDroppedLikeProduction() {
        assertEquals(500d, LanzadorController.parseIcebergPercentage("50,0"));
        assertEquals(500d, LanzadorController.parseIcebergPercentage("50.0"));
    }

    @Test
    void emptyOrZeroIcebergDoesNotMarkIceberg() {
        assertEquals(0d, LanzadorController.parseIcebergPercentage(""));
        assertEquals(0d, LanzadorController.parseIcebergPercentage("   "));
        assertEquals(0d, LanzadorController.parseIcebergPercentage(null));
        assertEquals(0d, LanzadorController.parseIcebergPercentage("0"));
        assertEquals(0d, LanzadorController.parseIcebergPercentage("0%"));
        assertEquals(0d, LanzadorController.parseIcebergPercentage("abc"));
        // y con 0 el maxFloor tampoco se calcula
        assertEquals(0, LanzadorController.calculateVisibleMaxFloor(100_000d,
                LanzadorController.parseIcebergPercentage(" ")));
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
