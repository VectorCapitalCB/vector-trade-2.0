package cl.vc.blotter.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class I18nTest {

    @Test
    void translatesTradingValuesWithoutChangingTheRawValue() {
        String rawSide = "BUY";
        String rawStatus = "PARTIALLY_FILLED";

        assertEquals("Compra", I18n.translate(Language.SPANISH, rawSide));
        assertEquals("Buy", I18n.translate(Language.ENGLISH, rawSide));
        assertEquals("Parcial", I18n.translate(Language.SPANISH, rawStatus));
        assertEquals("Partially filled", I18n.translate(Language.ENGLISH, rawStatus));
        assertEquals("BUY", rawSide);
        assertEquals("PARTIALLY_FILLED", rawStatus);
    }

    @Test
    void translatesCommonVisualControlsInBothDirections() {
        assertEquals("Order Launcher", I18n.translate(Language.ENGLISH, "Lanzador de Ordenes"));
        assertEquals("Lanzador de Órdenes", I18n.translate(Language.SPANISH, "Order Launcher"));
        assertEquals("Settings", I18n.translate(Language.ENGLISH, "Configuraciones"));
        assertEquals("Configuraciones", I18n.translate(Language.SPANISH, "Settings"));
        assertEquals("All statuses", I18n.translate(Language.ENGLISH, "ALL_STATUS"));
        assertEquals("Todos los estados", I18n.translate(Language.SPANISH, "ALL_STATUS"));
    }

    @Test
    void preservesDynamicIdentifiersWhileTranslatingTheirPrefix() {
        assertEquals("User: daedo", I18n.translate(Language.ENGLISH, "Usuario: daedo"));
        assertEquals("Entorno: QA", I18n.translate(Language.SPANISH, "Environment: QA"));
        assertEquals("Basket: BKT-123", I18n.translate(Language.ENGLISH, "Canasta: BKT-123"));
        assertEquals("Working (25)", I18n.translate(Language.ENGLISH, "Trabajando (25)"));
        assertEquals("Latest Instrument Trades SQM-B (10)",
                I18n.translate(Language.ENGLISH, "Últimas Operaciones Nemo SQM-B (10)"));
    }
}
