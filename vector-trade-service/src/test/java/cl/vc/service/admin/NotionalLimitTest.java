package cl.vc.service.admin;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static cl.vc.module.protocolbuff.routing.RoutingMessage.SecurityExchangeRouting.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests para la validación de monto nominal (precio × cantidad) por destino.
 *
 * Defaults configurados:
 *  - XSGO / NUAM  →  30.000.000
 *  - IB_SMART     →  10.000
 *  - ALPACA       →  10.000
 */
class NotionalLimitTest {

    /** Guarda los límites originales para restaurarlos después de cada test. */
    @BeforeEach
    void backupDefaults() {
        MainApp.getNotionalLimits().put("XSGO",     30_000_000.0);
        MainApp.getNotionalLimits().put("NUAM",     30_000_000.0);
        MainApp.getNotionalLimits().put("IB_SMART",     10_000.0);
        MainApp.getNotionalLimits().put("ALPACA",       10_000.0);
    }

    @AfterEach
    void restoreDefaults() {
        backupDefaults();
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Valores default
    // ─────────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("Límites default")
    class DefaultLimits {

        @Test
        @DisplayName("XSGO tiene límite de 30.000.000")
        void xsgo_defaultLimit() {
            assertEquals(30_000_000.0, MainApp.getNotionalLimit(XSGO));
        }

        @Test
        @DisplayName("NUAM tiene límite de 30.000.000")
        void nuam_defaultLimit() {
            assertEquals(30_000_000.0, MainApp.getNotionalLimit(NUAM));
        }

        @Test
        @DisplayName("IB_SMART tiene límite de 10.000")
        void ibSmart_defaultLimit() {
            assertEquals(10_000.0, MainApp.getNotionalLimit(IB_SMART));
        }

        @Test
        @DisplayName("ALPACA tiene límite de 10.000")
        void alpaca_defaultLimit() {
            assertEquals(10_000.0, MainApp.getNotionalLimit(ALPACA));
        }

        @Test
        @DisplayName("Destino no configurado devuelve MAX_VALUE (sin restricción)")
        void unknown_returnsMaxValue() {
            assertEquals(Double.MAX_VALUE, MainApp.getNotionalLimit(BASKETS));
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Validación checkNotionalLimit — por debajo del límite
    // ─────────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("checkNotionalLimit — dentro del límite")
    class WithinLimit {

        @Test
        @DisplayName("XSGO: 1.000 × 29.000 = 29.000.000 → OK")
        void xsgo_belowLimit_returnsEmpty() {
            Optional<String> result = MainApp.checkNotionalLimit(XSGO, 29_000.0, 1_000.0);
            assertTrue(result.isEmpty(), "No debe rechazar cuando el notional está por debajo del límite");
        }

        @Test
        @DisplayName("XSGO: justo en el límite 30.000 × 1.000 = 30.000.000 → OK")
        void xsgo_atLimit_returnsEmpty() {
            Optional<String> result = MainApp.checkNotionalLimit(XSGO, 30_000.0, 1_000.0);
            assertTrue(result.isEmpty(), "No debe rechazar cuando el notional es exactamente igual al límite");
        }

        @Test
        @DisplayName("IB_SMART: 50 × 100 = 5.000 → OK")
        void ibSmart_belowLimit_returnsEmpty() {
            Optional<String> result = MainApp.checkNotionalLimit(IB_SMART, 50.0, 100.0);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("ALPACA: 99 × 100 = 9.900 → OK")
        void alpaca_belowLimit_returnsEmpty() {
            Optional<String> result = MainApp.checkNotionalLimit(ALPACA, 99.0, 100.0);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("BASKETS sin límite: cualquier monto → OK")
        void baskets_noLimit_alwaysOk() {
            Optional<String> result = MainApp.checkNotionalLimit(BASKETS, 1_000_000.0, 1_000_000.0);
            assertTrue(result.isEmpty(), "BASKETS no tiene límite configurado");
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Validación checkNotionalLimit — supera el límite
    // ─────────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("checkNotionalLimit — supera el límite")
    class ExceedsLimit {

        @Test
        @DisplayName("XSGO: 31.000 × 1.000 = 31.000.000 → RECHAZADO")
        void xsgo_exceedsLimit_returnsMessage() {
            Optional<String> result = MainApp.checkNotionalLimit(XSGO, 31_000.0, 1_000.0);
            assertTrue(result.isPresent(), "Debe rechazar cuando el notional supera el límite");
        }

        @Test
        @DisplayName("NUAM: 30.001 × 1.000 → RECHAZADO")
        void nuam_exceedsLimit_returnsMessage() {
            Optional<String> result = MainApp.checkNotionalLimit(NUAM, 30_001.0, 1_000.0);
            assertTrue(result.isPresent());
        }

        @Test
        @DisplayName("IB_SMART: 101 × 100 = 10.100 → RECHAZADO")
        void ibSmart_exceedsLimit_returnsMessage() {
            Optional<String> result = MainApp.checkNotionalLimit(IB_SMART, 101.0, 100.0);
            assertTrue(result.isPresent());
        }

        @Test
        @DisplayName("ALPACA: 100 × 101 = 10.100 → RECHAZADO")
        void alpaca_exceedsLimit_returnsMessage() {
            Optional<String> result = MainApp.checkNotionalLimit(ALPACA, 100.0, 101.0);
            assertTrue(result.isPresent());
        }

        @Test
        @DisplayName("El mensaje de rechazo contiene el destino, el notional y el límite")
        void rejectionMessage_containsRelevantInfo() {
            Optional<String> result = MainApp.checkNotionalLimit(XSGO, 35_000.0, 1_000.0);
            assertTrue(result.isPresent());
            String msg = result.get();
            assertTrue(msg.contains("XSGO"),   "Debe mencionar el destino");
            assertTrue(msg.contains("35"),      "Debe mencionar el precio");
            assertTrue(msg.contains("1"),       "Debe mencionar la cantidad");
        }

        @Test
        @DisplayName("El mensaje de rechazo menciona 'supera el límite'")
        void rejectionMessage_mentionsLimitExceeded() {
            Optional<String> result = MainApp.checkNotionalLimit(IB_SMART, 200.0, 100.0);
            assertTrue(result.isPresent());
            assertTrue(result.get().toLowerCase().contains("supera"),
                    "El mensaje debe indicar que se supera el límite");
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Actualización dinámica de límites
    // ─────────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("setNotionalLimit — actualización en caliente")
    class DynamicUpdate {

        @Test
        @DisplayName("Después de actualizar el límite de XSGO, se usa el nuevo valor")
        void setLimit_updatesGetLimit() {
            MainApp.setNotionalLimit(XSGO, 50_000_000.0);
            assertEquals(50_000_000.0, MainApp.getNotionalLimit(XSGO));
        }

        @Test
        @DisplayName("Orden que antes era rechazada pasa con límite aumentado")
        void setLimit_higherLimit_allowsPreviouslyRejectedOrder() {
            // Con el límite default (30M) → rechazado
            Optional<String> before = MainApp.checkNotionalLimit(XSGO, 31_000.0, 1_000.0);
            assertTrue(before.isPresent(), "Debe rechazar con el límite default");

            // Aumentar límite a 40M → ahora pasa
            MainApp.setNotionalLimit(XSGO, 40_000_000.0);
            Optional<String> after = MainApp.checkNotionalLimit(XSGO, 31_000.0, 1_000.0);
            assertTrue(after.isEmpty(), "Debe aceptar después de aumentar el límite");
        }

        @Test
        @DisplayName("Orden que antes pasaba es rechazada con límite reducido")
        void setLimit_lowerLimit_rejectsPreviouslyAcceptedOrder() {
            // Con el límite default (30M) → pasa
            Optional<String> before = MainApp.checkNotionalLimit(XSGO, 29_000.0, 1_000.0);
            assertTrue(before.isEmpty(), "Debe aceptar con el límite default");

            // Reducir límite a 25M → ahora es rechazado
            MainApp.setNotionalLimit(XSGO, 25_000_000.0);
            Optional<String> after = MainApp.checkNotionalLimit(XSGO, 29_000.0, 1_000.0);
            assertTrue(after.isPresent(), "Debe rechazar después de reducir el límite");
        }

        @Test
        @DisplayName("Actualizar IB_SMART no afecta el límite de ALPACA")
        void setLimit_isolatedPerExchange() {
            MainApp.setNotionalLimit(IB_SMART, 500_000.0);
            assertEquals(10_000.0, MainApp.getNotionalLimit(ALPACA),
                    "El límite de ALPACA no debe cambiar al modificar IB_SMART");
        }

        @Test
        @DisplayName("El mapa notionalLimits se actualiza tras setNotionalLimit")
        void setLimit_reflectsInMap() {
            MainApp.setNotionalLimit(NUAM, 99_999.0);
            assertEquals(99_999.0, MainApp.getNotionalLimits().get("NUAM"));
        }
    }
}
