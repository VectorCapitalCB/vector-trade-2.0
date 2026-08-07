package cl.vc.service.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifica la lógica de throttle/dedup y de "listo para enviar" del notificador de Telegram.
 * (El envío HTTP real no se prueba aquí; es best-effort y fail-safe.)
 */
class TelegramNotifierTest {

    @Test
    void throttle_mismaKey_unaVezPorIntervalo() {
        TelegramNotifier.configure(true, "token", "123", 300); // 300s = 300_000ms
        long t0 = 1_000_000L;
        assertTrue(TelegramNotifier.allow("ERR_X", t0), "primera vez: permite");
        assertFalse(TelegramNotifier.allow("ERR_X", t0 + 299_000), "dentro del intervalo: bloquea");
        assertTrue(TelegramNotifier.allow("ERR_X", t0 + 301_000), "pasado el intervalo: permite");
        // otra key no se ve afectada
        assertTrue(TelegramNotifier.allow("ERR_Y", t0 + 299_000), "otra key: permite");
    }

    @Test
    void isReady_requiereEnabledTokenYChatId() {
        TelegramNotifier.configure(true, "token", "123", 300);
        assertTrue(TelegramNotifier.isReady());

        TelegramNotifier.configure(false, "token", "123", 300);
        assertFalse(TelegramNotifier.isReady(), "deshabilitado");

        TelegramNotifier.configure(true, "token", "", 300);
        assertFalse(TelegramNotifier.isReady(), "sin chatId");

        TelegramNotifier.configure(true, "", "123", 300);
        assertFalse(TelegramNotifier.isReady(), "sin token");
    }

    @Test
    void alert_noLanza_aunqueNoEsteListo() {
        TelegramNotifier.configure(true, "", "", 300); // no listo
        assertDoesNotThrow(() -> TelegramNotifier.alert("k", "detalle"));
    }
}
