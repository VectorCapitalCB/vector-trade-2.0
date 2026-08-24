package cl.vc.blotter.controller;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChatControllerTest {

    @Test
    void normalizesEmojiPresentationAndWhitespaceForMessageIdentity() {
        assertEquals(
                ChatController.normalizeMessageIdentity("  Hola   ❤️  "),
                ChatController.normalizeMessageIdentity("Hola ❤")
        );
    }

    @Test
    void consumesNormalizedEchoAndSuppressesARepeatedServerEcho() {
        ChatController controller = new ChatController();

        controller.registerPendingEcho("vnazar", "❤️ ");

        assertTrue(controller.consumePendingEcho("VNAZAR", "❤"));
        assertTrue(controller.consumePendingEcho("vnazar", "❤ "));
        assertFalse(controller.consumePendingEcho("vnazar", "🔥"));
    }

    @Test
    void removesConsecutiveSavedDuplicatesWithoutChangingOriginalText() {
        List<String> messages = List.of(
                "[14:35:22] fricci: ❤️ ",
                "[14:35:22] fricci: ❤",
                "[14:36:04] vnazar: hola"
        );

        assertEquals(
                List.of("[14:35:22] fricci: ❤️ ", "[14:36:04] vnazar: hola"),
                ChatController.deduplicateConsecutiveMessages(messages)
        );
    }
}
