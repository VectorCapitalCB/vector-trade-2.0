package cl.vc.blotter.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientLogCollectorTest {

    @TempDir
    Path tempDir;

    @Test
    void sanitizesCredentialsAndAuthorizationHeaders() {
        String sanitized = ClientLogCollector.sanitize(
                "Authorization: Bearer abc.def password=supersecret token: qwerty "
                        + "mongodb://admin:dbpassword@10.0.1.9/db "
                        + "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature");

        assertFalse(sanitized.contains("abc.def"));
        assertFalse(sanitized.contains("supersecret"));
        assertFalse(sanitized.contains("qwerty"));
        assertFalse(sanitized.contains("dbpassword"));
        assertFalse(sanitized.contains("eyJhbGci"));
        assertTrue(sanitized.contains("[REDACTED]"));
    }

    @Test
    void keepsOnlyRequestedTimeWindow() throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS")
                .withZone(ZoneId.of("America/Santiago"));
        Instant now = Instant.now();
        String oldLine = "[" + formatter.format(now.minusSeconds(7200)) + "] [ERROR] old\n";
        String recentLine = "[" + formatter.format(now.minusSeconds(60)) + "] [ERROR] recent\n"
                + "stack trace continuation\n";
        Files.writeString(tempDir.resolve("vector-trade.log"), oldLine + recentLine);

        ClientLogCollector.CollectionResult result = ClientLogCollector.collectWindow(
                List.of(tempDir), now.minusSeconds(1800), 64 * 1024);

        assertFalse(result.content().contains("old"));
        assertTrue(result.content().contains("recent"));
        assertTrue(result.content().contains("stack trace continuation"));
    }

    @Test
    void readsCurrentQaLogNameAndTimestampFormat() throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.of("America/Santiago"));
        Instant now = Instant.now();
        Files.writeString(tempDir.resolve("vector-trade-2.log"),
                formatter.format(now.minusSeconds(30)) + " ERROR diagnostico actual\n");

        ClientLogCollector.CollectionResult result = ClientLogCollector.collectWindow(
                List.of(tempDir), now.minusSeconds(300), 64 * 1024);

        assertTrue(result.content().contains("diagnostico actual"));
    }

    @Test
    void neverCollectsBeforeStartOfCurrentDay() {
        ZoneId zone = ZoneId.of("America/Santiago");
        Instant now = LocalDate.of(2026, 8, 13).atTime(1, 15).atZone(zone).toInstant();

        Instant cutoff = ClientLogCollector.effectiveCutoff(now, 240);

        assertEquals(LocalDate.of(2026, 8, 13).atStartOfDay(zone).toInstant(), cutoff);
    }

    @Test
    void prefersCurrentVectorTrade2LogAndAvoidsDuplicateAppenderOutput() throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.of("America/Santiago"));
        Instant now = Instant.now();
        String timestamp = formatter.format(now.minusSeconds(30));
        Files.writeString(tempDir.resolve("vector-trade.log"), timestamp + " INFO duplicado antiguo\n");
        Files.writeString(tempDir.resolve("vector-trade-2.log"), timestamp + " INFO diagnostico unico\n");

        ClientLogCollector.CollectionResult result = ClientLogCollector.collectWindow(
                List.of(tempDir), now.minusSeconds(300), 64 * 1024);

        assertFalse(result.content().contains("duplicado antiguo"));
        assertTrue(result.content().contains("diagnostico unico"));
    }

    @Test
    void reportsBoundedHardwareSummary() {
        String summary = ClientLogCollector.hardwareSummary();

        assertTrue(summary.contains("CPU:"));
        assertTrue(summary.contains("nucleos logicos"));
        assertTrue(summary.contains("RAM:"));
        assertTrue(summary.contains("JVM:"));
        assertTrue(summary.length() < 500);
    }

    @Test
    void capsOutputUsingUtf8Bytes() throws Exception {
        StringBuilder content = new StringBuilder();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS")
                .withZone(ZoneId.of("America/Santiago"));
        String timestamp = formatter.format(Instant.now());
        for (int i = 0; i < 4000; i++) {
            content.append('[').append(timestamp).append("] [INFO] línea ").append(i).append(" contenido\n");
        }
        Files.writeString(tempDir.resolve("vector-trade.log"), content);

        int limit = 16 * 1024;
        ClientLogCollector.CollectionResult result = ClientLogCollector.collectWindow(
                List.of(tempDir), Instant.now().minusSeconds(600), limit);

        assertTrue(result.truncated());
        assertTrue(result.content().getBytes(StandardCharsets.UTF_8).length <= limit);
    }
}
