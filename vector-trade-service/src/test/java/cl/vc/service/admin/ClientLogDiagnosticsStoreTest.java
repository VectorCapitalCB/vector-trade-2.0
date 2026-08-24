package cl.vc.service.admin;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientLogDiagnosticsStoreTest {

    @Test
    void sanitizesSecretsAgainOnTheCore() {
        String sanitized = ClientLogDiagnosticsStore.sanitize(
                "Authorization=Basic Zm9vOmJhcg== token=abc123 password: secret123 "
                        + "redis://admin:redispass@localhost:6379 "
                        + "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature");

        assertFalse(sanitized.contains("Zm9vOmJhcg"));
        assertFalse(sanitized.contains("abc123"));
        assertFalse(sanitized.contains("secret123"));
        assertFalse(sanitized.contains("redispass"));
        assertFalse(sanitized.contains("eyJhbGci"));
        assertTrue(sanitized.contains("[REDACTED]"));
    }

    @Test
    void rebuildsPendingRequestForReconnectedUser() {
        ClientLogDiagnosticsStore.Entry entry =
                ClientLogDiagnosticsStore.createPending("reconnect-test-user", 35, 128 * 1024);

        var pending = ClientLogDiagnosticsStore.pendingRequestsFor("reconnect-test-user");

        assertTrue(pending.stream().anyMatch(request -> request.getRequestId().equals(entry.getRequestId())));
        var request = pending.stream()
                .filter(item -> item.getRequestId().equals(entry.getRequestId()))
                .findFirst()
                .orElseThrow();
        assertEquals(35, request.getMinutes());
        assertEquals(128 * 1024, request.getMaxBytes());
    }

    @Test
    void recordsUserRejectionAsASeparateStatus() {
        ClientLogDiagnosticsStore.Entry entry =
                ClientLogDiagnosticsStore.createPending("reject-test-user", 15, 64 * 1024);
        BlotterMessage.ClientLogResponse response = BlotterMessage.ClientLogResponse.newBuilder()
                .setRequestId(entry.getRequestId())
                .setUsername("reject-test-user")
                .setError("Solicitud rechazada por el usuario")
                .build();

        ClientLogDiagnosticsStore.accept("reject-test-user", response);

        assertEquals("REJECTED", ClientLogDiagnosticsStore.find(entry.getRequestId()).getStatus());
    }

    @Test
    void deletesDiagnosticMetadataAndRemovesItFromTheIndex() {
        ClientLogDiagnosticsStore.Entry entry =
                ClientLogDiagnosticsStore.createPending("delete-test-user", 15, 64 * 1024);

        assertTrue(ClientLogDiagnosticsStore.delete(entry.getRequestId()));
        assertNull(ClientLogDiagnosticsStore.find(entry.getRequestId()));
        assertFalse(ClientLogDiagnosticsStore.delete(entry.getRequestId()));
    }
}
