package cl.vc.service.admin;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/** Stores bounded, sanitized client diagnostics requested from the admin panel. */
@Slf4j
public final class ClientLogDiagnosticsStore {

    public static final int DEFAULT_MAX_BYTES = 512 * 1024;
    private static final long PENDING_TIMEOUT_MS = Duration.ofMinutes(3).toMillis();
    private static final Pattern AUTHORIZATION = Pattern.compile(
            "(?i)(authorization\\s*[:=]\\s*(?:bearer|basic)\\s+)[^\\s,;}]+"
    );
    private static final Pattern SECRET = Pattern.compile(
            "(?i)(password|passwd|token|secret|credential|credencial)(\\s*[:=]\\s*)[^\\s,;}]+"
    );
    private static final Pattern URL_PASSWORD = Pattern.compile(
            "(?i)([a-z][a-z0-9+.-]*://[^\\s/:@]+:)[^\\s/@]+(@)"
    );
    private static final Pattern JWT = Pattern.compile(
            "(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{10,}\\.[A-Za-z0-9_-]{10,}(?:\\.[A-Za-z0-9_-]+)?"
    );
    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();
    private static final ExecutorService IO_EXECUTOR = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "client-log-storage");
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY);
        return thread;
    });
    private static volatile boolean loaded;

    private ClientLogDiagnosticsStore() {}

    public static Entry createPending(String username, int minutes, int maxBytes) {
        ensureLoaded();
        cleanup();
        long now = System.currentTimeMillis();
        Entry entry = new Entry();
        entry.requestId = UUID.randomUUID().toString();
        entry.username = username;
        entry.minutes = minutes;
        entry.maxBytes = Math.min(Math.max(maxBytes, 16 * 1024), DEFAULT_MAX_BYTES);
        entry.requestedAt = now;
        entry.status = "PENDING";
        ENTRIES.put(entry.requestId, entry);
        persistMetadata(entry);
        return entry;
    }

    public static void acceptAsync(String authenticatedUsername, BlotterMessage.ClientLogResponse response) {
        IO_EXECUTOR.execute(() -> accept(authenticatedUsername, response));
    }

    static void accept(String authenticatedUsername, BlotterMessage.ClientLogResponse response) {
        ensureLoaded();
        if (response == null || response.getRequestId().isBlank()) return;

        Entry entry = ENTRIES.get(response.getRequestId());
        if (entry == null) {
            log.warn("[ClientLogs] Respuesta ignorada para request desconocido {}", response.getRequestId());
            return;
        }
        if (authenticatedUsername == null || !entry.username.equalsIgnoreCase(authenticatedUsername)) {
            log.warn("[ClientLogs] Respuesta rechazada: request={} esperado={} autenticado={}",
                    entry.requestId, entry.username, authenticatedUsername);
            return;
        }

        try {
            byte[] raw = response.getContent().toByteArray();
            int accepted = Math.min(raw.length, Math.min(entry.maxBytes, DEFAULT_MAX_BYTES));
            String text = new String(raw, 0, accepted, StandardCharsets.UTF_8);
            text = sanitize(text);

            entry.deviceId = safeText(response.getDeviceId(), 120);
            entry.appVersion = safeText(response.getAppVersion(), 80);
            entry.os = safeText(response.getOs(), 180);
            entry.hardware = safeText(response.getHardware(), 500);
            entry.generatedAt = response.getGeneratedAt() > 0
                    ? response.getGeneratedAt() : System.currentTimeMillis();
            entry.receivedAt = System.currentTimeMillis();
            entry.truncated = response.getTruncated() || raw.length > accepted;
            entry.error = safeText(response.getError(), 500);
            entry.sizeBytes = text.getBytes(StandardCharsets.UTF_8).length;
            entry.status = entry.error.isBlank()
                    ? "RECEIVED"
                    : ("Solicitud rechazada por el usuario".equals(entry.error) ? "REJECTED" : "ERROR");

            if (!text.isBlank()) {
                Files.createDirectories(baseDirectory());
                Files.writeString(contentPath(entry.requestId), text, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }
            persistMetadata(entry);
            log.info("[ClientLogs] Diagnostico recibido usuario={} equipo={} request={} bytes={} truncado={}",
                    entry.username, entry.deviceId, entry.requestId, entry.sizeBytes, entry.truncated);
        } catch (Exception e) {
            entry.status = "ERROR";
            entry.error = "No se pudo almacenar el diagnostico: " + safeText(e.getMessage(), 300);
            entry.receivedAt = System.currentTimeMillis();
            persistMetadata(entry);
            log.error("[ClientLogs] Error almacenando request {}: {}", entry.requestId, e.getMessage(), e);
        }
    }

    public static List<Entry> list() {
        ensureLoaded();
        expirePending();
        cleanup();
        List<Entry> result = new ArrayList<>(ENTRIES.values());
        result.sort(Comparator.comparingLong(Entry::getRequestedAt).reversed());
        return result;
    }

    public static List<BlotterMessage.ClientLogRequest> pendingRequestsFor(String username) {
        ensureLoaded();
        expirePending();
        if (username == null || username.isBlank()) return List.of();
        return ENTRIES.values().stream()
                .filter(entry -> "PENDING".equals(entry.status))
                .filter(entry -> entry.username.equalsIgnoreCase(username))
                .sorted(Comparator.comparingLong(Entry::getRequestedAt))
                .map(entry -> BlotterMessage.ClientLogRequest.newBuilder()
                        .setRequestId(entry.requestId)
                        .setUsername(entry.username)
                        .setMinutes(entry.minutes)
                        .setMaxBytes(entry.maxBytes)
                        .setRequestedAt(entry.requestedAt)
                        .build())
                .toList();
    }

    public static Entry find(String requestId) {
        ensureLoaded();
        expirePending();
        return ENTRIES.get(requestId);
    }

    public static String readContent(String requestId) throws IOException {
        Entry entry = find(requestId);
        if (entry == null || !Files.exists(contentPath(requestId))) return "";
        return Files.readString(contentPath(requestId), StandardCharsets.UTF_8);
    }

    public static boolean delete(String requestId) {
        ensureLoaded();
        if (requestId == null || requestId.isBlank()) return false;
        Entry removed = ENTRIES.remove(requestId);
        if (removed == null) return false;
        deleteFiles(requestId);
        log.info("[ClientLogs] Diagnostico eliminado usuario={} request={}",
                removed.username, removed.requestId);
        return true;
    }

    public static int deleteAll() {
        ensureLoaded();
        List<String> requestIds = new ArrayList<>(ENTRIES.keySet());
        requestIds.forEach(requestId -> {
            ENTRIES.remove(requestId);
            deleteFiles(requestId);
        });
        log.info("[ClientLogs] Se eliminaron {} diagnosticos", requestIds.size());
        return requestIds.size();
    }

    static String sanitize(String value) {
        if (value == null || value.isEmpty()) return "";
        String sanitized = AUTHORIZATION.matcher(value).replaceAll("$1[REDACTED]");
        sanitized = SECRET.matcher(sanitized).replaceAll("$1$2[REDACTED]");
        sanitized = URL_PASSWORD.matcher(sanitized).replaceAll("$1[REDACTED]$2");
        return JWT.matcher(sanitized).replaceAll("[REDACTED_JWT]");
    }

    private static void ensureLoaded() {
        if (loaded) return;
        synchronized (ClientLogDiagnosticsStore.class) {
            if (loaded) return;
            try {
                Path dir = baseDirectory();
                Files.createDirectories(dir);
                try (var files = Files.list(dir)) {
                    files.filter(path -> path.getFileName().toString().endsWith(".json"))
                            .forEach(path -> {
                                try {
                                    Entry entry = Entry.fromJson(new JSONObject(Files.readString(path)));
                                    if (!entry.requestId.isBlank()) ENTRIES.put(entry.requestId, entry);
                                } catch (Exception e) {
                                    log.warn("[ClientLogs] Metadata invalida {}: {}", path, e.getMessage());
                                }
                            });
                }
            } catch (Exception e) {
                log.error("[ClientLogs] No se pudo cargar el repositorio: {}", e.getMessage(), e);
            }
            loaded = true;
        }
    }

    private static void expirePending() {
        long now = System.currentTimeMillis();
        ENTRIES.values().stream()
                .filter(entry -> "PENDING".equals(entry.status) && now - entry.requestedAt > PENDING_TIMEOUT_MS)
                .forEach(entry -> {
                    entry.status = "TIMEOUT";
                    entry.error = "El front no respondio dentro de 3 minutos";
                    entry.receivedAt = now;
                    persistMetadata(entry);
                });
    }

    private static void cleanup() {
        int retentionDays = 15;
        try {
            retentionDays = Integer.parseInt(MainApp.getProperties()
                    .getProperty("client.logs.retention.days", "15"));
        } catch (Exception ignored) {}
        long cutoff = Instant.now().minus(Duration.ofDays(Math.max(1, retentionDays))).toEpochMilli();
        ENTRIES.values().removeIf(entry -> {
            if (entry.requestedAt >= cutoff) return false;
            try {
                Files.deleteIfExists(metadataPath(entry.requestId));
                Files.deleteIfExists(contentPath(entry.requestId));
            } catch (IOException e) {
                log.warn("[ClientLogs] No se pudo eliminar diagnostico {}: {}", entry.requestId, e.getMessage());
            }
            return true;
        });
    }

    private static void persistMetadata(Entry entry) {
        try {
            Files.createDirectories(baseDirectory());
            Files.writeString(metadataPath(entry.requestId), entry.toJson().toString(2),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            log.error("[ClientLogs] No se pudo persistir metadata {}: {}", entry.requestId, e.getMessage());
        }
    }

    private static void deleteFiles(String requestId) {
        try {
            Files.deleteIfExists(metadataPath(requestId));
            Files.deleteIfExists(contentPath(requestId));
        } catch (IOException e) {
            log.warn("[ClientLogs] No se pudieron eliminar todos los archivos de {}: {}",
                    requestId, e.getMessage());
        }
    }

    private static Path baseDirectory() {
        String logs = "./logs";
        try {
            logs = MainApp.getProperties().getProperty("path.logs", logs);
            String configured = MainApp.getProperties().getProperty("client.logs.dir", "");
            if (!configured.isBlank()) return Path.of(configured).toAbsolutePath().normalize();
        } catch (Exception ignored) {}
        return Path.of(logs, "client-diagnostics").toAbsolutePath().normalize();
    }

    private static Path metadataPath(String requestId) {
        return baseDirectory().resolve(safeRequestId(requestId) + ".json");
    }

    private static Path contentPath(String requestId) {
        return baseDirectory().resolve(safeRequestId(requestId) + ".log");
    }

    private static String safeRequestId(String requestId) {
        return requestId == null ? "invalid" : requestId.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private static String safeText(String value, int maxLength) {
        if (value == null) return "";
        String clean = value.replace('\r', ' ').replace('\n', ' ').trim();
        return clean.length() <= maxLength ? clean : clean.substring(0, maxLength);
    }

    public static final class Entry {
        private String requestId = "";
        private String username = "";
        private String deviceId = "";
        private String appVersion = "";
        private String os = "";
        private String hardware = "";
        private String status = "PENDING";
        private String error = "";
        private int minutes;
        private int maxBytes;
        private int sizeBytes;
        private long requestedAt;
        private long generatedAt;
        private long receivedAt;
        private boolean truncated;

        public String getRequestId() { return requestId; }
        public String getUsername() { return username; }
        public String getDeviceId() { return deviceId; }
        public String getAppVersion() { return appVersion; }
        public String getOs() { return os; }
        public String getHardware() { return hardware; }
        public String getStatus() { return status; }
        public String getError() { return error; }
        public int getMinutes() { return minutes; }
        public int getMaxBytes() { return maxBytes; }
        public int getSizeBytes() { return sizeBytes; }
        public long getRequestedAt() { return requestedAt; }
        public long getGeneratedAt() { return generatedAt; }
        public long getReceivedAt() { return receivedAt; }
        public boolean isTruncated() { return truncated; }

        public JSONObject toJson() {
            return new JSONObject()
                    .put("requestId", requestId)
                    .put("username", username)
                    .put("deviceId", deviceId)
                    .put("appVersion", appVersion)
                    .put("os", os)
                    .put("hardware", hardware)
                    .put("status", status)
                    .put("error", error)
                    .put("minutes", minutes)
                    .put("maxBytes", maxBytes)
                    .put("sizeBytes", sizeBytes)
                    .put("requestedAt", requestedAt)
                    .put("generatedAt", generatedAt)
                    .put("receivedAt", receivedAt)
                    .put("truncated", truncated);
        }

        static Entry fromJson(JSONObject json) {
            Entry entry = new Entry();
            entry.requestId = json.optString("requestId");
            entry.username = json.optString("username");
            entry.deviceId = json.optString("deviceId");
            entry.appVersion = json.optString("appVersion");
            entry.os = json.optString("os");
            entry.hardware = json.optString("hardware");
            entry.status = json.optString("status", "ERROR");
            entry.error = json.optString("error");
            entry.minutes = json.optInt("minutes");
            entry.maxBytes = json.optInt("maxBytes", DEFAULT_MAX_BYTES);
            entry.sizeBytes = json.optInt("sizeBytes");
            entry.requestedAt = json.optLong("requestedAt");
            entry.generatedAt = json.optLong("generatedAt");
            entry.receivedAt = json.optLong("receivedAt");
            entry.truncated = json.optBoolean("truncated");
            return entry;
        }
    }
}
