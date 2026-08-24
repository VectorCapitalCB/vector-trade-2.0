package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Collects a bounded and sanitized tail of the local front logs off the UI/market-data threads. */
@Slf4j
public final class ClientLogCollector {

    private static final int ABSOLUTE_MAX_BYTES = 512 * 1024;
    private static final int SCAN_MULTIPLIER = 4;
    private static final Pattern LOG_FILE = Pattern.compile(
            "vector-trade(?:-2)?(?:\\.\\d{4}-\\d{2}-\\d{2})?\\.log"
    );
    private static final Pattern LOG_TIMESTAMP = Pattern.compile(
            "^\\[?(\\d{4}-\\d{2}-\\d{2})[ T](\\d{2}:\\d{2}:\\d{2})[,.](\\d{3})"
    );
    private static final DateTimeFormatter LOG_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
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
    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "client-log-diagnostics");
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY);
        return thread;
    });

    private ClientLogCollector() {}

    public static CompletableFuture<BlotterMessage.ClientLogResponse> collectAsync(
            BlotterMessage.ClientLogRequest request, String authenticatedUsername) {
        return CompletableFuture.supplyAsync(() -> collect(request, authenticatedUsername), EXECUTOR);
    }

    public static BlotterMessage.ClientLogResponse rejectedResponse(
            BlotterMessage.ClientLogRequest request, String authenticatedUsername) {
        int minutes = Math.min(Math.max(request.getMinutes(), 5), 240);
        return baseResponse(request, authenticatedUsername, minutes)
                .setError("Solicitud rechazada por el usuario")
                .build();
    }

    static BlotterMessage.ClientLogResponse collect(
            BlotterMessage.ClientLogRequest request, String authenticatedUsername) {
        int minutes = Math.min(Math.max(request.getMinutes(), 5), 240);
        int maxBytes = Math.min(Math.max(request.getMaxBytes(), 16 * 1024), ABSOLUTE_MAX_BYTES);
        BlotterMessage.ClientLogResponse.Builder response = baseResponse(request, authenticatedUsername, minutes);
        try {
            Instant now = Instant.now();
            CollectionResult result = collectWindow(logDirectories(), effectiveCutoff(now, minutes), maxBytes);
            response.setContent(ByteString.copyFrom(result.content(), StandardCharsets.UTF_8));
            response.setTruncated(result.truncated());
            if (result.content().isBlank()) {
                response.setError("No se encontraron lineas de log dentro del rango solicitado");
            }
        } catch (Exception e) {
            log.warn("[ClientLogs] No se pudo preparar el diagnostico: {}", e.getMessage());
            response.setError("No se pudo leer el log local: " + safeText(e.getMessage(), 240));
        }
        return response.build();
    }

    static Instant effectiveCutoff(Instant now, int minutes) {
        ZoneId zone = Repository.getZoneID() == null ? ZoneId.systemDefault() : Repository.getZoneID();
        Instant requestedCutoff = now.minusSeconds(minutes * 60L);
        Instant startOfToday = LocalDate.ofInstant(now, zone).atStartOfDay(zone).toInstant();
        return requestedCutoff.isAfter(startOfToday) ? requestedCutoff : startOfToday;
    }

    static CollectionResult collectWindow(List<Path> directories, Instant cutoff, int maxBytes) throws Exception {
        List<Path> files = new ArrayList<>();
        for (Path directory : directories) {
            if (!Files.isDirectory(directory)) continue;
            try (var paths = Files.list(directory)) {
                paths.filter(Files::isRegularFile)
                        .filter(path -> LOG_FILE.matcher(path.getFileName().toString()).matches())
                        .filter(path -> lastModified(path).isAfter(cutoff.minusSeconds(24 * 60 * 60L)))
                        .forEach(files::add);
            }
        }
        boolean hasPreferredLog = files.stream()
                .anyMatch(path -> path.getFileName().toString().startsWith("vector-trade-2"));
        if (hasPreferredLog) {
            files.removeIf(path -> !path.getFileName().toString().startsWith("vector-trade-2"));
        }
        files.sort(Comparator.comparing(ClientLogCollector::lastModified));

        StringBuilder included = new StringBuilder();
        boolean truncated = false;
        int scanLimit = Math.min(maxBytes * SCAN_MULTIPLIER, 2 * 1024 * 1024);
        for (Path file : files) {
            Tail tail = readTail(file, scanLimit);
            truncated |= tail.truncated();
            String filtered = filterByTime(tail.content(), cutoff);
            if (!filtered.isBlank()) {
                if (!included.isEmpty()) included.append('\n');
                included.append("===== ").append(file.getFileName()).append(" =====\n");
                included.append(filtered);
            }
        }

        String sanitized = sanitize(included.toString());
        byte[] bytes = sanitized.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxBytes) {
            sanitized = utf8Tail(bytes, maxBytes);
            truncated = true;
        }
        return new CollectionResult(sanitized, truncated);
    }

    static String sanitize(String value) {
        if (value == null || value.isEmpty()) return "";
        String sanitized = AUTHORIZATION.matcher(value).replaceAll("$1[REDACTED]");
        sanitized = SECRET.matcher(sanitized).replaceAll("$1$2[REDACTED]");
        sanitized = URL_PASSWORD.matcher(sanitized).replaceAll("$1[REDACTED]$2");
        return JWT.matcher(sanitized).replaceAll("[REDACTED_JWT]");
    }

    private static String filterByTime(String content, Instant cutoff) {
        StringBuilder result = new StringBuilder();
        boolean include = false;
        boolean timestampFound = false;
        ZoneId zone = Repository.getZoneID() == null ? ZoneId.systemDefault() : Repository.getZoneID();
        for (String line : content.split("\\R", -1)) {
            Matcher matcher = LOG_TIMESTAMP.matcher(line);
            if (matcher.find()) {
                timestampFound = true;
                try {
                    LocalDateTime local = LocalDateTime.parse(
                            matcher.group(1) + " " + matcher.group(2) + "." + matcher.group(3),
                            LOG_TIME_FORMAT);
                    include = !local.atZone(zone).toInstant().isBefore(cutoff);
                } catch (Exception ignored) {
                    include = true;
                }
            }
            if (include) result.append(line).append('\n');
        }
        return timestampFound ? result.toString() : content;
    }

    private static Tail readTail(Path file, int maxScanBytes) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            long length = raf.length();
            long start = Math.max(0L, length - maxScanBytes);
            raf.seek(start);
            if (start > 0) raf.readLine();
            int size = (int) Math.min(Integer.MAX_VALUE, length - raf.getFilePointer());
            byte[] data = new byte[size];
            raf.readFully(data);
            return new Tail(new String(data, StandardCharsets.UTF_8), start > 0);
        }
    }

    private static String utf8Tail(byte[] bytes, int maxBytes) {
        int start = Math.max(0, bytes.length - maxBytes);
        while (start < bytes.length && (bytes[start] & 0xC0) == 0x80) start++;
        String value = new String(bytes, start, bytes.length - start, StandardCharsets.UTF_8);
        int newline = value.indexOf('\n');
        return newline >= 0 && newline + 1 < value.length() ? value.substring(newline + 1) : value;
    }

    private static BlotterMessage.ClientLogResponse.Builder baseResponse(
            BlotterMessage.ClientLogRequest request, String username, int minutes) {
        return BlotterMessage.ClientLogResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setUsername(username == null ? "" : username)
                .setDeviceId(deviceId())
                .setAppVersion(appVersion())
                .setOs(System.getProperty("os.name", "") + " "
                        + System.getProperty("os.version", "") + " / "
                        + System.getProperty("os.arch", ""))
                .setHardware(hardwareSummary())
                .setGeneratedAt(System.currentTimeMillis())
                .setMinutes(minutes);
    }

    static String hardwareSummary() {
        int processors = Runtime.getRuntime().availableProcessors();
        long physicalMemory = physicalMemoryBytes();
        long jvmLimit = Runtime.getRuntime().maxMemory();
        return "CPU: " + cpuModel()
                + " (" + processors + " nucleos logicos)"
                + " | RAM: " + formatMemory(physicalMemory)
                + " | JVM: " + safeText(System.getProperty("java.vm.name", "Java"), 80)
                + " " + safeText(System.getProperty("java.version", ""), 40)
                + " (max " + formatMemory(jvmLimit) + ")";
    }

    private static long physicalMemoryBytes() {
        try {
            var bean = ManagementFactory.getOperatingSystemMXBean();
            if (bean instanceof com.sun.management.OperatingSystemMXBean osBean) {
                return osBean.getTotalMemorySize();
            }
        } catch (Exception e) {
            log.debug("[ClientLogs] RAM fisica no disponible: {}", e.getMessage());
        }
        return -1L;
    }

    private static String cpuModel() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("mac")) {
            String value = commandOutput("sysctl", "-n", "machdep.cpu.brand_string");
            if (!value.isBlank()) return safeText(value, 120);
        } else if (os.contains("win")) {
            String value = System.getenv("PROCESSOR_IDENTIFIER");
            if (value != null && !value.isBlank()) return safeText(value, 120);
        } else if (os.contains("linux")) {
            try {
                for (String line : Files.readAllLines(Path.of("/proc/cpuinfo"))) {
                    if (line.toLowerCase().startsWith("model name")) {
                        int separator = line.indexOf(':');
                        if (separator >= 0) return safeText(line.substring(separator + 1), 120);
                    }
                }
            } catch (Exception ignored) {
                log.debug("[ClientLogs] Modelo de CPU Linux no disponible");
            }
        }
        return safeText(System.getProperty("os.arch", "desconocido"), 120);
    }

    private static String commandOutput(String... command) {
        Process process = null;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
            if (!process.waitFor(2, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return "";
            }
            return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (Exception e) {
            return "";
        } finally {
            if (process != null) process.destroy();
        }
    }

    private static String formatMemory(long bytes) {
        if (bytes <= 0L) return "No disponible";
        double gib = bytes / (1024d * 1024d * 1024d);
        return gib >= 10d ? String.format(java.util.Locale.ROOT, "%.0f GB", gib)
                : String.format(java.util.Locale.ROOT, "%.1f GB", gib);
    }

    private static List<Path> logDirectories() {
        Path home = Path.of(System.getProperty("user.home", "."), "vc");
        return List.of(home.resolve("VectorTrade/logs"), home.resolve("VectorTradeQA/logs"));
    }

    private static String deviceId() {
        Path file = Path.of(System.getProperty("user.home", "."), "vc", ".vector-trade-device-id");
        try {
            if (Files.exists(file)) return safeText(Files.readString(file), 120);
            Files.createDirectories(file.getParent());
            String id = UUID.randomUUID().toString();
            Files.writeString(file, id, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            return id;
        } catch (Exception e) {
            try {
                return safeText(InetAddress.getLocalHost().getHostName(), 120);
            } catch (Exception ignored) {
                return "unknown-device";
            }
        }
    }

    private static String appVersion() {
        String version = Repository.getVersion();
        if (version != null && !version.isBlank()) return safeText(version, 80);
        Package pkg = ClientLogCollector.class.getPackage();
        if (pkg != null && pkg.getImplementationVersion() != null) return pkg.getImplementationVersion();
        return "3.1.7";
    }

    private static Instant lastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toInstant();
        } catch (Exception e) {
            return Instant.EPOCH;
        }
    }

    private static String safeText(String value, int maxLength) {
        if (value == null) return "";
        String clean = value.replace('\r', ' ').replace('\n', ' ').trim();
        return clean.length() <= maxLength ? clean : clean.substring(0, maxLength);
    }

    record CollectionResult(String content, boolean truncated) {}
    private record Tail(String content, boolean truncated) {}
}
