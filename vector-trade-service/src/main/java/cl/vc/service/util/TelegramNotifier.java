package cl.vc.service.util;

import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Envía alertas a un chat de Telegram (errores graves del sistema).
 *
 * <p>Diseño:
 * <ul>
 *   <li><b>Fail-safe:</b> nunca lanza al caller — alertar JAMÁS debe romper el trading.</li>
 *   <li><b>Async:</b> el envío HTTP va en un hilo daemon aparte (no bloquea el hilo de logging/negocio).</li>
 *   <li><b>Throttle/dedup:</b> por clave; un mismo error que se repite miles de veces manda UNA alerta
 *       por intervalo (evita inundar el chat).</li>
 * </ul>
 *
 * Configuración (application.properties):
 * {@code telegram.enabled}, {@code telegram.bot.token}, {@code telegram.chat.id},
 * {@code telegram.min.interval.seconds}.
 */
@Slf4j
public final class TelegramNotifier {

    private static volatile boolean enabled = false;
    private static volatile String token = "";
    private static volatile java.util.List<String> chatIds = java.util.List.of();
    private static volatile long minIntervalMs = 300_000L;

    private static final ConcurrentHashMap<String, Long> lastSent = new ConcurrentHashMap<>();

    private static final ExecutorService pool = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "telegram-notifier");
        t.setDaemon(true);
        return t;
    });

    private static final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private TelegramNotifier() {
    }

    public static void configure(boolean enabledFlag, String botToken, String chat, long minIntervalSeconds) {
        enabled = enabledFlag;
        token = botToken == null ? "" : botToken.trim();
        // chat puede traer varios destinos separados por coma
        java.util.List<String> ids = new java.util.ArrayList<>();
        if (chat != null) {
            for (String c : chat.split(",")) {
                String id = c.trim();
                if (!id.isEmpty()) {
                    ids.add(id);
                }
            }
        }
        chatIds = java.util.List.copyOf(ids);
        if (minIntervalSeconds > 0) {
            minIntervalMs = minIntervalSeconds * 1000L;
        }
        log.info("[Telegram] notifier configurado: enabled={} tokenSet={} chats={} minInterval={}s",
                enabled, !token.isEmpty(), chatIds.size(), minIntervalMs / 1000);
    }

    /** Listo para enviar (habilitado y con token + al menos un chatId). */
    public static boolean isReady() {
        return enabled && !token.isEmpty() && !chatIds.isEmpty();
    }

    /**
     * Envía una alerta (best-effort). {@code key} se usa para throttle/dedup: alertas con la misma
     * key dentro del intervalo se descartan. NUNCA lanza.
     */
    public static void alert(String key, String detail) {
        try {
            if (!isReady()) {
                return;
            }
            String k = key == null ? "" : key;
            if (!allow(k, System.currentTimeMillis())) {
                return; // throttled
            }
            String text = "🚨 *Vector Trade* — alerta\n" + safe(detail);
            pool.submit(() -> send(text));
        } catch (Throwable ignore) {
            // alertar nunca debe propagar errores al caller
        }
    }

    /** Decide si se puede enviar para esa key (throttle/dedup). Visible para test. */
    static boolean allow(String key, long now) {
        Long last = lastSent.get(key);
        if (last != null && (now - last) < minIntervalMs) {
            return false;
        }
        lastSent.put(key, now);
        return true;
    }

    private static void send(String text) {
        for (String chatId : chatIds) {
            try {
                String body = "chat_id=" + URLEncoder.encode(chatId, StandardCharsets.UTF_8)
                        + "&parse_mode=Markdown"
                        + "&disable_web_page_preview=true"
                        + "&text=" + URLEncoder.encode(text, StandardCharsets.UTF_8);

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("https://api.telegram.org/bot" + token + "/sendMessage"))
                        .timeout(Duration.ofSeconds(8))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();

                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 200) {
                    log.warn("[Telegram] sendMessage chat={} HTTP {} body={}", chatId, resp.statusCode(), resp.body());
                }
            } catch (Throwable t) {
                log.warn("[Telegram] no se pudo enviar la alerta a chat {}: {}", chatId, t.getMessage());
            }
        }
    }

    private static String safe(String s) {
        if (s == null) {
            return "";
        }
        return s.length() > 3500 ? s.substring(0, 3500) + "…" : s;
    }
}
