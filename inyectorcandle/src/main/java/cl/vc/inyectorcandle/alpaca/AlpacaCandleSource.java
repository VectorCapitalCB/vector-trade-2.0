package cl.vc.inyectorcandle.alpaca;

import cl.vc.inyectorcandle.model.Candle;
import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.mongo.MongoMarketRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Origen de velas de Alpaca. A diferencia de BCS/ITCH, Alpaca publica las barras ya agregadas
 * (endpoint /v2/stocks/bars y canal websocket "bars"), asi que aca no se agrega nada: se traduce
 * la barra al modelo {@link Candle} y se encola a Mongo.
 */
public final class AlpacaCandleSource implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AlpacaCandleSource.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DESTINATION = "ALPACA_MKD";
    private static final String SETTLEMENT = "REGULAR";
    private static final String SECURITY_TYPE = "CS";
    private static final int PAGE_LIMIT = 10_000;

    private final MongoMarketRepository repository;
    private final AlpacaConfig config;
    private final List<Duration> timeframes;
    private final String symbolsCsv;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "alpaca-bars");
        t.setDaemon(true);
        return t;
    });

    private volatile WebSocket webSocket;

    public AlpacaCandleSource(MongoMarketRepository repository, AlpacaConfig config, List<Duration> timeframes) {
        this.repository = repository;
        this.config = config;
        this.timeframes = timeframes.stream().filter(AlpacaCandleSource::isSupported).toList();
        this.symbolsCsv = String.join(",", config.symbols());

        List<Duration> unsupported = timeframes.stream().filter(tf -> !isSupported(tf)).toList();
        if (!unsupported.isEmpty()) {
            LOG.warn("Timeframes no soportados por Alpaca, se omiten: {}", unsupported);
        }
    }

    public void start() {
        if (config.symbols().isEmpty()) {
            LOG.warn("alpaca.symbols vacio, no se inicia el origen Alpaca");
            return;
        }

        Instant backfillFrom = Instant.now().minus(Duration.ofDays(Math.max(1, config.backfillDays())));
        for (Duration timeframe : timeframes) {
            int loaded = loadBars(timeframe, backfillFrom, Instant.now());
            LOG.info("Backfill Alpaca timeframe={} velas={}", toAlpacaTimeframe(timeframe), loaded);

            // Refresco periodico: nunca mas rapido que el propio timeframe ni que el piso configurado.
            long periodSeconds = Math.max(config.pollSeconds(), timeframe.getSeconds());
            scheduler.scheduleAtFixedRate(() -> refresh(timeframe), periodSeconds, periodSeconds, TimeUnit.SECONDS);
        }

        connectStream();
    }

    private void refresh(Duration timeframe) {
        try {
            // Se repiden las 2 ultimas barras: la ultima puede seguir abierta y Alpaca la corrige.
            Instant from = Instant.now().minus(timeframe.multipliedBy(2));
            loadBars(timeframe, from, Instant.now());
        } catch (Exception e) {
            LOG.error("Fallo refrescando barras Alpaca timeframe={}", toAlpacaTimeframe(timeframe), e);
        }
    }

    private int loadBars(Duration timeframe, Instant start, Instant end) {
        int total = 0;
        String pageToken = null;
        try {
            do {
                StringBuilder url = new StringBuilder(config.dataUrl())
                        .append("/v2/stocks/bars?symbols=").append(encode(symbolsCsv))
                        .append("&timeframe=").append(encode(toAlpacaTimeframe(timeframe)))
                        .append("&start=").append(encode(start.toString()))
                        .append("&end=").append(encode(end.toString()))
                        .append("&limit=").append(PAGE_LIMIT)
                        .append("&adjustment=raw")
                        .append("&sort=asc");
                if (config.feed() != null && !config.feed().isBlank()) {
                    url.append("&feed=").append(encode(config.feed()));
                }
                if (pageToken != null) {
                    url.append("&page_token=").append(encode(pageToken));
                }

                HttpResponse<String> response = http.send(
                        HttpRequest.newBuilder(URI.create(url.toString()))
                                .header("APCA-API-KEY-ID", config.keyId())
                                .header("APCA-API-SECRET-KEY", config.secretKey())
                                .header("Accept", "application/json")
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    LOG.error("Alpaca bars HTTP {} timeframe={} body={}",
                            response.statusCode(), toAlpacaTimeframe(timeframe), response.body());
                    return total;
                }

                JsonNode root = MAPPER.readTree(response.body());
                JsonNode bars = root.path("bars");
                for (Iterator<Map.Entry<String, JsonNode>> it = bars.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    for (JsonNode bar : entry.getValue()) {
                        repository.upsertCandle(toCandle(entry.getKey(), timeframe, bar));
                        total++;
                    }
                }

                JsonNode next = root.path("next_page_token");
                pageToken = next.isNull() || next.isMissingNode() ? null : next.asText(null);
            } while (pageToken != null && !pageToken.isBlank());
        } catch (Exception e) {
            LOG.error("Fallo consultando barras Alpaca timeframe={}", toAlpacaTimeframe(timeframe), e);
        }
        return total;
    }

    private void connectStream() {
        try {
            String url = config.streamUrl() + "/" + config.feed();
            webSocket = http.newWebSocketBuilder()
                    .buildAsync(URI.create(url), new StreamListener())
                    .join();
            LOG.info("Websocket Alpaca conectado url={}", url);
        } catch (Exception e) {
            LOG.error("No se pudo conectar el websocket Alpaca, se seguira solo con REST", e);
        }
    }

    private void onStreamMessage(String payload) {
        try {
            JsonNode messages = MAPPER.readTree(payload);
            for (JsonNode message : messages) {
                String type = message.path("T").asText("");
                switch (type) {
                    case "success" -> {
                        if ("connected".equals(message.path("msg").asText())) {
                            send("{\"action\":\"auth\",\"key\":\"" + config.keyId()
                                    + "\",\"secret\":\"" + config.secretKey() + "\"}");
                        } else if ("authenticated".equals(message.path("msg").asText())) {
                            String symbols = config.symbols().stream()
                                    .map(s -> "\"" + s + "\"")
                                    .reduce((a, b) -> a + "," + b)
                                    .orElse("");
                            send("{\"action\":\"subscribe\",\"bars\":[" + symbols + "],\"updatedBars\":[" + symbols + "]}");
                            LOG.info("Suscrito a barras Alpaca symbols={}", config.symbols());
                        }
                    }
                    // "b" barra de 1 minuto, "u" barra corregida por trades tardios.
                    case "b", "u" -> repository.upsertCandle(
                            toCandle(message.path("S").asText(), Duration.ofMinutes(1), message));
                    case "error" -> LOG.error("Alpaca stream error code={} msg={}",
                            message.path("code").asInt(), message.path("msg").asText());
                    default -> { }
                }
            }
        } catch (Exception e) {
            LOG.error("Fallo procesando mensaje del stream Alpaca", e);
        }
    }

    private void send(String text) {
        WebSocket ws = webSocket;
        if (ws != null) {
            ws.sendText(text, true);
        }
    }

    private static Candle toCandle(String symbol, Duration timeframe, JsonNode bar) {
        Instant bucketStart = Instant.parse(bar.path("t").asText());
        BigDecimal volume = BigDecimal.valueOf(bar.path("v").asDouble());
        BigDecimal vwap = BigDecimal.valueOf(bar.path("vw").asDouble());

        return new Candle(
                InstrumentKey.fromValues(symbol, SETTLEMENT, DESTINATION, "USD", SECURITY_TYPE),
                timeframe,
                bucketStart,
                bucketStart.plus(timeframe),
                BigDecimal.valueOf(bar.path("o").asDouble()),
                BigDecimal.valueOf(bar.path("h").asDouble()),
                BigDecimal.valueOf(bar.path("l").asDouble()),
                BigDecimal.valueOf(bar.path("c").asDouble()),
                volume,
                vwap.multiply(volume),
                bar.path("n").asLong());
    }

    /** Alpaca acepta [1-59]Min, [1-23]Hour, 1Day, 1Week y [1,2,3,4,6,12]Month. */
    static String toAlpacaTimeframe(Duration timeframe) {
        long seconds = timeframe.getSeconds();
        if (seconds % 86_400 == 0) {
            long days = seconds / 86_400;
            if (days == 1) {
                return "1Day";
            }
            if (days == 7) {
                return "1Week";
            }
            throw new IllegalArgumentException("Timeframe no soportado por Alpaca: " + timeframe);
        }
        if (seconds % 3_600 == 0) {
            return (seconds / 3_600) + "Hour";
        }
        if (seconds % 60 == 0) {
            return (seconds / 60) + "Min";
        }
        throw new IllegalArgumentException("Timeframe no soportado por Alpaca: " + timeframe);
    }

    private static boolean isSupported(Duration timeframe) {
        try {
            toAlpacaTimeframe(timeframe);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        WebSocket ws = webSocket;
        if (ws != null) {
            ws.abort();
        }
    }

    private final class StreamListener implements WebSocket.Listener {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String payload = buffer.toString();
                buffer.setLength(0);
                onStreamMessage(payload);
            }
            ws.request(1);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            LOG.error("Websocket Alpaca con error, reconectando en 5s", error);
            scheduler.schedule(AlpacaCandleSource.this::connectStream, 5, TimeUnit.SECONDS);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            LOG.warn("Websocket Alpaca cerrado status={} reason={}, reconectando en 5s", statusCode, reason);
            scheduler.schedule(AlpacaCandleSource.this::connectStream, 5, TimeUnit.SECONDS);
            return null;
        }
    }

    public record AlpacaConfig(
            String keyId,
            String secretKey,
            String feed,
            List<String> symbols,
            String dataUrl,
            String streamUrl,
            int backfillDays,
            long pollSeconds
    ) {
        public AlpacaConfig {
            symbols = symbols == null ? List.of() : new ArrayList<>(symbols);
        }
    }
}
