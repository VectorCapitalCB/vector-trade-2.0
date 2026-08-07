package cl.vc.inyectorcandle.config;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public record AppConfig(
        String fixConfigFile,
        String mongoUri,
        String mongoDatabase,
        String rawData,
        String username,
        String password,
        List<Duration> candleTimeframes,
        Duration rankingInterval,
        int marketDataThrottleMs,
        int securitySubscriptionPauseMs,
        boolean processSnapshots,
        boolean processSnapshotTrades,
        String securityListRequestType,
        String securityListScope,
        String securitySubscriptionDestinationFilter,
        boolean replayEnabled,
        String replayInputPath,
        String replayLogZoneId,
        long replaySleepMs,
        long replayMaxLines,
        boolean replayPreserveTiming,
        double replayTimingSpeed,
        long replayTimingMaxSleepMs,
        boolean replayPurgeDayBeforeInject,
        boolean statsReplayEnabled,
        String statsReplayInputPath,
        String statsMarket,
        String statsCurrency,
        boolean itchReplayEnabled,
        String itchReplayInputPath,
        String statsZone,
        String statsSessionOpen,
        String statsSessionClose,
        Duration statsLiveInterval,
        int mongoWriteQueueSize,
        int mongoWriteBatchSize,
        long mongoWriteFlushMs,
        long statsThrottleMs,
        long openCandleFlushMs,
        String mkdSource,
        String itchHost,
        int itchPort,
        String itchExchange,
        String itchCurrency,
        boolean itchSubscribe,
        String itchLogPath,
        boolean alpacaEnabled,
        String alpacaKeyId,
        String alpacaSecretKey,
        String alpacaFeed,
        List<String> alpacaSymbols,
        String alpacaDataUrl,
        String alpacaStreamUrl,
        int alpacaBackfillDays,
        long alpacaPollSeconds
) {

    public boolean itchSource() {
        return "itch".equalsIgnoreCase(mkdSource);
    }

    public static AppConfig fromProperties(Properties properties) {
        String mkdSource = properties.getProperty("mkd.source", "fix").trim().toLowerCase();
        return new AppConfig(
                "itch".equals(mkdSource)
                        ? properties.getProperty("fix.config.file", "")
                        : required(properties, "fix.config.file"),
                required(properties, "mongo.uri"),
                properties.getProperty("mongo.database", "inyectorcandle"),
                blankToNull(properties.getProperty("fix.logon.rawData")),
                blankToNull(properties.getProperty("fix.logon.username")),
                blankToNull(properties.getProperty("fix.logon.password")),
                parseTimeframes(properties.getProperty("candles.timeframes", "PT1M,PT5M,PT15M,PT1H,P1D")),
                Duration.parse(properties.getProperty("rankings.interval", "PT30S")),
                Integer.parseInt(properties.getProperty("fix.marketdata.throttle.ms", "0")),
                Integer.parseInt(properties.getProperty("fix.subscription.pause.ms", "5")),
                Boolean.parseBoolean(properties.getProperty("fix.process.snapshots", "true")),
                Boolean.parseBoolean(properties.getProperty("fix.process.snapshot.trades", "false")),
                properties.getProperty("fix.securitylist.request.type", "4"),
                properties.getProperty("fix.securitylist.scope", "ALL"),
                blankToNull(properties.getProperty("fix.subscription.destination.filter")),
                Boolean.parseBoolean(properties.getProperty("replay.enabled", "false")),
                stripWrappingQuotes(properties.getProperty("replay.input.path", "").trim()),
                properties.getProperty("replay.log.zone", "America/Santiago"),
                Long.parseLong(properties.getProperty("replay.sleep.ms", "0")),
                Long.parseLong(properties.getProperty("replay.max.lines", "0")),
                Boolean.parseBoolean(properties.getProperty("replay.preserve.timing", "true")),
                Double.parseDouble(properties.getProperty("replay.timing.speed", "1.0")),
                Long.parseLong(properties.getProperty("replay.timing.max.sleep.ms", "2000")),
                Boolean.parseBoolean(properties.getProperty("replay.purge.day.before.inject", "true")),
                Boolean.parseBoolean(properties.getProperty("stats.replay.enabled", "false")),
                stripWrappingQuotes(properties.getProperty("stats.replay.input.path", "").trim()),
                properties.getProperty("stats.market", "BCS").trim().toUpperCase(),
                properties.getProperty("stats.currency", "CLP").trim().toUpperCase(),
                Boolean.parseBoolean(properties.getProperty("itch.replay.enabled", "false")),
                stripWrappingQuotes(properties.getProperty("itch.replay.input.path", "").trim()),
                properties.getProperty("stats.zone", "America/Santiago").trim(),
                properties.getProperty("stats.session.open", "09:30").trim(),
                properties.getProperty("stats.session.close", "16:00").trim(),
                Duration.parse(properties.getProperty("stats.live.interval", "PT60S")),
                Integer.parseInt(properties.getProperty("mongo.write.queue.size", "200000")),
                Integer.parseInt(properties.getProperty("mongo.write.batch.size", "500")),
                Long.parseLong(properties.getProperty("mongo.write.flush.ms", "200")),
                Long.parseLong(properties.getProperty("stats.throttle.ms", "500")),
                Long.parseLong(properties.getProperty("candles.open.flush.ms", "1000")),
                mkdSource,
                properties.getProperty("mkd.itch.host", "172.16.0.7"),
                Integer.parseInt(properties.getProperty("mkd.itch.port", "9095")),
                properties.getProperty("mkd.itch.exchange", "BCS"),
                properties.getProperty("mkd.itch.currency", "CLP"),
                Boolean.parseBoolean(properties.getProperty("mkd.itch.subscribe", "false")),
                properties.getProperty("mkd.itch.log.path", "./logs/itch-mkd"),
                Boolean.parseBoolean(properties.getProperty("alpaca.enabled", "false")),
                blankToNull(properties.getProperty("alpaca.key.id")),
                blankToNull(properties.getProperty("alpaca.secret.key")),
                properties.getProperty("alpaca.feed", "iex"),
                parseSymbols(properties.getProperty("alpaca.symbols", "")),
                properties.getProperty("alpaca.data.url", "https://data.alpaca.markets"),
                properties.getProperty("alpaca.stream.url", "wss://stream.data.alpaca.markets/v2"),
                Integer.parseInt(properties.getProperty("alpaca.backfill.days", "5")),
                Long.parseLong(properties.getProperty("alpaca.poll.seconds", "60"))
        );
    }

    private static List<String> parseSymbols(String raw) {
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .map(String::toUpperCase)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return value;
    }

    private static List<Duration> parseTimeframes(String raw) {
        List<String> tokens = Arrays.stream(raw.split(","))
                .map(String::trim)
                .map(AppConfig::stripWrappingQuotes)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        List<Duration> out = new ArrayList<>(tokens.size());
        for (String token : tokens) {
            try {
                out.add(Duration.parse(token));
            } catch (DateTimeParseException ex) {
                throw new IllegalArgumentException(
                        "Invalid candles.timeframes token: '" + token + "' in value '" + raw
                                + "'. Use ISO-8601 durations like PT1M,PT5M,PT15M,PT1H,P1D",
                        ex
                );
            }
        }
        return out;
    }

    private static String blankToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value;
    }

    private static String stripWrappingQuotes(String value) {
        if (value == null || value.length() < 2) {
            return value;
        }
        if ((value.startsWith("\"") && value.endsWith("\""))
                || (value.startsWith("'") && value.endsWith("'"))) {
            return value.substring(1, value.length() - 1).trim();
        }
        return value;
    }
}
