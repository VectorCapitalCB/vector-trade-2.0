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
        String securityListRequestType,
        String securityListScope,
        boolean replayEnabled,
        String replayInputPath,
        String replayLogZoneId,
        long replaySleepMs,
        long replayMaxLines,
        boolean replayPreserveTiming,
        double replayTimingSpeed,
        long replayTimingMaxSleepMs,
        boolean replayPurgeDayBeforeInject
) {

    public static AppConfig fromProperties(Properties properties) {
        return new AppConfig(
                required(properties, "fix.config.file"),
                required(properties, "mongo.uri"),
                properties.getProperty("mongo.database", "inyectorcandle"),
                blankToNull(properties.getProperty("fix.logon.rawData")),
                blankToNull(properties.getProperty("fix.logon.username")),
                blankToNull(properties.getProperty("fix.logon.password")),
                parseTimeframes(properties.getProperty("candles.timeframes", "PT1M,PT5M,PT15M,PT1H,P1D")),
                Duration.parse(properties.getProperty("rankings.interval", "PT30S")),
                Integer.parseInt(properties.getProperty("fix.marketdata.throttle.ms", "0")),
                Integer.parseInt(properties.getProperty("fix.subscription.pause.ms", "5")),
                properties.getProperty("fix.securitylist.request.type", "4"),
                properties.getProperty("fix.securitylist.scope", "ALL"),
                Boolean.parseBoolean(properties.getProperty("replay.enabled", "false")),
                stripWrappingQuotes(properties.getProperty("replay.input.path", "").trim()),
                properties.getProperty("replay.log.zone", "America/Santiago"),
                Long.parseLong(properties.getProperty("replay.sleep.ms", "0")),
                Long.parseLong(properties.getProperty("replay.max.lines", "0")),
                Boolean.parseBoolean(properties.getProperty("replay.preserve.timing", "true")),
                Double.parseDouble(properties.getProperty("replay.timing.speed", "1.0")),
                Long.parseLong(properties.getProperty("replay.timing.max.sleep.ms", "2000")),
                Boolean.parseBoolean(properties.getProperty("replay.purge.day.before.inject", "true"))
        );
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
