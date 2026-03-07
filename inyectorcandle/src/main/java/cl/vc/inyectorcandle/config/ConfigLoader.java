package cl.vc.inyectorcandle.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ConfigLoader {
    private ConfigLoader() {
    }

    public static AppConfig load() {
        Properties properties = new Properties();

        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (in == null) {
                throw new IllegalStateException("application.properties not found in classpath");
            }
            properties.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Cannot load application.properties", e);
        }

        overrideFromEnv(properties, "FIX_CONFIG_FILE", "fix.config.file");
        overrideFromEnv(properties, "MONGO_URI", "mongo.uri");
        overrideFromEnv(properties, "MONGO_DATABASE", "mongo.database");
        overrideFromEnv(properties, "FIX_LOGON_RAWDATA", "fix.logon.rawData");
        overrideFromEnv(properties, "FIX_LOGON_USERNAME", "fix.logon.username");
        overrideFromEnv(properties, "FIX_LOGON_PASSWORD", "fix.logon.password");
        overrideFromEnv(properties, "REPLAY_ENABLED", "replay.enabled");
        overrideFromEnv(properties, "REPLAY_INPUT_PATH", "replay.input.path");
        overrideFromEnv(properties, "REPLAY_LOG_ZONE", "replay.log.zone");
        overrideFromEnv(properties, "REPLAY_SLEEP_MS", "replay.sleep.ms");
        overrideFromEnv(properties, "REPLAY_MAX_LINES", "replay.max.lines");
        overrideFromEnv(properties, "REPLAY_PRESERVE_TIMING", "replay.preserve.timing");
        overrideFromEnv(properties, "REPLAY_TIMING_SPEED", "replay.timing.speed");
        overrideFromEnv(properties, "REPLAY_TIMING_MAX_SLEEP_MS", "replay.timing.max.sleep.ms");
        overrideFromEnv(properties, "REPLAY_PURGE_DAY_BEFORE_INJECT", "replay.purge.day.before.inject");

        return AppConfig.fromProperties(properties);
    }

    private static void overrideFromEnv(Properties properties, String envKey, String propertyKey) {
        String value = System.getenv(envKey);
        if (value != null && !value.isBlank()) {
            properties.setProperty(propertyKey, value);
        }
    }
}
