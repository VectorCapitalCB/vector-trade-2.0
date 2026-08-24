package cl.vc.blotter.utils;

import javafx.application.Platform;
import javafx.scene.media.AudioClip;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.EnumMap;
import java.util.Map;

@Slf4j
public final class SoundPlayer {

    private static final Map<Effect, AudioClip> CLIPS = new EnumMap<>(Effect.class);
    private static boolean initialized;

    private SoundPlayer() {
    }

    public static void initialize() {
        runOnFxThread(SoundPlayer::initializeNow);
    }

    public static void playNew() {
        play(Effect.NEW);
    }

    public static void playRejected() {
        play(Effect.REJECTED);
    }

    public static void playTrade() {
        play(Effect.TRADE);
    }

    public static void shutdown() {
        runOnFxThread(() -> {
            CLIPS.values().forEach(AudioClip::stop);
            CLIPS.clear();
            initialized = false;
        });
    }

    private static void play(Effect effect) {
        runOnFxThread(() -> {
            initializeNow();
            AudioClip clip = CLIPS.get(effect);
            if (clip == null) {
                log.warn("Sonido no disponible: {}", effect.resourceName);
                return;
            }

            try {
                clip.stop();
                clip.play();
            } catch (RuntimeException exception) {
                log.error("No se pudo reproducir el sonido {}", effect.resourceName, exception);
            }
        });
    }

    private static synchronized void initializeNow() {
        if (initialized) {
            return;
        }

        for (Effect effect : Effect.values()) {
            AudioClip clip = createClip(effect);
            if (clip != null) {
                CLIPS.put(effect, clip);
            }
        }
        initialized = true;
        log.info("Sonidos inicializados: {}/{}", CLIPS.size(), Effect.values().length);
    }

    private static AudioClip createClip(Effect effect) {
        for (String extension : new String[]{"wav", "mp3"}) {
            URL resource = SoundPlayer.class.getResource("/sounds/" + effect.resourceName + "." + extension);
            if (resource == null) {
                continue;
            }

            try {
                return new AudioClip(resource.toExternalForm());
            } catch (RuntimeException exception) {
                log.warn("Formato de audio no disponible para {}: {}", resource, exception.getMessage());
            }
        }
        return null;
    }

    private static void runOnFxThread(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
            return;
        }

        try {
            Platform.runLater(action);
        } catch (IllegalStateException exception) {
            log.error("JavaFX todavía no está disponible para reproducir sonidos", exception);
        }
    }

    private enum Effect {
        NEW("new"),
        REJECTED("rejected"),
        TRADE("trade");

        private final String resourceName;

        Effect(String resourceName) {
            this.resourceName = resourceName;
        }
    }
}
