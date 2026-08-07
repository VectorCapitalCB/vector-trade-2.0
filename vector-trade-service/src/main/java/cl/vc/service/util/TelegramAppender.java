package cl.vc.service.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Appender de logback que reenvía a Telegram los logs de nivel ERROR (o superior) — "errores graves".
 *
 * <p>Usa {@link TelegramNotifier} (async + throttle). La clave de dedup es estable
 * (logger + clase de excepción, o logger + prefijo del mensaje), de modo que un error que se
 * repite miles de veces genera UNA alerta por intervalo, no miles.
 *
 * <p>Se engancha programáticamente con {@link #attachToRoot()} en el arranque; no requiere editar
 * logback.xml. Si Telegram no está habilitado/configurado, no hace nada.
 */
public class TelegramAppender extends AppenderBase<ILoggingEvent> {

    @Override
    protected void append(ILoggingEvent event) {
        try {
            if (!TelegramNotifier.isReady()) {
                return;
            }
            if (event.getLevel().toInt() < Level.ERROR.toInt()) {
                return;
            }
            String logger = event.getLoggerName() == null ? "" : event.getLoggerName();
            // Evitar bucles: no alertar sobre el propio notifier/appender
            if (logger.contains("TelegramNotifier") || logger.contains("TelegramAppender")) {
                return;
            }

            String msg = event.getFormattedMessage() == null ? "" : event.getFormattedMessage();
            IThrowableProxy tp = event.getThrowableProxy();

            // Clave estable para throttle/dedup
            String key = tp != null
                    ? logger + " | " + tp.getClassName()
                    : logger + " | " + msg.substring(0, Math.min(80, msg.length()));

            StringBuilder detail = new StringBuilder()
                    .append("[").append(event.getLevel()).append("] ").append(logger).append("\n")
                    .append(msg);
            if (tp != null) {
                detail.append("\n").append(tp.getClassName());
                if (tp.getMessage() != null) {
                    detail.append(": ").append(tp.getMessage());
                }
            }

            TelegramNotifier.alert(key, detail.toString());
        } catch (Throwable ignore) {
            // un appender JAMÁS debe romper el logging
        }
    }

    /** Engancha este appender al root logger (idempotente). Llamar en el arranque. */
    public static void attachToRoot() {
        try {
            ch.qos.logback.classic.Logger root =
                    (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            if (root.getAppender("TELEGRAM") != null) {
                return; // ya enganchado
            }
            LoggerContext ctx = root.getLoggerContext();
            TelegramAppender appender = new TelegramAppender();
            appender.setContext(ctx);
            appender.setName("TELEGRAM");
            appender.start();
            root.addAppender(appender);
        } catch (Throwable t) {
            // si falla el enganche, el sistema sigue normal (sin alertas Telegram)
        }
    }
}
