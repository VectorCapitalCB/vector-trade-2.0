package cl.vc.blotter.view;

import cl.vc.blotter.controller.CandleController;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sonda para capturar la configuracion que necesita native-image.
 *
 * NO valida nada: su unico proposito es EJERCITAR en la JVM exactamente lo que falla en el
 * ejecutable nativo de Windows — cargar Candle.fxml y Settings.fxml — para que el agente de
 * GraalVM registre toda la reflexion, los recursos y los ResourceBundle involucrados.
 *
 * Se corre asi (el agente escribe al terminar):
 *   mvn test -Dtest=NativeConfigProbeTest \
 *     -DargLine="-agentlib:native-image-agent=config-output-dir=/tmp/ni-config"
 *
 * y despues:
 *   python3 tools/diff-native-config.py /tmp/ni-config
 *
 * No necesita login ni conexion al OMS: solo instancia el arbol de nodos del FXML, que es
 * justo donde revienta el nativo (al construir ChartViewer).
 */
public class NativeConfigProbeTest {

    private static void arrancarToolkit() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        try {
            Platform.startup(started::countDown);
        } catch (IllegalStateException alreadyStarted) {
            started.countDown();
        }
        assertTrue(started.await(15, TimeUnit.SECONDS), "el toolkit de JavaFX no arranco");
    }

    /** Carga un FXML en el hilo de FX y devuelve el error si lo hubo (no falla el test). */
    private static Throwable cargar(String recurso, javafx.util.Callback<Class<?>, Object> factory)
            throws Exception {
        AtomicReference<Throwable> fallo = new AtomicReference<>();
        CountDownLatch listo = new CountDownLatch(1);
        Platform.runLater(() -> {
            try {
                FXMLLoader loader = new FXMLLoader(NativeConfigProbeTest.class.getResource(recurso));
                if (factory != null) loader.setControllerFactory(factory);
                loader.load();
            } catch (Throwable t) {
                fallo.set(t);
            } finally {
                listo.countDown();
            }
        });
        assertTrue(listo.await(30, TimeUnit.SECONDS), "timeout cargando " + recurso);
        return fallo.get();
    }

    @Test
    public void ejercitaLosFxmlQueFallanEnNativo() throws Exception {
        arrancarToolkit();

        // Candle.fxml: es el que abre el doble click en Tendencia y el boton de velas de
        // Estadisticas. Instancia ChartViewer (JFreeChart), que es el sospechoso principal.
        Throwable candle = cargar("/view/Candle.fxml", type -> new CandleController("SQM-B"));
        System.out.println("[SONDA] Candle.fxml   -> " + (candle == null ? "OK" : candle));

        // Settings.fxml: pantalla nueva, su controller podria no estar completo en reflect-config.
        Throwable settings = cargar("/view/Settings.fxml", null);
        System.out.println("[SONDA] Settings.fxml -> " + (settings == null ? "OK" : settings));

        // En la JVM ambos deberian cargar; si aca fallan, el problema no es de nativo.
        assertTrue(candle == null, "Candle.fxml no carga ni en la JVM: " + candle);
    }
}
