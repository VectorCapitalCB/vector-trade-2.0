package cl.vc.blotter.view;

import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.CandleWindow;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Stage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sonda para capturar la configuracion que necesita native-image.
 *
 * NO valida nada: replica EXACTAMENTE lo que hace MainApp.runFxmlSmoke() —el paso que esta
 * fallando en el pipeline— para que el agente de GraalVM registre toda la reflexion involucrada.
 *
 * IMPORTANTE: hace applyCss() y layout(), no solo load(). La primera version de esta sonda solo
 * cargaba el FXML y por eso capturo de menos: JavaFX resuelve el CSS por reflexion y JFreeChart
 * recien DIBUJA durante el layout. Ahi esta la reflexion que faltaba.
 *
 * Uso:
 *   mvn test -Dtest=NativeConfigProbeTest \
 *     -DargLine="-agentlib:native-image-agent=config-output-dir=/tmp/ni-config"
 *   python3 tools/diff-native-config.py /tmp/ni-config
 */
public class NativeConfigProbeTest {

    private static void arrancarToolkit() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        try {
            Platform.startup(started::countDown);
        } catch (IllegalStateException alreadyStarted) {
            started.countDown();
        }
        assertTrue(started.await(20, TimeUnit.SECONDS), "el toolkit de JavaFX no arranco");
    }

    @Test
    public void replicaElSmokeDelPipeline() throws Exception {
        arrancarToolkit();

        AtomicReference<Throwable> falloPrincipal = new AtomicReference<>();
        AtomicReference<Throwable> falloCandle = new AtomicReference<>();
        CountDownLatch listo = new CountDownLatch(1);

        Platform.runLater(() -> {
            Stage principal = new Stage();
            try {
                // ---- Igual que runFxmlSmoke: PrincipalView + css + layout ----
                Repository.principal = principal;
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/PrincipalView.fxml"));
                AnchorPane root = loader.load();
                principal.setScene(new Scene(root));
                principal.show();
                root.applyCss();
                root.layout();
            } catch (Throwable t) {
                falloPrincipal.set(t);
            }

            try {
                // ---- Y el grafico, que es el que revienta en Windows ----
                CandleWindow.verifyNativeOpen("SQM-B");
            } catch (Throwable t) {
                falloCandle.set(t);
            }

            try {
                principal.close();
            } catch (Throwable ignore) {
                // nada: la sonda ya capturo lo que importaba
            }
            listo.countDown();
        });

        assertTrue(listo.await(90, TimeUnit.SECONDS), "timeout ejecutando la sonda");
        System.out.println("[SONDA] PrincipalView.fxml -> "
                + (falloPrincipal.get() == null ? "OK" : falloPrincipal.get()));
        System.out.println("[SONDA] Candle (verifyNativeOpen) -> "
                + (falloCandle.get() == null ? "OK" : falloCandle.get()));
        // No se afirma nada: en la JVM esto pasa; el valor esta en lo que capturo el agente.
    }
}
