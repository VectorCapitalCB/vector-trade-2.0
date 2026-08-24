package cl.vc.blotter.controller;

import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FooterFxmlTest {

    @BeforeAll
    static void startToolkit() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        try {
            Platform.startup(started::countDown);
        } catch (IllegalStateException alreadyStarted) {
            started.countDown();
        }
        assertTrue(started.await(10, TimeUnit.SECONDS));
    }

    @Test
    void muestraHistoricasYOcultaAdministracion() throws Exception {
        CountDownLatch loaded = new CountDownLatch(1);
        AtomicReference<FooterController> controller = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Platform.runLater(() -> {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/Footer.fxml"));
                loader.load();
                controller.set(loader.getController());
            } catch (Throwable throwable) {
                error.set(throwable);
            } finally {
                loaded.countDown();
            }
        });

        assertTrue(loaded.await(10, TimeUnit.SECONDS));
        if (error.get() != null) throw new AssertionError("No se pudo cargar Footer.fxml", error.get());
        assertNotNull(controller.get().getBtnHistoricalOrders());
        assertTrue(controller.get().getBtnHistoricalOrders().isManaged());
        assertTrue(controller.get().getBtnHistoricalOrders().isVisible());
        assertFalse(controller.get().btnAdminUser.isManaged());
        assertFalse(controller.get().btnAdminUser.isVisible());
    }
}
