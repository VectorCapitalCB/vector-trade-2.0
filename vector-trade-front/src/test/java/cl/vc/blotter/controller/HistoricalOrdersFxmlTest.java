package cl.vc.blotter.controller;

import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistoricalOrdersFxmlTest {

    private static volatile boolean toolkitStarted;

    @BeforeAll
    static void startToolkit() throws Exception {
        if (toolkitStarted) return;
        CountDownLatch started = new CountDownLatch(1);
        try {
            Platform.startup(started::countDown);
        } catch (IllegalStateException alreadyStarted) {
            started.countDown();
        }
        assertTrue(started.await(10, TimeUnit.SECONDS));
        toolkitStarted = true;
    }

    @Test
    void cargaLaVistaHistoricaCompleta() throws Exception {
        CountDownLatch loaded = new CountDownLatch(1);
        AtomicReference<Parent> root = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Platform.runLater(() -> {
            try {
                root.set(FXMLLoader.load(getClass().getResource("/view/HistoricalOrders.fxml")));
            } catch (Throwable throwable) {
                error.set(throwable);
            } finally {
                loaded.countDown();
            }
        });

        assertTrue(loaded.await(10, TimeUnit.SECONDS));
        if (error.get() != null) throw new AssertionError("No se pudo cargar HistoricalOrders.fxml", error.get());
        assertNotNull(root.get());
        assertNotNull(root.get().lookup("#buyAveragePriceMetric"));
        assertNotNull(root.get().lookup("#sellAveragePriceMetric"));
        assertNotNull(root.get().lookup("#exportAllButton"));
        assertNotNull(root.get().lookup("#buyAmountMetric"));
        assertNotNull(root.get().lookup("#amountBySymbolTitle"));
        assertNotNull(root.get().lookup("#amountBySymbolChart"));
        assertNotNull(root.get().lookup("#amountBySymbolLegend"));
    }
}
