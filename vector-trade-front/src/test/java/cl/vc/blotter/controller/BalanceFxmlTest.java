package cl.vc.blotter.controller;

import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BalanceFxmlTest {

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
    void hidesGuaranteesAndBasketOrdersWithoutRemovingThem() throws Exception {
        CountDownLatch loaded = new CountDownLatch(1);
        AtomicReference<Node> guarantees = new AtomicReference<>();
        AtomicReference<Node> basketOrders = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        Platform.runLater(() -> {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/Balance.fxml"));
                loader.load();
                guarantees.set((Node) loader.getNamespace().get("garantiasSection"));
                basketOrders.set((Node) loader.getNamespace().get("ordenesCestaSection"));
            } catch (Throwable throwable) {
                error.set(throwable);
            } finally {
                loaded.countDown();
            }
        });

        assertTrue(loaded.await(10, TimeUnit.SECONDS));
        if (error.get() != null) throw new AssertionError("No se pudo cargar Balance.fxml", error.get());
        assertHiddenAndUnmanaged(guarantees.get());
        assertHiddenAndUnmanaged(basketOrders.get());
    }

    private void assertHiddenAndUnmanaged(Node node) {
        assertNotNull(node);
        assertFalse(node.isVisible());
        assertFalse(node.isManaged());
    }
}
