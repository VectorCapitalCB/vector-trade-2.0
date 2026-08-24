package cl.vc.blotter.controller;

import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookVerticalControllerTest {

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
    void reservesTheSameScrollbarGutterForBothBookSides() {
        assertEquals(112.0, BookVerticalController.bookColumnWidth(240.0));
        assertEquals(152.0, BookVerticalController.bookColumnWidth(320.0));
    }

    @Test
    void verticalBookCanShrinkWithTheMainSplitPane() throws Exception {
        CountDownLatch loaded = new CountDownLatch(1);
        AtomicReference<BookVerticalController> controller = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Platform.runLater(() -> {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/BookVerticalView.fxml"));
                loader.load();
                controller.set(loader.getController());
            } catch (Throwable throwable) {
                error.set(throwable);
            } finally {
                loaded.countDown();
            }
        });

        assertTrue(loaded.await(10, TimeUnit.SECONDS));
        if (error.get() != null) throw new AssertionError("No se pudo cargar BookVerticalView.fxml", error.get());
        assertEquals(0.0, controller.get().getBookSplit().getMinHeight());
        assertEquals(0.0, controller.get().getOfferViewTable().getMinHeight());
        assertEquals(0.0, controller.get().getBidViewTable().getMinHeight());
    }
}
