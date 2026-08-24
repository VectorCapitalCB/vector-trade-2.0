package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import javafx.geometry.Rectangle2D;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;

import java.util.prefs.Preferences;

/** Guarda y restaura la geometría de ventanas flotantes por usuario. */
public final class WindowGeometryStore {

    private static final Preferences ROOT = Preferences.userNodeForPackage(WindowGeometryStore.class)
            .node("floating-windows");

    private WindowGeometryStore() {
    }

    public static void restore(Stage stage, String windowKey, double defaultWidth, double defaultHeight) {
        Preferences state = state(windowKey);
        double width = state.getDouble("width", defaultWidth);
        double height = state.getDouble("height", defaultHeight);
        double x = state.getDouble("x", Double.NaN);
        double y = state.getDouble("y", Double.NaN);

        stage.setWidth(Math.max(240, width));
        stage.setHeight(Math.max(180, height));

        if (Double.isFinite(x) && Double.isFinite(y)
                && !Screen.getScreensForRectangle(x, y, stage.getWidth(), stage.getHeight()).isEmpty()) {
            stage.setX(x);
            stage.setY(y);
        } else {
            centerOnOwnerScreen(stage);
        }
        stage.setMaximized(state.getBoolean("maximized", false));
        stage.xProperty().addListener((observable, oldValue, newValue) -> save(stage, windowKey));
        stage.yProperty().addListener((observable, oldValue, newValue) -> save(stage, windowKey));
        stage.widthProperty().addListener((observable, oldValue, newValue) -> save(stage, windowKey));
        stage.heightProperty().addListener((observable, oldValue, newValue) -> save(stage, windowKey));
        stage.maximizedProperty().addListener((observable, oldValue, newValue) -> save(stage, windowKey));
        stage.addEventHandler(WindowEvent.WINDOW_HIDING, event -> save(stage, windowKey));
    }

    public static void save(Stage stage, String windowKey) {
        if (stage == null) {
            return;
        }
        Preferences state = state(windowKey);
        state.putDouble("x", stage.getX());
        state.putDouble("y", stage.getY());
        state.putDouble("width", stage.getWidth());
        state.putDouble("height", stage.getHeight());
        state.putBoolean("maximized", stage.isMaximized());
    }

    private static void centerOnOwnerScreen(Stage stage) {
        Stage owner = Repository.getPrincipal();
        Screen screen = owner == null ? Screen.getPrimary() : screenFor(owner.getX(), owner.getY(),
                owner.getWidth(), owner.getHeight());
        Rectangle2D bounds = screen.getVisualBounds();
        stage.setX(bounds.getMinX() + Math.max(0, (bounds.getWidth() - stage.getWidth()) / 2));
        stage.setY(bounds.getMinY() + Math.max(0, (bounds.getHeight() - stage.getHeight()) / 2));
    }

    private static Screen screenFor(double x, double y, double width, double height) {
        return Screen.getScreens().stream()
                .filter(screen -> screen.getBounds().contains(x + width / 2, y + height / 2))
                .findFirst()
                .orElse(Screen.getPrimary());
    }

    private static Preferences state(String windowKey) {
        String username = Repository.getUsername();
        String userKey = safe(username == null || username.isBlank() ? "default" : username);
        return ROOT.node(userKey).node(safe(windowKey));
    }

    private static String safe(String value) {
        return value.trim().replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
