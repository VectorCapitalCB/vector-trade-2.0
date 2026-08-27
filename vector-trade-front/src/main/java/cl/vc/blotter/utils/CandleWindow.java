package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import cl.vc.blotter.controller.CandleController;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Rectangle2D;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Screen;
import javafx.stage.Stage;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Ventana del grafico de velas.
 *
 * Existe para tener UN solo camino de apertura. Antes estaba duplicado en dos controllers y cada
 * copia tenia la mitad buena: {@code FooterController.openCandleView} reusaba la ventana y la
 * centraba en pantalla pero no aceptaba simbolo, y {@code StadisticsController.openCandlesForSymbol}
 * pasaba el simbolo pero abria una ventana nueva en cada click y aplicaba el CSS oscuro fijo,
 * ignorando el modo dia.
 *
 * Reglas:
 *  - Una ventana por simbolo: si ya esta abierta se le da foco en vez de apilar copias.
 *  - Respeta modo dia/noche igual que el resto de los dialogos del blotter.
 *  - Todo corre en el FX thread; se puede llamar desde cualquier hilo.
 */
@Slf4j
public final class CandleWindow {

    /** Clave usada para la ventana sin simbolo (la del footer). */
    private static final String SIN_SIMBOLO = "\u0000general";

    /** Solo se toca en el FX thread, por eso HashMap y no ConcurrentHashMap. */
    private static final Map<String, Stage> abiertas = new HashMap<>();

    private CandleWindow() {
    }

    /** Abre (o enfoca) el grafico de velas de un simbolo. */
    public static void open(String symbol) {
        if (Platform.isFxApplicationThread()) {
            abrirConProteccion(symbol);
        } else {
            Platform.runLater(() -> abrirConProteccion(symbol));
        }
    }

    private static void abrirConProteccion(String symbol) {
        try {
            crearYMostrar(symbol);
        } catch (Throwable error) {
            String sym = normalizar(symbol);
            log.error("[CandleWindow] No se pudo abrir el grafico de velas de {}", sym, error);
            try {
                Notifier.INSTANCE.notifyError("Grafico de velas",
                        "No se pudo abrir el grafico. El detalle quedo registrado en el log.");
            } catch (Throwable notificationError) {
                log.warn("[CandleWindow] Tampoco se pudo mostrar la notificacion: {}",
                        notificationError.getMessage());
            }
        }
    }

    /**
     * Recorre la misma ruta que usa el doble click y falla fuerte si el ejecutable nativo no puede
     * construir el FXML o el ChartViewer. Solo se invoca desde {@code --fxml-smoke} en CI.
     */
    public static void verifyNativeOpen(String symbol) throws Exception {
        if (!Platform.isFxApplicationThread()) {
            throw new IllegalStateException("El smoke del grafico debe ejecutarse en el FX thread");
        }
        Stage stage = crearYMostrar(symbol);
        try {
            Parent root = stage.getScene().getRoot();
            root.applyCss();
            root.layout();
        } finally {
            stage.close();
        }
    }

    private static Stage crearYMostrar(String symbol) throws Exception {
        String sym = normalizar(symbol);
        String clave = (sym == null) ? SIN_SIMBOLO : sym;

        Stage previa = abiertas.get(clave);
        if (previa != null && previa.isShowing()) {
            previa.requestFocus();
            previa.toFront();
            return previa;
        }
        abiertas.remove(clave);

        FXMLLoader loader = new FXMLLoader(CandleWindow.class.getResource("/view/Candle.fxml"));
        // Con simbolo se fuerza el constructor que lo recibe; sin simbolo se deja el
        // fx:controller del FXML (constructor sin argumentos), como hacia el footer.
        if (sym != null) {
            final String s = sym;
            loader.setControllerFactory(type -> new CandleController(s));
        }
        Parent root = loader.load();
        CandleController controller = loader.getController();
        if (controller == null || !controller.hasChartSurface()) {
            throw new IllegalStateException("Candle.fxml no inicializo CandleController/superficie de grafico");
        }

        Rectangle2D bounds = Screen.getPrimary().getVisualBounds();
        double w = Math.min(1100, Math.max(900, bounds.getWidth() - 40));
        double h = Math.min(700, Math.max(600, bounds.getHeight() - 40));

        Scene scene = new Scene(root, w, h);
        aplicarEstilo(scene);

        Stage stage = new Stage();
        stage.setScene(scene);
        stage.setTitle(sym == null ? "Grafico de Velas" : "Velas - " + sym);
        stage.setMaxWidth(bounds.getWidth());
        stage.setMaxHeight(bounds.getHeight());
        stage.setX(bounds.getMinX() + (bounds.getWidth() - w) / 2);
        stage.setY(bounds.getMinY() + (bounds.getHeight() - h) / 2);
        final String k = clave;
        stage.setOnHidden(e -> abiertas.remove(k));
        abiertas.put(clave, stage);
        stage.show();
        return stage;
    }

    private static String normalizar(String symbol) {
        String sym = (symbol == null) ? null : symbol.trim();
        if (sym != null && sym.isEmpty()) sym = null;
        return sym;
    }

    private static void aplicarEstilo(Scene scene) {
        if (scene == null) return;
        scene.getStylesheets().clear();
        try {
            boolean dia = Repository.getPrincipalController() != null
                    && Repository.getPrincipalController().isDayMode();
            String css = dia ? "/blotter/css/daymode.css" : Repository.getSTYLE();
            scene.getStylesheets().add(
                    Objects.requireNonNull(CandleWindow.class.getResource(css)).toExternalForm());
        } catch (Exception e) {
            // Sin CSS el grafico igual sirve: no vale abortar la apertura por el tema.
            log.warn("No se pudo aplicar el estilo al grafico de velas: {}", e.getMessage());
        }
    }
}
