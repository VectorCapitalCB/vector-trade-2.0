package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import cl.vc.blotter.controller.CandleController;
import cl.vc.blotter.model.HistoricalCandle;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Rectangle2D;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Screen;
import javafx.stage.Stage;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private static final Map<String, Apertura> abiertas = new HashMap<>();

    /** Barras que siembra el smoke: suficientes para que SMA20/EMA20 salgan del warmup. */
    private static final int VELAS_SMOKE = 120;

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
     * Recorre la misma ruta que el doble click en Tendencia y falla fuerte si el ejecutable
     * nativo no puede DIBUJAR el grafico. Solo se invoca desde {@code --fxml-smoke} en CI.
     *
     * <p>Antes solo cargaba el FXML y comprobaba que el objeto del grafico no fuera null. Eso
     * pasaba igual con la ventana mostrando "Sin datos para mostrar", asi que el gate dejo salir
     * releases con el grafico muerto. Ahora siembra una serie sintetica, fuerza el render y
     * exige velas pintadas de verdad.
     */
    public static void verifyNativeOpen(String symbol) throws Exception {
        if (!Platform.isFxApplicationThread()) {
            throw new IllegalStateException("El smoke del grafico debe ejecutarse en el FX thread");
        }
        String sym = normalizar(symbol);
        if (sym == null) {
            throw new IllegalStateException("El smoke del grafico necesita un simbolo");
        }
        sembrarSerieSintetica(sym);

        Apertura apertura = crearYMostrar(sym);
        CandleController controller = apertura.controller();
        if (controller == null) {
            throw new IllegalStateException("Candle.fxml no entrego el CandleController");
        }
        try {
            Parent root = apertura.stage().getScene().getRoot();
            // selectSymbol fuerza renderChart() sobre la serie ya sembrada; layout() dimensiona
            // el Canvas y dispara redraw(), que es donde se pintan las velas.
            controller.selectSymbol(sym);
            root.applyCss();
            root.layout();

            boolean nativa = controller.isNativeSurface();
            int dibujadas = controller.drawnCandles();
            if (!controller.hasRenderedData()) {
                throw new IllegalStateException("El grafico no recibio las " + VELAS_SMOKE
                        + " velas sembradas: el camino de datos esta cortado (superficie "
                        + (nativa ? "JavaFX/Canvas" : "JFreeChart") + ")");
            }
            if (nativa && dibujadas <= 0) {
                throw new IllegalStateException("El Canvas nativo no pinto ninguna vela con "
                        + VELAS_SMOKE + " sembradas (velas dibujadas=" + dibujadas + "). Este es"
                        + " exactamente el sintoma de \"no abre el grafico\" en Windows.");
            }
            log.info("[CandleWindow] smoke del grafico OK: superficie={}, velas dibujadas={}",
                    nativa ? "JavaFX/Canvas" : "JFreeChart", dibujadas);
        } finally {
            apertura.stage().close();
        }
    }

    /**
     * Siembra una serie diaria sintetica para que el smoke ejerza el camino de dibujo real.
     *
     * <p>En CI el ejecutable corre sin login y sin websocket, asi que
     * {@code CandleController.requestData} sale por su early-return y el grafico se queda en
     * "Sin datos para mostrar". Sin serie, todo el codigo que pinta velas era codigo muerto
     * para el gate. Con datos, {@code redraw()} recorre grilla, cuerpos de vela, medias y
     * etiquetas de tiempo.
     *
     * <p>Solo se llama desde {@link #verifyNativeOpen(String)}, y ese camino termina en
     * {@code System.exit} dentro de {@code --fxml-smoke}, asi que no puede contaminar una
     * sesion real.
     */
    private static void sembrarSerieSintetica(String symbol) {
        List<HistoricalCandle> serie = new ArrayList<>(VELAS_SMOKE);
        LocalDate dia = LocalDate.of(2026, 1, 5);
        double base = 1000d;
        for (int i = 0; i < VELAS_SMOKE; i++) {
            // Oscilacion determinista: alterna velas verdes y rojas para ejercer las dos ramas
            // de color, y deja high/low con rango no degenerado.
            double open = base + Math.sin(i / 6d) * 25d;
            double close = base + Math.sin((i + 1) / 6d) * 25d;
            double high = Math.max(open, close) + 6d;
            double low = Math.min(open, close) - 6d;
            serie.add(new HistoricalCandle(symbol, dia.plusDays(i), open, high, low, close, 1000d + i));
            base += 0.4d;
        }
        Repository.setClosePriceHistory(symbol, serie);
    }

    /** Ventana abierta junto a su controller; el smoke necesita el controller para las aserciones. */
    private record Apertura(Stage stage, CandleController controller) {
    }

    private static Apertura crearYMostrar(String symbol) throws Exception {
        String sym = normalizar(symbol);
        String clave = (sym == null) ? SIN_SIMBOLO : sym;

        Apertura previa = abiertas.get(clave);
        if (previa != null && previa.stage().isShowing()) {
            previa.stage().requestFocus();
            previa.stage().toFront();
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
        Apertura apertura = new Apertura(stage, controller);
        stage.setOnHidden(e -> abiertas.remove(k));
        abiertas.put(clave, apertura);
        stage.show();
        return apertura;
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
