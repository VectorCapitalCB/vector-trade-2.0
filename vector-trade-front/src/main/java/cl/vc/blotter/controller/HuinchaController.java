package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.Sparkline;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import javafx.animation.AnimationTimer;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.canvas.Canvas;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.util.Duration;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

/**
 * Huincha de papeles entre el lanzador y Datos del Mercado: simbolo, ultimo precio, mini
 * grafico intradia y variacion del dia, desplazandose de derecha a izquierda.
 *
 * Fuente: el ranking mas_tranzado del BolsaStats en vivo (llega cada ~2 s por el canal
 * candle) y las series intradia que ya calcula el backend. No agrega suscripciones nuevas.
 */
@Slf4j
public class HuinchaController implements Initializable {

    /** Papeles del ranking que entran en la huincha. */
    private static final int PAPELES = 12;

    /** Dos vueltas del mismo set: con el reciclado nunca se ve un hueco al final. */
    private static final int COPIAS = 2;

    private static final double PX_POR_SEG = 45;

    private static final Color VERDE = Color.web("#23a126");
    private static final Color ROJO = Color.web("#de292c");
    private static final Color NEUTRO = Color.web("#9aa0a6");

    private static final DecimalFormat FORMATO_VAR = new DecimalFormat("0.00");

    @FXML
    private Pane viewport;

    @FXML
    private HBox track;

    /** Una entrada por topic; cada topic tiene COPIAS celdas que se refrescan juntas. */
    private final Map<String, List<Celda>> celdas = new LinkedHashMap<>();

    private final List<String> simbolos = new ArrayList<>(PAPELES);

    private double desplazamiento;

    private final AnimationTimer scroller = new AnimationTimer() {
        private long anterior;

        @Override
        public void handle(long ahora) {
            long previo = anterior;
            anterior = ahora;
            if (previo == 0 || track.getChildren().isEmpty()) {
                return;
            }
            double dt = (ahora - previo) / 1_000_000_000d;
            // Ventana minimizada o pausa larga: retomar sin dar un salto.
            if (dt > 0.25) {
                return;
            }

            desplazamiento -= PX_POR_SEG * dt;

            Node primero = track.getChildren().get(0);
            double ancho = primero.getBoundsInParent().getWidth();
            if (desplazamiento + ancho < 0) {
                track.getChildren().remove(0);
                track.getChildren().add(primero);
                // El resto se corre ese ancho a la izquierda en el layout de este mismo
                // pulso: compensarlo aca deja el movimiento continuo.
                desplazamiento += ancho + track.getSpacing();
            }
            track.setTranslateX(desplazamiento);
        }
    };

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        try {
            Rectangle recorte = new Rectangle();
            recorte.widthProperty().bind(viewport.widthProperty());
            recorte.heightProperty().bind(viewport.heightProperty());
            viewport.setClip(recorte);

            track.prefHeightProperty().bind(viewport.heightProperty());

            // Sin BolsaStats no hay nada que mostrar: mejor que no ocupe la franja.
            viewport.setVisible(false);
            viewport.setManaged(false);

            // Pausa al pasar el mouse: sin esto no se alcanza a hacer clic en un papel.
            viewport.setOnMouseEntered(e -> scroller.stop());
            viewport.setOnMouseExited(e -> scroller.start());

            Repository.getStatsProperty().addListener((obs, ant, act) -> refrescar(act));
            refrescar(Repository.getStats());

            // Las series del backend se piden aparte del ranking: el primer pedido no puede
            // salir antes de que el canal candle este arriba.
            Timeline series = new Timeline(
                    new KeyFrame(Duration.seconds(6), e -> pedirSeries()),
                    new KeyFrame(Duration.seconds(60), e -> pedirSeries()));
            series.setCycleCount(Timeline.INDEFINITE);
            series.play();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void refrescar(MarketDataMessage.BolsaStats stats) {
        if (stats == null) {
            return;
        }
        List<MarketDataMessage.RankinSymbol> top = stats.getMasTranzadoList().stream()
                .filter(r -> r.getSymbol() != null && !r.getSymbol().isBlank())
                .limit(PAPELES)
                .toList();
        if (top.isEmpty()) {
            return;
        }

        Set<String> topics = new LinkedHashSet<>();
        top.forEach(r -> topics.add(topic(r)));

        // Solo se reconstruye cuando cambia QUE papeles se muestran. El ranking se reordena
        // a cada push y rearmar la huincha por eso daria un salto visual cada 2 s.
        if (!topics.equals(celdas.keySet())) {
            reconstruir(top);
        }

        for (MarketDataMessage.RankinSymbol r : top) {
            List<Celda> lista = celdas.get(topic(r));
            if (lista != null) {
                lista.forEach(c -> c.actualizar(r));
            }
        }
    }

    private void reconstruir(List<MarketDataMessage.RankinSymbol> top) {
        celdas.clear();
        simbolos.clear();
        track.getChildren().clear();
        desplazamiento = 0;
        track.setTranslateX(0);

        for (int copia = 0; copia < COPIAS; copia++) {
            for (MarketDataMessage.RankinSymbol r : top) {
                Celda celda = new Celda(r);
                celdas.computeIfAbsent(topic(r), k -> new ArrayList<>(COPIAS)).add(celda);
                track.getChildren().add(celda.nodo);
            }
        }
        top.forEach(r -> simbolos.add(r.getSymbol()));

        viewport.setVisible(true);
        viewport.setManaged(true);

        pedirSeries();
        scroller.start();
    }

    /**
     * Pide al candle-service la serie intradia de los papeles de la huincha. La calcula el
     * backend desde los trades del dia, asi el mini grafico se ve completo de entrada en vez
     * de esperar a acumular ticks en el cliente.
     */
    private void pedirSeries() {
        try {
            if (Repository.getCandleClientService() == null || simbolos.isEmpty()) {
                return;
            }
            JSONObject peticion = new JSONObject()
                    .put("action", "load_intraday_series")
                    .put("symbols", new JSONArray(simbolos));
            Repository.getCandleClientService().sendMessage(peticion.toString());
        } catch (Exception e) {
            log.error("No se pudo pedir la serie intradia de la huincha", e);
        }
    }

    /** Mismo formato que TopicGenerator.getTopicMKD: es la clave de las series intradia. */
    private static String topic(MarketDataMessage.RankinSymbol r) {
        if (r.getId() != null && !r.getId().isBlank()) {
            return r.getId();
        }
        return r.getSymbol() + r.getSecurityExchange().name() + r.getSettlType().name() + r.getSecurityType().name();
    }

    private void cargarEnLanzador(String simbolo) {
        try {
            LanzadorController lanzador = Repository.getLanzadorController();
            if (lanzador == null || simbolo == null || simbolo.isBlank()) {
                return;
            }
            // Mismo camino que la lista de sugerencias del ticket: cambio programatico y
            // despues la suscripcion, para no disparar el popup de busqueda.
            lanzador.setUpdatingFromPortfolio(true);
            lanzador.getTicket().setText(simbolo);
            lanzador.getQuantity().clear();
            lanzador.getPriceOrder().clear();
            lanzador.setUpdatingFromPortfolio(false);

            if (Repository.getPrincipalController() != null) {
                Repository.getPrincipalController().subscribeSymbol();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private final class Celda {

        private final HBox nodo = new HBox(6);
        private final Label simbolo = new Label();
        private final Label precio = new Label();
        private final Label variacion = new Label();
        private final Canvas grafico = new Canvas(52, 16);

        private Celda(MarketDataMessage.RankinSymbol r) {
            simbolo.getStyleClass().add("huincha-simbolo");
            precio.getStyleClass().add("huincha-precio");
            variacion.getStyleClass().add("huincha-var");
            nodo.getStyleClass().add("huincha-item");
            nodo.setAlignment(Pos.CENTER_LEFT);
            nodo.getChildren().addAll(simbolo, precio, grafico, variacion);
            nodo.setOnMouseClicked(e -> cargarEnLanzador(simbolo.getText()));
            actualizar(r);
        }

        private void actualizar(MarketDataMessage.RankinSymbol r) {
            simbolo.setText(r.getSymbol());
            precio.setText(NumberGenerator.formatDouble(r.getPrecioUltimo()));

            // Flechas por escape unicode: los .java del proyecto conviven con dos encodings.
            double var = r.getVariacionPct();
            if (var > 0) {
                variacion.setText("\u25B2 +" + FORMATO_VAR.format(var) + "%");
                variacion.setTextFill(VERDE);
            } else if (var < 0) {
                variacion.setText("\u25BC " + FORMATO_VAR.format(var) + "%");
                variacion.setTextFill(ROJO);
            } else {
                variacion.setText("\u25AC " + FORMATO_VAR.format(0) + "%");
                variacion.setTextFill(NEUTRO);
            }

            Sparkline.pintar(grafico, Repository.getSerieIntradia(topic(r)));
        }
    }
}
