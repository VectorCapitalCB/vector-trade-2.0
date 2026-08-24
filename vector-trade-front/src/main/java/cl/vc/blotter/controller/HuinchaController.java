package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.model.StatisticVO;
import cl.vc.blotter.utils.BannerPrefs;
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
import java.util.ArrayDeque;
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

    /** Papeles por defecto si no hay preferencia guardada. Configurable en Configuración > Banner. */
    private static final int PAPELES = BannerPrefs.DEFAULT_PAPELES;
    private static final int MAX_PUNTOS_TENDENCIA = 180;

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
    private final Map<String, MarketDataMessage.RankinSymbol> rankingActual = new LinkedHashMap<>();
    private final Map<String, LiveTrendBuffer> tendenciasVivas = new LinkedHashMap<>();

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
            Repository.seriesIntradiaVersionProperty().addListener((obs, ant, act) -> repintarSeries());
            Repository.candleConnectedProperty().addListener((obs, ant, conectado) -> {
                if (conectado) {
                    pedirSeries();
                }
            });
            Repository.setHuinchaController(this);
            refrescar(Repository.getStats());

            // Ranking y conexion disparan la carga inicial. El minuto es solo un respaldo.
            Timeline series = new Timeline(new KeyFrame(Duration.seconds(60), e -> pedirSeries()));
            series.setCycleCount(Timeline.INDEFINITE);
            series.play();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Llamado desde Configuración > Banner al cambiar fuente o cantidad. Fuerza la reconstruccion
     * incluso si el conjunto de topics no cambio (el guard anti-parpadeo de refrescar lo saltaria).
     */
    public void aplicarPreferencias() {
        celdas.clear();
        track.getChildren().clear();
        refrescar(Repository.getStats());
    }

    /** Simbolos de los portafolios del operador, en mayusculas. Vacio si no hay ninguno cargado. */
    private Set<String> simbolosDelPortafolio() {
        Set<String> out = new LinkedHashSet<>();
        try {
            Repository.getBookPortMaps().values().forEach(book -> {
                if (book == null || book.getStatisticVO() == null
                        || book.getStatisticVO().getStatistic() == null) return;
                String s = book.getStatisticVO().getStatistic().getSymbol();
                if (s != null && !s.isBlank()) out.add(s.trim().toUpperCase());
            });
        } catch (Exception e) {
            log.warn("No se pudieron leer los simbolos del portafolio: {}", e.getMessage());
        }
        return out;
    }

    /**
     * Cierre de ayer del papel, desde el BookVO ya suscrito. El RankinSymbol del BolsaStats no lo
     * trae, y sin el la tendencia se mediria contra el primer precio del dia en vez de contra ayer.
     * NaN si el papel no esta suscrito: Sparkline cae al comportamiento anterior.
     */
    private double cierreAnterior(String topic) {
        try {
            var book = Repository.getBookPortMaps().get(topic);
            if (book == null || book.getStatisticVO() == null
                    || book.getStatisticVO().getStatistic() == null) return Double.NaN;
            return book.getStatisticVO().getStatistic().getPreviusClose();
        } catch (Exception e) {
            return Double.NaN;
        }
    }

    private void refrescar(MarketDataMessage.BolsaStats stats) {
        if (stats == null) {
            return;
        }
        int cuantos = BannerPrefs.papeles();
        List<MarketDataMessage.RankinSymbol> candidatos = stats.getMasTranzadoList();
        if (BannerPrefs.fuente() == BannerPrefs.Fuente.PORTAFOLIO) {
            // Los papeles del portafolio ya estan suscritos, asi que el precio viene en vivo.
            // Se filtra el ranking por ellos para conservar el mismo RankinSymbol (precio, var%)
            // y no tener que construirlo a mano desde otra fuente.
            Set<String> delPortafolio = simbolosDelPortafolio();
            if (!delPortafolio.isEmpty()) {
                List<MarketDataMessage.RankinSymbol> filtrado = candidatos.stream()
                        .filter(r -> r.getSymbol() != null
                                && delPortafolio.contains(r.getSymbol().trim().toUpperCase()))
                        .toList();
                // Si el portafolio no aparece en el ranking del dia (papeles sin transar), se cae
                // al ranking en vez de dejar el banner vacio.
                if (!filtrado.isEmpty()) candidatos = filtrado;
            }
        }
        List<MarketDataMessage.RankinSymbol> top = candidatos.stream()
                .filter(r -> r.getSymbol() != null && !r.getSymbol().isBlank())
                .limit(cuantos)
                .toList();
        if (top.isEmpty()) {
            return;
        }

        Set<String> topics = new LinkedHashSet<>();
        top.forEach(r -> topics.add(topic(r)));
        rankingActual.keySet().retainAll(topics);
        tendenciasVivas.keySet().retainAll(topics);
        top.forEach(r -> {
            String key = topic(r);
            rankingActual.put(key, r);
            tendenciasVivas.computeIfAbsent(key, ignored -> new LiveTrendBuffer())
                    .registrar(r.getPrecioUltimo());
        });

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

    /** Repinta los Canvas cuando llega la respuesta de Candle, aunque no cambie el ranking. */
    private void repintarSeries() {
        rankingActual.forEach((topic, rank) -> {
            List<Celda> lista = celdas.get(topic);
            if (lista != null) lista.forEach(celda -> celda.actualizarGrafico(rank));
        });
    }

    /** Mismo formato que TopicGenerator.getTopicMKD: es la clave de las series intradia. */
    static String topic(MarketDataMessage.RankinSymbol r) {
        // El ranking nacional usa un id ...NATIONAL..., pero las series Candle conservan
        // la liquidacion tecnica (...T2...). El id del ranking no sirve para cruzarlas.
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

    private TrendData resolverTendencia(MarketDataMessage.RankinSymbol rank) {
        String key = topic(rank);
        double ultimo = rank.getPrecioUltimo();
        double referencia = cierreAnterior(key);
        if (!Double.isFinite(referencia) || referencia <= 0d) {
            referencia = referenciaRanking(rank);
        }

        LiveTrendBuffer buffer = tendenciasVivas.get(key);
        double[] serie = buffer == null ? null : buffer.copiar(referencia);

        BookVO book = Repository.getBookPortMaps().get(key);
        if (book != null && book.getStatisticVO() != null) {
            StatisticVO vo = book.getStatisticVO();
            if (vo.getReferenciaTendencia() > 0d) referencia = vo.getReferenciaTendencia();
            if (vo.hasIntradayTrades()) serie = vo.getSerieIntradia();
        }

        double[] backend = Repository.getSerieIntradia(key);
        if (contarValidos(backend) > contarValidos(serie)
                && serieCompatible(backend, ultimo)) {
            serie = backend;
        }

        if (referencia <= 0d) referencia = primerValido(serie);
        if (referencia <= 0d && ultimo > 0d) referencia = ultimo;
        return new TrendData(agregarUltimo(serie, referencia, ultimo), referencia);
    }

    private static double referenciaRanking(MarketDataMessage.RankinSymbol rank) {
        double ultimo = rank.getPrecioUltimo();
        if (ultimo <= 0d) return 0d;
        double divisor = 1d + rank.getVariacionPct() / 100d;
        return divisor > 0d ? ultimo / divisor : ultimo;
    }

    private static double[] agregarUltimo(double[] serie, double referencia, double ultimo) {
        if (contarValidos(serie) == 0) {
            if (referencia <= 0d) return new double[0];
            return new double[]{referencia, ultimo > 0d ? ultimo : referencia};
        }
        if (ultimo <= 0d) return serie;
        double ultimoSerie = 0d;
        for (double value : serie) {
            if (Double.isFinite(value) && value > 0d) ultimoSerie = value;
        }
        if (Double.compare(ultimoSerie, ultimo) == 0) return serie;
        double[] completa = java.util.Arrays.copyOf(serie, serie.length + 1);
        completa[completa.length - 1] = ultimo;
        return completa;
    }

    private static boolean serieCompatible(double[] serie, double actual) {
        if (actual <= 0d) return true;
        double ultimo = 0d;
        if (serie != null) {
            for (double value : serie) {
                if (Double.isFinite(value) && value > 0d) ultimo = value;
            }
        }
        return ultimo > 0d && Math.abs(ultimo - actual) / actual <= 0.01d;
    }

    private static int contarValidos(double[] serie) {
        if (serie == null) return 0;
        int validos = 0;
        for (double value : serie) {
            if (Double.isFinite(value) && value > 0d) validos++;
        }
        return validos;
    }

    private static double primerValido(double[] serie) {
        if (serie == null) return 0d;
        for (double value : serie) {
            if (Double.isFinite(value) && value > 0d) return value;
        }
        return 0d;
    }

    private record TrendData(double[] serie, double referencia) {
    }

    private static final class LiveTrendBuffer {
        private static final long MUESTREO_PLANO_MS = 5_000L;
        private final ArrayDeque<Double> puntos = new ArrayDeque<>(MAX_PUNTOS_TENDENCIA);
        private long ultimaMuestraMs;

        synchronized void registrar(double precio) {
            if (!Double.isFinite(precio) || precio <= 0d) return;
            long ahora = System.currentTimeMillis();
            Double anterior = puntos.peekLast();
            if (anterior != null && Double.compare(anterior, precio) == 0
                    && ahora - ultimaMuestraMs < MUESTREO_PLANO_MS) return;
            if (puntos.size() == MAX_PUNTOS_TENDENCIA) puntos.removeFirst();
            puntos.addLast(precio);
            ultimaMuestraMs = ahora;
        }

        synchronized double[] copiar(double referencia) {
            int offset = referencia > 0d ? 1 : 0;
            double[] serie = new double[puntos.size() + offset];
            int index = 0;
            if (offset == 1) serie[index++] = referencia;
            for (double punto : puntos) serie[index++] = punto;
            return serie;
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

            actualizarGrafico(r);
        }

        private void actualizarGrafico(MarketDataMessage.RankinSymbol rank) {
            TrendData tendencia = resolverTendencia(rank);
            Sparkline.pintar(grafico, tendencia.serie(), tendencia.referencia());
        }
    }
}
