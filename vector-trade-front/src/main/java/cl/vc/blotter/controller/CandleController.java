package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.model.HistoricalCandle;
import cl.vc.blotter.model.TradeCandle;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import javafx.collections.FXCollections;
import javafx.animation.PauseTransition;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.MenuButton;
import javafx.scene.control.MenuItem;
import javafx.scene.control.CheckMenuItem;
import javafx.scene.control.SeparatorMenuItem;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cl.vc.blotter.utils.ChartIndicator;
import cl.vc.blotter.utils.IndicatorSettings;
import cl.vc.blotter.utils.IndicatorSettingsDialog;
import cl.vc.blotter.utils.Indicators;
import cl.vc.blotter.utils.NativeCandleChart;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.fx.ChartCanvas;
import org.jfree.chart.fx.ChartViewer;
import org.jfree.chart.fx.interaction.MouseHandlerFX;
import org.jfree.chart.fx.interaction.PanHandlerFX;
import org.jfree.chart.fx.interaction.ScrollHandlerFX;
import org.jfree.chart.fx.interaction.ZoomHandlerFX;
import org.jfree.chart.plot.Marker;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.CombinedDomainXYPlot;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.CandlestickRenderer;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.chart.renderer.xy.XYDifferenceRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.ui.Layer;
import org.jfree.chart.ui.RectangleAnchor;
import org.jfree.chart.ui.TextAnchor;
import org.jfree.data.Range;
import org.jfree.data.xy.DefaultOHLCDataset;
import org.jfree.data.xy.OHLCDataItem;
import org.jfree.data.xy.OHLCDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.data.xy.XYDataset;

import java.awt.Color;
import java.awt.Paint;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.DayOfWeek;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CandleController implements Initializable {
    private static final Logger log = LoggerFactory.getLogger(CandleController.class);
    private static final ZoneId MARKET_ZONE = ZoneId.of("America/Santiago");
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(MARKET_ZONE);
    private static final DateTimeFormatter DAY_MARKER_FMT = DateTimeFormatter.ofPattern("dd/MM");
    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 5);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(17, 0);

    /**
     * Colores AWT del renderer JFreeChart, aislados en un holder para que NO se inicialicen
     * al instanciar el controller.
     *
     * <p>Estaban como {@code static final} de CandleController, asi que vivian en su
     * {@code <clinit>} y se ejecutaban en {@code loader.load()} de {@link CandleWindow},
     * mucho antes del {@code if (nativeChart != null)} de {@link #renderChart()}. Y
     * {@code java.awt.Color.<clinit>} llama {@code Toolkit.loadLibraries()}, que hace
     * {@code System.loadLibrary("awt")}: en el ejecutable nativo de Gluon no hay awt,
     * asi que el doble click en Tendencia moria con UnsatisfiedLinkError antes de poder
     * elegir la superficie JavaFX. La rama nativa llegaba tarde por construccion.
     *
     * <p>Con el holder, la clase recien se inicializa al leer {@code JfreeColors.UP}, que
     * solo ocurre despues del early-return nativo. Si se vuelven a subir a campos estaticos
     * del controller, el grafico nativo se rompe de nuevo y en silencio.
     */
    private static final class JfreeColors {
        static final Color UP = new Color(0x22, 0xc5, 0x5e);
        static final Color DOWN = new Color(0xef, 0x44, 0x44);
    }

    @FXML
    private StackPane chartHost;
    private ChartViewer chartViewer;
    private NativeCandleChart nativeChart;
    @FXML
    private ComboBox<String> cmbSymbol;
    @FXML
    private ComboBox<String> cmbTimeframe;
    @FXML
    private Label lblDataState;
    @FXML
    private Label lblLastTradeAt;
    @FXML
    private Label lblSma20;
    @FXML
    private Label lblEma20;
    @FXML
    private Label lblRsi14;
    @FXML
    private Label lblMacd;
    @FXML
    private Label lblTradeRange;
    @FXML
    private MenuButton mnuIndicadores;
    // Rotulos de la fila numerica: llevan el periodo, asi que se reescriben al cambiar parametros.
    @FXML
    private Label lblSma20Cap;
    @FXML
    private Label lblEma20Cap;
    @FXML
    private Label lblRsi14Cap;
    @FXML
    private Label lblMacdCap;

    /** Indicadores encendidos. Se lee de Preferences al abrir y se guarda en cada cambio. */
    private final java.util.Set<ChartIndicator> activos = ChartIndicator.cargar();
    /** Parametros de cada indicador, editables desde el menu y persistidos. */
    private IndicatorSettings params = IndicatorSettings.cargar();

    private final String initialSymbol;
    private final Set<String> requestedSymbols = new java.util.HashSet<>();
    private final Map<String, PauseTransition> requestTimeouts = new java.util.HashMap<>();
    private String renderedViewKey = "";
    private boolean renderedHasData;

    public CandleController() {
        this(null);
    }

    public CandleController(String initialSymbol) {
        this.initialSymbol = initialSymbol == null ? "" : initialSymbol.trim().toUpperCase(Locale.ROOT);
    }

    public boolean hasChartSurface() {
        return chartViewer != null || nativeChart != null;
    }

    /** true si la superficie activa es el Canvas JavaFX, es decir el ejecutable nativo. */
    public boolean isNativeSurface() {
        return nativeChart != null;
    }

    /** true si el ultimo render recibio velas. Complementa a {@link #drawnCandles()}. */
    public boolean hasRenderedData() {
        return renderedHasData;
    }

    /**
     * Velas efectivamente pintadas en la ultima pasada del Canvas nativo, o -1 si la superficie
     * activa es JFreeChart.
     *
     * <p>Lo consume el smoke de CI para distinguir un grafico dibujado de una ventana mostrando
     * "Sin datos para mostrar". {@link #hasChartSurface()} no alcanza: se cumple con solo tener
     * el objeto construido, que es justo lo que pasaba en los releases que salieron verdes con
     * el grafico muerto.
     */
    public int drawnCandles() {
        return nativeChart == null ? -1 : nativeChart.getLastDrawnCandles();
    }

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        initializeChartSurface();
        configureChartInteractions();
        Repository.closePriceHistoryVersionProperty().addListener((obs, oldV, newV) -> renderChart());
        Repository.tradeCandlesVersionProperty().addListener((obs, oldV, newV) -> renderChart());
        Repository.candleRequestErrorProperty().addListener((obs, oldV, newV) -> renderChart());
        Repository.candleConnectedProperty().addListener((obs, wasConnected, isConnected) -> {
            if (Boolean.TRUE.equals(isConnected)) {
                requestedSymbols.clear();
                requestCurrentData();
            }
        });
        if (cmbSymbol != null) {
            cmbSymbol.valueProperty().addListener((obs, oldV, newV) -> {
                requestCurrentData();
                renderChart();
            });
        }
        if (cmbTimeframe != null) {
            cmbTimeframe.setItems(FXCollections.observableArrayList(
                    "1D", "4h", "1h", "30m", "15m", "5m", "1m"));
            cmbTimeframe.getSelectionModel().select("1D");
            cmbTimeframe.setDisable(false);
            cmbTimeframe.valueProperty().addListener((obs, oldV, newV) -> {
                requestCurrentData();
                renderChart();
            });
        }
        buildIndicatorMenu();
        refreshSymbolList();
        if (!initialSymbol.isBlank()) {
            selectSymbol(initialSymbol);
        } else {
            requestCurrentData();
        }
        renderChart();
    }

    private void initializeChartSurface() {
        if (chartHost == null) {
            return;
        }
        if (isNativeImage()) {
            nativeChart = new NativeCandleChart();
            chartHost.getChildren().setAll(nativeChart);
        } else {
            chartViewer = new ChartViewer();
            chartViewer.setStyle("-fx-background-color: #121820;");
            chartHost.getChildren().setAll(chartViewer);
        }
    }

    private static boolean isNativeImage() {
        return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
    }

    /**
     * Selector de indicadores: un MenuButton con checks agrupados (sobre el precio / paneles),
     * mas atajos para dejar el grafico limpio o volver al default.
     *
     * Va como menu y no como botonera de toggles para que ocupe un solo lugar en la barra y siga
     * escalando cuando se agreguen indicadores.
     */
    private void buildIndicatorMenu() {
        if (mnuIndicadores == null) return;
        mnuIndicadores.getItems().clear();

        ChartIndicator.Grupo grupoActual = null;
        for (ChartIndicator ind : ChartIndicator.values()) {
            if (grupoActual != ind.grupo) {
                grupoActual = ind.grupo;
                // Titulo de grupo: un item deshabilitado hace de encabezado sin pelear con el CSS.
                MenuItem cabecera = new MenuItem(grupoActual.titulo.toUpperCase(Locale.ROOT));
                cabecera.setDisable(true);
                if (!mnuIndicadores.getItems().isEmpty()) {
                    mnuIndicadores.getItems().add(new SeparatorMenuItem());
                }
                mnuIndicadores.getItems().add(cabecera);
            }
            CheckMenuItem item = new CheckMenuItem(ind.etiqueta(params));
            item.setSelected(activos.contains(ind));
            item.setOnAction(e -> {
                if (item.isSelected()) activos.add(ind); else activos.remove(ind);
                ChartIndicator.guardar(activos);
                renderChart();
            });
            mnuIndicadores.getItems().add(item);
        }

        mnuIndicadores.getItems().add(new SeparatorMenuItem());

        MenuItem parametros = new MenuItem("Parámetros…");
        parametros.setOnAction(e -> abrirParametros());
        mnuIndicadores.getItems().add(parametros);
        mnuIndicadores.getItems().add(new SeparatorMenuItem());

        MenuItem soloVelas = new MenuItem("Sólo velas");
        soloVelas.setOnAction(e -> aplicarSeleccion(java.util.Set.of()));
        MenuItem porDefecto = new MenuItem("Restaurar por defecto");
        porDefecto.setOnAction(e -> aplicarSeleccion(ChartIndicator.porDefectoSet()));
        mnuIndicadores.getItems().addAll(soloVelas, porDefecto);
    }

    /** Reemplaza la seleccion completa, resincroniza los checks y redibuja. */
    private void aplicarSeleccion(java.util.Set<ChartIndicator> seleccion) {
        activos.clear();
        activos.addAll(seleccion);
        ChartIndicator.guardar(activos);
        buildIndicatorMenu();   // los CheckMenuItem no se enteran solos del cambio
        renderChart();
    }

    /** Abre el dialogo de parametros; si se aplica, guarda, reetiqueta el menu y redibuja. */
    private void abrirParametros() {
        javafx.stage.Window owner = mnuIndicadores != null && mnuIndicadores.getScene() != null
                ? mnuIndicadores.getScene().getWindow() : null;
        IndicatorSettingsDialog.mostrar(params, owner).ifPresent(nuevos -> {
            params = nuevos;
            params.guardar();
            buildIndicatorMenu();   // las etiquetas llevan los parametros: hay que reconstruirlas
            renderChart();
        });
    }

    private boolean on(ChartIndicator ind) {
        return activos.contains(ind);
    }

    private void renderChart() {
        String symbol = cmbSymbol != null ? cmbSymbol.getValue() : null;
        int timeframeMinutes = getTimeframeMinutes();
        boolean daily = timeframeMinutes >= 1440;
        String viewKey = normalizeSymbol(symbol) + "|" + timeframeMinutes;
        Range previousDomainRange = currentDomainRange(viewKey);
        List<HistoricalCandle> history = daily ? Repository.getClosePriceHistory(symbol) : List.of();
        List<TradeCandle> intraday = daily ? List.of() : Repository.getTradeCandles(symbol, timeframeMinutes);
        boolean responseReceived = daily
                ? Repository.hasClosePriceHistoryResponse(symbol)
                : Repository.hasTradeCandlesResponse(symbol, timeframeMinutes);
        if (responseReceived) {
            cancelRequestTimeout(requestKey(symbol, timeframeMinutes));
        }
        DatasetBuildResult built = daily
                ? buildDatasetFromHistory(history)
                : buildDatasetFromTradeCandles(intraday);
        if (nativeChart != null) {
            renderNativeChart(symbol, timeframeMinutes, built);
            renderedViewKey = viewKey;
            renderedHasData = !built.items.isEmpty();
            updateDataState(daily, history, intraday, responseReceived, built);
            return;
        }
        OHLCDataset dataset = built.dataset;

        DateAxis timeAxis = new DateAxis("Tiempo");
        timeAxis.setTimeZone(TimeZone.getTimeZone(MARKET_ZONE));
        SimpleDateFormat axisFmt = new SimpleDateFormat(timeframeMinutes >= 1440 ? "dd/MM/yyyy" : "dd/MM HH:mm");
        axisFmt.setTimeZone(TimeZone.getTimeZone(MARKET_ZONE));
        timeAxis.setDateFormatOverride(axisFmt);
        timeAxis.setLowerMargin(0.0);
        timeAxis.setUpperMargin(0.0);
        timeAxis.setLabelPaint(Color.WHITE);
        timeAxis.setTickLabelPaint(Color.WHITE);
        if (built.firstBucket != null && built.lastBucket != null) {
            Instant lower = daily
                    ? buildVisibleRangeStart(built.firstBucket, built.lastBucket, timeframeMinutes)
                    : intradayRangeStart(built.lastBucket);
            Instant upper = daily
                    ? buildVisibleRangeEnd(built.firstBucket, built.lastBucket, timeframeMinutes)
                    : intradayRangeEnd(built.lastBucket);
            if (upper.isAfter(lower)) {
                timeAxis.setAutoRange(false);
                timeAxis.setRange(Date.from(lower), Date.from(upper));
            }
        }
        if (previousDomainRange != null) {
            timeAxis.setAutoRange(false);
            timeAxis.setRange(previousDomainRange);
        }

        NumberAxis priceAxis = new NumberAxis("Precio");
        priceAxis.setAutoRangeIncludesZero(false);
        priceAxis.setLabelPaint(Color.WHITE);
        priceAxis.setTickLabelPaint(Color.WHITE);

        CandlestickRenderer candleRenderer = new DirectionalCandlestickRenderer(dataset, JfreeColors.UP, JfreeColors.DOWN);
        candleRenderer.setAutoWidthMethod(CandlestickRenderer.WIDTHMETHOD_SMALLEST);
        candleRenderer.setAutoWidthFactor(0.72d);
        candleRenderer.setAutoWidthGap(1.0d);
        candleRenderer.setMaxCandleWidthInMilliseconds(maxCandleWidthMillis(timeframeMinutes));
        candleRenderer.setDrawVolume(false);
        candleRenderer.setUpPaint(JfreeColors.UP);
        candleRenderer.setDownPaint(JfreeColors.DOWN);
        candleRenderer.setUseOutlinePaint(true);

        // Series OHLCV como arreglos para alimentar Indicators (que trabaja con double[]).
        Ohlcv ohlcv = Ohlcv.from(built.items);
        boolean split = !daily;   // intradia: cortar las lineas en el hueco nocturno

        // ---------------- panel de PRECIO ----------------
        XYPlot pricePlot = new XYPlot(dataset, null, priceAxis, candleRenderer);
        pricePlot.setDataset(0, dataset);
        pricePlot.setRenderer(0, candleRenderer);

        int idx = 1;
        if (on(ChartIndicator.SMA20)) {
            idx = addOverlay(pricePlot, idx, ChartIndicator.SMA20.etiqueta(params), built.items,
                    Indicators.sma(ohlcv.close, params.smaPeriod), split, new Color(0xFF, 0xD1, 0x66), 1.8f);
        }
        if (on(ChartIndicator.EMA20)) {
            idx = addOverlay(pricePlot, idx, ChartIndicator.EMA20.etiqueta(params), built.items,
                    Indicators.ema(ohlcv.close, params.emaPeriod), split, new Color(0x6B, 0xD4, 0xFF), 1.8f);
        }

        if (on(ChartIndicator.BOLLINGER)) {
            Indicators.Bollinger bb = Indicators.bollinger(ohlcv.close, params.bollingerPeriod, params.bollingerK);
            Color bbColor = new Color(0x9C, 0xA3, 0xAF);
            idx = addOverlay(pricePlot, idx, "BB sup", built.items, bb.upper, split, bbColor, 1.1f);
            idx = addOverlay(pricePlot, idx, "BB inf", built.items, bb.lower, split, bbColor, 1.1f);
        }

        if (on(ChartIndicator.VWAP)) {
            // Referencia de ejecucion de la mesa. Se reinicia cada dia, asi que se corta por dia
            // SIEMPRE (incluso en diario) para no unir sesiones distintas con un trazo recto.
            idx = addOverlay(pricePlot, idx, "VWAP", built.items,
                    Indicators.vwapSession(ohlcv.high, ohlcv.low, ohlcv.close, ohlcv.volume, ohlcv.time, MARKET_ZONE),
                    true, new Color(0xE8, 0x79, 0xF9), 2.0f);
        }

        if (on(ChartIndicator.ICHIMOKU)) {
            // Tenkan/Kijun alineadas al precio; Senkou A y B proyectadas 26 barras adelante forman
            // la nube; Chikou es el cierre 26 barras atras.
            Indicators.Ichimoku ichi = Indicators.ichimoku(ohlcv.high, ohlcv.low, ohlcv.close,
                    params.ichimokuTenkan, params.ichimokuKijun, params.ichimokuSenkouB, params.ichimokuShift);
            idx = addOverlay(pricePlot, idx, "Tenkan", built.items, ichi.tenkan, split,
                    new Color(0x38, 0xBD, 0xF8), 1.2f);
            idx = addOverlay(pricePlot, idx, "Kijun", built.items, ichi.kijun, split,
                    new Color(0xA7, 0x8B, 0xFA), 1.2f);
            idx = addOverlay(pricePlot, idx, "Chikou", built.items, ichi.chikou, split,
                    new Color(0x94, 0xA3, 0xB8), 1.0f);

            // La nube se rellena entre Senkou A y B con XYDifferenceRenderer: el color indica si A
            // va sobre B (alcista) o debajo (bajista).
            pricePlot.setDataset(idx, unionSeries(
                    projectedSeries("Senkou A", built.items, ichi.senkouA),
                    projectedSeries("Senkou B", built.items, ichi.senkouB)));
            XYDifferenceRenderer nube = new XYDifferenceRenderer(
                    new Color(0x22, 0xC5, 0x5E, 40), new Color(0xEF, 0x44, 0x44, 40), false);
            nube.setSeriesPaint(0, new Color(0x22, 0xC5, 0x5E, 120));
            nube.setSeriesPaint(1, new Color(0xEF, 0x44, 0x44, 120));
            nube.setSeriesStroke(0, new java.awt.BasicStroke(1.0f));
            nube.setSeriesStroke(1, new java.awt.BasicStroke(1.0f));
            pricePlot.setRenderer(idx, nube);
            idx++;
        }

        addTradingDayMarkers(pricePlot, built.items, timeframeMinutes);
        addLastPriceMarker(pricePlot, built.items);

        Color bg = new Color(0x12, 0x18, 0x20);
        stylePlot(pricePlot, bg);
        pricePlot.setRangeCrosshairVisible(true);
        pricePlot.setRangeCrosshairPaint(new Color(0xb8, 0xc4, 0xd1, 140));

        // ---------------- combinado, eje de tiempo compartido ----------------
        // El precio pesa 6 y cada panel 2: los de abajo son de apoyo, no protagonistas. Los paneles
        // apagados NO se agregan, asi el precio recupera todo el alto disponible.
        CombinedDomainXYPlot combined = new CombinedDomainXYPlot(timeAxis);
        combined.setGap(8d);
        combined.setDomainPannable(true);
        combined.setRangePannable(false);
        combined.setBackgroundPaint(bg);
        combined.setDomainCrosshairVisible(true);
        combined.setDomainCrosshairPaint(new Color(0xb8, 0xc4, 0xd1, 140));
        combined.add(pricePlot, 6);

        if (on(ChartIndicator.RSI)) {
            XYPlot rsiPlot = subPlot(ChartIndicator.RSI.etiqueta(params), bg, 0d, 100d);
            addOverlay(rsiPlot, 0, ChartIndicator.RSI.etiqueta(params), built.items,
                    Indicators.rsi(ohlcv.close, params.rsiPeriod), split, new Color(0xFB, 0xBF, 0x24), 1.6f);
            // Bandas de sobrecompra/sobreventa: sin ellas el RSI no se lee de un vistazo.
            rsiPlot.addRangeMarker(banda(70d, new Color(0xEF, 0x44, 0x44, 150)), Layer.BACKGROUND);
            rsiPlot.addRangeMarker(banda(30d, new Color(0x22, 0xC5, 0x5E, 150)), Layer.BACKGROUND);
            rsiPlot.addRangeMarker(banda(50d, new Color(0x66, 0x73, 0x82, 90)), Layer.BACKGROUND);
            combined.add(rsiPlot, 2);
        }

        if (on(ChartIndicator.MACD)) {
            Indicators.Macd macd = Indicators.macd(ohlcv.close, params.macdFast, params.macdSlow, params.macdSignal);
            XYPlot macdPlot = subPlot("MACD", bg, Double.NaN, Double.NaN);
            // El histograma va de fondo (dataset 0) y las lineas encima.
            XYBarRenderer histRenderer = new XYBarRenderer(0.05d);
            histRenderer.setShadowVisible(false);
            histRenderer.setDrawBarOutline(false);
            histRenderer.setDefaultPaint(new Color(0x64, 0x74, 0x8B));
            macdPlot.setDataset(0, seriesOf("Histograma", built.items, macd.histogram, split));
            macdPlot.setRenderer(0, histRenderer);
            addOverlay(macdPlot, 1, "MACD", built.items, macd.line, split, new Color(0x60, 0xA5, 0xFA), 1.6f);
            addOverlay(macdPlot, 2, "Senal", built.items, macd.signal, split, new Color(0xF9, 0x73, 0x16), 1.4f);
            macdPlot.addRangeMarker(banda(0d, new Color(0x66, 0x73, 0x82, 110)), Layer.BACKGROUND);
            combined.add(macdPlot, 2);
        }

        if (on(ChartIndicator.ATR)) {
            XYPlot atrPlot = subPlot(ChartIndicator.ATR.etiqueta(params), bg, Double.NaN, Double.NaN);
            addOverlay(atrPlot, 0, ChartIndicator.ATR.etiqueta(params), built.items,
                    Indicators.atr(ohlcv.high, ohlcv.low, ohlcv.close, params.atrPeriod), split,
                    new Color(0x38, 0xBD, 0xF8), 1.6f);
            combined.add(atrPlot, 2);
        }

        String tf = cmbTimeframe == null || cmbTimeframe.getValue() == null
                ? "1D" : cmbTimeframe.getValue();
        String title = symbol == null || symbol.isBlank()
                ? "Velas (" + tf + ")"
                : "Velas (" + tf + ") - " + symbol;
        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, combined, false);
        chart.setBackgroundPaint(bg);
        chart.getTitle().setPaint(new Color(0xe7, 0xec, 0xf2));
        chartViewer.setChart(chart);
        chartViewer.setStyle("-fx-background-color: #121820;");
        bindVisiblePriceRange(timeAxis, priceAxis, built.items);
        renderedViewKey = viewKey;
        renderedHasData = !built.items.isEmpty();

        updateDataState(daily, history, intraday, responseReceived, built);
    }

    private void renderNativeChart(String symbol, int timeframeMinutes, DatasetBuildResult built) {
        Ohlcv ohlcv = Ohlcv.from(built.items);
        double[] sma = on(ChartIndicator.SMA20)
                ? Indicators.sma(ohlcv.close, params.smaPeriod) : null;
        double[] ema = on(ChartIndicator.EMA20)
                ? Indicators.ema(ohlcv.close, params.emaPeriod) : null;
        String tf = cmbTimeframe == null || cmbTimeframe.getValue() == null
                ? "1D" : cmbTimeframe.getValue();
        String title = symbol == null || symbol.isBlank()
                ? "Velas (" + tf + ")"
                : "Velas (" + tf + ") - " + symbol;
        List<NativeCandleChart.CandlePoint> points = built.items.stream()
                .map(item -> new NativeCandleChart.CandlePoint(
                        item.getDate().toInstant(),
                        item.getOpen().doubleValue(),
                        item.getHigh().doubleValue(),
                        item.getLow().doubleValue(),
                        item.getClose().doubleValue()))
                .toList();
        nativeChart.setData(title, points, timeframeMinutes, sma, ema);
    }

    private void updateDataState(boolean daily,
                                 List<HistoricalCandle> history,
                                 List<TradeCandle> intraday,
                                 boolean responseReceived,
                                 DatasetBuildResult built) {
        if (daily) {
            updateHistoricalDataState(history, responseReceived);
            updateHistoricalRange(history);
        } else {
            updateTradeCandleState(intraday, responseReceived);
            updateTradeRange(built.firstTradeAt, built.lastTradeAt);
        }
        updateIndicators(built.closes);
    }

    private void configureChartInteractions() {
        if (chartViewer == null) {
            return;
        }
        ChartCanvas canvas = chartViewer.getCanvas();
        MouseHandlerFX defaultZoom = canvas.getMouseHandler("zoom");
        if (defaultZoom != null) {
            canvas.removeMouseHandler(defaultZoom);
        }
        canvas.addMouseHandler(new ZoomHandlerFX(
                "zoom-selection", chartViewer, false, false, false, true));
        canvas.addMouseHandler(new PanHandlerFX("pan-direct"));

        MouseHandlerFX defaultScroll = canvas.getMouseHandler("scroll");
        if (defaultScroll != null) {
            canvas.removeAuxiliaryMouseHandler(defaultScroll);
        }
        ScrollHandlerFX scroll = new ScrollHandlerFX("scroll-focus");
        scroll.setZoomFactor(0.075d);
        canvas.addAuxiliaryMouseHandler(scroll);
        canvas.setDomainZoomable(true);
        canvas.setRangeZoomable(false);
    }

    private Range currentDomainRange(String viewKey) {
        if (!renderedHasData || !viewKey.equals(renderedViewKey)
                || chartViewer == null || chartViewer.getChart() == null
                || !(chartViewer.getChart().getPlot() instanceof XYPlot currentPlot)) {
            return null;
        }
        return currentPlot.getDomainAxis().getRange();
    }

    private void bindVisiblePriceRange(DateAxis timeAxis, NumberAxis priceAxis, List<OHLCDataItem> items) {
        if (items == null || items.isEmpty()) {
            return;
        }
        Runnable adjustRange = () -> adjustVisiblePriceRange(timeAxis, priceAxis, items);
        timeAxis.addChangeListener(event -> adjustRange.run());
        adjustRange.run();
    }

    private void adjustVisiblePriceRange(DateAxis timeAxis, NumberAxis priceAxis, List<OHLCDataItem> items) {
        Range visibleTime = timeAxis.getRange();
        Range visiblePrice = visiblePriceRange(items, visibleTime);
        if (visiblePrice == null || visiblePrice.equals(priceAxis.getRange())) {
            return;
        }
        priceAxis.setAutoRange(false);
        priceAxis.setRange(visiblePrice);
    }

    static Range visiblePriceRange(List<OHLCDataItem> items, Range visibleTime) {
        if (items == null || items.isEmpty() || visibleTime == null) {
            return null;
        }
        long lower = (long) visibleTime.getLowerBound();
        long upper = (long) visibleTime.getUpperBound();
        int from = lowerBound(items, lower);
        double minimum = Double.POSITIVE_INFINITY;
        double maximum = Double.NEGATIVE_INFINITY;
        for (int i = from; i < items.size(); i++) {
            OHLCDataItem item = items.get(i);
            long timestamp = item.getDate().getTime();
            if (timestamp > upper) {
                break;
            }
            minimum = Math.min(minimum, item.getLow().doubleValue());
            maximum = Math.max(maximum, item.getHigh().doubleValue());
        }
        if (!Double.isFinite(minimum) || !Double.isFinite(maximum)) {
            return null;
        }
        double span = maximum - minimum;
        double padding = span > 0d
                ? span * 0.08d
                : Math.max(Math.abs(maximum) * 0.005d, 0.0001d);
        return new Range(minimum - padding, maximum + padding);
    }

    private static int lowerBound(List<OHLCDataItem> items, long timestamp) {
        int low = 0;
        int high = items.size();
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (items.get(middle).getDate().getTime() < timestamp) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    DatasetBuildResult buildDatasetFromHistory(List<HistoricalCandle> history) {
        if (history == null || history.isEmpty()) {
            return DatasetBuildResult.empty();
        }
        List<OHLCDataItem> items = new ArrayList<>();
        List<Double> closes = new ArrayList<>();
        history.stream()
                .filter(c -> c != null && c.date() != null && c.close() > 0d)
                .sorted(Comparator.comparing(HistoricalCandle::date))
                .forEach(candle -> {
                    double open = candle.open() > 0d ? candle.open() : candle.close();
                    double high = Math.max(candle.high(), Math.max(open, candle.close()));
                    double low = candle.low() > 0d
                            ? Math.min(candle.low(), Math.min(open, candle.close()))
                            : Math.min(open, candle.close());
                    Instant day = candle.date().atStartOfDay(MARKET_ZONE).toInstant();
                    items.add(new OHLCDataItem(Date.from(day), open, high, low,
                            candle.close(), Math.max(0d, candle.volume())));
                    closes.add(candle.close());
                });
        if (items.isEmpty()) {
            return DatasetBuildResult.empty();
        }
        Instant first = items.get(0).getDate().toInstant();
        Instant last = items.get(items.size() - 1).getDate().toInstant();
        return new DatasetBuildResult(
                new DefaultOHLCDataset("OHLC", items.toArray(new OHLCDataItem[0])),
                items, closes, first, last, first, last);
    }

    DatasetBuildResult buildDatasetFromTrades(List<MarketDataMessage.TradeGeneral> trades, int timeframeMinutes) {
        if (trades == null || trades.isEmpty()) {
            return DatasetBuildResult.empty();
        }

        List<MarketDataMessage.TradeGeneral> ordered = trades.stream()
                .filter(t -> t != null && t.hasT())
                .filter(t -> t.getPrice() > 0)
                .sorted(Comparator.comparingLong(t -> t.getT().getSeconds()))
                .collect(Collectors.toList());

        if (ordered.isEmpty()) {
            return DatasetBuildResult.empty();
        }

        Map<Instant, Ohlc> rawBuckets = new TreeMap<>();
        Instant firstTradeAt = null;
        Instant lastTradeAt = null;
        for (MarketDataMessage.TradeGeneral trade : ordered) {
            Instant tradeTime = Instant.ofEpochSecond(trade.getT().getSeconds(), trade.getT().getNanos());
            if (!isWithinMarketSession(tradeTime)) {
                continue;
            }
            if (firstTradeAt == null || tradeTime.isBefore(firstTradeAt)) {
                firstTradeAt = tradeTime;
            }
            if (lastTradeAt == null || tradeTime.isAfter(lastTradeAt)) {
                lastTradeAt = tradeTime;
            }
            Instant bucket = bucketize(tradeTime, Math.max(1, timeframeMinutes));
            rawBuckets.computeIfAbsent(bucket, k -> new Ohlc()).update(trade.getPrice(), trade.getQty());
        }

        if (rawBuckets.isEmpty()) {
            return DatasetBuildResult.empty();
        }

        List<OHLCDataItem> items = new ArrayList<>();
        List<Double> closes = new ArrayList<>();
        for (Map.Entry<Instant, Ohlc> entry : rawBuckets.entrySet()) {
            Instant bucket = entry.getKey();
            if (!isTradingBucket(bucket, timeframeMinutes)) {
                continue;
            }
            Ohlc o = entry.getValue();
            if (o == null || !o.initialized) {
                continue;
            }
            items.add(new OHLCDataItem(Date.from(bucket), o.open, o.high, o.low, o.close, o.volume));
            closes.add(o.close);
        }

        if (items.isEmpty()) {
            return DatasetBuildResult.empty();
        }

        Instant first = items.get(0).getDate().toInstant();
        Instant last = items.get(items.size() - 1).getDate().toInstant();
        OHLCDataset dataset = new DefaultOHLCDataset("OHLC", items.toArray(new OHLCDataItem[0]));
        return new DatasetBuildResult(dataset, items, closes, first, last, firstTradeAt, lastTradeAt);
    }

    DatasetBuildResult buildDatasetFromTradeCandles(List<TradeCandle> candles) {
        if (candles == null || candles.isEmpty()) {
            return DatasetBuildResult.empty();
        }
        List<OHLCDataItem> items = new ArrayList<>();
        List<Double> closes = new ArrayList<>();
        candles.stream()
                .filter(c -> c != null && c.start() != null && c.close() > 0d)
                .sorted(Comparator.comparing(TradeCandle::start))
                .forEach(candle -> {
                    double open = candle.open() > 0d ? candle.open() : candle.close();
                    double high = Math.max(candle.high(), Math.max(open, candle.close()));
                    double low = candle.low() > 0d
                            ? Math.min(candle.low(), Math.min(open, candle.close()))
                            : Math.min(open, candle.close());
                    items.add(new OHLCDataItem(Date.from(candle.start()), open, high, low,
                            candle.close(), Math.max(0d, candle.volume())));
                    closes.add(candle.close());
                });
        if (items.isEmpty()) {
            return DatasetBuildResult.empty();
        }
        Instant first = items.get(0).getDate().toInstant();
        Instant last = items.get(items.size() - 1).getDate().toInstant();
        return new DatasetBuildResult(
                new DefaultOHLCDataset("OHLC", items.toArray(new OHLCDataItem[0])),
                items, closes, first, last, first, last);
    }

    private void refreshSymbolList() {
        if (cmbSymbol == null) {
            return;
        }
        String current = cmbSymbol.getValue();
        Set<String> symbols = Repository.getCandleTradeGenerales().stream()
                .map(MarketDataMessage.TradeGeneral::getSymbol)
                .map(this::normalizeSymbol)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toCollection(java.util.TreeSet::new));
        symbols.addAll(Repository.getClosePriceHistorySymbols());
        if (!initialSymbol.isBlank()) {
            symbols.add(initialSymbol);
        }
        MarketDataMessage.BolsaStats stats = Repository.getStats();
        if (stats != null) {
            stats.getMasTranzadoList().forEach(row -> symbols.add(normalizeSymbol(row.getSymbol())));
            stats.getMasVolatilList().forEach(row -> symbols.add(normalizeSymbol(row.getSymbol())));
            stats.getBestRankinList().forEach(row -> symbols.add(normalizeSymbol(row.getSymbol())));
            stats.getWorseRankinList().forEach(row -> symbols.add(normalizeSymbol(row.getSymbol())));
        }
        symbols.remove("");

        var items = FXCollections.observableArrayList(symbols);
        cmbSymbol.setItems(items);

        if (current != null && items.contains(current)) {
            cmbSymbol.getSelectionModel().select(current);
        } else if (!initialSymbol.isBlank() && items.contains(initialSymbol)) {
            cmbSymbol.getSelectionModel().select(initialSymbol);
        } else {
            cmbSymbol.getSelectionModel().selectFirst();
        }
    }

    public void selectSymbol(String symbol) {
        String normalized = normalizeSymbol(symbol);
        if (normalized.isBlank() || cmbSymbol == null) {
            return;
        }
        if (!cmbSymbol.getItems().contains(normalized)) {
            cmbSymbol.getItems().add(normalized);
            FXCollections.sort(cmbSymbol.getItems());
        }
        cmbSymbol.getSelectionModel().select(normalized);
        requestCurrentData();
        renderChart();
    }

    private void requestCurrentData() {
        requestData(cmbSymbol == null ? null : cmbSymbol.getValue(), getTimeframeMinutes());
    }

    private void requestData(String symbol, int timeframeMinutes) {
        String normalized = normalizeSymbol(symbol);
        String key = requestKey(normalized, timeframeMinutes);
        if (normalized.isBlank() || !requestedSymbols.add(key)) {
            return;
        }
        if (Repository.getCandleClientService() == null || !Repository.candleConnectedProperty().get()) {
            requestedSymbols.remove(key);
            return;
        }
        Repository.setCandleRequestError("");
        JSONObject request = new JSONObject().put("market", "BCS").put("symbol", normalized);
        if (timeframeMinutes >= 1440) {
            request.put("action", "load_close_history").put("limit", 180);
        } else {
            request.put("action", "load_trade_candles")
                    .put("bucketMinutes", timeframeMinutes)
                    .put("historyDays", 20);
        }
        Repository.getCandleClientService().sendMessage(request.toString());
        scheduleRequestTimeout(key);
    }

    private void scheduleRequestTimeout(String key) {
        cancelRequestTimeout(key);
        PauseTransition timeout = new PauseTransition(javafx.util.Duration.seconds(12));
        timeout.setOnFinished(event -> {
            requestTimeouts.remove(key);
            requestedSymbols.remove(key);
            Repository.setCandleRequestError("Candle no respondió dentro del tiempo esperado");
            renderChart();
        });
        requestTimeouts.put(key, timeout);
        timeout.play();
    }

    private void cancelRequestTimeout(String key) {
        PauseTransition timeout = requestTimeouts.remove(key);
        if (timeout != null) timeout.stop();
    }

    private String requestKey(String symbol, int timeframeMinutes) {
        String normalized = normalizeSymbol(symbol);
        return (timeframeMinutes >= 1440 ? "daily" : "intraday-" + timeframeMinutes)
                + "|" + normalized;
    }

    private void updateTradeCandleState(List<TradeCandle> candles, boolean responseReceived) {
        if (lblDataState == null || lblLastTradeAt == null) {
            return;
        }
        if (candles == null || candles.isEmpty()) {
            updateEmptyState(responseReceived, "SIN TRADES DISPONIBLES", "CARGANDO TRADES");
            lblLastTradeAt.setText("-");
            return;
        }
        TradeCandle last = candles.get(candles.size() - 1);
        lblDataState.setText("TRADES DEL DÍA");
        lblDataState.setStyle("-fx-text-fill: #39c16c; -fx-font-weight: bold;");
        lblLastTradeAt.setText(TS_FMT.format(last.start()) + " CL");
    }

    private void updateHistoricalDataState(List<HistoricalCandle> history, boolean responseReceived) {
        if (lblDataState == null || lblLastTradeAt == null) {
            return;
        }
        if (history == null || history.isEmpty()) {
            updateEmptyState(responseReceived, "SIN HISTÓRICO DISPONIBLE", "CARGANDO HISTÓRICO");
            lblLastTradeAt.setText("-");
            return;
        }
        HistoricalCandle last = history.get(history.size() - 1);
        lblDataState.setText("HISTÓRICO MONGO");
        lblDataState.setStyle("-fx-text-fill: #39c16c; -fx-font-weight: bold;");
        lblLastTradeAt.setText(last.date().toString());
    }

    private void updateEmptyState(boolean responseReceived, String emptyText, String loadingText) {
        String error = Repository.getCandleRequestError();
        if (error != null && !error.isBlank()) {
            lblDataState.setText("ERROR DE DATOS");
            lblDataState.setStyle("-fx-text-fill: #ff5f5f; -fx-font-weight: bold;");
        } else if (responseReceived) {
            lblDataState.setText(emptyText);
            lblDataState.setStyle("-fx-text-fill: #ffb347; -fx-font-weight: bold;");
        } else {
            lblDataState.setText(loadingText);
            lblDataState.setStyle("-fx-text-fill: #ffb347; -fx-font-weight: bold;");
        }
    }

    private void updateHistoricalRange(List<HistoricalCandle> history) {
        if (lblTradeRange == null) {
            return;
        }
        if (history == null || history.isEmpty()) {
            lblTradeRange.setText("-");
            return;
        }
        lblTradeRange.setText(history.get(0).date() + " -> " + history.get(history.size() - 1).date());
    }

    private void updateDataState(List<MarketDataMessage.TradeGeneral> filtered) {
        if (lblDataState == null || lblLastTradeAt == null) {
            return;
        }

        if (filtered == null || filtered.isEmpty()) {
            lblDataState.setText("SIN DATOS REALES");
            lblDataState.setStyle("-fx-text-fill: #ff5f5f; -fx-font-weight: bold;");
            lblLastTradeAt.setText("-");
            return;
        }

        MarketDataMessage.TradeGeneral last = filtered.stream()
                .filter(t -> t.hasT())
                .max(Comparator.comparingLong(t -> t.getT().getSeconds()))
                .orElse(null);

        if (last == null || !last.hasT()) {
            lblDataState.setText("SIN TIMESTAMP");
            lblDataState.setStyle("-fx-text-fill: #ffb347; -fx-font-weight: bold;");
            lblLastTradeAt.setText("-");
            return;
        }

        Instant ts = Instant.ofEpochSecond(last.getT().getSeconds(), last.getT().getNanos());
        long ageMinutes = Duration.between(ts, Instant.now()).toMinutes();
        lblLastTradeAt.setText(TS_FMT.format(ts) + " CL");
        if (ageMinutes <= 10) {
            lblDataState.setText("REAL-TIME");
            lblDataState.setStyle("-fx-text-fill: #39c16c; -fx-font-weight: bold;");
        } else {
            lblDataState.setText("ATRASADO");
            lblDataState.setStyle("-fx-text-fill: #ffb347; -fx-font-weight: bold;");
        }
    }

    private void updateIndicators(List<Double> closes) {
        if (lblSma20 == null || lblEma20 == null || lblRsi14 == null || lblMacd == null) {
            return;
        }
        if (closes == null || closes.isEmpty()) {
            lblSma20.setText("-");
            lblEma20.setText("-");
            lblRsi14.setText("-");
            lblMacd.setText("-");
            return;
        }

        // Misma matematica que la del grafico: Indicators es la unica fuente. Antes esta clase
        // tenia su propia copia (y buildMovingAverageDataset una tercera), asi que el numero del
        // label podia no coincidir con la linea dibujada.
        double[] c = new double[closes.size()];
        for (int i = 0; i < c.length; i++) {
            Double v = closes.get(i);
            c[i] = v == null ? Double.NaN : v;
        }

        if (lblSma20Cap != null) lblSma20Cap.setText("SMA" + params.smaPeriod + ":");
        if (lblEma20Cap != null) lblEma20Cap.setText("EMA" + params.emaPeriod + ":");
        if (lblRsi14Cap != null) lblRsi14Cap.setText("RSI" + params.rsiPeriod + ":");
        if (lblMacdCap != null) {
            lblMacdCap.setText("MACD(" + params.macdFast + "," + params.macdSlow + "," + params.macdSignal + "):");
        }

        lblSma20.setText(formatVal(Indicators.last(Indicators.sma(c, params.smaPeriod))));
        lblEma20.setText(formatVal(Indicators.last(Indicators.ema(c, params.emaPeriod))));
        lblRsi14.setText(formatVal(Indicators.last(Indicators.rsi(c, params.rsiPeriod))));

        Indicators.Macd macd = Indicators.macd(c, params.macdFast, params.macdSlow, params.macdSignal);
        Double line = Indicators.last(macd.line);
        Double signal = Indicators.last(macd.signal);
        Double hist = Indicators.last(macd.histogram);
        lblMacd.setText(line == null ? "-" : String.format(Locale.US, "M:%s S:%s H:%s",
                formatVal(line), formatVal(signal), formatVal(hist)));
    }

    private void updateTradeRange(Instant firstTradeAt, Instant lastTradeAt) {
        if (lblTradeRange == null) {
            return;
        }
        if (firstTradeAt == null || lastTradeAt == null) {
            lblTradeRange.setText("-");
            return;
        }
        lblTradeRange.setText(TS_FMT.format(firstTradeAt) + " -> " + TS_FMT.format(lastTradeAt) + " (CL)");
    }

    private String formatVal(Double value) {
        if (value == null || value.isNaN() || value.isInfinite()) {
            return "-";
        }
        return String.format(Locale.US, "%.4f", value);
    }

    private int getTimeframeMinutes() {
        String tf = cmbTimeframe != null ? cmbTimeframe.getValue() : "1D";
        if (tf == null) return 1440;
        return switch (tf) {
            case "1m" -> 1;
            case "5m" -> 5;
            case "15m" -> 15;
            case "30m" -> 30;
            case "1h" -> 60;
            case "4h" -> 240;
            case "1D" -> 1440;
            default -> 1440;
        };
    }

    // =====================================================================
    //  Indicadores: OHLCV -> arreglos -> series de JFreeChart
    // =====================================================================

    /** Vista de las velas como arreglos, que es lo que consume {@link Indicators}. */
    private static final class Ohlcv {
        final double[] high;
        final double[] low;
        final double[] close;
        final double[] volume;
        final long[] time;

        private Ohlcv(int n) {
            high = new double[n];
            low = new double[n];
            close = new double[n];
            volume = new double[n];
            time = new long[n];
        }

        static Ohlcv from(List<OHLCDataItem> items) {
            int n = items == null ? 0 : items.size();
            Ohlcv o = new Ohlcv(n);
            for (int i = 0; i < n; i++) {
                OHLCDataItem it = items.get(i);
                o.high[i] = num(it.getHigh());
                o.low[i] = num(it.getLow());
                o.close[i] = num(it.getClose());
                o.volume[i] = num(it.getVolume());
                o.time[i] = it.getDate().getTime();
            }
            return o;
        }

        private static double num(Number v) {
            return v == null ? Double.NaN : v.doubleValue();
        }
    }

    /**
     * Convierte una serie de indicador en datos graficables.
     *
     * Dos cosas que importan:
     *  - Los NaN del warm-up se saltan; no se dibuja un 0 que parezca un dato.
     *  - Con splitByTradingDay se abre una serie nueva por dia de mercado, para que la linea no
     *    cruce el hueco nocturno con un trazo recto que no existio.
     */
    private XYSeriesCollection seriesOf(String name, List<OHLCDataItem> items,
                                        double[] values, boolean splitByTradingDay) {
        XYSeriesCollection out = new XYSeriesCollection();
        int n = Math.min(items == null ? 0 : items.size(), values == null ? 0 : values.length);
        XYSeries series = null;
        java.time.LocalDate seriesDate = null;
        for (int i = 0; i < n; i++) {
            if (Double.isNaN(values[i])) continue;
            java.time.LocalDate day = items.get(i).getDate().toInstant().atZone(MARKET_ZONE).toLocalDate();
            if (series == null || (splitByTradingDay && !day.equals(seriesDate))) {
                seriesDate = day;
                series = new XYSeries(splitByTradingDay ? name + " " + day : name);
                out.addSeries(series);
            }
            series.add(items.get(i).getDate().getTime(), values[i]);
        }
        if (out.getSeriesCount() == 0) out.addSeries(new XYSeries(name));
        return out;
    }

    /** Agrega una serie de linea al plot en el slot indicado y devuelve el siguiente slot libre. */
    private int addOverlay(XYPlot plot, int index, String name, List<OHLCDataItem> items,
                           double[] values, boolean split, Color color, float width) {
        plot.setDataset(index, seriesOf(name, items, values, split));
        XYLineAndShapeRenderer r = new XYLineAndShapeRenderer(true, false);
        r.setDefaultPaint(color);
        r.setDefaultStroke(new java.awt.BasicStroke(width));
        plot.setRenderer(index, r);
        return index + 1;
    }

    /**
     * Serie de Ichimoku proyectada hacia adelante: los ultimos `shift` puntos van mas alla de la
     * ultima vela, asi que sus timestamps se extrapolan con el paso de la ultima barra conocida.
     *
     * Es una aproximacion a proposito: en intradia el paso real salta el cierre de sesion. Sirve
     * para que la nube se vea adelantada, que es su unico objetivo; no son barras reales.
     */
    private XYSeriesCollection projectedSeries(String name, List<OHLCDataItem> items, double[] values) {
        XYSeriesCollection out = new XYSeriesCollection();
        XYSeries series = new XYSeries(name);
        int n = items == null ? 0 : items.size();
        if (n == 0 || values == null) {
            out.addSeries(series);
            return out;
        }
        long step = n >= 2
                ? items.get(n - 1).getDate().getTime() - items.get(n - 2).getDate().getTime()
                : 60_000L;
        if (step <= 0) step = 60_000L;
        long lastTs = items.get(n - 1).getDate().getTime();
        for (int i = 0; i < values.length; i++) {
            if (Double.isNaN(values[i])) continue;
            long ts = i < n ? items.get(i).getDate().getTime() : lastTs + (long) (i - n + 1) * step;
            series.add(ts, values[i]);
        }
        out.addSeries(series);
        return out;
    }

    /**
     * Junta dos colecciones en una sola, en orden. XYDifferenceRenderer exige las dos series
     * (A y B) en el MISMO dataset, en los indices 0 y 1, para poder rellenar entre ambas.
     */
    private XYSeriesCollection unionSeries(XYSeriesCollection a, XYSeriesCollection b) {
        XYSeriesCollection out = new XYSeriesCollection();
        for (int i = 0; i < a.getSeriesCount(); i++) out.addSeries(a.getSeries(i));
        for (int i = 0; i < b.getSeriesCount(); i++) out.addSeries(b.getSeries(i));
        return out;
    }

    /** Panel de indicador: eje propio, sin eje de tiempo (lo aporta el plot combinado). */
    private XYPlot subPlot(String axisLabel, Color bg, double lower, double upper) {
        NumberAxis axis = new NumberAxis(axisLabel);
        axis.setAutoRangeIncludesZero(false);
        axis.setLabelPaint(Color.WHITE);
        axis.setTickLabelPaint(Color.WHITE);
        axis.setLabelFont(axis.getLabelFont().deriveFont(10f));
        if (!Double.isNaN(lower) && !Double.isNaN(upper)) {
            axis.setAutoRange(false);
            axis.setRange(lower, upper);
        }
        XYPlot plot = new XYPlot(null, null, axis, null);
        stylePlot(plot, bg);
        return plot;
    }

    /** Marcador horizontal (niveles 30/70 del RSI, cero del MACD). */
    private ValueMarker banda(double value, Color color) {
        ValueMarker m = new ValueMarker(value);
        m.setPaint(color);
        m.setStroke(new java.awt.BasicStroke(1.0f, java.awt.BasicStroke.CAP_BUTT,
                java.awt.BasicStroke.JOIN_MITER, 10f, new float[]{4f, 4f}, 0f));
        return m;
    }

    /** Fondo, grillas y borde comunes a todos los paneles. */
    private void stylePlot(XYPlot plot, Color bg) {
        plot.setBackgroundPaint(bg);
        plot.setDomainGridlinesVisible(true);
        plot.setRangeGridlinesVisible(true);
        plot.setDomainGridlinePaint(new Color(0x66, 0x73, 0x82, 65));
        plot.setRangeGridlinePaint(new Color(0x66, 0x73, 0x82, 65));
        plot.setOutlinePaint(new Color(0x3a, 0x47, 0x55));
    }

    private Instant bucketize(Instant instant, int minutes) {
        ZonedDateTime z = instant.atZone(MARKET_ZONE).truncatedTo(ChronoUnit.MINUTES);
        if (minutes <= 1) {
            return z.toInstant();
        }
        if (minutes >= 1440) {
            return z.truncatedTo(ChronoUnit.DAYS).toInstant();
        }
        ZonedDateTime sessionStart = z.toLocalDate().atTime(MARKET_OPEN).atZone(MARKET_ZONE);
        long minutesFromOpen = Math.max(0L, ChronoUnit.MINUTES.between(sessionStart, z));
        return sessionStart.plusMinutes((minutesFromOpen / minutes) * minutes).toInstant();
    }

    private boolean isTradingBucket(Instant instant, int timeframeMinutes) {
        ZonedDateTime z = instant.atZone(MARKET_ZONE);
        DayOfWeek day = z.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return false;
        }
        if (timeframeMinutes >= 1440) {
            return true;
        }
        LocalTime t = z.toLocalTime();
        return !t.isBefore(MARKET_OPEN) && !t.isAfter(MARKET_CLOSE);
    }

    private boolean isWithinMarketSession(Instant instant) {
        ZonedDateTime z = instant.atZone(MARKET_ZONE);
        DayOfWeek day = z.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return false;
        }
        LocalTime t = z.toLocalTime();
        return !t.isBefore(MARKET_OPEN) && !t.isAfter(MARKET_CLOSE);
    }

    private Instant buildVisibleRangeStart(Instant firstBucket, Instant lastBucket, int timeframeMinutes) {
        if (timeframeMinutes >= 1440) {
            return firstBucket.minus(Duration.ofDays(1));
        }
        return firstBucket.minusSeconds(visibleRangePaddingSeconds(firstBucket, lastBucket, timeframeMinutes));
    }

    private Instant buildVisibleRangeEnd(Instant firstBucket, Instant lastBucket, int timeframeMinutes) {
        if (timeframeMinutes >= 1440) {
            return lastBucket.plus(Duration.ofDays(2));
        }
        long stepSeconds = Duration.ofMinutes(Math.max(1, timeframeMinutes)).toSeconds();
        long paddingSeconds = visibleRangePaddingSeconds(firstBucket, lastBucket, timeframeMinutes);
        return lastBucket.plusSeconds(stepSeconds + paddingSeconds);
    }

    Instant intradayRangeStart(Instant referenceBucket) {
        return referenceBucket.atZone(MARKET_ZONE).toLocalDate()
                .atTime(MARKET_OPEN.minusMinutes(15))
                .atZone(MARKET_ZONE)
                .toInstant();
    }

    Instant intradayRangeEnd(Instant referenceBucket) {
        return referenceBucket.atZone(MARKET_ZONE).toLocalDate()
                .atTime(MARKET_CLOSE.plusMinutes(15))
                .atZone(MARKET_ZONE)
                .toInstant();
    }

    private long maxCandleWidthMillis(int timeframeMinutes) {
        if (timeframeMinutes >= 1440) {
            return Duration.ofHours(18).toMillis();
        }
        if (timeframeMinutes >= 240) {
            return Duration.ofMinutes(45).toMillis();
        }
        if (timeframeMinutes >= 60) {
            return Duration.ofMinutes(30).toMillis();
        }
        if (timeframeMinutes >= 30) {
            return Duration.ofMinutes(15).toMillis();
        }
        if (timeframeMinutes >= 15) {
            return Duration.ofMinutes(5).toMillis();
        }
        if (timeframeMinutes >= 5) {
            return Duration.ofMinutes(3).toMillis();
        }
        return Duration.ofSeconds(45).toMillis();
    }

    private long visibleRangePaddingSeconds(Instant firstBucket, Instant lastBucket, int timeframeMinutes) {
        long stepSeconds = Duration.ofMinutes(Math.max(1, timeframeMinutes)).toSeconds();
        long spanSeconds = Math.max(stepSeconds, Duration.between(firstBucket, lastBucket).abs().toSeconds());
        if (firstBucket.equals(lastBucket)) {
            return Math.max(600L, stepSeconds / 2L);
        }
        return Math.max(stepSeconds / 2L, Math.min(1800L, Math.max(600L, spanSeconds / 20L)));
    }

    private void addTradingDayMarkers(XYPlot plot, List<OHLCDataItem> items, int timeframeMinutes) {
        if (plot == null || items == null || items.isEmpty()) {
            return;
        }
        if (timeframeMinutes >= 1440) {
            return;
        }
        java.time.LocalDate lastDate = null;
        for (OHLCDataItem item : items) {
            if (item == null || item.getDate() == null) {
                continue;
            }
            ZonedDateTime z = item.getDate().toInstant().atZone(MARKET_ZONE);
            java.time.LocalDate currentDate = z.toLocalDate();
            if (currentDate.equals(lastDate)) {
                continue;
            }
            lastDate = currentDate;
            Marker marker = new ValueMarker(item.getDate().getTime());
            marker.setPaint(new Color(255, 255, 255, 75));
            marker.setStroke(new java.awt.BasicStroke(0.8f));
            marker.setLabel(DAY_MARKER_FMT.format(z));
            marker.setLabelPaint(new Color(220, 220, 220));
            marker.setLabelAnchor(RectangleAnchor.TOP_LEFT);
            marker.setLabelTextAnchor(TextAnchor.TOP_LEFT);
            plot.addDomainMarker(marker, Layer.BACKGROUND);
        }
    }

    private void addLastPriceMarker(XYPlot plot, List<OHLCDataItem> items) {
        if (plot == null || items == null || items.isEmpty()) {
            return;
        }
        double lastPrice = items.get(items.size() - 1).getClose().doubleValue();
        Marker marker = new ValueMarker(lastPrice);
        marker.setPaint(new Color(0x9c, 0xa9, 0xb8));
        marker.setStroke(new java.awt.BasicStroke(
                0.9f,
                java.awt.BasicStroke.CAP_BUTT,
                java.awt.BasicStroke.JOIN_MITER,
                1f,
                new float[]{5f, 4f},
                0f
        ));
        marker.setLabel("Último " + String.format(Locale.US, "%,.4f", lastPrice));
        marker.setLabelPaint(new Color(0xd7, 0xdf, 0xe8));
        marker.setLabelAnchor(RectangleAnchor.TOP_RIGHT);
        marker.setLabelTextAnchor(TextAnchor.BOTTOM_RIGHT);
        plot.addRangeMarker(marker, Layer.FOREGROUND);
    }

    private String normalizeSymbol(String symbol) {
        if (symbol == null) {
            return "";
        }
        return symbol.trim().toUpperCase();
    }

    private void logRenderDebug(String symbol, String tf, List<MarketDataMessage.TradeGeneral> filtered, DatasetBuildResult built) {
        if (built == null) {
            return;
        }


    }

    private static class Ohlc {
        double open;
        double high;
        double low;
        double close;
        double volume;
        boolean initialized;

        void update(double price, double qty) {
            if (!initialized) {
                open = high = low = close = price;
                volume = qty;
                initialized = true;
                return;
            }
            high = Math.max(high, price);
            low = Math.min(low, price);
            close = price;
            volume += qty;
        }

    }

    static final class DirectionalCandlestickRenderer extends CandlestickRenderer {
        private final OHLCDataset dataset;
        private final Paint upPaint;
        private final Paint downPaint;

        DirectionalCandlestickRenderer(OHLCDataset dataset, Paint upPaint, Paint downPaint) {
            this.dataset = dataset;
            this.upPaint = upPaint;
            this.downPaint = downPaint;
        }

        @Override
        public Paint getItemOutlinePaint(int row, int column) {
            if (dataset == null || column < 0 || column >= dataset.getItemCount(row)) {
                return super.getItemOutlinePaint(row, column);
            }
            Number open = dataset.getOpen(row, column);
            Number close = dataset.getClose(row, column);
            if (open == null || close == null) {
                return super.getItemOutlinePaint(row, column);
            }
            return close.doubleValue() >= open.doubleValue() ? upPaint : downPaint;
        }
    }

    static class DatasetBuildResult {
        final OHLCDataset dataset;
        final List<OHLCDataItem> items;
        final List<Double> closes;
        final Instant firstBucket;
        final Instant lastBucket;
        final Instant firstTradeAt;
        final Instant lastTradeAt;

        DatasetBuildResult(
                OHLCDataset dataset,
                List<OHLCDataItem> items,
                List<Double> closes,
                Instant firstBucket,
                Instant lastBucket,
                Instant firstTradeAt,
                Instant lastTradeAt
        ) {
            this.dataset = dataset;
            this.items = items;
            this.closes = closes;
            this.firstBucket = firstBucket;
            this.lastBucket = lastBucket;
            this.firstTradeAt = firstTradeAt;
            this.lastTradeAt = lastTradeAt;
        }

        static DatasetBuildResult empty() {
            return new DatasetBuildResult(
                    new DefaultOHLCDataset("OHLC", new OHLCDataItem[0]),
                    new ArrayList<>(),
                    new ArrayList<>(),
                    null,
                    null,
                    null,
                    null
            );
        }
    }

}
