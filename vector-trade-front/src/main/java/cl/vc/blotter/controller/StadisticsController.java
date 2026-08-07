package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import javafx.application.Platform;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.XYChart;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.input.*;
import javafx.scene.layout.FlowPane;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.json.JSONObject;

import java.math.RoundingMode;
import java.text.DecimalFormatSymbols;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.stream.Collectors;

public class StadisticsController {


    @FXML private ComboBox<MarketDataMessage.SecurityExchangeMarketData> cmbSecurityExchange;
    @FXML public  ComboBox<RoutingMessage.SettlType> cmbSettlType;
    @FXML private ComboBox<String> filterCombo;
    @FXML private ComboBox<String> cmbSymbol;
    @FXML private ComboBox<String> cmbHistoryRange;
    @FXML private ComboBox<String> cmbHistoryTf;
    @FXML private DatePicker dpStatsDate;
    @FXML private FlowPane kpiWrap;


    @FXML private Label totalVolume;
    @FXML private Label montoTotal;
    @FXML private Label totalMarketCap;
    @FXML private Label positiveSentimentLabel;
    @FXML private Label negativeSentimentLabel;
    @FXML private Label marketTrendLabel;
    @FXML private Label avgVolatilityLabel;
    @FXML private Label lblRangoPromedio;
    @FXML private Label lblIndicePromedio;
    @FXML private Label lblIndiceMaximo;
    @FXML private Label lblIndiceMinimo;

    @FXML private Label lblLiquidezMedia;
    @FXML private Label lblNumeroTotalTrades;

    @FXML private Label lblCapitalizacionPromedio;
    @FXML private Label lblPrecioPromedioAcumulado;
    @FXML private Label lblPrecioMaximoAcumulado;

    @FXML private Label lblTendenciaPromedio;


    @FXML private TableView<MarketDataMessage.RankinSymbol> rankinSymbolTable;
    @FXML private TableColumn<MarketDataMessage.RankinSymbol, String> colSymbol;
    @FXML private TableColumn<MarketDataMessage.RankinSymbol, String> colPrecioUltimo;
    @FXML private TableColumn<MarketDataMessage.RankinSymbol, String> colVariacionPct;
    @FXML private TableColumn<MarketDataMessage.RankinSymbol, String> colVolumen;
    @FXML private TableColumn<MarketDataMessage.RankinSymbol, String> colMonto;
    @FXML private TableColumn<MarketDataMessage.RankinSymbol, String> colLiquid;
    @FXML private LineChart<String, Number> marketOverviewChart;
    @FXML private BarChart<String, Number> topVolumeChart;

    private final ObservableList<MarketDataMessage.RankinSymbol> tableItems = FXCollections.observableArrayList();
    private volatile MarketDataMessage.BolsaStats lastStats;
    private volatile LocalDate selectedStatsDate;
    /** Historico indexado por fecha, para pintar el calendario sin recorrer la lista por celda. */
    private volatile Map<LocalDate, MarketDataMessage.BolsaStats> statsPorDia = java.util.Collections.emptyMap();


    private final DecimalFormat dfPrice  = new DecimalFormat("#,##0.########");
    private final DecimalFormat dfPct    = new DecimalFormat("#,##0.00'%'");
    private final DecimalFormat dfNumber = new DecimalFormat("#,##0.##");
    private final DecimalFormat dfInt    = new DecimalFormat("#,##0");
    private final DecimalFormat dfRatio  = new DecimalFormat("#,##0.####");

    private static final KeyCodeCombination COPIAR = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);
    private final Map<String, Label> kpiPorNombre = new java.util.LinkedHashMap<>();
    private final Tooltip avisoCopia = new Tooltip("Copiado");

    public void initialize() {
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
        dfPct.setDecimalFormatSymbols(symbols);
        dfPct.setRoundingMode(RoundingMode.HALF_UP);

        if (kpiWrap != null) {
            kpiWrap.sceneProperty().addListener((obs, oldScene, newScene) -> {
                if (newScene != null) {
                    kpiWrap.prefWrapLengthProperty().bind(newScene.widthProperty().subtract(80));
                }
            });
        }

        if (filterCombo != null) {
            filterCombo.setItems(FXCollections.observableArrayList(
                    "Más tranzado",
                    "Más volátil",
                    "Mejores",
                    "Peores",
                    "Más cayó",
                    "Menos cayó"
            ));
            filterCombo.getSelectionModel().select("Más tranzado");
            filterCombo.valueProperty().addListener((obs, o, n) -> refreshTable());
        }

        if (cmbSettlType != null) {
            cmbSettlType.setItems(FXCollections.observableArrayList(
                    RoutingMessage.SettlType.T2,
                    RoutingMessage.SettlType.CASH,
                    RoutingMessage.SettlType.NEXT_DAY
            ));
            cmbSettlType.getSelectionModel().clearSelection();
            cmbSettlType.setPromptText("Todas");
            cmbSettlType.valueProperty().addListener((obs, oldValue, newValue) -> refreshTable());
        }

        if (cmbHistoryRange != null) {
            cmbHistoryRange.setItems(FXCollections.observableArrayList("Hoy", "7D", "30D"));
            cmbHistoryRange.getSelectionModel().select("7D");
            cmbHistoryRange.valueProperty().addListener((obs, o, n) -> refreshHistoryChart());
        }
        if (cmbHistoryTf != null) {
            cmbHistoryTf.setItems(FXCollections.observableArrayList("1d", "1h"));
            cmbHistoryTf.getSelectionModel().select("1d");
            cmbHistoryTf.valueProperty().addListener((obs, o, n) -> refreshHistoryChart());
        }
        if (dpStatsDate != null) {
            dpStatsDate.setValue(LocalDate.now(ZoneId.of("America/Santiago")));
            setupStatsDateCalendar();
        }

        // Combo mercado
        if (cmbSecurityExchange != null) {
            // Solo agregar BCS al ComboBox
            cmbSecurityExchange.setItems(FXCollections.observableArrayList(MarketDataMessage.SecurityExchangeMarketData.BCS));
            cmbSecurityExchange.getSelectionModel().select(MarketDataMessage.SecurityExchangeMarketData.BCS);
            cmbSecurityExchange.setDisable(true); // Desactivar el ComboBox para que no se pueda cambiar
        }

        if (Repository.getStats() != null) {
            lastStats = Repository.getStats();
            if (dpStatsDate != null) {
                dpStatsDate.setValue(resolveStatsDate(lastStats));
            }
            updateHeader(lastStats);
            refreshSymbolCombo(lastStats);
            refreshTable();
            refreshCharts(lastStats);
        } else {
            MarketDataMessage.BolsaStats emptyStats = MarketDataMessage.BolsaStats.newBuilder()
                    .setTendenciaGeneral("neutral")
                    .build();
            updateHeader(emptyStats);
            refreshSymbolCombo(emptyStats);
            refreshTable();
            refreshCharts(emptyStats);
        }
        refreshHistoryChart();

        // Columnas
        colSymbol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().getSymbol()));
        colPrecioUltimo.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(dfPrice.format(cd.getValue().getPrecioUltimo())));
        colVariacionPct.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(formatPercentage(cd.getValue().getVariacionPct())));
        colVolumen.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(dfNumber.format(cd.getValue().getVolumen())));
        colMonto.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(dfNumber.format(cd.getValue().getMonto())));
        colLiquid.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().getSettlType().name()));

        // Colorear variación 24h
        colVariacionPct.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(String value, boolean empty) {
                super.updateItem(value, empty);
                setText(empty ? null : value);
                getStyleClass().removeAll("positive", "negative");
                if (!empty) {
                    // Parsear con el mismo DecimalFormat que formateo el texto: manipular los
                    // separadores a mano depende del locale por defecto de la JVM.
                    try {
                        double v = dfPct.parse(value).doubleValue();
                        if (v > 0) getStyleClass().add("positive");
                        else if (v < 0) getStyleClass().add("negative");
                    } catch (Exception ignore) {}
                }
            }
        });

        rankinSymbolTable.setItems(tableItems);
        rankinSymbolTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        configurarCopia();
    }

    public void add(MarketDataMessage.BolsaStats stats) {
        if (stats == null) return;
        if (selectedStatsDate != null) {
            LocalDate incomingDate = resolveStatsDate(stats);
            if (!selectedStatsDate.equals(incomingDate)) {
                return;
            }
        }
        lastStats = stats;
        Platform.runLater(() -> {
            try {
                updateHeader(stats);
                refreshSymbolCombo(stats);
                refreshTable();
                refreshCharts(stats);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    public void addHistorical(MarketDataMessage.BolsaStats stats) {
        if (stats == null) {
            return;
        }
        Platform.runLater(() -> {
            if (selectedStatsDate != null) {
                refreshBySelectedDate();
            } else {
                refreshHistoryChart();
            }
        });
    }

    private void updateHeader(MarketDataMessage.BolsaStats s) {

        totalVolume.setText(dfNumber.format(s.getTotalVolumen()));
        montoTotal.setText(dfNumber.format(s.getMontoTotal()));
        totalMarketCap.setText(dfNumber.format(s.getCapitalizacionTotal()));
        positiveSentimentLabel.setText(dfPct.format(s.getSentimientoPositivo()));
        negativeSentimentLabel.setText(dfPct.format(s.getSentimientoNegativo()));
        marketTrendLabel.setText(s.getTendenciaGeneral().isBlank() ? "neutral" : s.getTendenciaGeneral());
        avgVolatilityLabel.setText(dfPct.format(s.getVolatilidadPromedio() * 100.0));
        lblRangoPromedio.setText(dfPrice.format(s.getRangoPromedio()));
        lblIndicePromedio.setText(dfPrice.format(s.getIndicePromedio()));
        lblIndiceMaximo.setText(dfPrice.format(s.getIndiceMaximo()));
        lblIndiceMinimo.setText(dfPrice.format(s.getIndiceMinimo()));
        lblLiquidezMedia.setText(dfRatio.format(s.getLiquidezMedia()));
        lblNumeroTotalTrades.setText(dfInt.format(s.getNumeroTotalTrades()));
        lblCapitalizacionPromedio.setText(dfNumber.format(s.getCapitalizacionPromedio()));
        lblPrecioPromedioAcumulado.setText(dfNumber.format(s.getPrecioPromedioAcumulado()));
        lblPrecioMaximoAcumulado.setText(dfNumber.format(s.getPrecioMaximoAcumulado()));
        lblTendenciaPromedio.setText(dfPct.format(s.getTendenciaPromedio()));
    }

    public void refreshTable() {
        if (lastStats == null) {
            tableItems.clear();
            refreshTopVolumeChart(List.of());
            return;
        }

        String sel = (filterCombo != null && filterCombo.getValue() != null)
                ? filterCombo.getValue() : "Más tranzado";


        List<MarketDataMessage.RankinSymbol> src;
        switch (sel) {
            case "Más volátil":  src = lastStats.getMasVolatilList(); break;
            case "Mejores":      src = lastStats.getBestRankinList(); break;
            case "Peores":       src = lastStats.getWorseRankinList(); break;
            case "Más cayó":     src = lastStats.getMasCayoList(); break;
            case "Menos cayó":   src = lastStats.getMenosCayoList(); break;
            case "Más tranzado":
            default:             src = lastStats.getMasTranzadoList(); break;
        }

        RoutingMessage.SettlType selectedSettlType =
                (cmbSettlType != null) ? cmbSettlType.getValue() : null;

        if (selectedSettlType != null) {
            src = filterBySettlType(src, selectedSettlType);
        }

        src = dedupeBySymbol(src);
        tableItems.setAll(src);
        refreshTopVolumeChart(src);
    }

    private List<MarketDataMessage.RankinSymbol> filterBySettlType(
            List<MarketDataMessage.RankinSymbol> source,
            RoutingMessage.SettlType selectedSettlType) {
        return source.stream()
                .filter(rs -> rs.getSettlType() == selectedSettlType)
                .collect(Collectors.toList());
    }

    /**
     * La key incluye la liquidacion: el servidor ya manda una fila por (symbol, exchange, settlType),
     * asi que colapsar solo por symbol borraba de la tabla el mismo papel en CASH o NEXT_DAY cuando
     * el filtro "Liquidacion" esta en "Todas".
     */
    private List<MarketDataMessage.RankinSymbol> dedupeBySymbol(List<MarketDataMessage.RankinSymbol> source) {
        return source.stream()
                .collect(Collectors.toMap(
                        rs -> normalizeSymbol(rs.getSymbol()) + "|" + rs.getSettlType().name(),
                        rs -> rs,
                        (a, b) -> a,
                        java.util.LinkedHashMap::new
                ))
                .values()
                .stream()
                .toList();
    }

    private void refreshCharts(MarketDataMessage.BolsaStats stats) {
        refreshHistoryChart();
        refreshTopVolumeChart(lastStats != null ? lastStats.getMasTranzadoList() : List.of());
    }

    private void refreshHistoryChart() {
        if (marketOverviewChart == null) {
            return;
        }
        String tf = cmbHistoryTf != null && cmbHistoryTf.getValue() != null ? cmbHistoryTf.getValue() : "1d";
        Instant from = computeFromInstant();

        List<HistoryPoint> points = new ArrayList<>();
        for (MarketDataMessage.BolsaStats s : Repository.getBolsaStatsHistory()) {
            HistoryPoint p = toHistoryPoint(s);
            if (p == null) {
                continue;
            }
            if (!tf.equalsIgnoreCase(p.tf)) {
                continue;
            }
            if (p.ts.isBefore(from)) {
                continue;
            }
            points.add(p);
        }
        points.sort(Comparator.comparing(p -> p.ts));

        XYChart.Series<String, Number> indiceSerie = new XYChart.Series<>();
        indiceSerie.setName("Indice Promedio");
        XYChart.Series<String, Number> montoSerie = new XYChart.Series<>();
        montoSerie.setName("Monto Total");
        ZoneId cl = ZoneId.of("America/Santiago");
        Map<String, HistoryPoint> byBucket = points.stream()
                .collect(Collectors.toMap(
                        p -> tf.equals("1h")
                                ? p.ts.atZone(cl).truncatedTo(ChronoUnit.HOURS).toString().substring(0, 16)
                                : p.ts.atZone(cl).toLocalDate().toString(),
                        p -> p,
                        (a, b) -> {
                            if (b.montoTotal > a.montoTotal) return b;
                            if (Double.compare(b.montoTotal, a.montoTotal) == 0 && b.ts.isAfter(a.ts)) return b;
                            return a;
                        },
                        java.util.LinkedHashMap::new
                ));

        byBucket.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.comparing(h -> h.ts)))
                .forEach(e -> {
                    HistoryPoint p = e.getValue();
                    String label = e.getKey();
                    indiceSerie.getData().add(new XYChart.Data<>(label, p.indicePromedio));
                    montoSerie.getData().add(new XYChart.Data<>(label, p.montoTotal));
                });

        marketOverviewChart.getData().setAll(indiceSerie, montoSerie);
    }

    @FXML
    private void applyDateFilter() {
        if (dpStatsDate == null) {
            return;
        }
        selectedStatsDate = dpStatsDate.getValue();
        refreshBySelectedDate();
    }

    @FXML
    private void clearDateFilter() {
        selectedStatsDate = null;
        if (dpStatsDate != null) {
            dpStatsDate.setValue(LocalDate.now(ZoneId.of("America/Santiago")));
        }
        MarketDataMessage.BolsaStats live = Repository.getStats();
        if (live != null) {
            lastStats = live;
            updateHeader(live);
            refreshSymbolCombo(live);
            refreshTable();
            refreshCharts(live);
        } else {
            tableItems.clear();
            refreshHistoryChart();
        }
    }

    private void refreshBySelectedDate() {
        if (selectedStatsDate == null) {
            return;
        }
        MarketDataMessage.BolsaStats selected = Repository.getBolsaStatsHistory().stream()
                .filter(s -> selectedStatsDate.equals(resolveStatsDate(s)))
                .max(Comparator.comparing(this::resolveStatsInstant))
                .orElse(null);

        if (selected == null) {
            requestHistoricalStatsForDate(selectedStatsDate);
            MarketDataMessage.BolsaStats empty = MarketDataMessage.BolsaStats.newBuilder()
                    .setTendenciaGeneral("neutral")
                    .build();
            lastStats = empty;
            updateHeader(empty);
            refreshSymbolCombo(empty);
            tableItems.clear();
            if (topVolumeChart != null) {
                topVolumeChart.getData().clear();
            }
            if (marketOverviewChart != null) {
                marketOverviewChart.getData().clear();
            }
            return;
        }

        lastStats = selected;
        updateHeader(selected);
        refreshSymbolCombo(selected);
        refreshTable();
        refreshHistoryChart();
    }

    private void requestHistoricalStatsForDate(LocalDate date) {
        if (date == null || Repository.getCandleClientService() == null) {
            return;
        }
        JSONObject request = new JSONObject()
                .put("action", "load_day_stats")
                .put("date", date.toString());
        Repository.getCandleClientService().sendMessage(request.toString());
    }

    private Instant computeFromInstant() {
        String range = cmbHistoryRange != null && cmbHistoryRange.getValue() != null ? cmbHistoryRange.getValue() : "7D";
        ZoneId cl = ZoneId.of("America/Santiago");
        if ("Hoy".equalsIgnoreCase(range)) {
            return LocalDate.now(cl).atStartOfDay(cl).toInstant();
        }
        if ("30D".equalsIgnoreCase(range)) {
            return Instant.now().minus(30, ChronoUnit.DAYS);
        }
        return Instant.now().minus(7, ChronoUnit.DAYS);
    }

    private HistoryPoint toHistoryPoint(MarketDataMessage.BolsaStats s) {
        if (s == null) {
            return null;
        }
        String tf = "1d";
        Instant ts = parseInstantSafe(s.getHoraFin(), Instant.now());
        String id = s.getId();
        if (id != null && id.startsWith("hist:")) {
            String[] parts = id.split(":", 3);
            if (parts.length == 3) {
                tf = parts[1];
                ts = parseInstantSafe(parts[2], ts);
            }
        }
        return new HistoryPoint(ts, tf, s.getIndicePromedio(), s.getMontoTotal());
    }

    private Instant parseInstantSafe(String value, Instant fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            return fallback;
        }
    }

    /**
     * Pinta bajo el numero de cada dia la cantidad de trades que hay para esa fecha, para
     * poder ver de un vistazo que dias tienen data antes de seleccionarlos.
     *
     * La fuente es Repository.getBolsaStatsHistory(), que el candle-service llena con hasta
     * mongo.market.history.days de historico. No se deshabilitan los dias sin data: al
     * seleccionar una fecha se pide igual al servidor (requestHistoricalStatsForDate), asi
     * que bloquearlos impediria consultar un dia que el front todavia no tiene cacheado.
     */
    private void setupStatsDateCalendar() {
        ObservableList<MarketDataMessage.BolsaStats> historial = Repository.getBolsaStatsHistory();
        reindexarHistorial(historial);

        dpStatsDate.setDayCellFactory(picker -> new DateCell() {
            @Override
            public void updateItem(LocalDate item, boolean empty) {
                super.updateItem(item, empty);
                getStyleClass().remove("stats-day-con-data");
                if (empty || item == null) {
                    setGraphic(null);
                    setTooltip(null);
                    return;
                }
                MarketDataMessage.BolsaStats stats = statsPorDia.get(item);
                if (stats == null) {
                    setGraphic(null);
                    setTooltip(null);
                    return;
                }
                getStyleClass().add("stats-day-con-data");
                setContentDisplay(ContentDisplay.BOTTOM);
                setGraphicTextGap(0);

                // Hay dias con volumen y monto reales pero sin conteo de trades (llegaron por la
                // estadistica diaria, no por trades individuales). Marcarlos con un punto en vez de
                // dejar la celda en blanco: si no, un dia con 140 mil millones transados se ve vacio.
                boolean conTrades = stats.getNumeroTotalTrades() > 0;
                Label marca = new Label(conTrades ? compacto(stats.getNumeroTotalTrades()) : "·");
                marca.getStyleClass().add("stats-day-dato");
                setGraphic(marca);

                setTooltip(new Tooltip(
                        (conTrades
                                ? "Trades: " + dfInt.format(stats.getNumeroTotalTrades())
                                : "Trades: sin conteo (estadistica diaria)")
                                + "\nMonto: " + dfNumber.format(stats.getMontoTotal())
                                + "\nVolumen: " + dfNumber.format(stats.getTotalVolumen())));
            }

        });

        // El historico llega por WebSocket despues de abrir la ventana: hay que repintar.
        historial.addListener((javafx.collections.ListChangeListener<MarketDataMessage.BolsaStats>) c -> {
            reindexarHistorial(historial);
            Platform.runLater(this::repintarCalendario);
        });
    }

    /**
     * Indexa el historico por fecha una sola vez por cambio, en vez de recorrer la lista
     * dentro de cada celda: son ~42 celdas por repintado contra hasta 2000 entradas.
     * Se queda con el ultimo stats de cada dia, que es el mas completo.
     */
    private void reindexarHistorial(List<MarketDataMessage.BolsaStats> historial) {
        Map<LocalDate, MarketDataMessage.BolsaStats> indice = new java.util.HashMap<>();
        for (MarketDataMessage.BolsaStats s : historial) {
            indice.put(resolveStatsDate(s), s);
        }
        statsPorDia = indice;
    }

    /** Fuerza a DatePicker a reconstruir las celdas reaplicando el factory. */
    private void repintarCalendario() {
        if (dpStatsDate == null) return;
        javafx.util.Callback<DatePicker, DateCell> factory = dpStatsDate.getDayCellFactory();
        dpStatsDate.setDayCellFactory(null);
        dpStatsDate.setDayCellFactory(factory);
    }

    /** 12400 -> "12,4K". Cabe en la celda del calendario. */
    private String compacto(long valor) {
        if (valor <= 0) return "";
        if (valor >= 1_000_000) return dfNumber.format(valor / 1_000_000d) + "M";
        if (valor >= 1_000)     return dfNumber.format(valor / 1_000d) + "K";
        return String.valueOf(valor);
    }

    private LocalDate resolveStatsDate(MarketDataMessage.BolsaStats stats) {
        Instant ts = resolveStatsInstant(stats);
        return ts.atZone(ZoneId.of("America/Santiago")).toLocalDate();
    }

    private Instant resolveStatsInstant(MarketDataMessage.BolsaStats stats) {
        if (stats == null) {
            return Instant.EPOCH;
        }
        String id = stats.getId();
        if (id != null && id.startsWith("hist:")) {
            String[] parts = id.split(":", 3);
            if (parts.length == 3) {
                return parseInstantSafe(parts[2], Instant.EPOCH);
            }
        }
        Instant byHoraFin = parseInstantSafe(stats.getHoraFin(), Instant.EPOCH);
        if (!Instant.EPOCH.equals(byHoraFin)) {
            return byHoraFin;
        }
        String day = stats.getHoraInicio();
        if (day != null && !day.isBlank()) {
            try {
                return LocalDateTime.parse(day).atZone(ZoneId.of("America/Santiago")).toInstant();
            } catch (Exception ignored) {
            }
        }
        // Sin marca de tiempo son estadisticas en vivo: valen para hoy. Devolver EPOCH las
        // fechaba en 31-12-1969 (EPOCH en America/Santiago) y arrastraba esa fecha al
        // DatePicker, al calendario y al eje del grafico historico.
        return Instant.now();
    }

    private void refreshTopVolumeChart(List<MarketDataMessage.RankinSymbol> source) {
        if (topVolumeChart == null) {
            return;
        }
        XYChart.Series<String, Number> serie = new XYChart.Series<>();
        serie.setName("Volumen");
        source.stream()
                .limit(10)
                .forEach(r -> serie.getData().add(new XYChart.Data<>(r.getSymbol(), r.getVolumen())));
        topVolumeChart.getData().setAll(serie);
    }

    private void refreshSymbolCombo(MarketDataMessage.BolsaStats stats) {
        if (cmbSymbol == null || stats == null) {
            return;
        }

        String selected = cmbSymbol.getValue();
        LinkedHashSet<String> symbols = new LinkedHashSet<>();
        stats.getMasTranzadoList().forEach(r -> symbols.add(normalizeSymbol(r.getSymbol())));
        stats.getMasVolatilList().forEach(r -> symbols.add(normalizeSymbol(r.getSymbol())));
        stats.getBestRankinList().forEach(r -> symbols.add(normalizeSymbol(r.getSymbol())));
        stats.getWorseRankinList().forEach(r -> symbols.add(normalizeSymbol(r.getSymbol())));
        symbols.remove("");

        ObservableList<String> items = FXCollections.observableArrayList(symbols);
        cmbSymbol.setItems(items);
        if (selected != null && items.contains(selected)) {
            cmbSymbol.getSelectionModel().select(selected);
        } else if (!items.isEmpty()) {
            cmbSymbol.getSelectionModel().selectFirst();
        }
    }

    @FXML
    private void openCandlesForSymbol() {
        try {
            String symbol = cmbSymbol != null ? cmbSymbol.getValue() : null;
            if (symbol == null || symbol.isBlank()) {
                return;
            }

            Repository.setSelectedCandleSymbol(symbol);

            FXMLLoader loader = new FXMLLoader(this.getClass().getResource("/view/Candle.fxml"));
            Parent mainPane = loader.load();
            Stage stage = new Stage();
            Scene scene = new Scene(mainPane, 1100, 700);
            scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource(Repository.getSTYLE())).toExternalForm());
            stage.setScene(scene);
            stage.setTitle("Velas - " + symbol);
            stage.show();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Los valores del panel se validan fuera de la app (planillas, scripts contra Mongo), asi que
     * tienen que poder copiarse en vez de transcribirse a mano: click en un KPI copia su valor,
     * Ctrl+click copia "etiqueta<TAB>valor", Ctrl+C copia las filas seleccionadas de la tabla y
     * "Copiar Datos" copia el panel completo en TSV.
     */
    private void configurarCopia() {
        registrarKpi("Volumen Total", totalVolume);
        registrarKpi("Monto Total", montoTotal);
        registrarKpi("Capitalizacion Total", totalMarketCap);
        registrarKpi("Tendencia General", marketTrendLabel);
        registrarKpi("Volatilidad Promedio", avgVolatilityLabel);
        registrarKpi("Sentimiento Positivo", positiveSentimentLabel);
        registrarKpi("Sentimiento Negativo", negativeSentimentLabel);
        registrarKpi("Rango Promedio", lblRangoPromedio);
        registrarKpi("Indice Promedio", lblIndicePromedio);
        registrarKpi("Indice Maximo", lblIndiceMaximo);
        registrarKpi("Indice Minimo", lblIndiceMinimo);
        registrarKpi("Liquidez Media", lblLiquidezMedia);
        registrarKpi("N Total Trades", lblNumeroTotalTrades);
        registrarKpi("Cap. Promedio", lblCapitalizacionPromedio);
        registrarKpi("Precio Prom. Acum.", lblPrecioPromedioAcumulado);
        registrarKpi("Precio Max. Acum.", lblPrecioMaximoAcumulado);
        registrarKpi("Tendencia Promedio", lblTendenciaPromedio);

        kpiPorNombre.forEach((nombre, valor) -> {
            valor.setCursor(javafx.scene.Cursor.HAND);
            Tooltip.install(valor, new Tooltip("Click: copia el valor\nCtrl+click: copia etiqueta y valor"));
            valor.setOnMouseClicked(e ->
                    copiar(e.isShortcutDown() ? nombre + "\t" + valor.getText() : valor.getText(), valor));
        });

        if (rankinSymbolTable == null) {
            return;
        }
        rankinSymbolTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        MenuItem copiarSeleccion = new MenuItem("Copiar seleccion");
        copiarSeleccion.setOnAction(e -> copiar(tablaComoTsv(true), rankinSymbolTable));
        MenuItem copiarTabla = new MenuItem("Copiar tabla completa");
        copiarTabla.setOnAction(e -> copiar(tablaComoTsv(false), rankinSymbolTable));
        MenuItem copiarPanel = new MenuItem("Copiar todo el panel");
        copiarPanel.setOnAction(e -> copiarTodo());
        rankinSymbolTable.setContextMenu(new ContextMenu(copiarSeleccion, copiarTabla, copiarPanel));

        rankinSymbolTable.setOnKeyPressed(e -> {
            if (COPIAR.match(e)) {
                copiar(tablaComoTsv(true), rankinSymbolTable);
                e.consume();
            }
        });
    }

    private void registrarKpi(String nombre, Label valor) {
        if (valor != null) {
            kpiPorNombre.put(nombre, valor);
        }
    }

    /** Copia el panel entero: contexto del filtro + KPIs + tabla, en TSV pegable en una planilla. */
    @FXML
    private void copiarTodo() {
        StringBuilder sb = new StringBuilder();
        sb.append("Fecha Stats\t").append(dpStatsDate != null && dpStatsDate.getValue() != null ? dpStatsDate.getValue() : "").append('\n');
        sb.append("Indicador\t").append(filterCombo != null && filterCombo.getValue() != null ? filterCombo.getValue() : "").append('\n');
        sb.append("Liquidacion\t").append(cmbSettlType != null && cmbSettlType.getValue() != null ? cmbSettlType.getValue().name() : "Todas").append('\n');
        if (lastStats != null) {
            sb.append("Id\t").append(lastStats.getId()).append('\n');
            sb.append("Hora Inicio\t").append(lastStats.getHoraInicio()).append('\n');
            sb.append("Hora Fin\t").append(lastStats.getHoraFin()).append('\n');
        }
        sb.append('\n');
        kpiPorNombre.forEach((nombre, valor) -> sb.append(nombre).append('\t').append(valor.getText()).append('\n'));
        sb.append('\n').append(tablaComoTsv(false));
        copiar(sb.toString(), kpiWrap != null ? kpiWrap : rankinSymbolTable);
    }

    /** Toma el texto de las columnas, no el modelo: lo que se copia es exactamente lo que se ve. */
    private String tablaComoTsv(boolean soloSeleccion) {
        List<MarketDataMessage.RankinSymbol> filas =
                soloSeleccion && !rankinSymbolTable.getSelectionModel().getSelectedItems().isEmpty()
                        ? new ArrayList<>(rankinSymbolTable.getSelectionModel().getSelectedItems())
                        : new ArrayList<>(tableItems);

        StringBuilder sb = new StringBuilder("Instrumento\tPrecio\t24h %\tVolumen\tMonto\tLiquidacion\n");
        for (MarketDataMessage.RankinSymbol fila : filas) {
            sb.append(colSymbol.getCellData(fila)).append('\t')
              .append(colPrecioUltimo.getCellData(fila)).append('\t')
              .append(colVariacionPct.getCellData(fila)).append('\t')
              .append(colVolumen.getCellData(fila)).append('\t')
              .append(colMonto.getCellData(fila)).append('\t')
              .append(colLiquid.getCellData(fila)).append('\n');
        }
        return sb.toString();
    }

    private void copiar(String texto, Node ancla) {
        if (texto == null || texto.isBlank()) {
            return;
        }
        ClipboardContent contenido = new ClipboardContent();
        contenido.putString(texto);
        Clipboard.getSystemClipboard().setContent(contenido);
        avisarCopiado(ancla);
    }

    /** Sin este aviso no hay forma de saber si el click copio algo. */
    private void avisarCopiado(Node ancla) {
        if (ancla == null || ancla.getScene() == null || ancla.getScene().getWindow() == null) {
            return;
        }
        javafx.geometry.Bounds bounds = ancla.localToScreen(ancla.getBoundsInLocal());
        if (bounds == null) {
            return;
        }
        avisoCopia.show(ancla, bounds.getMinX(), bounds.getMaxY());
        javafx.animation.PauseTransition cierre = new javafx.animation.PauseTransition(javafx.util.Duration.millis(700));
        cierre.setOnFinished(e -> avisoCopia.hide());
        cierre.play();
    }

    private String normalizeSymbol(String symbol) {
        if (symbol == null) {
            return "";
        }
        return symbol.trim().toUpperCase();
    }

    private String formatPercentage(double value) {
        return dfPct.format(value);
    }

    private record HistoryPoint(Instant ts, String tf, double indicePromedio, double montoTotal) { }

}
