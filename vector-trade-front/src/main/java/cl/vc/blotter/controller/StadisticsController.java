package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import javafx.application.Platform;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.XYChart;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

import java.text.DecimalFormat;
import java.util.List;
import java.util.stream.Collectors;

public class StadisticsController {


    @FXML private ComboBox<MarketDataMessage.SecurityExchangeMarketData> cmbSecurityExchange;
    @FXML public  ComboBox<RoutingMessage.SettlType> cmbSettlType;
    @FXML private ComboBox<String> filterCombo;


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


    private final DecimalFormat dfPrice  = new DecimalFormat("#,##0.########");
    private final DecimalFormat dfPct    = new DecimalFormat("#,##0.00'%'");
    private final DecimalFormat dfNumber = new DecimalFormat("#,##0.##");
    private final DecimalFormat dfInt    = new DecimalFormat("#,##0");
    private final DecimalFormat dfRatio  = new DecimalFormat("#,##0.####");

    public void initialize() {





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
            cmbSettlType.getSelectionModel().select(RoutingMessage.SettlType.T2);
            cmbSettlType.valueProperty().addListener((obs, oldValue, newValue) -> refreshTable());
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
            updateHeader(lastStats);
            refreshTable();
            refreshCharts(lastStats);
        }

        // Columnas
        colSymbol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().getSymbol()));
        colPrecioUltimo.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(dfPrice.format(cd.getValue().getPrecioUltimo())));
        colVariacionPct.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(dfPct.format(cd.getValue().getVariacionPct())));
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
                    String raw = value.replace("%","").replace(".","").replace(",",".");
                    try {
                        double v = Double.parseDouble(raw);
                        if (v > 0) getStyleClass().add("positive");
                        else if (v < 0) getStyleClass().add("negative");
                    } catch (Exception ignore) {}
                }
            }
        });

        rankinSymbolTable.setItems(tableItems);
        rankinSymbolTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
    }

    public void add(MarketDataMessage.BolsaStats stats) {
        if (stats == null) return;
        lastStats = stats;
        Platform.runLater(() -> {
            try {
                updateHeader(stats);
                refreshTable();
                refreshCharts(stats);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    private void updateHeader(MarketDataMessage.BolsaStats s) {

        totalVolume.setText(dfNumber.format(s.getTotalVolumen()));
        montoTotal.setText(dfNumber.format(s.getMontoTotal()));
        totalMarketCap.setText(dfNumber.format(s.getCapitalizacionTotal()));
        positiveSentimentLabel.setText(dfPct.format(s.getSentimientoPositivo()));
        negativeSentimentLabel.setText(dfPct.format(s.getSentimientoNegativo()));
        marketTrendLabel.setText(s.getTendenciaGeneral());
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
        if (lastStats == null) return;

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

    private void refreshCharts(MarketDataMessage.BolsaStats stats) {
        if (marketOverviewChart != null) {
            XYChart.Series<String, Number> serie = new XYChart.Series<>();
            serie.setName("Resumen");
            serie.getData().add(new XYChart.Data<>("Volatilidad", stats.getVolatilidadPromedio() * 100.0));
            serie.getData().add(new XYChart.Data<>("Sent. +", stats.getSentimientoPositivo()));
            serie.getData().add(new XYChart.Data<>("Sent. -", stats.getSentimientoNegativo()));
            serie.getData().add(new XYChart.Data<>("Tendencia", stats.getTendenciaPromedio()));
            marketOverviewChart.getData().setAll(serie);
        }
        refreshTopVolumeChart(lastStats != null ? lastStats.getMasTranzadoList() : List.of());
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

}
