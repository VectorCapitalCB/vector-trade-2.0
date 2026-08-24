package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.model.HistoricalTradingAnalytics;
import cl.vc.blotter.utils.Notifier;
import cl.vc.blotter.utils.I18n;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.animation.PauseTransition;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.chart.PieChart;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.util.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.LegendPosition;
import org.apache.poi.xddf.usermodel.chart.XDDFChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFChartLegend;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFNumericalDataSource;
import org.apache.poi.xssf.usermodel.XSSFChart;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFDrawing;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class HistoricalOrdersController {

    private static final String ALL_ACCOUNTS = "Todas";
    private static final int EXPORT_LIMIT = 10_000;
    private static final int REPORT_LAST_COLUMN = 13;
    private static final DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    @FXML private ComboBox<String> accountFilter;
    @FXML private DatePicker fromDate;
    @FXML private DatePicker toDate;
    @FXML private TextField symbolFilter;
    @FXML private Button refreshButton;
    @FXML private Button exportAllButton;
    @FXML private Label resultLabel;
    @FXML private Label ordersMetric;
    @FXML private Label fillsMetric;
    @FXML private Label buyAveragePriceMetric;
    @FXML private Label sellAveragePriceMetric;
    @FXML private Label buyAmountMetric;
    @FXML private Label sellAmountMetric;
    @FXML private Label realizedPnlMetric;
    @FXML private Label amountBySymbolTitle;
    @FXML private PieChart amountBySymbolChart;
    @FXML private VBox amountBySymbolLegend;
    @FXML private TableView<BlotterMessage.HistoricalOrderGroup> ordersTable;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> dateColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, RoutingMessage.Side> sideColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> symbolColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> accountColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> orderQtyColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> executedQtyColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> avgPriceColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> amountColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> statusColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, Number> fillsColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, String> orderIdColumn;
    @FXML private TableColumn<BlotterMessage.HistoricalOrderGroup, Void> detailColumn;

    private final ObservableList<BlotterMessage.HistoricalOrderGroup> data = FXCollections.observableArrayList();
    private final PauseTransition liveRefresh = new PauseTransition(Duration.seconds(1.5));
    private String pendingRequestId = "";
    private String pendingExportRequestId = "";
    private String pendingSymbol = "";
    private File pendingExportFile;
    private ExportContext pendingExportContext;
    private boolean active;
    private HistoricalTradingAnalytics.Snapshot analytics = HistoricalTradingAnalytics.calculate(List.of());

    @FXML
    private void initialize() {
        LocalDate today = LocalDate.now(Repository.getZoneID());
        fromDate.setValue(today.minusDays(7));
        toDate.setValue(today);
        symbolFilter.setTextFormatter(new TextFormatter<String>(change -> {
            change.setText(change.getText().toUpperCase(Locale.ROOT));
            return change;
        }));

        dateColumn.setCellValueFactory(c -> new SimpleStringProperty(formatTime(c.getValue().getSummary())));
        sideColumn.setCellValueFactory(c -> new SimpleObjectProperty<>(c.getValue().getSummary().getSide()));
        sideColumn.setCellFactory(ignored -> sideCell());
        symbolColumn.setCellValueFactory(c -> new SimpleStringProperty(c.getValue().getSummary().getSymbol()));
        accountColumn.setCellValueFactory(c -> new SimpleStringProperty(c.getValue().getSummary().getAccount()));
        orderQtyColumn.setCellValueFactory(c -> new SimpleStringProperty(formatQuantity(c.getValue().getSummary().getOrderQty())));
        executedQtyColumn.setCellValueFactory(c -> new SimpleStringProperty(formatQuantity(c.getValue().getSummary().getCumQty())));
        avgPriceColumn.setCellValueFactory(c -> new SimpleStringProperty(formatPrice(c.getValue().getSummary().getAvgPrice())));
        amountColumn.setCellValueFactory(c -> new SimpleStringProperty(formatMoney(executedAmount(c.getValue()))));
        statusColumn.setCellValueFactory(c -> new SimpleStringProperty(c.getValue().getSummary().getOrdStatus().name()));
        statusColumn.setCellFactory(ignored -> statusCell());
        fillsColumn.setCellValueFactory(c -> new SimpleObjectProperty<>(c.getValue().getExecutionsCount()));
        orderIdColumn.setCellValueFactory(c -> new SimpleStringProperty(fixOrderId(c.getValue())));
        configureDetailColumn();
        ordersTable.setItems(data);
        ordersTable.getStyleClass().add("historical-orders-table");
        ordersTable.getColumns().forEach(column -> column.setStyle("-fx-alignment: CENTER;"));

        liveRefresh.setOnFinished(event -> {
            if (active) requestHistory();
        });
    }

    public void activate() {
        active = true;
        syncAccounts();
        requestHistory();
    }

    public void deactivate() {
        active = false;
        liveRefresh.stop();
    }

    public void onLiveExecution(RoutingMessage.Order order) {
        if (!active || order == null || order.getExecType() != RoutingMessage.ExecutionType.EXEC_TRADE) return;
        RoutingMessage.OrderStatus status = order.getOrdStatus();
        if (status != RoutingMessage.OrderStatus.FILLED
                && status != RoutingMessage.OrderStatus.PARTIALLY_FILLED) return;
        liveRefresh.playFromStart();
    }

    @FXML
    private void refresh() {
        requestHistory();
    }

    private void requestHistory() {
        if (Repository.getClientService() == null) {
            resultLabel.setText("Sin conexión al core");
            return;
        }
        syncAccounts();
        LocalDate from = fromDate.getValue();
        LocalDate to = toDate.getValue();
        if (from != null && to != null && from.isAfter(to)) {
            Notifier.INSTANCE.notifyError("Órdenes Históricas", "La fecha Desde no puede ser posterior a Hasta.");
            return;
        }

        pendingRequestId = UUID.randomUUID().toString();
        pendingSymbol = symbolFilter.getText() == null ? "" : symbolFilter.getText().trim();
        BlotterMessage.HistoricalOrdersRequest.Builder request =
                BlotterMessage.HistoricalOrdersRequest.newBuilder()
                        .setRequestId(pendingRequestId)
                        .setSymbol(pendingSymbol)
                        .setFrom(from == null ? "" : from.toString())
                        .setTo(to == null ? "" : to.toString())
                        .setLimit(500);
        String selectedAccount = accountFilter.getValue();
        if (selectedAccount != null && !ALL_ACCOUNTS.equals(selectedAccount)) {
            request.addAccounts(selectedAccount);
        }

        refreshButton.setDisable(true);
        resultLabel.setText("Consultando…");
        Repository.getClientService().sendMessage(request.build());
    }

    public void applySnapshot(BlotterMessage.HistoricalOrdersSnapshot snapshot) {
        if (snapshot == null) return;
        if (snapshot.getRequestId().equals(pendingExportRequestId)) {
            applyExportSnapshot(snapshot);
            return;
        }
        if (!snapshot.getRequestId().equals(pendingRequestId)) return;
        Platform.runLater(() -> {
            refreshButton.setDisable(false);
            if (!snapshot.getError().isBlank()) {
                resultLabel.setText(snapshot.getError());
                return;
            }
            data.setAll(snapshot.getOrdersList());
            updateAnalytics();
            resultLabel.setText(data.size() + (data.size() == 1 ? " orden" : " órdenes")
                    + (snapshot.getTruncated() ? " · mostrando las primeras 500" : ""));
        });
    }

    private void syncAccounts() {
        String previous = accountFilter.getValue();
        List<String> accounts = Repository.getUser() == null ? List.of() : Repository.getUser().getAccountList();
        ObservableList<String> values = FXCollections.observableArrayList(ALL_ACCOUNTS);
        values.addAll(accounts);
        accountFilter.setItems(values);
        accountFilter.setValue(previous != null && values.contains(previous) ? previous : ALL_ACCOUNTS);
    }

    private void configureDetailColumn() {
        detailColumn.setCellFactory(ignored -> new TableCell<>() {
            private final Button button = new Button("Detalle");
            {
                button.getStyleClass().add("historical-detail-button");
                button.setOnAction(event -> {
                    BlotterMessage.HistoricalOrderGroup group = getTableRow().getItem();
                    if (group != null) showExecutions(group);
                });
            }

            @Override
            protected void updateItem(Void item, boolean empty) {
                super.updateItem(item, empty);
                setGraphic(empty ? null : button);
                setAlignment(Pos.CENTER);
            }
        });
    }

    private void showExecutions(BlotterMessage.HistoricalOrderGroup group) {
        RoutingMessage.Order summary = group.getSummary();
        double orderPnl = analytics.realizedPnl(group);
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Detalle de ejecuciones");
        dialog.setHeaderText(summary.getSymbol() + " · " + sideText(summary.getSide())
                + " · OrderID FIX " + fixOrderId(group));
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);

        TableView<RoutingMessage.Order> table = new TableView<>();
        table.setItems(FXCollections.observableArrayList(group.getExecutionsList()));
        table.getStyleClass().addAll("table-execution-style", "historical-detail-table");
        table.getColumns().add(stringColumn("Hora", 130, this::formatTime));
        table.getColumns().add(stringColumn("Exec ID", 160, RoutingMessage.Order::getExecId));
        table.getColumns().add(stringColumn("Cantidad", 105, o -> formatQuantity(o.getLastQty())));
        table.getColumns().add(stringColumn("Precio", 105, o -> formatPrice(o.getLastPx())));
        table.getColumns().add(stringColumn("Monto", 135, o -> formatMoney(o.getLastQty() * o.getLastPx())));
        table.getColumns().add(stringColumn("Acumulado", 110, o -> formatQuantity(o.getCumQty())));
        table.getColumns().add(stringColumn("Estado", 95, o -> statusText(o.getOrdStatus())));
        if (hasCounterparty(group.getExecutionsList())) {
            table.getColumns().add(stringColumn("Contraparte", 105, RoutingMessage.Order::getContraBroker));
        }
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);

        Label totals = new Label(group.getExecutionsCount() + " palos · "
                + formatQuantity(summary.getCumQty()) + " ejecutadas · Promedio "
                + formatPrice(summary.getAvgPrice()) + " · Monto " + formatMoney(executedAmount(group)));
        totals.getStyleClass().add("historical-detail-total");
        Button exportButton = new Button("Exportar Excel");
        exportButton.getStyleClass().add("historical-export-button");
        exportButton.setOnAction(event -> exportExecutions(group));
        Region summarySpacer = new Region();
        HBox.setHgrow(summarySpacer, Priority.ALWAYS);
        HBox summaryBar = new HBox(10, totals, summarySpacer, exportButton);
        summaryBar.setAlignment(Pos.CENTER_LEFT);
        HBox metrics = new HBox(8);
        metrics.setAlignment(Pos.CENTER_LEFT);
        metrics.getChildren().addAll(
                detailMetric("Cantidad orden", formatQuantity(summary.getOrderQty()), ""),
                detailMetric("Cantidad ejecutada", formatQuantity(summary.getCumQty()), "metric-buy"),
                detailMetric("Precio promedio", formatPrice(summary.getAvgPrice()), ""),
                detailMetric("Monto ejecutado", formatMoney(executedAmount(group)), ""),
                detailMetric("P&L realizado (rango)", formatSignedMoney(orderPnl), pnlStyle(orderPnl)));
        metrics.getStyleClass().add("historical-detail-metrics");

        PieChart priceDistribution = executionPriceChart(group);
        HBox analyticsBar = new HBox(10, metrics, priceDistribution);
        analyticsBar.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(metrics, Priority.ALWAYS);

        VBox content = new VBox(8, summaryBar, analyticsBar, table);
        content.getStyleClass().add("historical-detail-content");
        VBox.setVgrow(table, Priority.ALWAYS);
        content.setPrefSize(1240, 570);
        dialog.getDialogPane().setContent(content);
        if (ordersTable.getScene() != null) {
            dialog.initOwner(ordersTable.getScene().getWindow());
        }
        String theme = Repository.isDayMode() ? "/blotter/css/daymode.css" : Repository.getSTYLE();
        dialog.getDialogPane().getStylesheets().setAll(
                Objects.requireNonNull(getClass().getResource(theme)).toExternalForm());
        dialog.getDialogPane().getStyleClass().add("historical-detail-dialog");
        dialog.showAndWait();
    }

    private TableColumn<RoutingMessage.Order, String> stringColumn(
            String title, double width, java.util.function.Function<RoutingMessage.Order, String> mapper) {
        TableColumn<RoutingMessage.Order, String> column = new TableColumn<>(title);
        column.setPrefWidth(width);
        column.setCellValueFactory(c -> new SimpleStringProperty(mapper.apply(c.getValue())));
        column.setStyle("-fx-alignment: CENTER;");
        return column;
    }

    @FXML
    private void exportAll() {
        if (Repository.getClientService() == null) {
            Notifier.INSTANCE.notifyError("Órdenes Históricas", "Sin conexión al core.");
            return;
        }
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Exportar historial de órdenes");
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Excel (*.xlsx)", "*.xlsx"));
        String fileSymbol = symbolFilter.getText() == null || symbolFilter.getText().isBlank()
                ? "todos" : safeFilePart(symbolFilter.getText().trim());
        chooser.setInitialFileName("ordenes_historicas_" + fileSymbol + "_"
                + LocalDate.now(Repository.getZoneID()) + ".xlsx");
        File downloads = new File(System.getProperty("user.home"), "Downloads");
        if (downloads.isDirectory()) chooser.setInitialDirectory(downloads);
        File file = chooser.showSaveDialog(ordersTable.getScene().getWindow());
        if (file == null) return;
        if (!file.getName().toLowerCase(Locale.ROOT).endsWith(".xlsx")) {
            file = new File(file.getParentFile(), file.getName() + ".xlsx");
        }

        LocalDate from = fromDate.getValue();
        LocalDate to = toDate.getValue();
        String selectedAccount = accountFilter.getValue();
        String symbol = symbolFilter.getText() == null ? "" : symbolFilter.getText().trim();
        pendingExportFile = file;
        pendingExportContext = new ExportContext(
                selectedAccount == null ? ALL_ACCOUNTS : selectedAccount,
                from,
                to,
                symbol,
                DATE_TIME.format(Instant.now().atZone(Repository.getZoneID())));
        pendingExportRequestId = UUID.randomUUID().toString();

        BlotterMessage.HistoricalOrdersRequest.Builder request =
                BlotterMessage.HistoricalOrdersRequest.newBuilder()
                        .setRequestId(pendingExportRequestId)
                        .setSymbol(symbol)
                        .setFrom(from == null ? "" : from.toString())
                        .setTo(to == null ? "" : to.toString())
                        .setLimit(EXPORT_LIMIT);
        if (selectedAccount != null && !ALL_ACCOUNTS.equals(selectedAccount)) {
            request.addAccounts(selectedAccount);
        }

        exportAllButton.setDisable(true);
        exportAllButton.setText("Preparando…");
        Repository.getClientService().sendMessage(request.build());
    }

    private void applyExportSnapshot(BlotterMessage.HistoricalOrdersSnapshot snapshot) {
        Platform.runLater(() -> {
            if (!snapshot.getRequestId().equals(pendingExportRequestId)) return;
            String requestId = pendingExportRequestId;
            File outputFile = pendingExportFile;
            ExportContext context = pendingExportContext;
            pendingExportRequestId = "";
            pendingExportFile = null;
            pendingExportContext = null;

            if (!snapshot.getError().isBlank()) {
                finishExport();
                Notifier.INSTANCE.notifyError("Órdenes Históricas", snapshot.getError());
                return;
            }
            List<BlotterMessage.HistoricalOrderGroup> groups = List.copyOf(snapshot.getOrdersList());
            if (groups.isEmpty()) {
                finishExport();
                Notifier.INSTANCE.notifyInfo("Órdenes Históricas",
                        "No hay información para exportar con estos filtros.");
                return;
            }
            HistoricalTradingAnalytics.Snapshot reportAnalytics = HistoricalTradingAnalytics.calculate(groups);
            CompletableFuture.runAsync(() -> writeHistoricalWorkbook(
                            outputFile, groups, reportAnalytics, context))
                    .whenComplete((ignored, error) -> Platform.runLater(() -> {
                        finishExport();
                        if (error != null) {
                            log.error("No se pudo exportar el historial global request={}", requestId, error);
                            Notifier.INSTANCE.notifyError("Órdenes Históricas",
                                    "No se pudo crear el Excel: " + rootMessage(error));
                        } else {
                            String suffix = snapshot.getTruncated()
                                    ? " (máximo de " + EXPORT_LIMIT + " órdenes alcanzado)" : "";
                            Notifier.INSTANCE.notifySuccess("Órdenes Históricas",
                                    "Excel global guardado: " + outputFile.getName() + suffix);
                        }
                    }));
        });
    }

    private void writeHistoricalWorkbook(File file,
                                         List<BlotterMessage.HistoricalOrderGroup> groups,
                                         HistoricalTradingAnalytics.Snapshot reportAnalytics,
                                         ExportContext context) {
        try (Workbook workbook = createHistoricalWorkbook(groups, reportAnalytics, context);
             FileOutputStream output = new FileOutputStream(file)) {
            workbook.write(output);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    Workbook createHistoricalWorkbook() throws Exception {
        return createHistoricalWorkbook(List.copyOf(data), analytics,
                new ExportContext(accountFilter.getValue(), fromDate.getValue(), toDate.getValue(),
                        pendingSymbol, DATE_TIME.format(Instant.now().atZone(Repository.getZoneID()))));
    }

    Workbook createHistoricalWorkbook(List<BlotterMessage.HistoricalOrderGroup> groups,
                                      HistoricalTradingAnalytics.Snapshot reportAnalytics,
                                      ExportContext context) throws Exception {
        Workbook workbook = new XSSFWorkbook();
        ExcelStyles styles = new ExcelStyles(workbook);
        writeOverviewSheet(workbook, styles, reportAnalytics, context);
        writeOrdersSheet(workbook, styles, groups, reportAnalytics);
        writeExecutionsSheet(workbook, styles, groups);
        return workbook;
    }

    private void writeOverviewSheet(Workbook workbook, ExcelStyles styles,
                                    HistoricalTradingAnalytics.Snapshot reportAnalytics,
                                    ExportContext context) throws Exception {
        Sheet sheet = workbook.createSheet("Resumen");
        sheet.setDisplayGridlines(false);
        sheet.setZoom(115);
        sheet.setFitToPage(true);
        sheet.getPrintSetup().setLandscape(true);
        sheet.getPrintSetup().setFitWidth((short) 1);
        sheet.getPrintSetup().setFitHeight((short) 0);
        sheet.setMargin(Sheet.LeftMargin, 0.25d);
        sheet.setMargin(Sheet.RightMargin, 0.25d);
        sheet.setMargin(Sheet.TopMargin, 0.35d);
        sheet.setMargin(Sheet.BottomMargin, 0.35d);
        for (int column = 0; column <= REPORT_LAST_COLUMN; column++) {
            sheet.setColumnWidth(column, (column == 1 ? 18 : 12) * 256);
        }
        for (int row = 0; row <= 3; row++) getOrCreateRow(sheet, row).setHeightInPoints(24);

        addVectorLogo(workbook, sheet);
        writeMergedValue(sheet, 0, 1, 2, REPORT_LAST_COLUMN,
                "ÓRDENES HISTÓRICAS", styles.title);
        writeMergedValue(sheet, 2, 3, 2, REPORT_LAST_COLUMN,
                "Vector Capital · Reporte consolidado de operaciones ejecutadas", styles.subtitle);

        writeMergedValue(sheet, 5, 5, 0, REPORT_LAST_COLUMN, "FILTROS DEL REPORTE", styles.sectionBand);
        writeFilterItem(sheet, 6, 0, 1, 2, 3, "Cuenta", context.account(), styles);
        writeFilterItem(sheet, 6, 4, 5, 6, 7, "Desde", context.from(), styles);
        writeFilterItem(sheet, 6, 8, 9, 10, 13, "Hasta", context.to(), styles);
        writeFilterItem(sheet, 7, 0, 1, 2, 3, "Instrumento",
                context.symbol().isBlank() ? "Todos" : context.symbol(), styles);
        writeFilterItem(sheet, 7, 4, 5, 6, 13, "Generado", context.generatedAt(), styles);

        int row = 9;
        writeMergedValue(sheet, row++, row - 1, 0, REPORT_LAST_COLUMN,
                "RESUMEN DEL PERIODO", styles.sectionBand);
        String[] metricNames = {"Órdenes", "Palos", "Precio prom. compra", "Precio prom. venta",
                "Monto compras", "Monto ventas", "P&L realizado"};
        double[] metricValues = {reportAnalytics.orders(), reportAnalytics.fills(),
                reportAnalytics.buyAveragePrice(), reportAnalytics.sellAveragePrice(),
                reportAnalytics.buyAmount(), reportAnalytics.sellAmount(), reportAnalytics.realizedPnl()};
        getOrCreateRow(sheet, row).setHeightInPoints(28);
        getOrCreateRow(sheet, row + 1).setHeightInPoints(25);
        getOrCreateRow(sheet, row + 2).setHeightInPoints(25);
        for (int metric = 0; metric < metricNames.length; metric++) {
            int firstColumn = metric * 2;
            writeMergedValue(sheet, row, row, firstColumn, firstColumn + 1,
                    metricNames[metric], styles.metricLabel);
            CellStyle valueStyle = switch (metric) {
                case 0, 1 -> styles.metricQuantity;
                case 2 -> styles.metricBuyPrice;
                case 3 -> styles.metricSellPrice;
                case 4 -> styles.metricBuyMoney;
                case 5 -> styles.metricSellMoney;
                default -> metricValues[metric] < 0d ? styles.metricNegativeMoney : styles.metricPositiveMoney;
            };
            writeMergedNumber(sheet, row + 1, row + 2, firstColumn, firstColumn + 1,
                    metricValues[metric], valueStyle);
        }

        row += 4;
        writeMergedValue(sheet, row++, row - 1, 0, REPORT_LAST_COLUMN,
                "MONTO OPERADO POR INSTRUMENTO", styles.sectionBand);
        String[] distributionHeaders = {"Ranking", "Instrumento", "Monto operado", "Participación"};
        Row distributionHeader = getOrCreateRow(sheet, row++);
        distributionHeader.setHeightInPoints(23);
        for (int column = 0; column < distributionHeaders.length; column++) {
            setText(distributionHeader, column, distributionHeaders[column], styles.header);
        }
        int distributionStartRow = row;
        Map<String, Double> visibleDistribution = topSlices(reportAnalytics.amountBySymbol(), 5);
        double total = visibleDistribution.values().stream().mapToDouble(Double::doubleValue).sum();
        int rank = 1;
        for (Map.Entry<String, Double> entry : visibleDistribution.entrySet()) {
            Row valueRow = getOrCreateRow(sheet, row++);
            valueRow.setHeightInPoints(21);
            setNumeric(valueRow, 0, rank++, styles.quantity);
            setText(valueRow, 1, entry.getKey(), styles.text);
            setNumeric(valueRow, 2, entry.getValue(), styles.money);
            setNumeric(valueRow, 3, total > 0d ? entry.getValue() / total : 0d, styles.percent);
        }
        if (!visibleDistribution.isEmpty()) {
            addDistributionChart(sheet, distributionStartRow, row - 1);
        }

        int noteRow = Math.max(row + 1, 27);
        writeMergedValue(sheet, noteRow, noteRow + 1, 0, REPORT_LAST_COLUMN,
                "Incluye únicamente órdenes calzadas y parcialmente calzadas. "
                        + "El P&L se calcula por costo promedio móvil dentro del rango filtrado.",
                styles.note);
        workbook.setPrintArea(workbook.getSheetIndex(sheet), 0, REPORT_LAST_COLUMN, 0, noteRow + 1);
    }

    private void writeOrdersSheet(Workbook workbook, ExcelStyles styles,
                                  List<BlotterMessage.HistoricalOrderGroup> groups,
                                  HistoricalTradingAnalytics.Snapshot reportAnalytics) {
        Sheet sheet = workbook.createSheet("Órdenes");
        String[] headers = {"Última ejecución", "Tipo", "Instrumento", "Cuenta", "Cantidad orden",
                "Cantidad ejecutada", "Precio promedio", "Monto ejecutado", "P&L realizado",
                "Estado", "Palos", "OrderID FIX"};
        Row header = sheet.createRow(0);
        for (int column = 0; column < headers.length; column++) {
            setText(header, column, headers[column], styles.header);
        }
        int rowIndex = 1;
        for (BlotterMessage.HistoricalOrderGroup group : groups) {
            RoutingMessage.Order summary = group.getSummary();
            Row row = sheet.createRow(rowIndex++);
            setText(row, 0, formatTime(summary), styles.text);
            setText(row, 1, sideText(summary.getSide()), styles.text);
            setText(row, 2, summary.getSymbol(), styles.text);
            setText(row, 3, summary.getAccount(), styles.text);
            setNumeric(row, 4, summary.getOrderQty(), styles.quantity);
            setNumeric(row, 5, summary.getCumQty(), styles.quantity);
            setNumeric(row, 6, summary.getAvgPrice(), styles.price);
            setNumeric(row, 7, executedAmount(group), styles.money);
            setNumeric(row, 8, reportAnalytics.realizedPnl(group), styles.money);
            setText(row, 9, statusText(summary.getOrdStatus()), styles.text);
            setNumeric(row, 10, group.getExecutionsCount(), styles.quantity);
            setText(row, 11, fixOrderId(group), styles.text);
        }
        finishDataSheet(sheet, headers.length, rowIndex);
    }

    private void writeExecutionsSheet(Workbook workbook, ExcelStyles styles,
                                      List<BlotterMessage.HistoricalOrderGroup> groups) {
        Sheet sheet = workbook.createSheet("Ejecuciones");
        boolean includeCounterparty = groups.stream()
                .anyMatch(group -> hasCounterparty(group.getExecutionsList()));
        String[] headers = includeCounterparty
                ? new String[]{"Hora", "OrderID FIX", "Exec ID", "Tipo", "Instrumento", "Cuenta",
                "Cantidad", "Precio", "Monto", "Acumulado", "Estado", "Contraparte"}
                : new String[]{"Hora", "OrderID FIX", "Exec ID", "Tipo", "Instrumento", "Cuenta",
                "Cantidad", "Precio", "Monto", "Acumulado", "Estado"};
        Row header = sheet.createRow(0);
        for (int column = 0; column < headers.length; column++) {
            setText(header, column, headers[column], styles.header);
        }
        int rowIndex = 1;
        for (BlotterMessage.HistoricalOrderGroup group : groups) {
            for (RoutingMessage.Order execution : group.getExecutionsList()) {
                Row row = sheet.createRow(rowIndex++);
                setText(row, 0, formatTime(execution), styles.text);
                setText(row, 1, fixOrderId(group), styles.text);
                setText(row, 2, execution.getExecId(), styles.text);
                setText(row, 3, sideText(execution.getSide()), styles.text);
                setText(row, 4, execution.getSymbol(), styles.text);
                setText(row, 5, execution.getAccount(), styles.text);
                setNumeric(row, 6, execution.getLastQty(), styles.quantity);
                setNumeric(row, 7, execution.getLastPx(), styles.price);
                setNumeric(row, 8, execution.getLastQty() * execution.getLastPx(), styles.money);
                setNumeric(row, 9, execution.getCumQty(), styles.quantity);
                setText(row, 10, statusText(execution.getOrdStatus()), styles.text);
                if (includeCounterparty && !normalized(execution.getContraBroker()).isEmpty()) {
                    setText(row, 11, execution.getContraBroker(), styles.text);
                }
            }
        }
        finishDataSheet(sheet, headers.length, rowIndex);
    }

    private void addVectorLogo(Workbook workbook, Sheet sheet) throws Exception {
        try (InputStream input = getClass().getResourceAsStream("/blotter/img/logo.jpg")) {
            if (input == null) return;
            int pictureId = workbook.addPicture(input.readAllBytes(), Workbook.PICTURE_TYPE_PNG);
            CreationHelper helper = workbook.getCreationHelper();
            Drawing<?> drawing = sheet.createDrawingPatriarch();
            ClientAnchor anchor = helper.createClientAnchor();
            anchor.setCol1(0);
            anchor.setRow1(0);
            anchor.setCol2(2);
            anchor.setRow2(4);
            drawing.createPicture(anchor, pictureId);
        }
    }

    private void addDistributionChart(Sheet sheet, int firstRow, int lastRow) {
        if (!(sheet instanceof XSSFSheet xssfSheet) || firstRow > lastRow) return;
        XSSFDrawing drawing = xssfSheet.createDrawingPatriarch();
        XSSFClientAnchor anchor = drawing.createAnchor(0, 0, 0, 0, 5, firstRow - 1, 14, 27);
        XSSFChart chart = drawing.createChart(anchor);
        chart.setTitleText("Monto operado · Top 5 + Otros");
        chart.setTitleOverlay(false);
        XDDFChartLegend legend = chart.getOrAddLegend();
        legend.setPosition(LegendPosition.RIGHT);

        XDDFDataSource<String> categories = XDDFDataSourcesFactory.fromStringCellRange(
                xssfSheet, new CellRangeAddress(firstRow, lastRow, 1, 1));
        XDDFNumericalDataSource<Double> amounts = XDDFDataSourcesFactory.fromNumericCellRange(
                xssfSheet, new CellRangeAddress(firstRow, lastRow, 2, 2));
        XDDFChartData data = chart.createData(ChartTypes.PIE, null, null);
        XDDFChartData.Series series = data.addSeries(categories, amounts);
        series.setTitle("Monto operado", null);
        chart.plot(data);
    }

    private void writeFilterItem(Sheet sheet, int row,
                                 int labelFirstColumn, int labelLastColumn,
                                 int valueFirstColumn, int valueLastColumn,
                                 String label, Object value, ExcelStyles styles) {
        writeMergedValue(sheet, row, row, labelFirstColumn, labelLastColumn, label, styles.label);
        writeMergedValue(sheet, row, row, valueFirstColumn, valueLastColumn,
                value == null ? "" : value.toString(), styles.text);
        getOrCreateRow(sheet, row).setHeightInPoints(24);
    }

    private void writeMergedValue(Sheet sheet, int firstRow, int lastRow,
                                  int firstColumn, int lastColumn,
                                  String value, CellStyle style) {
        Cell firstCell = prepareMergedRange(sheet, firstRow, lastRow, firstColumn, lastColumn, style);
        firstCell.setCellValue(value == null ? "" : value);
    }

    private void writeMergedNumber(Sheet sheet, int firstRow, int lastRow,
                                   int firstColumn, int lastColumn,
                                   double value, CellStyle style) {
        Cell firstCell = prepareMergedRange(sheet, firstRow, lastRow, firstColumn, lastColumn, style);
        firstCell.setCellValue(value);
    }

    private Cell prepareMergedRange(Sheet sheet, int firstRow, int lastRow,
                                    int firstColumn, int lastColumn, CellStyle style) {
        if (firstRow != lastRow || firstColumn != lastColumn) {
            sheet.addMergedRegion(new CellRangeAddress(firstRow, lastRow, firstColumn, lastColumn));
        }
        Cell firstCell = null;
        for (int rowIndex = firstRow; rowIndex <= lastRow; rowIndex++) {
            Row row = getOrCreateRow(sheet, rowIndex);
            for (int column = firstColumn; column <= lastColumn; column++) {
                Cell cell = row.getCell(column, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                cell.setCellStyle(style);
                if (firstCell == null) firstCell = cell;
            }
        }
        return firstCell;
    }

    private static Row getOrCreateRow(Sheet sheet, int rowIndex) {
        Row row = sheet.getRow(rowIndex);
        return row == null ? sheet.createRow(rowIndex) : row;
    }

    private void setText(Row row, int column, String value, CellStyle style) {
        Cell cell = row.createCell(column);
        cell.setCellValue(value == null ? "" : value);
        cell.setCellStyle(style);
    }

    private void setNumeric(Row row, int column, double value, CellStyle style) {
        Cell cell = row.createCell(column);
        cell.setCellValue(value);
        cell.setCellStyle(style);
    }

    private void finishDataSheet(Sheet sheet, int columns, int rows) {
        sheet.setDisplayGridlines(false);
        sheet.setZoom(95);
        sheet.setFitToPage(true);
        sheet.getPrintSetup().setLandscape(true);
        sheet.getPrintSetup().setFitWidth((short) 1);
        sheet.getPrintSetup().setFitHeight((short) 0);
        sheet.createFreezePane(0, 1);
        if (rows > 1) sheet.setAutoFilter(new CellRangeAddress(0, rows - 1, 0, columns - 1));
        Row header = sheet.getRow(0);
        if (header != null) header.setHeightInPoints(25);
        for (int column = 0; column < columns; column++) {
            sheet.autoSizeColumn(column);
            sheet.setColumnWidth(column, Math.min(42 * 256, Math.max(12 * 256,
                    sheet.getColumnWidth(column) + 700)));
        }
    }

    private static final class ExcelStyles {
        private final CellStyle title;
        private final CellStyle subtitle;
        private final CellStyle header;
        private final CellStyle sectionHeader;
        private final CellStyle sectionBand;
        private final CellStyle label;
        private final CellStyle text;
        private final CellStyle number;
        private final CellStyle quantity;
        private final CellStyle price;
        private final CellStyle money;
        private final CellStyle percent;
        private final CellStyle metricLabel;
        private final CellStyle metricQuantity;
        private final CellStyle metricBuyPrice;
        private final CellStyle metricSellPrice;
        private final CellStyle metricBuyMoney;
        private final CellStyle metricSellMoney;
        private final CellStyle metricPositiveMoney;
        private final CellStyle metricNegativeMoney;
        private final CellStyle note;

        private ExcelStyles(Workbook workbook) {
            title = style(workbook, IndexedColors.DARK_BLUE, IndexedColors.WHITE, true, 18);
            title.setAlignment(HorizontalAlignment.CENTER);
            title.setVerticalAlignment(VerticalAlignment.CENTER);
            subtitle = style(workbook, IndexedColors.WHITE, IndexedColors.DARK_BLUE, true, 11);
            subtitle.setAlignment(HorizontalAlignment.CENTER);
            subtitle.setVerticalAlignment(VerticalAlignment.CENTER);
            header = style(workbook, IndexedColors.DARK_BLUE, IndexedColors.WHITE, true, 10);
            header.setAlignment(HorizontalAlignment.CENTER);
            header.setVerticalAlignment(VerticalAlignment.CENTER);
            sectionHeader = style(workbook, IndexedColors.BLUE_GREY, IndexedColors.WHITE, true, 9);
            sectionHeader.setAlignment(HorizontalAlignment.CENTER);
            sectionBand = style(workbook, IndexedColors.DARK_BLUE, IndexedColors.WHITE, true, 10);
            sectionBand.setAlignment(HorizontalAlignment.LEFT);
            sectionBand.setVerticalAlignment(VerticalAlignment.CENTER);
            label = style(workbook, IndexedColors.BLUE_GREY, IndexedColors.WHITE, true, 10);
            label.setAlignment(HorizontalAlignment.LEFT);
            label.setVerticalAlignment(VerticalAlignment.CENTER);
            text = style(workbook, IndexedColors.WHITE, IndexedColors.BLACK, false, 10);
            text.setVerticalAlignment(VerticalAlignment.CENTER);
            number = numericStyle(workbook, "#,##0.0000");
            quantity = numericStyle(workbook, "#,##0");
            price = numericStyle(workbook, "#,##0.0000");
            money = numericStyle(workbook, "$#,##0.00;[Red]($#,##0.00);-");
            percent = numericStyle(workbook, "0.00%");
            metricLabel = style(workbook, IndexedColors.BLUE_GREY, IndexedColors.WHITE, true, 9);
            metricLabel.setAlignment(HorizontalAlignment.CENTER);
            metricLabel.setVerticalAlignment(VerticalAlignment.CENTER);
            metricLabel.setWrapText(true);
            metricQuantity = metricStyle(workbook, "#,##0", IndexedColors.DARK_BLUE);
            metricBuyPrice = metricStyle(workbook, "#,##0.0000", IndexedColors.DARK_GREEN);
            metricSellPrice = metricStyle(workbook, "#,##0.0000", IndexedColors.DARK_RED);
            metricBuyMoney = metricStyle(workbook, "$#,##0.00;[Red]($#,##0.00);-", IndexedColors.DARK_GREEN);
            metricSellMoney = metricStyle(workbook, "$#,##0.00;[Red]($#,##0.00);-", IndexedColors.DARK_RED);
            metricPositiveMoney = metricStyle(workbook, "$#,##0.00;[Red]($#,##0.00);-", IndexedColors.DARK_GREEN);
            metricNegativeMoney = metricStyle(workbook, "$#,##0.00;[Red]($#,##0.00);-", IndexedColors.DARK_RED);
            note = style(workbook, IndexedColors.GREY_25_PERCENT, IndexedColors.DARK_BLUE, false, 9);
            note.setAlignment(HorizontalAlignment.LEFT);
            note.setVerticalAlignment(VerticalAlignment.CENTER);
            note.setWrapText(true);
        }

        private static CellStyle style(Workbook workbook, IndexedColors background,
                                       IndexedColors foreground, boolean bold, int size) {
            CellStyle style = workbook.createCellStyle();
            style.setFillForegroundColor(background.getIndex());
            style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            style.setBorderBottom(BorderStyle.THIN);
            style.setBorderTop(BorderStyle.THIN);
            style.setBorderLeft(BorderStyle.THIN);
            style.setBorderRight(BorderStyle.THIN);
            Font font = workbook.createFont();
            font.setColor(foreground.getIndex());
            font.setBold(bold);
            font.setFontHeightInPoints((short) size);
            style.setFont(font);
            return style;
        }

        private static CellStyle numericStyle(Workbook workbook, String format) {
            CellStyle style = style(workbook, IndexedColors.WHITE, IndexedColors.BLACK, false, 10);
            style.setDataFormat(workbook.createDataFormat().getFormat(format));
            style.setAlignment(HorizontalAlignment.RIGHT);
            style.setVerticalAlignment(VerticalAlignment.CENTER);
            return style;
        }

        private static CellStyle metricStyle(Workbook workbook, String format, IndexedColors fontColor) {
            CellStyle style = style(workbook, IndexedColors.WHITE, fontColor, true, 13);
            style.setDataFormat(workbook.createDataFormat().getFormat(format));
            style.setAlignment(HorizontalAlignment.CENTER);
            style.setVerticalAlignment(VerticalAlignment.CENTER);
            return style;
        }
    }

    private void finishExport() {
        exportAllButton.setDisable(false);
        exportAllButton.setText("Exportar Excel");
    }

    private String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    record ExportContext(String account, LocalDate from, LocalDate to,
                         String symbol, String generatedAt) {
    }

    private void exportExecutions(BlotterMessage.HistoricalOrderGroup group) {
        RoutingMessage.Order summary = group.getSummary();
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Exportar detalle de ejecuciones");
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Excel (*.xlsx)", "*.xlsx"));
        chooser.setInitialFileName("orden_" + safeFilePart(summary.getSymbol()) + "_"
                + safeFilePart(fixOrderId(group)) + ".xlsx");
        File downloads = new File(System.getProperty("user.home"), "Downloads");
        if (downloads.isDirectory()) chooser.setInitialDirectory(downloads);
        File file = chooser.showSaveDialog(ordersTable.getScene().getWindow());
        if (file == null) return;
        if (!file.getName().toLowerCase(Locale.ROOT).endsWith(".xlsx")) {
            file = new File(file.getParentFile(), file.getName() + ".xlsx");
        }

        try (Workbook workbook = new XSSFWorkbook(); FileOutputStream output = new FileOutputStream(file)) {
            Sheet sheet = workbook.createSheet("Detalle orden");
            CellStyle headerStyle = excelHeaderStyle(workbook);
            int rowIndex = 0;
            rowIndex = writeSummaryRow(sheet, rowIndex, "OrderID FIX", fixOrderId(group));
            rowIndex = writeSummaryRow(sheet, rowIndex, "Instrumento", summary.getSymbol());
            rowIndex = writeSummaryRow(sheet, rowIndex, "Tipo", sideText(summary.getSide()));
            rowIndex = writeSummaryRow(sheet, rowIndex, "Cuenta", summary.getAccount());
            rowIndex = writeSummaryRow(sheet, rowIndex, "Cantidad orden", summary.getOrderQty());
            rowIndex = writeSummaryRow(sheet, rowIndex, "Cantidad ejecutada", summary.getCumQty());
            rowIndex = writeSummaryRow(sheet, rowIndex, "Precio promedio", summary.getAvgPrice());
            rowIndex = writeSummaryRow(sheet, rowIndex, "Monto ejecutado", executedAmount(group));
            rowIndex = writeSummaryRow(sheet, rowIndex, "P&L realizado (rango)", analytics.realizedPnl(group));
            rowIndex = writeSummaryRow(sheet, rowIndex, "Estado", statusText(summary.getOrdStatus()));
            rowIndex = writeSummaryRow(sheet, rowIndex, "Palos", group.getExecutionsCount());
            rowIndex++;

            boolean includeCounterparty = hasCounterparty(group.getExecutionsList());
            String[] headers = includeCounterparty
                    ? new String[]{"Hora", "Exec ID", "Cantidad", "Precio", "Monto", "Acumulado", "Estado", "Contraparte"}
                    : new String[]{"Hora", "Exec ID", "Cantidad", "Precio", "Monto", "Acumulado", "Estado"};
            Row header = sheet.createRow(rowIndex++);
            for (int i = 0; i < headers.length; i++) {
                Cell cell = header.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }
            for (RoutingMessage.Order execution : group.getExecutionsList()) {
                Row row = sheet.createRow(rowIndex++);
                row.createCell(0).setCellValue(formatTime(execution));
                row.createCell(1).setCellValue(execution.getExecId());
                row.createCell(2).setCellValue(execution.getLastQty());
                row.createCell(3).setCellValue(execution.getLastPx());
                row.createCell(4).setCellValue(execution.getLastQty() * execution.getLastPx());
                row.createCell(5).setCellValue(execution.getCumQty());
                row.createCell(6).setCellValue(statusText(execution.getOrdStatus()));
                if (includeCounterparty && !normalized(execution.getContraBroker()).isEmpty()) {
                    row.createCell(7).setCellValue(execution.getContraBroker());
                }
            }
            for (int column = 0; column < headers.length; column++) sheet.autoSizeColumn(column);
            workbook.write(output);
            Notifier.INSTANCE.notifySuccess("Órdenes Históricas", "Excel guardado: " + file.getName());
        } catch (Exception e) {
            log.error("No se pudo exportar el detalle de la orden {}", fixOrderId(group), e);
            Notifier.INSTANCE.notifyError("Órdenes Históricas", "No se pudo crear el Excel: " + e.getMessage());
        }
    }

    private int writeSummaryRow(Sheet sheet, int rowIndex, String label, Object value) {
        Row row = sheet.createRow(rowIndex);
        row.createCell(0).setCellValue(label);
        Cell valueCell = row.createCell(1);
        if (value instanceof Number number) valueCell.setCellValue(number.doubleValue());
        else valueCell.setCellValue(value == null ? "" : value.toString());
        return rowIndex + 1;
    }

    private CellStyle excelHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(IndexedColors.DARK_BLUE.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        Font font = workbook.createFont();
        font.setBold(true);
        font.setColor(IndexedColors.WHITE.getIndex());
        style.setFont(font);
        return style;
    }

    private String safeFilePart(String value) {
        return value == null ? "orden" : value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private static String fixOrderId(BlotterMessage.HistoricalOrderGroup group) {
        if (group == null) return "-";
        String summaryFixId = normalized(group.getSummary().getOrderID());
        if (!summaryFixId.isEmpty()) return summaryFixId;
        return group.getExecutionsList().stream()
                .map(RoutingMessage.Order::getOrderID)
                .map(HistoricalOrdersController::normalized)
                .filter(value -> !value.isEmpty())
                .findFirst()
                .orElse("-");
    }

    private static boolean hasCounterparty(List<RoutingMessage.Order> executions) {
        return executions != null && executions.stream()
                .map(RoutingMessage.Order::getContraBroker)
                .anyMatch(value -> !normalized(value).isEmpty());
    }

    private static String normalized(String value) {
        return value == null ? "" : value.trim();
    }

    private void updateAnalytics() {
        analytics = HistoricalTradingAnalytics.calculate(data);
        ordersMetric.setText(formatQuantity(analytics.orders()));
        fillsMetric.setText(formatQuantity(analytics.fills()));
        buyAveragePriceMetric.setText(formatAveragePrice(analytics.buyAveragePrice()));
        sellAveragePriceMetric.setText(formatAveragePrice(analytics.sellAveragePrice()));
        buyAmountMetric.setText(formatMoney(analytics.buyAmount()));
        sellAmountMetric.setText(formatMoney(analytics.sellAmount()));
        realizedPnlMetric.setText(formatSignedMoney(analytics.realizedPnl()));
        realizedPnlMetric.getStyleClass().removeAll("metric-buy", "metric-sell", "metric-neutral");
        realizedPnlMetric.getStyleClass().add(pnlStyle(analytics.realizedPnl()));

        Map<String, Double> visibleSlices = topSlices(analytics.amountBySymbol(), 5);
        ObservableList<PieChart.Data> chartData = FXCollections.observableArrayList(
                visibleSlices.entrySet().stream()
                        .map(entry -> new PieChart.Data(entry.getKey(), entry.getValue()))
                        .toList());
        amountBySymbolChart.setData(chartData);
        amountBySymbolTitle.setText(pendingSymbol.isBlank()
                ? "Monto operado · Top 5 + Otros"
                : "Monto operado · " + pendingSymbol);
        updateSymbolLegend(chartData);
        installSliceTooltips(chartData);
    }

    private void updateSymbolLegend(ObservableList<PieChart.Data> chartData) {
        amountBySymbolLegend.getChildren().clear();
        double total = chartData.stream().mapToDouble(PieChart.Data::getPieValue).sum();
        for (int index = 0; index < chartData.size(); index++) {
            PieChart.Data slice = chartData.get(index);
            double percentage = total > 0d ? slice.getPieValue() * 100d / total : 0d;
            Region swatch = new Region();
            swatch.setMinSize(8, 8);
            swatch.setPrefSize(8, 8);
            swatch.setMaxSize(8, 8);
            swatch.getStyleClass().add("historical-slice-" + Math.min(index, 5));

            Label label = new Label(slice.getName() + "  " + formatter("0.0").format(percentage) + "%");
            label.getStyleClass().add("historical-chart-legend-label");
            HBox row = new HBox(5, swatch, label);
            row.setAlignment(Pos.CENTER_LEFT);
            Tooltip tooltip = new Tooltip(slice.getName() + "\n" + formatMoney(slice.getPieValue()));
            Tooltip.install(row, tooltip);
            amountBySymbolLegend.getChildren().add(row);
        }
    }

    private void installSliceTooltips(ObservableList<PieChart.Data> chartData) {
        double total = chartData.stream().mapToDouble(PieChart.Data::getPieValue).sum();
        chartData.forEach(slice -> slice.nodeProperty().addListener((obs, oldNode, node) -> {
            if (node == null) return;
            double percentage = total > 0d ? (slice.getPieValue() * 100d / total) : 0d;
            Tooltip.install(node, new Tooltip(slice.getName() + "\n"
                    + formatMoney(slice.getPieValue()) + " · "
                    + formatter("0.00").format(percentage) + "%"));
        }));
    }

    private VBox detailMetric(String title, String value, String valueStyle) {
        Label titleLabel = new Label(title);
        titleLabel.getStyleClass().add("historical-metric-title");
        Label valueLabel = new Label(value);
        valueLabel.getStyleClass().add("historical-metric-value");
        if (!valueStyle.isBlank()) valueLabel.getStyleClass().add(valueStyle);
        VBox metric = new VBox(4, titleLabel, valueLabel);
        metric.setAlignment(Pos.CENTER);
        metric.setPrefSize(170, 58);
        metric.getStyleClass().add("historical-metric");
        return metric;
    }

    private PieChart executionPriceChart(BlotterMessage.HistoricalOrderGroup group) {
        Map<String, Double> byPrice = new LinkedHashMap<>();
        for (RoutingMessage.Order execution : group.getExecutionsList()) {
            if (execution.getLastQty() <= 0d || execution.getLastPx() <= 0d) continue;
            byPrice.merge(formatPrice(execution.getLastPx()),
                    execution.getLastQty() * execution.getLastPx(), Double::sum);
        }
        PieChart chart = new PieChart(FXCollections.observableArrayList(
                topSlices(byPrice, 5).entrySet().stream()
                        .map(entry -> new PieChart.Data(entry.getKey(), entry.getValue()))
                        .toList()));
        chart.setTitle("Distribución por precio");
        chart.setAnimated(false);
        chart.setLegendVisible(false);
        chart.setLabelsVisible(true);
        chart.setPrefSize(300, 155);
        chart.setMinSize(300, 155);
        chart.getStyleClass().add("historical-detail-chart");
        return chart;
    }

    private static Map<String, Double> topSlices(Map<String, Double> source, int limit) {
        LinkedHashMap<String, Double> result = new LinkedHashMap<>();
        double others = source.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .skip(limit)
                .mapToDouble(Map.Entry::getValue)
                .sum();
        source.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(limit)
                .forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        if (others > 0d) result.put("Otros", others);
        return result;
    }

    private static String pnlStyle(double pnl) {
        if (pnl > 0.000001d) return "metric-buy";
        if (pnl < -0.000001d) return "metric-sell";
        return "metric-neutral";
    }

    private TableCell<BlotterMessage.HistoricalOrderGroup, RoutingMessage.Side> sideCell() {
        return new TableCell<>() {
            @Override protected void updateItem(RoutingMessage.Side side, boolean empty) {
                super.updateItem(side, empty);
                setText(empty || side == null ? null : sideText(side));
                setAlignment(Pos.CENTER);
                setStyle(empty || side == null ? "" : side == RoutingMessage.Side.BUY
                        ? "-fx-text-fill: #38c66b; -fx-font-weight: bold;"
                        : "-fx-text-fill: #ff5353; -fx-font-weight: bold;");
            }
        };
    }

    private TableCell<BlotterMessage.HistoricalOrderGroup, String> statusCell() {
        return new TableCell<>() {
            @Override protected void updateItem(String status, boolean empty) {
                super.updateItem(status, empty);
                if (empty || status == null) {
                    setText(null);
                    setStyle("");
                } else {
                    setText(I18n.tr("FILLED".equals(status) ? "CALZADA" : "PARCIAL"));
                    setStyle("FILLED".equals(status)
                            ? "-fx-text-fill: #38c66b; -fx-font-weight: bold;"
                            : "-fx-text-fill: #e2c56f; -fx-font-weight: bold;");
                }
                setAlignment(Pos.CENTER);
            }
        };
    }

    private String formatTime(RoutingMessage.Order order) {
        if (order == null || !order.hasTime()) return "-";
        Instant instant = Instant.ofEpochSecond(order.getTime().getSeconds(), order.getTime().getNanos());
        return DATE_TIME.format(instant.atZone(Repository.getZoneID()));
    }

    private static String sideText(RoutingMessage.Side side) {
        return I18n.tr(side == RoutingMessage.Side.BUY ? "Compra" : "Venta");
    }

    private static String statusText(RoutingMessage.OrderStatus status) {
        return I18n.tr(status == RoutingMessage.OrderStatus.FILLED ? "CALZADA" : "PARCIAL");
    }

    private static double executedAmount(BlotterMessage.HistoricalOrderGroup group) {
        RoutingMessage.Order summary = group.getSummary();
        if (summary.getAvgPrice() > 0d && summary.getCumQty() > 0d) {
            return summary.getAvgPrice() * summary.getCumQty();
        }
        return group.getExecutionsList().stream()
                .mapToDouble(o -> o.getLastQty() * o.getLastPx()).sum();
    }

    private static String formatQuantity(double value) {
        return formatter("#,##0.##").format(value);
    }

    private static String formatPrice(double value) {
        return formatter("#,##0.0000").format(value);
    }

    private static String formatAveragePrice(double value) {
        return value > 0d ? formatPrice(value) : "-";
    }

    private static String formatMoney(double value) {
        return "$" + formatter("#,##0.00").format(value);
    }

    private static String formatSignedMoney(double value) {
        String prefix = value > 0d ? "+" : "";
        return prefix + formatMoney(value);
    }

    private static DecimalFormat formatter(String pattern) {
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
        formatter.applyPattern(pattern);
        return formatter;
    }
}
