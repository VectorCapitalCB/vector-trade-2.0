package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.adaptor.OrderPendingQuantityTracker;
import cl.vc.blotter.utils.ColumnConfig;
import cl.vc.blotter.utils.I18n;
import cl.vc.blotter.utils.Notifier;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.animation.PauseTransition;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.input.*;
import javafx.util.Callback;
import javafx.util.Duration;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Data
@Slf4j
public class ExecutionsController {

    private static final Set<String> ALWAYS_HIDDEN_ORDER_COLUMN_KEYS = Set.of("broker", "clOrdID");

    private record OrderColumnDefinition(String key, String label, TableColumn<RoutingMessage.Order, ?> column) {}

    private Runnable orderColumnConfigChangeHandler;

    public ObservableList<RoutingMessage.Order> data;

    @FXML
    private TableView<RoutingMessage.Order> tableExecutionReports;
    @FXML
    private TableColumn<RoutingMessage.Order, String> time;
    @FXML
    private TableColumn<RoutingMessage.Order, RoutingMessage.Side> side;
    @FXML
    private TableColumn<RoutingMessage.Order, String> symbol;
    @FXML
    private TableColumn<RoutingMessage.Order, String> market;
    @FXML
    private TableColumn<RoutingMessage.Order, String> broker;
    @FXML
    private TableColumn<RoutingMessage.Order, String> ordType;
    @FXML
    private TableColumn<RoutingMessage.Order, String> qty;
    @FXML
    private TableColumn<RoutingMessage.Order, String> iceberg;
    @FXML
    private TableColumn<RoutingMessage.Order, String> px;
    @FXML
    private TableColumn<RoutingMessage.Order, String> amount;
    @FXML
    private TableColumn<RoutingMessage.Order, String> lastpx;
    @FXML
    private TableColumn<RoutingMessage.Order, String> lastQty;
    @FXML
    private TableColumn<RoutingMessage.Order, String> avgPrice;
    @FXML
    private TableColumn<RoutingMessage.Order, String> execQty;
    @FXML
    private TableColumn<RoutingMessage.Order, String> leave;
    @FXML
    private TableColumn<RoutingMessage.Order, String> settlType;
    @FXML
    private TableColumn<RoutingMessage.Order, String> tif;
    @FXML
    private TableColumn<RoutingMessage.Order, String> handlInst;
    @FXML
    private TableColumn<RoutingMessage.Order, String> clOrdID;
    @FXML
    private TableColumn<RoutingMessage.Order, String> id;
    @FXML
    private TableColumn<RoutingMessage.Order, String> orderID;
    @FXML
    private TableColumn<RoutingMessage.Order, String> execID;
    @FXML
    private TableColumn<RoutingMessage.Order, String> execType;
    @FXML
    private TableColumn<RoutingMessage.Order, String> status;
    @FXML
    private TableColumn<RoutingMessage.Order, String> text;
    @FXML
    private TableColumn<RoutingMessage.Order, String> account;
    @FXML
    private TableColumn<RoutingMessage.Order, String> username;
    @FXML
    private TableColumn<RoutingMessage.Order, String> basket;
    @FXML
    private TableColumn<RoutingMessage.Order, Double> spread;
    @FXML
    private TableColumn<RoutingMessage.Order, Double> limit;
    @FXML
    private TableColumn<RoutingMessage.Order, String> strategy;
    private FilteredList<RoutingMessage.Order> filteredData;
    @Getter
    private SortedList<RoutingMessage.Order> sortedData;
    private final OrderPendingQuantityTracker pendingQuantityTracker = new OrderPendingQuantityTracker();


    private static final int MAX_RETRIES = 200;
    private int retries = 0;
    private boolean wired = false;
    // ==========================================

    @FXML
    private void initialize() {
        try {

            time.setCellValueFactory(cellData -> {
                long seconds = cellData.getValue().getTime().getSeconds();
                int nanos = cellData.getValue().getTime().getNanos();
                Instant instant = Instant.ofEpochSecond(seconds, nanos);
                ZonedDateTime zonedDateTime = instant.atZone(Repository.zoneID);
                long milisToAdd = 1000;
                zonedDateTime = zonedDateTime.plus(milisToAdd, ChronoUnit.MILLIS);
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                String formattedDateTime = zonedDateTime.format(formatter);
                return new SimpleStringProperty(formattedDateTime);
            });

            side.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getSide()));
            side.setCellFactory(createTypeColumnStyleCallback());

            px.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getPrice()).asString());
            id.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getId()));
            symbol.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getSymbol()));
            market.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getSecurityExchange().name()));
            broker.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getBroker().name()));
            ordType.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getOrdType().name()));
            qty.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getOrderQty()).asString());
            settlType.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getSettlType().name()));
            tif.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getTif().name()));
            handlInst.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getHandlInst().name()));

            status.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getOrdStatus().name()));
            status.setCellFactory(createStatusColumnStyleCallback());

            execID.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getExecId()));
            account.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getAccount()));
            username.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getOperator()));
            orderID.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getOrderID()));
            lastpx.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getLastPx()).asString());
            lastQty.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getLastQty()).asString());
            leave.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getLeaves()).asString());
            avgPrice.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getAvgPrice()).asString());
            execQty.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getCumQty()).asString());
            text.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getText()));
            execType.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getExecType().name()));
            basket.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getBasketID()));
            spread.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getSpread()));
            limit.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getLimit()));
            strategy.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getStrategyOrder().name()));
            iceberg.setCellValueFactory(cellData -> new SimpleObjectProperty<>(String.valueOf(cellData.getValue().getMaxFloor())));
            amount.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getAmount()).asString());

            tableExecutionReports.setRowFactory(createRowStyleCallback());
            tableExecutionReports.refresh();

            tableExecutionReports.setOnMouseClicked(event -> {

                if (event.getClickCount() == 1 && event.getButton() == MouseButton.PRIMARY) {

                    RoutingMessage.Order selectedItem = tableExecutionReports.getSelectionModel().getSelectedItem();
                    if (selectedItem == null) {
                        log.error("No order selected. `selectedItem` is null.");
                        return;
                    }
                    var pc = Repository.getPrincipalController();
                    var rc = Repository.getRoutingController();
                    var lanz = Repository.getLanzadorController();
                    if (pc == null || rc == null || lanz == null) {
                        Notifier.INSTANCE.notifyError("Error", "La vista principal aún no está lista.");
                        return;
                    }

                    pc.setOrderSelected(selectedItem);

                    String buttonStyleClass = selectedItem.getSide().name().equals("SELL") ? "sellbutton-hoverable" : "buybutton-hoverable";
                    lanz.clearButtonStyles();

                    RoutingMessage.OrderStatus status = selectedItem.getOrdStatus();

                    if (status.equals(RoutingMessage.OrderStatus.FILLED) ||
                            status.equals(RoutingMessage.OrderStatus.REJECTED) ||
                            status.equals(RoutingMessage.OrderStatus.CANCELED)) {

                        lanz.setValues(selectedItem);

                        rc.getEstrategia2().setDisable(true);
                        rc.getSpreadLabel().setDisable(true);
                        rc.getSpread2().setDisable(true);
                        rc.getLimitLabel().setDisable(true);
                        rc.getLimit2().setDisable(true);
                        rc.getBest().setDisable(true);
                        rc.getHit().setDisable(true);
                        rc.getTickMas().setDisable(true);
                        rc.getTickMenos().setDisable(true);
                        rc.getReplaceOrder().setDisable(true);
                        rc.getQuantity2().setDisable(true);
                        rc.getPriceOrder2().setDisable(true);
                        rc.getCancelOrder().setDisable(true);
                        rc.getLblquantity().setDisable(true);
                        rc.getLblpriceOrder2().setDisable(true);
                        rc.getVisible().setDisable(true);
                        rc.getVisibleid().setDisable(true);

                        rc.getHit().getStyleClass().add(buttonStyleClass);
                        rc.getBest().getStyleClass().add(buttonStyleClass);

                    } else {

                        rc.getEstrategiaLabel().setVisible(true);
                        rc.getEstrategia2().setVisible(true);
                        rc.getSpreadLabel().setVisible(true);
                        rc.getSpread2().setVisible(true);
                        rc.getLimitLabel().setVisible(true);
                        rc.getLimit2().setVisible(true);
                        rc.getBest().setVisible(true);
                        rc.getHit().setVisible(true);
                        rc.getTickMas().setVisible(true);
                        rc.getTickMenos().setVisible(true);
                        rc.getReplaceOrder().setVisible(true);
                        rc.getQuantity2().setVisible(true);
                        rc.getPriceOrder2().setVisible(true);
                        rc.getCancelOrder().setVisible(true);
                        rc.getLblquantity().setVisible(true);
                        rc.getLblpriceOrder2().setVisible(true);
                        rc.getVisible().setVisible(true);
                        rc.getVisibleid().setVisible(true);

                        rc.getEstrategiaLabel().setDisable(false);
                        rc.getEstrategia2().setDisable(false);
                        rc.getSpreadLabel().setDisable(false);
                        rc.getSpread2().setDisable(false);
                        rc.getLimitLabel().setDisable(false);
                        rc.getLimit2().setDisable(false);
                        rc.getBest().setDisable(false);
                        rc.getHit().setDisable(false);
                        rc.getTickMas().setDisable(false);
                        rc.getTickMenos().setDisable(false);
                        rc.getReplaceOrder().setDisable(false);
                        rc.getQuantity2().setDisable(false);
                        rc.getPriceOrder2().setDisable(false);
                        rc.getCancelOrder().setDisable(false);
                        rc.getLblquantity().setDisable(false);
                        rc.getLblpriceOrder2().setDisable(false);
                        rc.getVisible().setDisable(false);
                        rc.getVisibleid().setDisable(false);

                        lanz.setValues(selectedItem);
                        rc.getHit().getStyleClass().add(buttonStyleClass);
                        rc.getBest().getStyleClass().add(buttonStyleClass);
                    }
                }
            });

            this.tableExecutionReports.setCache(false);

            DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
            decimalFormat.applyPattern("#,##0.00");

            DecimalFormat decimalFormatFourDecimals = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
            decimalFormatFourDecimals.applyPattern("#,##0.0000");

            amount.setCellValueFactory(cellData -> {
                Double value = cellData.getValue().getPrice() * cellData.getValue().getOrderQty();
                String formattedValue = decimalFormat.format(value);
                return new SimpleObjectProperty<>(formattedValue);
            });

            px.setCellValueFactory(cellData -> {
                Double value = cellData.getValue().getPrice();
                String formattedValue = decimalFormatFourDecimals.format(value);
                return new SimpleObjectProperty<>(formattedValue);
            });

            qty.setCellValueFactory(cellData -> {
                Double value = cellData.getValue().getOrderQty();
                String formattedValue = decimalFormat.format(value);
                return new SimpleObjectProperty<>(formattedValue);
            });

            leave.setCellValueFactory(cellData -> {
                Double value = pendingQuantityTracker.pendingQuantity(cellData.getValue());
                String formattedValue = decimalFormat.format(value);
                return new SimpleObjectProperty<>(formattedValue);
            });

            execQty.setCellValueFactory(cellData -> {
                Double value = cellData.getValue().getCumQty();
                String formattedValue = decimalFormat.format(value);
                return new SimpleObjectProperty<>(formattedValue);
            });

            lastpx.setCellValueFactory(cellData -> {
                Double value = cellData.getValue().getLastPx();
                String formattedValue = decimalFormat.format(value);
                return new SimpleObjectProperty<>(formattedValue);
            });

            lastQty.setCellValueFactory(cellData -> {
                Double value = cellData.getValue().getLastQty();
                String formattedValue = decimalFormat.format(value);
                return new SimpleObjectProperty<>(formattedValue);
            });

            // px/lastpx/avgPrice respetan el selector global de decimales (override gana sobre lo de arriba).
            applyPriceFactories();

            this.data = FXCollections.observableArrayList(new ArrayList<>());
            this.filteredData = new FilteredList<>(this.data, p -> true);

            this.sortedData = new SortedList<>(this.filteredData);
            sortedData.comparatorProperty().bind(tableExecutionReports.comparatorProperty());

            this.tableExecutionReports.setItems(this.sortedData);

            this.tableExecutionReports.setEditable(true);
            this.tableExecutionReports.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
            this.tableExecutionReports.getSelectionModel().setCellSelectionEnabled(true);
            this.tableExecutionReports.getSortOrder().add(this.time);

            this.tableExecutionReports.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
            this.tableExecutionReports.autosize();

            tableExecutionReports.sort();
            installContextMenuSupport();
            applyOrderColumnConfig();

            Platform.runLater(this::wireAfterParentReady);

            if (Repository.getIsLight()) {
                isLight();
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    private void wireAfterParentReady() {
        if (wired) return;

        var pc = Repository.getPrincipalController();
        if (pc == null) {
            retryLater();
            return;
        }
        var rc = pc.getRoutingViewController();
        if (rc == null) {
            retryLater();
            return;
        }

        bindFiltersToPredicate(rc);

        wired = true;
        log.info("ExecutionsController: wiring diferido completo.");
    }

    private void retryLater() {
        if (retries++ >= MAX_RETRIES) {
            log.warn("PrincipalController/RoutingController no disponibles tras {} reintentos; omito wiring diferido.", retries);
            return;
        }
        PauseTransition t = new PauseTransition(Duration.millis(50));
        t.setOnFinished(e -> wireAfterParentReady());
        t.play();
    }

    public void updateOrderState(RoutingMessage.Order order) {
        pendingQuantityTracker.accept(order);
        if (tableExecutionReports != null) {
            tableExecutionReports.refresh();
        }
    }

    public void clearOrderState() {
        pendingQuantityTracker.clear();
    }


    private void bindFiltersToPredicate(RoutingController rc) {
        if (filteredData != null) {
            try { filteredData.predicateProperty().unbind(); } catch (Exception ignore) {}
        }

        filteredData.predicateProperty().bind(Bindings.createObjectBinding(() -> exec -> {
                    try {
                        tableExecutionReports.refresh();

                        // Defaults seguros cuando algo aún no está seleccionado
                        String accountSelected;
                        var accountSel = rc.getAccountFilter().getSelectionModel().getSelectedItem();
                        if (accountSel != null && !accountSel.isBlank()) {
                            accountSelected = accountSel;
                        } else {
                            accountSelected = Repository.getALL_ACCOUNT(); // “TODAS”
                        }

                        RoutingMessage.OrderStatus statusSelected =
                                rc.getStatusFilter().getSelectionModel().getSelectedItem() != null
                                        ? rc.getStatusFilter().getSelectionModel().getSelectedItem()
                                        : RoutingMessage.OrderStatus.ALL_STATUS;

                        RoutingMessage.SecurityExchangeRouting secExchangeSelected =
                                rc.getSecurityExchangeFilter().getSelectionModel().getSelectedItem() != null
                                        ? rc.getSecurityExchangeFilter().getSelectionModel().getSelectedItem()
                                        : RoutingMessage.SecurityExchangeRouting.ALL_SECURITY_EXCHANGE;

                        String symbolSelected = rc.getSymbolFilter().getText() != null
                                ? rc.getSymbolFilter().getText().trim()
                                : "";

                        String sideSelected = rc.getSideFilter().getSelectionModel().getSelectedItem() != null
                                ? rc.getSideFilter().getSelectionModel().getSelectedItem()
                                : "Todos";

                        Boolean filterstatus   = statusFilter(exec, statusSelected);
                        Boolean filterAccount  = filterAccount(exec, accountSelected);
                        Boolean filterExchange = filterExchange(exec, secExchangeSelected);
                        Boolean filterSymbol   = filterSymbol(exec, symbolSelected);
                        Boolean filterside     = filterSideString(exec, sideSelected);

                        return (filterstatus && filterAccount && filterExchange && filterSymbol && filterside);

                    } catch (Exception ex) {
                        log.error("Error en predicado de filtro", ex);
                        return false;
                    }
                },
                rc.getStatusFilter().valueProperty(),
                rc.getSideFilter().valueProperty(),
                rc.getAccountFilter().valueProperty(),
                rc.getSecurityExchangeFilter().valueProperty(),
                rc.getSymbolFilter().textProperty()
        ));
    }


    private Callback<TableView<RoutingMessage.Order>, TableRow<RoutingMessage.Order>> createRowStyleCallback() {
        return tableView -> {

            final TableRow<RoutingMessage.Order> row = new TableRow<RoutingMessage.Order>() {
                @Override
                protected void updateItem(RoutingMessage.Order item, boolean empty) {
                    super.updateItem(item, empty);

                    if (item == null) return;

                    RoutingMessage.Side side = item.getSide();
                    if (side.equals(RoutingMessage.Side.BUY)) {
                        setStyle("-fx-text-fill: white;");
                    } else if (side.equals(RoutingMessage.Side.SELL) || side.equals(RoutingMessage.Side.SELL_SHORT)) {
                        setStyle("-fx-text-fill: white;");
                    } else {
                        setStyle("");
                    }

                    if (!item.getText().equals("")) {
                        Tooltip tooltip = new Tooltip(item.getText());
                        setTooltip(tooltip);
                    }

                }
            };

            return row;
        };
    }

    private Callback<TableColumn<RoutingMessage.Order, RoutingMessage.Side>, TableCell<RoutingMessage.Order, RoutingMessage.Side>> createTypeColumnStyleCallback() {

        return tableColumn -> new TableCell<RoutingMessage.Order, RoutingMessage.Side>() {
            @Override
            protected void updateItem(RoutingMessage.Side side, boolean empty) {
                super.updateItem(side, empty);

                if (empty || side == null) {
                    setText(null);
                    setStyle("");
                    return;
                }

                setText(I18n.display(side));

                switch (side) {
                    case BUY:
                        setStyle("-fx-text-fill: green;"); // Verde
                        break;
                    case SELL:
                    case SELL_SHORT:
                        setStyle("-fx-text-fill: red;"); // Rojo
                        break;
                    default:
                        setStyle("");
                        break;
                }
            }
        };
    }

    private Callback<TableColumn<RoutingMessage.Order, String>, TableCell<RoutingMessage.Order, String>> createStatusColumnStyleCallback() {
        return tableColumn -> new TableCell<RoutingMessage.Order, String>() {
            @Override
            protected void updateItem(String status, boolean empty) {
                super.updateItem(status, empty);

                if (empty || status == null) {
                    setText(null);
                    setStyle("");
                    return;
                }

                switch (status) {
                    case "NEW":
                        setStyle("-fx-text-fill: #00BFFF;"); // Celeste
                        setText(I18n.tr("NUEVA"));
                        break;

                    case "FILLED":
                        setStyle("-fx-text-fill: green;"); // Verde
                        setText(I18n.tr("CALZADA"));
                        break;
                    case "PARTIALLY_FILLED":
                        setStyle("-fx-text-fill: #e7cb0d;"); // Verde claro
                        setText(I18n.tr("PARCIAL"));
                        break;
                    case "REJECTED":
                        setStyle("-fx-text-fill: red;"); // Rojo
                        setText(I18n.tr("RECHAZADA"));
                        break;
                    case "CANCELED":
                        setStyle("-fx-text-fill: red;"); // Rojo
                        setText(I18n.tr("CANCELADA"));
                        break;
                    case "REPLACED":
                        setStyle("-fx-text-fill: #ea6f08;"); // Rojo
                        setText(I18n.tr("REMPLAZADA"));
                        break;
                    default:
                        setStyle("");
                        setText(I18n.tr(status));
                        break;
                }
            }
        };
    }

    private void isLight() {
        id.setVisible(false);
        market.setVisible(false);
        broker.setVisible(false);
        ordType.setVisible(false);
        tif.setVisible(false);
        handlInst.setVisible(false);
        basket.setVisible(false);

        spread.setVisible(false);
        limit.setVisible(false);
        strategy.setVisible(false);
        iceberg.setVisible(false);
    }

    /** (Re)asigna los factories de las columnas de PRECIO honrando el selector global de decimales. */
    private void applyPriceFactories() {
        px.setCellValueFactory(cd -> new SimpleObjectProperty<>(
                Repository.priceFormatter(4).format(cd.getValue().getPrice())));
        lastpx.setCellValueFactory(cd -> new SimpleObjectProperty<>(
                Repository.priceFormatter(2).format(cd.getValue().getLastPx())));
        avgPrice.setCellValueFactory(cd -> new SimpleObjectProperty<>(
                Repository.priceFormatter(2).format(cd.getValue().getAvgPrice())));
    }

    /** Re-aplica el formato de decimales de precio a esta tabla (llamado por el selector global). */
    public void reapplyPriceFormat() {
        applyPriceFactories();
        if (tableExecutionReports != null) tableExecutionReports.refresh();
    }

    public void applyOrderColumnConfig() {
        ColumnConfig cfg = Repository.getColumnConfig();
        for (OrderColumnDefinition definition : orderColumnDefinitions()) {
            boolean visible = cfg.hasOrderColumn(definition.key())
                    ? cfg.isOrderColumnVisible(definition.key(), definition.column().isVisible())
                    : definition.column().isVisible();

            if (isAlwaysHiddenOrderColumn(definition.key()) || (Repository.getIsLight() && isLightHiddenColumn(definition.key()))) {
                visible = false;
            }

            definition.column().setVisible(visible);
        }
    }

    private List<OrderColumnDefinition> orderColumnDefinitions() {
        return List.of(
                new OrderColumnDefinition("time", "Fecha Ingreso", time),
                new OrderColumnDefinition("side", "Tipo", side),
                new OrderColumnDefinition("symbol", "Instrumento", symbol),
                new OrderColumnDefinition("settlType", "Condicion", settlType),
                new OrderColumnDefinition("market", "Mercado", market),
                new OrderColumnDefinition("broker", "Broker", broker),
                new OrderColumnDefinition("ordType", "T Orden", ordType),
                new OrderColumnDefinition("qty", "Cantidad", qty),
                new OrderColumnDefinition("iceberg", "Visible", iceberg),
                new OrderColumnDefinition("px", "Precio", px),
                new OrderColumnDefinition("amount", "Monto", amount),
                new OrderColumnDefinition("lastpx", "Ult. Precio", lastpx),
                new OrderColumnDefinition("lastQty", "Ult. Cantidad", lastQty),
                new OrderColumnDefinition("avgPrice", "Precio Promedio", avgPrice),
                new OrderColumnDefinition("execQty", "Cantidad Ejecutada", execQty),
                new OrderColumnDefinition("leave", "Cantidad Pendiente", leave),
                new OrderColumnDefinition("tif", "TIF", tif),
                new OrderColumnDefinition("handlInst", "HandlInst", handlInst),
                new OrderColumnDefinition("id", "ID", id),
                new OrderColumnDefinition("clOrdID", "ClOrdID", clOrdID),
                new OrderColumnDefinition("orderID", "OrderId FIX", orderID),
                new OrderColumnDefinition("execID", "ExecID", execID),
                new OrderColumnDefinition("execType", "Tipo de Ejecucion", execType),
                new OrderColumnDefinition("status", "Estado", status),
                new OrderColumnDefinition("text", "Motivo", text),
                new OrderColumnDefinition("account", "Cuenta", account),
                new OrderColumnDefinition("username", "Usuario", username),
                new OrderColumnDefinition("basket", "Basket", basket),
                new OrderColumnDefinition("spread", "Spread", spread),
                new OrderColumnDefinition("limit", "Limit", limit),
                new OrderColumnDefinition("strategy", "Strategy", strategy)
        );
    }

    private boolean isLightHiddenColumn(String key) {
        return Set.of("id", "market", "broker", "ordType", "tif", "handlInst", "basket", "spread", "limit", "strategy", "iceberg")
                .contains(key);
    }

    private boolean isAlwaysHiddenOrderColumn(String key) {
        return ALWAYS_HIDDEN_ORDER_COLUMN_KEYS.contains(key);
    }

    public void setOrderColumnConfigChangeHandler(Runnable orderColumnConfigChangeHandler) {
        this.orderColumnConfigChangeHandler = orderColumnConfigChangeHandler;
    }

    private void installContextMenuSupport() {
        tableExecutionReports.setOnContextMenuRequested(event -> {
            buildOrderContextMenu().show(tableExecutionReports, event.getScreenX(), event.getScreenY());
            event.consume();
        });

        tableExecutionReports.addEventFilter(MouseEvent.MOUSE_PRESSED, event -> {
            if (event.getButton() == MouseButton.SECONDARY) {
                selectCellUnderMouse(event);
            }
        });

        tableExecutionReports.addEventFilter(KeyEvent.KEY_PRESSED, event -> {
            if (event.isShortcutDown() && event.getCode() == KeyCode.C) {
                copyToClipboard(buildSelectedCellsText());
                event.consume();
            }
        });
    }

    private ContextMenu buildOrderContextMenu() {
        MenuItem copyCell = new MenuItem("Copiar celda");
        copyCell.setOnAction(event -> copyToClipboard(buildFirstSelectedCellText()));

        MenuItem copySelection = new MenuItem("Copiar seleccion");
        copySelection.setOnAction(event -> copyToClipboard(buildSelectedCellsText()));

        MenuItem copyRow = new MenuItem("Copiar fila");
        copyRow.setOnAction(event -> copyToClipboard(buildSelectedRowText()));

        ContextMenu menu = new ContextMenu(copyCell, copySelection, copyRow, new SeparatorMenuItem());
        ColumnConfig cfg = Repository.getColumnConfig();

        for (OrderColumnDefinition definition : orderColumnDefinitions()) {
            if (isAlwaysHiddenOrderColumn(definition.key())) {
                continue;
            }

            CheckMenuItem item = new CheckMenuItem(definition.label());
            item.setSelected(cfg.isOrderColumnVisible(definition.key(), definition.column().isVisible()));
            if (Repository.getIsLight() && isLightHiddenColumn(definition.key())) {
                item.setSelected(false);
                item.setDisable(true);
            }
            item.selectedProperty().addListener((obs, oldValue, selected) -> {
                cfg.setOrderColumnVisible(definition.key(), selected);
                Repository.saveColumnConfig();
                if (orderColumnConfigChangeHandler != null) {
                    orderColumnConfigChangeHandler.run();
                } else {
                    applyOrderColumnConfig();
                }
            });
            menu.getItems().add(item);
        }

        return menu;
    }

    @SuppressWarnings("unchecked")
    private void selectCellUnderMouse(MouseEvent event) {
        TableCell<?, ?> cell = findParentCell(event.getPickResult().getIntersectedNode());
        if (cell == null || cell.isEmpty()) {
            return;
        }

        TableColumn<RoutingMessage.Order, ?> column = (TableColumn<RoutingMessage.Order, ?>) cell.getTableColumn();
        tableExecutionReports.getSelectionModel().clearAndSelect(cell.getIndex(), column);
    }

    private TableCell<?, ?> findParentCell(Node node) {
        while (node != null && !(node instanceof TableCell<?, ?>)) {
            node = node.getParent();
        }
        return node instanceof TableCell<?, ?> cell ? cell : null;
    }

    private String buildFirstSelectedCellText() {
        ObservableList<TablePosition> selectedCells = tableExecutionReports.getSelectionModel().getSelectedCells();
        if (selectedCells == null || selectedCells.isEmpty()) {
            return buildSelectedRowText();
        }

        TablePosition position = selectedCells.get(0);
        RoutingMessage.Order row = tableExecutionReports.getItems().get(position.getRow());
        TableColumn<RoutingMessage.Order, ?> column = position.getTableColumn();
        return getCellText(row, column);
    }

    private String buildSelectedCellsText() {
        ObservableList<TablePosition> selectedCells = tableExecutionReports.getSelectionModel().getSelectedCells();
        if (selectedCells == null || selectedCells.isEmpty()) {
            return buildSelectedRowText();
        }

        Map<Integer, List<TablePosition>> cellsByRow = selectedCells.stream()
                .collect(Collectors.groupingBy(TablePosition::getRow, TreeMap::new, Collectors.toList()));

        return cellsByRow.values().stream()
                .map(rowCells -> rowCells.stream()
                        .sorted(Comparator.comparingInt(TablePosition::getColumn))
                        .map(position -> {
                            RoutingMessage.Order row = tableExecutionReports.getItems().get(position.getRow());
                            return getCellText(row, position.getTableColumn());
                        })
                        .collect(Collectors.joining("\t")))
                .collect(Collectors.joining(System.lineSeparator()));
    }

    private String buildSelectedRowText() {
        RoutingMessage.Order selected = tableExecutionReports.getSelectionModel().getSelectedItem();
        if (selected == null) {
            return "";
        }

        return orderColumnDefinitions().stream()
                .map(OrderColumnDefinition::column)
                .filter(TableColumn::isVisible)
                .map(column -> getCellText(selected, column))
                .collect(Collectors.joining("\t"));
    }

    private String getCellText(RoutingMessage.Order row, TableColumn<RoutingMessage.Order, ?> column) {
        if (row == null || column == null) {
            return "";
        }
        ObservableValue<?> value = column.getCellObservableValue(row);
        Object raw = value == null ? null : value.getValue();
        return raw == null ? "" : String.valueOf(raw);
    }

    private void copyToClipboard(String text) {
        if (text == null || text.isEmpty()) {
            return;
        }

        ClipboardContent content = new ClipboardContent();
        content.putString(text);
        Clipboard.getSystemClipboard().setContent(content);
        Notifier.INSTANCE.notifyInfo("Copiado", "Texto copiado al portapapeles");
    }

    public void basketOrder() {

        TableColumn<RoutingMessage.Order, Void> cancelColumn = new TableColumn<>("Cancelar");
        cancelColumn.setPrefWidth(80);
        cancelColumn.setSortable(false);

        cancelColumn.setCellFactory(new Callback<TableColumn<RoutingMessage.Order, Void>, TableCell<RoutingMessage.Order, Void>>() {
            @Override
            public TableCell<RoutingMessage.Order, Void> call(TableColumn<RoutingMessage.Order, Void> param) {
                return new TableCell<RoutingMessage.Order, Void>() {
                    private final Button cancelButton = new Button("Cancelar");
                    {
                        cancelButton.getStyleClass().add("btn-cancelOrder-style");
                        cancelButton.setOnAction(event -> {
                            RoutingMessage.Order o = rowOrder();
                            if (o == null) return;
                            RoutingMessage.OrderStatus st = o.getOrdStatus();

                            if (st.equals(RoutingMessage.OrderStatus.PENDING_NEW)) {
                                // Aún no salió al mercado -> solo se quita de la canasta.
                                data.remove(o);
                                Notifier.INSTANCE.notifyInfo("Canasta", "Orden quitada (no estaba en el mercado)");
                            } else if (st.equals(RoutingMessage.OrderStatus.NEW)
                                    || st.equals(RoutingMessage.OrderStatus.REPLACED)
                                    || st.equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                                    || st.equals(RoutingMessage.OrderStatus.PENDING_REPLACE)) {
                                // Viva en la bolsa -> cancelación real.
                                RoutingMessage.OrderCancelRequest req = RoutingMessage.OrderCancelRequest.newBuilder()
                                        .setId(o.getId()).build();
                                Repository.getClientService().sendMessage(req);
                                Notifier.INSTANCE.notifyInfo("Canasta", "Cancelación enviada: " + o.getSymbol());
                            } else {
                                Notifier.INSTANCE.notifyError("Canasta", "La orden ya está terminada (no se puede cancelar)");
                            }
                        });
                    }

                    /** Orden de la fila, con guardas de índice. */
                    private RoutingMessage.Order rowOrder() {
                        int i = getIndex();
                        if (i < 0 || getTableView() == null || i >= getTableView().getItems().size()) return null;
                        return getTableView().getItems().get(i);
                    }

                    @Override
                    protected void updateItem(Void item, boolean empty) {
                        super.updateItem(item, empty);
                        RoutingMessage.Order o = empty ? null : rowOrder();
                        if (o == null) {
                            setGraphic(null);
                            return;
                        }
                        RoutingMessage.OrderStatus st = o.getOrdStatus();
                        boolean terminal = st.equals(RoutingMessage.OrderStatus.FILLED)
                                || st.equals(RoutingMessage.OrderStatus.CANCELED)
                                || st.equals(RoutingMessage.OrderStatus.REJECTED);
                        setGraphic(terminal ? null : cancelButton);
                    }
                };
            }
        });

        tableExecutionReports.getColumns().add(cancelColumn);
    }

    public void setMKDController() {

    }

    public Boolean filterSymbol(RoutingMessage.Order order, String symbol) {
        if (symbol.equals("")) {
            return true;
        } else return order.getSymbol().contains(symbol);
    }

    public Boolean filterExchange(RoutingMessage.Order order, RoutingMessage.SecurityExchangeRouting exchange) {
        if (exchange.equals(RoutingMessage.SecurityExchangeRouting.ALL_SECURITY_EXCHANGE)) {
            return true;
        } else return order.getSecurityExchange().equals(exchange);
    }

    public Boolean filterSideString(RoutingMessage.Order order, String side) {
        if (side.contains("Todos")) {
            return true;
        } else if (order.getSide().equals(RoutingMessage.Side.BUY) && side.equals("Compra")) {
            return true;
        } else if (order.getSide().equals(RoutingMessage.Side.SELL) && side.equals("Venta")) {
            return true;
        } else return order.getSide().equals(RoutingMessage.Side.SELL_SHORT) && side.equals("Venta Corta");
    }

    public Boolean filterSide(RoutingMessage.Order order, RoutingMessage.Side side) {
        if (side.equals(RoutingMessage.Side.ALL_SIDE)) {
            return true;
        } else return order.getSide().equals(side);
    }

    // ====== CAMBIO: tolerante a null y “TODAS” ======
    public Boolean filterAccount(RoutingMessage.Order order, String account) {
        if (account == null || account.contains(Repository.getALL_ACCOUNT())) {
            return true;
        } else return order.getAccount().equals(account);
    }
    // ====== FIN CAMBIO ======

    public boolean statusFilter(RoutingMessage.Order order, RoutingMessage.OrderStatus orderStatus) {
        if (orderStatus.equals(RoutingMessage.OrderStatus.PENDING_ONLY)) {
            return order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_CANCEL);
        } else if (orderStatus.equals(RoutingMessage.OrderStatus.PENDING_LIVE)) {
            return (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_CANCEL) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED));
        } else if (orderStatus.equals(RoutingMessage.OrderStatus.LIVE)) {
            return (order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED));
        } else if (orderStatus.equals(RoutingMessage.OrderStatus.LIVE_TRADE)) {
            return (order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED));
        } else if (orderStatus.equals(RoutingMessage.OrderStatus.NEW)) {
            return (order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW));
        } else if (orderStatus.equals(RoutingMessage.OrderStatus.FILLED) || orderStatus.equals(RoutingMessage.OrderStatus.TRADE)) {
            return (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED));
        } else if (orderStatus.equals(RoutingMessage.OrderStatus.ABORTED)) {
            return (order.getOrdStatus().equals(RoutingMessage.OrderStatus.SUSPENDED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED) ||
                    order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED));
        } else if (orderStatus.equals(RoutingMessage.OrderStatus.ALL_STATUS)) {
            return true;
        }

        return false;
    }

    private boolean checkCbConditions(RoutingMessage.Order order,
                                      RoutingMessage.SecurityExchangeRouting secExchangeSelected,
                                      String accountSelected,
                                      RoutingMessage.Side sideSelected,
                                      String symbol) {

        return ((order.getSecurityExchange().equals(secExchangeSelected) || secExchangeSelected.equals(RoutingMessage.SecurityExchangeRouting.UNRECOGNIZED)) &&
                (order.getAccount().equals(accountSelected) || accountSelected.equals("All accounts")) &&
                (order.getSide().equals(sideSelected) || sideSelected.equals(RoutingMessage.Side.NONE_SIDE)) &&
                (order.getSymbol().contains(symbol) || symbol.equals("")));
    }

}
