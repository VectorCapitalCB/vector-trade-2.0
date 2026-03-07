package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.util.Callback;
import lombok.extern.slf4j.Slf4j;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

@Slf4j
public class PreDigitadosController {

    @FXML private TableView<RoutingMessage.Order> tableExecutionReports;

    @FXML private TableColumn<RoutingMessage.Order, String> time;
    @FXML private TableColumn<RoutingMessage.Order, RoutingMessage.Side> side;
    @FXML private TableColumn<RoutingMessage.Order, String> symbol;
    @FXML private TableColumn<RoutingMessage.Order, String> settltypeOrder;
    @FXML private TableColumn<RoutingMessage.Order, String> market;
    @FXML private TableColumn<RoutingMessage.Order, String> qty;
    @FXML private TableColumn<RoutingMessage.Order, String> px;
    @FXML private TableColumn<RoutingMessage.Order, String> amount;
    @FXML private TableColumn<RoutingMessage.Order, String> account;
    @FXML private TableColumn<RoutingMessage.Order, String> limit;

    private final ObservableList<RoutingMessage.Order> data = FXCollections.observableArrayList();
    private FilteredList<RoutingMessage.Order> filteredData;
    private SortedList<RoutingMessage.Order>   sortedData;

    // para no spamear snapshot
    private boolean requestedPreselectSnapshot = false;

    @FXML
    private void initialize() {
        try {

            Repository.setPreDigitadosController(this);

            filteredData = new FilteredList<>(data, p -> true);
            sortedData   = new SortedList<>(filteredData);
            tableExecutionReports.setItems(sortedData);
            sortedData.comparatorProperty().bind(tableExecutionReports.comparatorProperty());
            tableExecutionReports.setEditable(true);
            tableExecutionReports.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);


            DecimalFormat n2 = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
            n2.applyPattern("#,##0.00");
            DecimalFormat n4 = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
            n4.applyPattern("#,##0.0000");


            time.setCellValueFactory(cell -> {
                long s = cell.getValue().getTime().getSeconds();
                int n = cell.getValue().getTime().getNanos();
                ZonedDateTime zdt = Instant.ofEpochSecond(s, n)
                        .atZone(Repository.getZoneID())
                        .plus(1000, ChronoUnit.MILLIS);
                return new SimpleStringProperty(zdt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));
            });

            symbol.setCellValueFactory(c -> new SimpleObjectProperty<>(c.getValue().getSymbol()));
            market.setCellValueFactory(c -> new SimpleObjectProperty<>(c.getValue().getSecurityExchange().name()));

            side.setCellValueFactory(c -> new SimpleObjectProperty<>(c.getValue().getSide()));
            side.setCellFactory(createTypeColumnStyleCallback());

            if (settltypeOrder != null) {
                settltypeOrder.setCellValueFactory(c -> new SimpleObjectProperty<>(c.getValue().getSettlType().name()));
            }

            qty.setCellValueFactory(c -> new SimpleObjectProperty<>(n2.format(c.getValue().getOrderQty())));
            px.setCellValueFactory(c -> new SimpleObjectProperty<>(n4.format(c.getValue().getPrice())));
            amount.setCellValueFactory(c -> new SimpleObjectProperty<>(n2.format(c.getValue().getPrice() * c.getValue().getOrderQty())));
            account.setCellValueFactory(c -> new SimpleObjectProperty<>(c.getValue().getAccount()));
            limit.setCellValueFactory(c -> new SimpleObjectProperty<>(String.valueOf(c.getValue().getLimit())));

            if (!tableExecutionReports.getSortOrder().contains(time)) {
                tableExecutionReports.getSortOrder().add(time);
            }

            addDeleteColumn();

            var pending = Repository.takePendingPreselect();
            if (!pending.isEmpty()) {
                data.setAll(pending);
                tableExecutionReports.refresh();
                tableExecutionReports.sort();
            } else {
                tableExecutionReports.sceneProperty().addListener((obs, oldScene, newScene) -> {
                    if (newScene != null) {
                        newScene.windowProperty().addListener((o, ow, w) -> {
                            if (w != null) requestPreselectSnapshotIfNeeded();
                        });
                    }
                });
                requestPreselectSnapshotIfNeeded();
            }

        } catch (Exception e) {
            log.error("Error init PreDigitados", e);
        }
    }

    private void requestPreselectSnapshotIfNeeded() {
        if (requestedPreselectSnapshot) return;
        requestedPreselectSnapshot = true;

        var req = BlotterMessage.PreselectRequest.newBuilder()
                .setStatusPreselect(BlotterMessage.StatusPreselect.SNAPSHOT_PRESELECT)
                .setUsername(Repository.getUsername())
                .build();
        Repository.getClientService().sendMessage(req);
    }

    private void addDeleteColumn() {
        TableColumn<RoutingMessage.Order, Void> deleteColumn = new TableColumn<>("Eliminar");
        deleteColumn.setPrefWidth(60);
        deleteColumn.setSortable(false);

        deleteColumn.setCellFactory(new Callback<>() {
            @Override public TableCell<RoutingMessage.Order, Void> call(TableColumn<RoutingMessage.Order, Void> param) {
                return new TableCell<>() {
                    private final Button btn = new Button("X");
                    {
                        btn.setOnAction(e -> {
                            RoutingMessage.Order row = getTableView().getItems().get(getIndex());
                            var req = BlotterMessage.PreselectRequest.newBuilder()
                                    .setStatusPreselect(BlotterMessage.StatusPreselect.REMOVE_PRESELECT)
                                    .setOrders(row)
                                    .setUsername(Repository.getUsername())
                                    .build();
                            Repository.getClientService().sendMessage(req);
                        });
                    }
                    @Override protected void updateItem(Void item, boolean empty) {
                        super.updateItem(item, empty);
                        setGraphic(empty ? null : btn);
                    }
                };
            }
        });

        if (tableExecutionReports.getColumns().stream().noneMatch(c -> "Eliminar".equals(c.getText()))) {
            tableExecutionReports.getColumns().add(deleteColumn);
        }
    }

    private Callback<TableColumn<RoutingMessage.Order, RoutingMessage.Side>, TableCell<RoutingMessage.Order, RoutingMessage.Side>>
    createTypeColumnStyleCallback() {
        return tableColumn -> new TableCell<>() {
            @Override protected void updateItem(RoutingMessage.Side side, boolean empty) {
                super.updateItem(side, empty);
                if (empty || side == null) { setText(null); setStyle(""); return; }
                setText(side.name());
                switch (side) {
                    case BUY -> setStyle("-fx-text-fill: green;");
                    case SELL, SELL_SHORT -> setStyle("-fx-text-fill: red;");
                    default -> setStyle("");
                }
            }
        };
    }


    public TableView<RoutingMessage.Order> getTableExecutionReports() { return tableExecutionReports; }
    public ObservableList<RoutingMessage.Order> getData() { return data; }
    public FilteredList<RoutingMessage.Order> getFilteredData() { return filteredData; }
    public SortedList<RoutingMessage.Order> getSortedData() { return sortedData; }
}
