package cl.vc.blotter.controller;


import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.Node;
import javafx.scene.chart.PieChart;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.stage.StageStyle;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Locale;

@Slf4j
@Data
public class BasketTabController {

    public ObservableList<BktStrategyProtos.Basket> data = FXCollections.observableArrayList();
    NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);
    @FXML
    private TableView<BktStrategyProtos.Basket> basketMainTable;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, String> basketName;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> nBuy;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> nSell;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> totalAmount;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> amountPercDone;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> amountPercLeft;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> totalQty;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> totalAmountDone;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> totalAmountLeft;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> totalQtyDone;
    @FXML
    private TableColumn<BktStrategyProtos.Basket, Double> totalQtyLeft;
    @FXML
    private Button execButton;
    @FXML
    private Button cancelOrder;
    @FXML
    private HBox chartContainer;
    private FilteredList<BktStrategyProtos.Basket> filteredData;
    @Getter
    private SortedList<BktStrategyProtos.Basket> sortedData;
    @FXML
    private ExecutionsController executionsOrderController;

    @FXML
    private void initialize() {

        executionsOrderController.getHandlInst().setVisible(false);
        executionsOrderController.getSettlType().setVisible(false);
        executionsOrderController.getUsername().setVisible(true);
        executionsOrderController.getBasket().setVisible(true);
        executionsOrderController.getIceberg().setVisible(false);
        executionsOrderController.getExecType().setVisible(false);


        basketName.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getBasketID()));
        nBuy.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getNBuy()));
        nSell.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getNSell()));
        totalAmount.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getTotalAmount()));

        amountPercDone.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getAmountPercDone()));
        amountPercLeft.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getAmountPercLeft()));
        totalQty.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getTotalQty()));
        totalAmountDone.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getTotalAmountDone()));
        totalQtyDone.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getTotalQtyDone()));
        totalQtyLeft.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getTotalQtyLeft()));
        totalAmountLeft.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getTotalAmountLeft()));

        this.basketMainTable.setCache(false);
        this.data = FXCollections.observableArrayList(new ArrayList<>());
        this.filteredData = new FilteredList<>(this.data, p -> true);
        this.sortedData = new SortedList<>(this.filteredData);
        sortedData.comparatorProperty().bind(basketMainTable.comparatorProperty());
        this.basketMainTable.setItems(this.sortedData);

        numberFormat.setMaximumFractionDigits(2);

        totalAmount.setCellFactory(column -> new TableCell<BktStrategyProtos.Basket, Double>() {
            @Override
            protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (item == null || empty) {
                    setText(null);
                } else {
                    setText("$" + numberFormat.format(item));
                }
            }
        });


        totalAmountDone.setCellFactory(column -> new TableCell<BktStrategyProtos.Basket, Double>() {
            @Override
            protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (item == null || empty) {
                    setText(null);
                } else {
                    setText("$" + numberFormat.format(item));
                }
            }
        });

        totalAmountLeft.setCellFactory(column -> new TableCell<BktStrategyProtos.Basket, Double>() {
            @Override
            protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (item == null || empty) {
                    setText(null);
                } else {
                    setText("$" + numberFormat.format(item));
                }
            }
        });


        amountPercDone.setCellFactory(column -> new TableCell<BktStrategyProtos.Basket, Double>() {
            @Override
            protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (item == null || empty) {
                    setText(null);
                } else {
                    setText(String.format("%.2f", item) + "%");
                }
            }
        });

        amountPercLeft.setCellFactory(column -> new TableCell<BktStrategyProtos.Basket, Double>() {
            @Override
            protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (item == null || empty) {
                    setText(null);
                } else {
                    setText(String.format("%.2f", item) + "%");
                }
            }
        });

        executionsOrderController.basketOrder();

        executionsOrderController.getTableExecutionReports().getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
            if (newValue != null) {

                if (newValue.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) || newValue.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)
                        || newValue.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {
                    cancelOrder.setVisible(true);
                } else {
                    cancelOrder.setVisible(false);
                }

            }
        });


        executionsOrderController.getTableExecutionReports().getItems().addListener((ListChangeListener<RoutingMessage.Order>) change -> {


            Platform.runLater(() -> {

                BktStrategyProtos.Basket newValue = basketMainTable.getItems().get(0);

                String buyColor = "#00bfff";
                String sellColor = "#ff4500";
                String totalAmountColor = "#00ff7f";
                String amountLeftColor = "#ff69b4";
                String volumeDoneColor = "#98fb98";
                String volumeLeftColor = "#ffa500";

                double nBuy = newValue.getNBuy();
                double nSell = newValue.getNSell();

                ObservableList<PieChart.Data> pieChartData1 = FXCollections.observableArrayList(
                        new PieChart.Data("Buy", nBuy),
                        new PieChart.Data("Sell", nSell)
                );

                ObservableList<PieChart.Data> pieChartData2 = FXCollections.observableArrayList(
                        new PieChart.Data("Total Amount Done", newValue.getAmountPercDone()),
                        new PieChart.Data("Amount Left", newValue.getAmountPercLeft())
                );

                ObservableList<PieChart.Data> pieChartData3 = FXCollections.observableArrayList(
                        new PieChart.Data("Qty Done", newValue.getTotalQtyDone()),
                        new PieChart.Data("Qty Left", newValue.getTotalQtyLeft())
                );


                /*
                PieChart pieChart1 = createCustomPieChart(pieChartData1, "Buy vs Sell", buyColor, sellColor);
                PieChart pieChart2 = createCustomPieChart(pieChartData2, "Total Amount vs Amount Left", totalAmountColor, amountLeftColor);
                PieChart pieChart3 = createCustomPieChart(pieChartData3, "Qty Done vs Qty Left", volumeDoneColor, volumeLeftColor);

                chartContainer.getChildren().clear();
                chartContainer.getChildren().addAll(pieChart1, pieChart2, pieChart3);

                 */

            });
        });

    }


    private PieChart createCustomPieChart(ObservableList<PieChart.Data> data, String title, String... colors) {

        PieChart pieChart = new PieChart(data);
        pieChart.setTitle(title);

        int colorIndex = 0;
        for (PieChart.Data slice : data) {
            String color = colors[colorIndex % colors.length];
            slice.getNode().setStyle("-fx-pie-color: " + color + ";");
            colorIndex++;
        }

        return pieChart;
    }

    @FXML
    public void execAll(ActionEvent actionEvent) {


        if (!alertRoute("Exec All")) return;

        executionsOrderController.getData().forEach(s -> {
            if (s.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(s).build();
                Repository.getClientService().sendMessage(newOrderRequest);
            }
        });

    }

    @FXML
    public void cancelOrder(ActionEvent actionEvent) {

        if (!alertRoute("Cancel Order")) return;

        RoutingMessage.Order order = executionsOrderController.getTableExecutionReports().getSelectionModel().getSelectedItem();
        RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder().setId(order.getId()).build();
        Repository.getClientService().sendMessage(orderCancelRequest);

    }

    @FXML
    public void cancelAll(ActionEvent actionEvent) {

        if (!alertRoute("Cancel All")) return;

        executionsOrderController.getData().forEach(s -> {

            if (s.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                    s.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                    s.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {

                RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder().setId(s.getId()).build();
                Repository.getClientService().sendMessage(orderCancelRequest);

            }

        });

    }


    public Boolean alertRoute(String text) {

        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirm Action");

        alert.setContentText(text);
        String cssPath = getClass().getResource(Repository.getSTYLE()).toExternalForm();
        alert.getDialogPane().getStylesheets().add(cssPath);
        alert.getDialogPane().getStyleClass().add("alert-dialog");

        Node cancelButton = alert.getDialogPane().lookupButton(ButtonType.CANCEL);
        Node acceptButton = alert.getDialogPane().lookupButton(ButtonType.OK);

        cancelButton.getStyleClass().add("button");
        acceptButton.getStyleClass().addAll("button", "cancel");

        alert.initStyle(StageStyle.UTILITY);
        alert.showAndWait();

        if (alert.getResult() == ButtonType.OK) {
            return true;
        } else {
            return false;
        }
    }
}
