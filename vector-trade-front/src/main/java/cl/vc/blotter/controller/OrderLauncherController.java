package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.beans.value.ChangeListener;
import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextField;

public class OrderLauncherController {

    @FXML
    private TextField ticket;

    @FXML
    private ComboBox<String> sideOrder;

    @FXML
    private TextField priceOrder;

    @FXML
    private TextField quantity;

    @FXML
    private TextField amountField;

    @FXML
    private ComboBox<String> accountComboBox;

    @FXML
    private ComboBox<RoutingMessage.OrdType> typeOrder;

    @FXML
    private ComboBox<RoutingMessage.SecurityExchangeRouting> secExchOrder;


    private MarketDataMessage.DataBook dataBook;

    public void initialize() {

        sideOrder.getItems().addAll("Compra", "Venta");
        sideOrder.getSelectionModel().selectFirst();


        accountComboBox.getItems().addAll(Repository.getUser().getAccountList());
        accountComboBox.getSelectionModel().selectFirst();

        typeOrder.getItems().addAll(RoutingMessage.OrdType.values());
        typeOrder.getItems().remove(RoutingMessage.OrdType.UNRECOGNIZED);
        typeOrder.getItems().remove(RoutingMessage.OrdType.NONE);
        typeOrder.getSelectionModel().select(RoutingMessage.OrdType.LIMIT);


        ChangeListener<String> amountUpdater = (observable, oldValue, newValue) -> calculateAmount();

        quantity.textProperty().addListener(amountUpdater);
        priceOrder.textProperty().addListener(amountUpdater);
    }

    private void calculateAmount() {
        try {
            String quantityText = quantity.getText();
            String priceText = priceOrder.getText();

            quantityText = quantityText.replace(",", "").trim();
            priceText = priceText.replace(",", "").trim();

            double quantityValue = Double.parseDouble(quantityText);
            double priceValue = Double.parseDouble(priceText);
            double amount = quantityValue * priceValue;

            amountField.setText(String.format("%.2f", amount));
        } catch (NumberFormatException e) {
            amountField.setText("");
        }
    }

    public void initData(MarketDataMessage.DataBook dataBook) {
        this.dataBook = dataBook;

        ticket.setText(dataBook.getSymbol());
        priceOrder.setText(String.valueOf(dataBook.getPrice()));
        quantity.setText(String.valueOf(dataBook.getSize()));

        calculateAmount();
    }
}
