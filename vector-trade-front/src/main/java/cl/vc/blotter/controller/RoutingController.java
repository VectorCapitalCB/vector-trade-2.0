package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.Notifier;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.module.protocolbuff.utils.ProtoConverter;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.Region;
import javafx.stage.Popup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

@Data
@Slf4j
public class RoutingController {

    @FXML
    private ExecutionsController workingOrderController;
    @FXML
    private ExecutionsController executionsOrderController;
    @FXML
    private Button best;
    @FXML
    private Button hit;
    @FXML
    private Button tickMas;
    @FXML
    private Button tickMenos;
    @FXML
    private TextField limit2;
    @FXML
    private TextField priceOrder2;
    @FXML
    private Button replaceOrder;
    @FXML
    private TextField quantity2;
    @FXML
    private TextField spread2;
    @FXML
    private TextField visibleid;
    @FXML
    private Label lblquantity;
    @FXML
    private Label lblpriceOrder2;
    @FXML
    private Button cancelOrder;
    @FXML
    private Label estrategiaLabel;
    @FXML
    private ComboBox<RoutingMessage.StrategyOrder> estrategia2;
    @FXML
    private Label spreadLabel;
    @FXML
    private Label limitLabel;
    @FXML
    private Button StopAll;
    @FXML
    private Label visible;
    @FXML
    private TextField accountFilter;
    @FXML
    private ComboBox<RoutingMessage.SecurityExchangeRouting> securityExchangeFilter;
    @FXML
    private ComboBox<RoutingMessage.OrderStatus> statusFilter;
    @FXML
    private ComboBox<String> sideFilter;
    @FXML
    private TextField symbolFilter;
    @FXML
    private CheckBox hideIDs;
    @FXML
    public Tab tabRuteo;
    private boolean isVertical = true;

    private final ComboBox<String> accountFilterShim = new ComboBox<>();
    private FilteredList<String> filteredAccountList;
    private ListView<String> accountSuggestionsList;
    private Popup accountSuggestionsPopup;
    private boolean isUpdatingFromAccount = false;
    private boolean ignoreFirstAccountSelect = true;
    private final ObservableList<String> allAccounts = FXCollections.observableArrayList();

    @FXML
    private void initialize() {
        try {

            Repository.setRoutingController(this);

            if (Repository.getUser() != null) {
                allAccounts.setAll(Repository.getUser().getAccountList());
            }

            accountFilterShim.itemsProperty().addListener((obs, old, newItems) -> {
                if (newItems != null) allAccounts.setAll(newItems);
                else allAccounts.clear();
            });
            accountFilterShim.getSelectionModel().selectedItemProperty().addListener((o, old, sel) -> {
                if (ignoreFirstAccountSelect) {
                    ignoreFirstAccountSelect = false;
                    Platform.runLater(() -> {
                        accountFilter.clear();
                        accountFilterShim.getSelectionModel().clearSelection();
                    });
                    return;
                }
                if (sel != null) {
                    isUpdatingFromAccount = true;
                    accountFilter.setText(sel);
                    accountFilter.positionCaret(sel.length());
                    isUpdatingFromAccount = false;
                    if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();
                }
            });

            statusFilter.getItems().addAll(RoutingMessage.OrderStatus.values());
            statusFilter.getSelectionModel().select(RoutingMessage.OrderStatus.ALL_STATUS);

            Arrays.asList(RoutingMessage.Side.values()).forEach(s -> {
                if (!s.name().equals("NONE_SIDE")
                        && !s.name().equals("UNRECOGNIZED")
                        && !s.name().equals("ALL_SIDE")
                        && !s.name().equals("SELL_SHORT")) {
                    String value = ProtoConverter.routingDecryptStatus(s.name());
                    sideFilter.getItems().add(value);
                }
            });

            securityExchangeFilter.setItems(FXCollections.observableArrayList(RoutingMessage.SecurityExchangeRouting.values()));
            securityExchangeFilter.getSelectionModel().select(RoutingMessage.SecurityExchangeRouting.ALL_SECURITY_EXCHANGE);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.UNRECOGNIZED);

            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.XBOG);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.BASKETS);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.XSGO_OFS);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.XLIM);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.BINANCE);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.IB_SMART);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.ALPACA);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.BBG);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.MEXC);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.CRYPTO_MARKET);
            securityExchangeFilter.getItems().remove(RoutingMessage.SecurityExchangeRouting.XBCL);

            sideFilter.getItems().add("Todos");
            sideFilter.getSelectionModel().select("Todos");

            symbolFilter.setTextFormatter(new TextFormatter<String>(change -> {
                change.setText(change.getText().toUpperCase(Locale.ROOT));
                return change;
            }));

            workingOrderController.getTableExecutionReports().setOnMouseClicked(event -> {

                if (event.getClickCount() == 1 && event.getButton() == MouseButton.PRIMARY) {

                    RoutingMessage.Order selectedItem = workingOrderController.getTableExecutionReports().getSelectionModel().getSelectedItem();
                    if (selectedItem == null) {
                        log.error("No order selected. `selectedItem` is null.");
                        return;
                    }

                    Repository.getPrincipalController().setOrderSelected(selectedItem);

                    String buttonStyleClass = selectedItem.getSide().name().equals("SELL") ? "sellbutton-hoverable" : "buybutton-hoverable";
                    Repository.getPrincipalController().getLanzadorController().clearButtonStyles();

                    RoutingMessage.OrderStatus status = selectedItem.getOrdStatus();

                    if (status.equals(RoutingMessage.OrderStatus.FILLED) ||
                            status.equals(RoutingMessage.OrderStatus.REJECTED) ||
                            status.equals(RoutingMessage.OrderStatus.CANCELED)) {

                        Repository.getLanzadorController().setValues(selectedItem);

                        estrategiaLabel.setDisable(true);
                        estrategia2.setDisable(true);
                        spreadLabel.setDisable(true);
                        spread2.setDisable(true);
                        limitLabel.setDisable(true);
                        limit2.setDisable(true);
                        best.setDisable(true);
                        hit.setDisable(true);
                        tickMas.setDisable(true);
                        tickMenos.setDisable(true);
                        replaceOrder.setDisable(true);
                        quantity2.setDisable(true);
                        priceOrder2.setDisable(true);
                        cancelOrder.setDisable(true);
                        lblquantity.setDisable(true);
                        lblpriceOrder2.setDisable(true);
                        visible.setDisable(true);
                        visibleid.setDisable(true);

                        hit.getStyleClass().add(buttonStyleClass);
                        best.getStyleClass().add(buttonStyleClass);

                    } else {

                        estrategiaLabel.setVisible(true);
                        estrategia2.setVisible(true);
                        spreadLabel.setVisible(true);
                        spread2.setVisible(true);
                        limitLabel.setVisible(true);
                        limit2.setVisible(true);
                        best.setVisible(true);
                        hit.setVisible(true);
                        tickMas.setVisible(true);
                        tickMenos.setVisible(true);
                        replaceOrder.setVisible(true);
                        replaceOrder.setVisible(true);
                        quantity2.setVisible(true);
                        priceOrder2.setVisible(true);
                        cancelOrder.setVisible(true);
                        lblquantity.setVisible(true);
                        lblpriceOrder2.setVisible(true);
                        visible.setVisible(true);
                        visibleid.setVisible(true);

                        estrategiaLabel.setDisable(false);
                        estrategia2.setDisable(false);
                        spreadLabel.setDisable(false);
                        spread2.setDisable(false);
                        limitLabel.setDisable(false);
                        limit2.setDisable(false);
                        best.setDisable(false);
                        hit.setDisable(false);
                        tickMas.setDisable(false);
                        tickMenos.setDisable(false);
                        replaceOrder.setDisable(false);
                        quantity2.setDisable(false);
                        priceOrder2.setDisable(false);
                        cancelOrder.setDisable(false);
                        lblquantity.setDisable(false);
                        lblpriceOrder2.setDisable(false);
                        visible.setDisable(false);
                        visibleid.setDisable(false);

                        Repository.getLanzadorController().setValues(selectedItem);

                        hit.getStyleClass().add(buttonStyleClass);
                        best.getStyleClass().add(buttonStyleClass);
                    }
                }
            });

            setupAccountFilterAutocomplete();

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    @FXML
    public void bestAction2(ActionEvent actionEvent) {
        Repository.getLanzadorController().bestActions(actionEvent);
    }

    @FXML
    public void hitAction2(ActionEvent actionEvent) {
        Repository.getLanzadorController().hitActions(actionEvent);
    }

    public RoutingMessage.Order findOrder(RoutingMessage.Order name) {
        if (name == null) {
            log.error("Cannot search for a null order.");
            return null;
        }

        for (RoutingMessage.Order order : workingOrderController.getTableExecutionReports().getItems()) {
            if (order.getId().equals(name.getId())) {
                return order;
            }
        }
        log.info("Order with ID {} not found.", name.getId());
        return null;
    }

    @FXML
    public void stopAll() {
        ObservableList<RoutingMessage.Order> orders = Repository.getRoutingController().getWorkingOrderController().getTableExecutionReports().getItems();
        orders.forEach(s -> {
            if (s.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) || s.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)
                    || s.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {

                RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder()
                        .setId(s.getId())
                        .build();

                Repository.getClientService().sendMessage(orderCancelRequest);
            }
        });
    }

    @FXML
    public void tickMas() {
        try {
            RoutingMessage.StrategyOrder strategyOrder = Repository.getPrincipalController().getOrderSelected() != null ? Repository.getPrincipalController().getOrderSelected().getStrategyOrder() : null;

            if (strategyOrder != null && strategyOrder.equals(RoutingMessage.StrategyOrder.BEST)) {
                String limitText = limit2.getText();
                if (limitText == null || limitText.isEmpty()) {
                    tickMas.setDisable(true);
                    Notifier.INSTANCE.notifyError("Error", "El campo de límite está vacío.");
                    return;
                }
                tickMas.setDisable(false);
                BigDecimal currentLimit = new BigDecimal(limitText.replace(",", ""));
                BigDecimal tick = Ticks.getTick(Repository.getLanzadorController().getSecExchOrder().getSelectionModel().getSelectedItem(), currentLimit);
                BigDecimal newLimit = currentLimit.add(tick);
                limit2.setText(newLimit.toPlainString());
            } else {
                String priceText = priceOrder2.getText();
                if (priceText == null || priceText.isEmpty()) {
                    tickMas.setDisable(true);
                    Notifier.INSTANCE.notifyError("Error", "El campo de precio está vacío.");
                    return;
                }
                tickMas.setDisable(false);
                BigDecimal currentPrice = new BigDecimal(priceText.replace(",", ""));
                BigDecimal tick = Ticks.getTick(Repository.getLanzadorController().getSecExchOrder().getSelectionModel().getSelectedItem(), currentPrice);
                BigDecimal newPrice = currentPrice.add(tick);
                priceOrder2.setText(newPrice.toPlainString());
            }
        } catch (NumberFormatException e) {
            Notifier.INSTANCE.notifyError("Error", "Formato de límite no válido.");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Notifier.INSTANCE.notifyError("Error", "Ocurrió un error al procesar la orden.");
        }
    }

    @FXML
    public void tickMenos() {
        try {
            RoutingMessage.StrategyOrder strategyOrder = Repository.getPrincipalController().getOrderSelected() != null ? Repository.getPrincipalController().getOrderSelected().getStrategyOrder() : null;

            if (strategyOrder != null && strategyOrder.equals(RoutingMessage.StrategyOrder.BEST)) {
                String limitText = limit2.getText();
                if (limitText == null || limitText.isEmpty()) {
                    tickMenos.setDisable(true);
                    Notifier.INSTANCE.notifyError("Error", "El campo de límite está vacío.");
                    return;
                }
                tickMenos.setDisable(false);
                BigDecimal currentLimit = new BigDecimal(limitText.replace(",", ""));
                BigDecimal tick = Ticks.getTick(Repository.getLanzadorController().getSecExchOrder().getSelectionModel().getSelectedItem(), currentLimit);
                BigDecimal newLimit = currentLimit.subtract(tick);
                if (newLimit.compareTo(BigDecimal.ZERO) > 0) {
                    limit2.setText(newLimit.toPlainString());
                } else {
                    Notifier.INSTANCE.notifyError("Error", "El límite no puede ser negativo.");
                }
            } else {
                String priceText = priceOrder2.getText();
                if (priceText == null || priceText.isEmpty()) {
                    tickMenos.setDisable(true);
                    Notifier.INSTANCE.notifyError("Error", "El campo de precio está vacío.");
                    return;
                }
                tickMenos.setDisable(false);
                BigDecimal currentPrice = new BigDecimal(priceText.replace(",", ""));
                BigDecimal tick = Ticks.getTick(Repository.getLanzadorController().getSecExchOrder().getSelectionModel().getSelectedItem(), currentPrice);
                BigDecimal newPrice = currentPrice.subtract(tick);
                if (newPrice.compareTo(BigDecimal.ZERO) > 0) {
                    priceOrder2.setText(newPrice.toPlainString());
                } else {
                    Notifier.INSTANCE.notifyError("Error", "El precio no puede ser negativo.");
                }
            }
        } catch (NumberFormatException e) {
            Notifier.INSTANCE.notifyError("Error", "Formato de límite no válido.");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Notifier.INSTANCE.notifyError("Error", "Ocurrió un error al procesar la orden.");
        }
    }

    @FXML
    public void replaceOrderAction() {

        if (Repository.getPrincipalController().getOrderSelected() == null) {
            log.error("No order selected to replace. `orderSelected` is null.");
            return;
        }

        Repository.getPrincipalController().setOrderSelected(findOrder(Repository.getPrincipalController().getOrderSelected()));

        if (Repository.getPrincipalController().getOrderSelected() == null) {
            log.error("Order not found in the list for replacement.");
            return;
        }

        RoutingMessage.OrderReplaceRequest.Builder replace = RoutingMessage.OrderReplaceRequest.newBuilder();
        String message;

        if (Repository.getPrincipalController().getOrderSelected().getStrategyOrder().equals(RoutingMessage.StrategyOrder.BEST)) {
            replace.setId(Repository.getPrincipalController().getOrderSelected().getId())
                    .setLimit(Double.parseDouble(limit2.getText().replace(",", "")))
                    .setQuantity(Double.parseDouble(quantity2.getText().replace(",", "")));
            message = "Cantidad: " + quantity2.getText() + " - Límite: " + limit2.getText();

        } else if (Repository.getPrincipalController().getOrderSelected().getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_PASSIVE)
                || (Repository.getPrincipalController().getOrderSelected().getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_AGGRESSIVE)
                || (Repository.getPrincipalController().getOrderSelected().getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_LAST)))) {

            replace.setId(Repository.getPrincipalController().getOrderSelected().getId())
                    .setLimit(Double.parseDouble(limit2.getText().replace(",", "")))
                    .setQuantity(Double.parseDouble(quantity2.getText().replace(",", "")));
            message = "Cantidad: " + quantity2.getText() + " - Límite: " + limit2.getText();

        } else if (Repository.getPrincipalController().getOrderSelected().getStrategyOrder().equals(RoutingMessage.StrategyOrder.HOLGURA)) {
            replace.setId(Repository.getPrincipalController().getOrderSelected().getId())
                    .setPrice(Double.parseDouble(priceOrder2.getText().replace(",", "")))
                    .setSpread(Double.parseDouble(spread2.getText().replace(",", "")))
                    .setQuantity(Double.parseDouble(quantity2.getText().replace(",", "")));
            message = "Cantidad: " + quantity2.getText() + " - Precio: " + priceOrder2.getText() + " - Spread: " + spread2.getText();

        } else if (Repository.getPrincipalController().getOrderSelected().getStrategyOrder().equals(RoutingMessage.StrategyOrder.TRAILING)) {
            replace.setId(Repository.getPrincipalController().getOrderSelected().getId())
                    .setLimit(Double.parseDouble(Repository.getLanzadorController().getLimit().getText().replace(",", "")))
                    .setQuantity(Double.parseDouble(Repository.getLanzadorController().getQuantity().getText().replace(",", "")));
            message = "Cantidad: " + Repository.getLanzadorController().getQuantity().getText() + " - Límite: " + Repository.getLanzadorController().getLimit().getText();

        } else if (Repository.getPrincipalController().getOrderSelected().getStrategyOrder().equals(RoutingMessage.StrategyOrder.OCO)) {
            replace.setId(Repository.getPrincipalController().getOrderSelected().getId())
                    .setSpread(Double.parseDouble(Repository.getLanzadorController().getSpread().getText().replace(",", "")))
                    .setLimit(Double.parseDouble(Repository.getLanzadorController().getLimit().getText().replace(",", "")))
                    .setQuantity(Double.parseDouble(Repository.getLanzadorController().getQuantity().getText().replace(",", "")));
            message = "Cantidad: " + Repository.getLanzadorController().getQuantity().getText() + " - Spread: " + Repository.getLanzadorController().getSpread().getText() + " - Límite: " + Repository.getLanzadorController().getLimit().getText();

        } else {

            if (!visibleid.getText().isEmpty()) {
                double quantityValue = Double.parseDouble(quantity2.getText().replace(",", ""));
                double icebergValue = Double.parseDouble(visibleid.getText().replace("%", "").replace(".", "").replace(",", ""));
                double calculatedIceberg = (quantityValue * icebergValue) / 100;
                double minIceberg = (quantityValue * 0.1);
                if (calculatedIceberg < minIceberg) {
                    calculatedIceberg = minIceberg;
                }
                int maxFloor = (int) Math.ceil(calculatedIceberg);
                replace.setIcebergPercentage(String.valueOf(icebergValue));
                replace.setMaxFloor(maxFloor);
            }

            replace.setId(Repository.getPrincipalController().getOrderSelected().getId())
                    .setPrice(Double.parseDouble(priceOrder2.getText().replace(",", "")))
                    .setQuantity(Double.parseDouble(quantity2.getText().replace(",", "")));
            message = "Cantidad: " + quantity2.getText() + " - Precio: " + priceOrder2.getText();
        }

        if (Repository.getPrincipalController().getOrderSelected().getOrderQty() <= 0) {
            Notifier.INSTANCE.notifyError("Error", "La cantidad debe ser mayor que cero");
            return;
        }

        if (!Repository.getPrincipalController().getOrderSelected().getOrdType().equals(RoutingMessage.OrdType.MARKET) && Repository.getPrincipalController().getOrderSelected().getPrice() <= 0
                && !Repository.getPrincipalController().getOrderSelected().getStrategyOrder().equals(RoutingMessage.StrategyOrder.OCO)) {
            Notifier.INSTANCE.notifyError("Error", "El precio debe ser mayor que cero para una orden no de mercado.");
            return;
        }

        if (!visibleid.getText().isEmpty()) {
            double maxFloor = Double.parseDouble(quantity2.getText().replace(",", "")) *
                    Double.parseDouble(visibleid.getText().replace("%", "").replace(",", ".")) / 100;
            long maxFloorInt = Math.round(maxFloor);
            replace.setMaxFloor((double) maxFloorInt + 1);
            replace.setIcebergPercentage(visibleid.getText());
        }

        if (Repository.getLanzadorController().alertRoute2(message)) {
            Repository.getClientService().sendMessage(replace.build());
        }
    }

    @FXML
    public void cancelOrder() {
        Repository.getPrincipalController().setOrderSelected(findOrder(Repository.getPrincipalController().getOrderSelected()));

        if (Repository.getPrincipalController().getOrderSelected().getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                || Repository.getPrincipalController().getOrderSelected().getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                || Repository.getPrincipalController().getOrderSelected().getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)
                || Repository.getPrincipalController().getOrderSelected().getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_REPLACE)
                || Repository.getPrincipalController().getOrderSelected().getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)) {

            RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder()
                    .setId(Repository.getPrincipalController().getOrderSelected().getId()).build();

            Repository.getClientService().sendMessage(orderCancelRequest);
        }
    }

    public ComboBox<String> getAccountFilter() {
        return accountFilterShim;
    }

    private void setupAccountFilterAutocomplete() {
        filteredAccountList = new FilteredList<>(allAccounts, a -> true);

        accountSuggestionsList = new ListView<>();
        accountSuggestionsPopup = new Popup();
        accountSuggestionsPopup.getContent().add(accountSuggestionsList);
        accountSuggestionsPopup.setAutoHide(true);

        accountSuggestionsList.setOnMouseClicked(e -> {
            String sel = accountSuggestionsList.getSelectionModel().getSelectedItem();
            if (sel != null) {
                isUpdatingFromAccount = true;
                accountFilter.setText(sel);
                accountFilter.positionCaret(sel.length());
                isUpdatingFromAccount = false;
                accountFilterShim.getSelectionModel().select(sel);
                accountSuggestionsPopup.hide();
            }
        });

        accountFilter.setOnKeyPressed(e -> {
            switch (e.getCode()) {
                case DOWN -> {
                    if (!accountSuggestionsList.getItems().isEmpty()) {
                        accountSuggestionsList.requestFocus();
                        accountSuggestionsList.getSelectionModel().selectFirst();
                    }
                }
                case ESCAPE -> accountSuggestionsPopup.hide();
            }
        });
        accountSuggestionsList.setOnKeyPressed(e -> {
            switch (e.getCode()) {
                case ENTER -> {
                    String sel = accountSuggestionsList.getSelectionModel().getSelectedItem();
                    if (sel != null) {
                        isUpdatingFromAccount = true;
                        accountFilter.setText(sel);
                        accountFilter.positionCaret(sel.length());
                        isUpdatingFromAccount = false;
                        accountFilterShim.getSelectionModel().select(sel);
                        accountSuggestionsPopup.hide();
                    }
                }
                case UP -> {
                    if (accountSuggestionsList.getSelectionModel().getSelectedIndex() == 0) {
                        accountFilter.requestFocus();
                    }
                }
                case ESCAPE -> {
                    accountSuggestionsPopup.hide();
                    accountFilter.requestFocus();
                }
            }
        });

        accountFilter.textProperty().addListener((obs, old, txt) -> {
            if (isUpdatingFromAccount) return;

            String u = txt == null ? "" : txt.toUpperCase(Locale.ROOT);
            if (!u.equals(txt)) {
                isUpdatingFromAccount = true;
                int caret = accountFilter.getCaretPosition();
                accountFilter.setText(u);
                accountFilter.positionCaret(Math.min(caret, u.length()));
                isUpdatingFromAccount = false;
            }

            updateAccountFilterSuggestions(u);

            if (u.isEmpty()) {
                if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();

                String ALL = Repository.getALL_ACCOUNT();
                isUpdatingFromAccount = true;
                if (accountFilterShim.getItems().contains(ALL)) {
                    accountFilterShim.getSelectionModel().select(ALL);
                } else {
                    accountFilterShim.getSelectionModel().clearSelection();
                }
                isUpdatingFromAccount = false;
            }
        });
    }

    private void updateAccountFilterSuggestions(String text) {
        if (text == null || text.isEmpty()) {
            accountSuggestionsPopup.hide();
            return;
        }
        filteredAccountList.setPredicate(acc ->
                acc != null && acc.toUpperCase(Locale.ROOT).contains(text.toUpperCase(Locale.ROOT))
        );
        var limited = filteredAccountList.stream().limit(10).collect(Collectors.toList());
        accountSuggestionsList.setItems(FXCollections.observableArrayList(limited));
        if (!limited.isEmpty()) {
            if (!accountSuggestionsPopup.isShowing() && accountFilter.getScene() != null) {
                var b = accountFilter.localToScreen(accountFilter.getBoundsInLocal());
                accountSuggestionsList.setPrefWidth(accountFilter.getWidth());
                accountSuggestionsList.setPrefHeight(Region.USE_COMPUTED_SIZE);
                accountSuggestionsPopup.show(accountFilter, b.getMinX(), b.getMaxY());
            }
        } else {
            accountSuggestionsPopup.hide();
        }
    }

    private void selectAllAccounts() {
        String ALL = Repository.getALL_ACCOUNT();
        if (accountFilterShim.getItems().contains(ALL)) {
            accountFilterShim.getSelectionModel().select(ALL);
        } else {
            accountFilterShim.getSelectionModel().clearSelection();
        }
    }

    public boolean getIsVertical() {
        return isVertical;
    }
}
