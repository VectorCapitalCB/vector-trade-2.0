package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;

import dev.mccue.guava.collect.HashBasedTable;
import dev.mccue.guava.collect.Table;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.layout.Region;
import javafx.stage.Popup;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

@Slf4j
public class BalanceController {


    @FXML private TextField accountFilter;
    @FXML private ComboBox<String> divisaComboBox;
    @FXML private TextField cuentaField;
    @FXML private TextField carteraField;
    @FXML private TextField cupoField;
    @FXML private TextField saldoDisponibleField;
    @FXML private TextField garantiasConstituidasField;
    @FXML private TextField garantiasExigidasField;
    @FXML private TextField garantiasReservadasField;
    @FXML private TextField limiteFinancieroField;
    @FXML private TextField garantiasDisponibleField;
    @FXML private TextField ordenesActivasComprasField;
    @FXML private TextField ordenesActivasVentasField;
    @FXML private TextField ordenesCalzadasComprasField;
    @FXML private TextField ordenesCalzadasVentasField;
    @FXML private TextField ordenesCestaComprasField;
    @FXML private TextField ordenesCestaVentasField;
    @FXML private TextField rendimientoField;
    @FXML private TextField totalField;

    @FXML private TreeTableView<BlotterMessage.ValuesPatrimonio> patrimonioTreeTableView;
    @FXML private TreeTableColumn<BlotterMessage.ValuesPatrimonio, String> descripcionColumn;
    @FXML private TreeTableColumn<BlotterMessage.ValuesPatrimonio, String> porcentajeColumn;
    @FXML private TreeTableColumn<BlotterMessage.ValuesPatrimonio, String> montoColumn;

    private final HashMap<String, BlotterMessage.Balance> hashBalance = new HashMap<>();
    private final DecimalFormat decimalFormat = new DecimalFormat("#,##0");
    private final Table<String, RoutingMessage.Currency, BlotterMessage.Patrimonio> patrimonioTable = HashBasedTable.create();
    private final ObservableList<String> allAccounts = FXCollections.observableArrayList();
    private FilteredList<String> filteredAccountList;
    private Popup accountSuggestionsPopup;
    private ListView<String> accountSuggestionsList;
    private boolean isUpdatingFromAccount = false;

    @FXML
    public void initialize() {

        configurarPatrimonioTreeTableView();
        Repository.getBalanceControllerList().add(this);


        divisaComboBox.setItems(FXCollections.observableArrayList(
                RoutingMessage.Currency.CLP.name(),
                RoutingMessage.Currency.USD.name(),
                RoutingMessage.Currency.EUR.name()
        ));
        divisaComboBox.getSelectionModel().select(RoutingMessage.Currency.CLP.name());
        divisaComboBox.setOnAction(e -> actualizarPatrimonioTableView());

        try {
            List<String> cuentasIni = Repository.getAllAccounts();
            if (cuentasIni != null) allAccounts.setAll(cuentasIni);
        } catch (Exception ignored) {}
        filteredAccountList = new FilteredList<>(allAccounts, p -> true);

        setupAccountSuggestionsPopup();

        accountFilter.textProperty().addListener((obs, old, txt) -> {
            if (!isUpdatingFromAccount) {
                String t = txt == null ? "" : txt;
                if (!t.equals(t.toUpperCase(Locale.ROOT))) {
                    isUpdatingFromAccount = true;
                    accountFilter.setText(t.toUpperCase(Locale.ROOT));
                    accountFilter.positionCaret(accountFilter.getText().length());
                    isUpdatingFromAccount = false;
                }

                updateAccountSuggestions(accountFilter.getText());
                actualizarPatrimonioTableView();
                BlotterMessage.Balance b = hashBalance.get(accountFilter.getText());
                if (b != null) updateBalanceCombobox(b);
            }
        });
    }

    private void setupAccountSuggestionsPopup() {
        accountSuggestionsList = new ListView<>();
        accountSuggestionsPopup = new Popup();
        accountSuggestionsPopup.getContent().add(accountSuggestionsList);
        accountSuggestionsPopup.setAutoHide(true);

        accountSuggestionsList.setOnMouseClicked(e -> {
            String sel = accountSuggestionsList.getSelectionModel().getSelectedItem();
            if (sel != null) applyAccountSuggestion(sel);
        });

        accountFilter.setOnKeyPressed(event -> {
            switch (event.getCode()) {
                case DOWN:
                    if (!accountSuggestionsList.getItems().isEmpty()) {
                        accountSuggestionsList.requestFocus();
                        accountSuggestionsList.getSelectionModel().selectFirst();
                    }
                    break;
                case ENTER:

                    String typed = accountFilter.getText();
                    if (typed != null && allAccounts.contains(typed)) {
                        applyAccountSuggestion(typed);
                    } else if (!accountSuggestionsList.getItems().isEmpty()) {
                        applyAccountSuggestion(accountSuggestionsList.getItems().get(0));
                    }
                    break;
                case ESCAPE:
                    accountSuggestionsPopup.hide();
                    break;
                default:
                    break;
            }
        });

        accountSuggestionsList.setOnKeyPressed(event -> {
            switch (event.getCode()) {
                case ENTER:
                    String sel = accountSuggestionsList.getSelectionModel().getSelectedItem();
                    if (sel != null) applyAccountSuggestion(sel);
                    break;
                case ESCAPE:
                    accountSuggestionsPopup.hide();
                    accountFilter.requestFocus();
                    break;
                default:
                    break;
            }
        });
    }

    private void applyAccountSuggestion(String sel) {
        isUpdatingFromAccount = true;
        accountFilter.setText(sel);
        accountFilter.positionCaret(sel.length());
        isUpdatingFromAccount = false;
        accountSuggestionsPopup.hide();
        actualizarPatrimonioTableView();
        BlotterMessage.Balance b = hashBalance.get(sel);
        if (b != null) updateBalanceCombobox(b);
    }

    private void updateAccountSuggestions(String text) {
        if (text == null || text.isEmpty()) {
            accountSuggestionsPopup.hide();
            return;
        }
        filteredAccountList.setPredicate(acc ->
                acc != null && acc.toUpperCase(Locale.ROOT).contains(text.toUpperCase(Locale.ROOT))
        );
        var limited = filteredAccountList.stream().limit(10).toList();
        accountSuggestionsList.setItems(FXCollections.observableArrayList(limited));
        if (!limited.isEmpty()) showAccountSuggestionsPopup();
        else accountSuggestionsPopup.hide();
    }

    private void showAccountSuggestionsPopup() {
        if (!accountSuggestionsPopup.isShowing()) {
            var b = accountFilter.localToScreen(accountFilter.getBoundsInLocal());
            double x = b.getMinX();
            double y = b.getMaxY();
            accountSuggestionsList.setPrefWidth(accountFilter.getWidth());
            accountSuggestionsList.setPrefHeight(Region.USE_COMPUTED_SIZE);
            accountSuggestionsPopup.show(accountFilter, x, y);
        }
    }

    public void actualizarCuentas(List<String> cuentas) {
        if (cuentas == null) return;
        Platform.runLater(() -> {
            allAccounts.setAll(cuentas);
            if ((accountFilter.getText() == null || accountFilter.getText().isEmpty()) && !allAccounts.isEmpty()) {
                applyAccountSuggestion(allAccounts.get(0));
            }
        });
    }

    public void actualizarCuentasDesdeUsuario(List<String> cuentas) {
        actualizarCuentas(cuentas);
    }

    public void updateBalance(BlotterMessage.Balance balance) {
        if (balance == null) return;
        hashBalance.put(balance.getCuenta(), balance);

        Platform.runLater(() -> {
            String acc = balance.getCuenta();
            if (acc != null && !acc.isEmpty() && !allAccounts.contains(acc)) {
                allAccounts.add(acc);
            }
            if (accountFilter.getText() == null || accountFilter.getText().isEmpty()) {
                applyAccountSuggestion(acc);
            } else if (acc.equals(accountFilter.getText())) {
                updateBalanceCombobox(balance);
            }
        });
    }
    public void updateTreeTableView(BlotterMessage.Patrimonio patrimonioData) {
        if (patrimonioData == null) return;

        if (!patrimonioTable.contains(patrimonioData.getCuenta(), patrimonioData.getCurrency())) {
            patrimonioTable.put(patrimonioData.getCuenta(), patrimonioData.getCurrency(), patrimonioData);
        }

        Platform.runLater(() -> {
            String acc = patrimonioData.getCuenta();
            if (acc != null && !acc.isEmpty() && !allAccounts.contains(acc)) {
                allAccounts.add(acc);
            }
            if (accountFilter.getText() == null || accountFilter.getText().isEmpty()) {
                applyAccountSuggestion(acc);
            } else {
                actualizarPatrimonioTableView();
            }
        });
    }

    private void actualizarPatrimonioTableView() {
        String selectedCuenta = accountFilter.getText();
        String selectedDivisaString = divisaComboBox.getSelectionModel().getSelectedItem();

        if (selectedCuenta == null || selectedCuenta.isEmpty() || selectedDivisaString == null) {
            removeData();
            return;
        }

        RoutingMessage.Currency selectedDivisa;
        try {
            selectedDivisa = RoutingMessage.Currency.valueOf(selectedDivisaString);
        } catch (IllegalArgumentException e) {
            log.error("Divisa inválida seleccionada: {}", selectedDivisaString, e);
            removeData();
            return;
        }

        BlotterMessage.Patrimonio patrimonioData = patrimonioTable.get(selectedCuenta, selectedDivisa);
        if (patrimonioData == null) {
            removeData();
            return;
        }

        Platform.runLater(() -> {
            TreeItem<BlotterMessage.ValuesPatrimonio> rootItem = new TreeItem<>(patrimonioData.getActivos());

            if (patrimonioData.getLiquidez() != null) {
                TreeItem<BlotterMessage.ValuesPatrimonio> liquidezItem = new TreeItem<>(patrimonioData.getLiquidez());
                if (patrimonioData.getCaja() != null) liquidezItem.getChildren().add(new TreeItem<>(patrimonioData.getCaja()));
                if (patrimonioData.getCuentaTransitoriasPorCobrarPagar() != null) liquidezItem.getChildren().add(new TreeItem<>(patrimonioData.getCuentaTransitoriasPorCobrarPagar()));
                if (patrimonioData.getGarantiaEfectivo() != null) liquidezItem.getChildren().add(new TreeItem<>(patrimonioData.getGarantiaEfectivo()));
                rootItem.getChildren().add(liquidezItem);
            }

            if (patrimonioData.getRentaFija() != null) {
                rootItem.getChildren().add(new TreeItem<>(patrimonioData.getRentaFija()));
            }

            if (patrimonioData.getRentaVariable() != null) {
                TreeItem<BlotterMessage.ValuesPatrimonio> rvItem = new TreeItem<>(patrimonioData.getRentaVariable());
                if (patrimonioData.getAccionesNacionales() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getAccionesNacionales()));
                if (patrimonioData.getSimultaneas() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getSimultaneas()));
                if (patrimonioData.getPrestamos() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getPrestamos()));
                if (patrimonioData.getAccionesextranjeras() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getAccionesextranjeras()));
                if (patrimonioData.getFondosMutos() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getFondosMutos()));
                if (patrimonioData.getFondoInversionRentaVariable() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getFondoInversionRentaVariable()));
                if (patrimonioData.getActivosInmobiliarios() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getActivosInmobiliarios()));
                if (patrimonioData.getEftsRentaVariable() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getEftsRentaVariable()));
                if (patrimonioData.getInversionesAlternativas() != null) rvItem.getChildren().add(new TreeItem<>(patrimonioData.getInversionesAlternativas()));
                rootItem.getChildren().add(rvItem);
            }

            if (patrimonioData.getDerivados() != null) {
                rootItem.getChildren().add(new TreeItem<>(patrimonioData.getDerivados()));
            }

            patrimonioTreeTableView.setRoot(rootItem);
            patrimonioTreeTableView.setShowRoot(true);
            patrimonioTreeTableView.refresh();
        });

        BlotterMessage.Balance balance = hashBalance.get(selectedCuenta);
        if (balance != null) updateBalanceCombobox(balance);
    }

    public void updateBalanceCombobox(BlotterMessage.Balance balance) {
        if (balance == null) return;
        Platform.runLater(() -> {
            cuentaField.setText(balance.getCuenta());
            carteraField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getCartera())));
            cupoField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getCupo())));
            saldoDisponibleField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getSaldoDisponible())));
            garantiasConstituidasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getGarantiasConstituidas())));
            garantiasExigidasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getGarantiasExigidas())));
            garantiasReservadasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getGarantiasReservadas())));
            limiteFinancieroField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getLimiteFinanciero())));
            garantiasDisponibleField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getGarantiasDisponible())));
            ordenesActivasComprasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getOrdenesActivasCompras())));
            ordenesActivasVentasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getOrdenesActivasVentas())));
            ordenesCalzadasComprasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getOrdenesCalzadasCompras())));
            ordenesCalzadasVentasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getOrdenesCalzadasVentas())));
            ordenesCestaComprasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getOrdenesCestaCompras())));
            ordenesCestaVentasField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getOrdenesCestaVentas())));
            rendimientoField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getRendimiento())));
            totalField.setText(String.format("%,.0f", BigDecimal.valueOf(balance.getTotal())));
        });
    }

    public void removeData() {
        Platform.runLater(() -> {
            patrimonioTreeTableView.setRoot(null);
            patrimonioTreeTableView.refresh();

            cuentaField.clear();
            carteraField.clear();
            cupoField.clear();
            saldoDisponibleField.clear();
            garantiasConstituidasField.clear();
            garantiasExigidasField.clear();
            garantiasReservadasField.clear();
            limiteFinancieroField.clear();
            garantiasDisponibleField.clear();
            ordenesActivasComprasField.clear();
            ordenesActivasVentasField.clear();
            ordenesCalzadasComprasField.clear();
            ordenesCalzadasVentasField.clear();
            ordenesCestaComprasField.clear();
            ordenesCestaVentasField.clear();
            rendimientoField.clear();
            totalField.clear();
        });
    }

    private String formatDecimal(double value) { return decimalFormat.format(value); }

    private void configurarPatrimonioTreeTableView() {
        descripcionColumn.setCellValueFactory(cd -> new SimpleObjectProperty<>(
                cd.getValue().getValue() != null ? cd.getValue().getValue().getDescription() : ""
        ));
        porcentajeColumn.setCellValueFactory(cd -> new SimpleObjectProperty<>(
                formatPercentage(cd.getValue().getValue() != null ? cd.getValue().getValue().getPorcentage() : null)
        ));
        montoColumn.setCellValueFactory(cd -> new SimpleObjectProperty<>(
                formatAmount(cd.getValue().getValue() != null ? cd.getValue().getValue().getValues() : null)
        ));
    }

    private String formatPercentage(Double value) {
        try { return value != null ? String.format("%,.2f%%", BigDecimal.valueOf(value)) : ""; }
        catch (NumberFormatException e) { return ""; }
    }

    private String formatAmount(Double value) {
        try { return value != null ? String.format("%,.0f", BigDecimal.valueOf(value)) : ""; }
        catch (NumberFormatException e) { return ""; }
    }

    public class DecimalTreeTableCell<T> extends TreeTableCell<T, Double> {
        @Override protected void updateItem(Double item, boolean empty) {
            super.updateItem(item, empty);
            setText(empty || item == null ? null : formatDecimal(item));
        }
        private String formatDecimal(Double number) { return String.format("%,.2f", number); }
    }
}
