package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.Region;
import javafx.stage.Popup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Data
public class PositionSimultaneasController {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#,###,###,##0.0");
    private static final int MAX_SUGGESTIONS = 10;

    private final HashMap<String, BlotterMessage.SnapshotSimultaneas> snapshotSimultaneasHashMap = new HashMap<>();

    // En FXML debe ser TextField
    @FXML private TextField accountFilter;

    @FXML private TableView<BlotterMessage.Simultaneas> tableView;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> folio_Fact_TP;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> folio_Fact_PH;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> corredor_Compra;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> corredor_Venta;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> cantidad_Orig;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> cod_Inst;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> monto_Plazo;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> monto_Presente;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> costo_Diario2;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> monto_Contado;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> precio_Mercado;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> precio_Plazo;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> precio_PH;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> tasa;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> cantidad;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> nemotecnico;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> plazo_Rem;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> plazo;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> nombre_Cliente;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> fecha_Vcto;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> fecha_Operacion;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> num_Cuenta;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> tipo_Simul;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> detalle_Simultanea;
    @FXML private TableColumn<BlotterMessage.Simultaneas, String> ident_Cliente;

    private ObservableList<BlotterMessage.Simultaneas> data;
    private FilteredList<BlotterMessage.Simultaneas> filteredPositionsList;
    private SortedList<BlotterMessage.Simultaneas> sortedData;

    // Sugerencias
    private Popup accountSuggestionsPopup;
    private ListView<String> accountSuggestionsList;
    private final ObservableList<String> allAccounts = FXCollections.observableArrayList();
    private FilteredList<String> filteredAccountList;
    private boolean isUpdatingFromAccount = false;

    @FXML
    public void initialize() {
        try {
            Repository.setPositionSimultaneasController(this);

            data = FXCollections.observableArrayList();
            filteredPositionsList = new FilteredList<>(data);
            sortedData = new SortedList<>(filteredPositionsList);
            sortedData.comparatorProperty().bind(tableView.comparatorProperty());
            tableView.setItems(sortedData);


            cantidad_Orig.setCellValueFactory(new PropertyValueFactory<>("cantidadOrig"));
            corredor_Venta.setCellValueFactory(new PropertyValueFactory<>("corredorVenta"));
            corredor_Compra.setCellValueFactory(new PropertyValueFactory<>("corredorCompra"));
            folio_Fact_PH.setCellValueFactory(new PropertyValueFactory<>("folioFactPH"));
            folio_Fact_TP.setCellValueFactory(new PropertyValueFactory<>("folioFactTP"));
            cod_Inst.setCellValueFactory(new PropertyValueFactory<>("codInst"));
            monto_Presente.setCellValueFactory(new PropertyValueFactory<>("montoPresente"));
            costo_Diario2.setCellValueFactory(new PropertyValueFactory<>("costoDiario2"));
            monto_Contado.setCellValueFactory(new PropertyValueFactory<>("montoContado"));
            precio_Mercado.setCellValueFactory(new PropertyValueFactory<>("precioMercado"));
            precio_Plazo.setCellValueFactory(new PropertyValueFactory<>("precioPlazo"));
            precio_PH.setCellValueFactory(new PropertyValueFactory<>("precioPH"));
            tasa.setCellValueFactory(new PropertyValueFactory<>("tasa"));
            cantidad.setCellValueFactory(new PropertyValueFactory<>("cantidad"));
            nemotecnico.setCellValueFactory(new PropertyValueFactory<>("nemotecnico"));
            plazo_Rem.setCellValueFactory(new PropertyValueFactory<>("plazoRem"));
            plazo.setCellValueFactory(new PropertyValueFactory<>("plazo"));
            nombre_Cliente.setCellValueFactory(new PropertyValueFactory<>("nombreCliente"));
            fecha_Vcto.setCellValueFactory(new PropertyValueFactory<>("fechaVcto"));
            fecha_Operacion.setCellValueFactory(new PropertyValueFactory<>("fechaOperacion"));
            num_Cuenta.setCellValueFactory(new PropertyValueFactory<>("numCuenta"));
            tipo_Simul.setCellValueFactory(new PropertyValueFactory<>("tipoSimul"));
            monto_Plazo.setCellValueFactory(new PropertyValueFactory<>("montoPlazo"));
            detalle_Simultanea.setCellValueFactory(new PropertyValueFactory<>("detalleSimultanea"));
            ident_Cliente.setCellValueFactory(new PropertyValueFactory<>("identCliente"));

            setCellFactoryNet(monto_Plazo);
            setCellFactoryNet(monto_Contado);
            setCellFactoryNet(cantidad);
            setCellFactoryNet(costo_Diario2);
            setCellFactoryNet(monto_Presente);

            if (Repository.getUser() != null) {
                allAccounts.setAll(Repository.getUser().getAccountList());
            }
            filteredAccountList = new FilteredList<>(allAccounts, p -> true);


            setupAccountSuggestionsPopup();

            accountFilter.textProperty().addListener((obs, oldVal, newVal) -> {
                if (!isUpdatingFromAccount) {
                    if (newVal != null && !newVal.isEmpty()) updateAccountSuggestions(newVal);
                    else if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();
                }
            });


            accountFilter.textProperty().addListener((obs, o, n) -> {
                if (n != null && !n.isEmpty()) {
                    isUpdatingFromAccount = true;
                    accountFilter.setText(n.toUpperCase());
                    accountFilter.positionCaret(n.length());
                    isUpdatingFromAccount = false;
                }
            });


            filteredPositionsList.predicateProperty().bind(
                    Bindings.createObjectBinding(() -> this::predicateByAccount, accountFilter.textProperty())
            );

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private boolean predicateByAccount(BlotterMessage.Simultaneas row) {
        String txt = accountFilter != null ? accountFilter.getText() : null;
        if (txt == null || txt.isEmpty()) return true;
        String acc = row.getNumCuenta() == null ? "" : row.getNumCuenta();
        return acc.toLowerCase(Locale.ROOT).contains(txt.toLowerCase(Locale.ROOT));
    }

    private void setupAccountSuggestionsPopup() {
        accountSuggestionsList = new ListView<>();
        accountSuggestionsPopup = new Popup();
        accountSuggestionsPopup.getContent().add(accountSuggestionsList);
        accountSuggestionsPopup.setAutoHide(true);

        accountSuggestionsList.setOnMouseClicked(e -> {
            String sel = accountSuggestionsList.getSelectionModel().getSelectedItem();
            if (sel != null) {
                isUpdatingFromAccount = true;
                accountFilter.setText(sel);
                isUpdatingFromAccount = false;
                accountSuggestionsPopup.hide();
            }
        });

        accountFilter.setOnKeyPressed(event -> {
            switch (event.getCode()) {
                case DOWN:
                    if (!accountSuggestionsList.getItems().isEmpty()) {
                        accountSuggestionsList.requestFocus();
                        accountSuggestionsList.getSelectionModel().selectFirst();
                    }
                    break;
                case ESCAPE:
                    accountSuggestionsPopup.hide();
                    break;
                default: break;
            }
        });

        accountSuggestionsList.setOnKeyPressed(event -> {
            switch (event.getCode()) {
                case ENTER:
                    String sel = accountSuggestionsList.getSelectionModel().getSelectedItem();
                    if (sel != null) {
                        isUpdatingFromAccount = true;
                        accountFilter.setText(sel);
                        isUpdatingFromAccount = false;
                        accountSuggestionsPopup.hide();
                    }
                    break;
                case UP:
                    if (accountSuggestionsList.getSelectionModel().getSelectedIndex() == 0) {
                        accountFilter.requestFocus();
                    }
                    break;
                case ESCAPE:
                    accountSuggestionsPopup.hide();
                    accountFilter.requestFocus();
                    break;
                default: break;
            }
        });
    }

    private void updateAccountSuggestions(String text) {
        if (text == null || text.isEmpty()) {
            accountSuggestionsPopup.hide();
            return;
        }
        filteredAccountList.setPredicate(acc ->
                acc != null && acc.toLowerCase(Locale.ROOT).contains(text.toLowerCase(Locale.ROOT))
        );
        var limited = filteredAccountList.stream().limit(MAX_SUGGESTIONS).collect(Collectors.toList());
        accountSuggestionsList.setItems(FXCollections.observableArrayList(limited));
        if (!limited.isEmpty()) showAccountSuggestionsPopup();
        else accountSuggestionsPopup.hide();
    }

    private void showAccountSuggestionsPopup() {
        if (!accountSuggestionsPopup.isShowing()) {
            var b = accountFilter.localToScreen(accountFilter.getBoundsInLocal());
            if (b == null) return;
            double x = b.getMinX(), y = b.getMaxY();
            accountSuggestionsList.setPrefWidth(accountFilter.getWidth());
            accountSuggestionsList.setPrefHeight(Region.USE_COMPUTED_SIZE);
            accountSuggestionsPopup.show(accountFilter, x, y);
        }
    }

    public void clear() {
        Platform.runLater(() -> {
            data.clear();
            tableView.refresh();
        });
    }


    public void addSnapshot(BlotterMessage.SnapshotSimultaneas snapshotSimultaneas) {
        // Actualiza cache (map normal, no FX) en cualquier hilo
        snapshotSimultaneasHashMap.put(snapshotSimultaneas.getAccount(), snapshotSimultaneas);
        String acc = snapshotSimultaneas.getAccount();

        Platform.runLater(() -> {

            if (acc != null && !acc.isEmpty() && !allAccounts.contains(acc)) {
                allAccounts.add(acc);
            }


            var merged = snapshotSimultaneasHashMap.values().stream()
                    .flatMap(s -> s.getSimultaneasList().stream())
                    .collect(Collectors.toList());

            data.setAll(merged);
        });
    }

    public void updateAllAccounts(List<String> newAccounts) {
        Platform.runLater(() -> allAccounts.setAll(newAccounts));
    }

    private void setCellFactoryNet(TableColumn<BlotterMessage.Simultaneas, String> tableColumn) {
        tableColumn.setCellFactory(column -> new TableCell<BlotterMessage.Simultaneas, String>() {
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null || item.isBlank()) {
                    setText("");
                } else {
                    try {
                        setText(NumberGenerator.getFormatNumberMil().format(Double.parseDouble(item)));
                    } catch (Exception ex) {
                        setText(item);
                    }
                    setStyle("-fx-alignment: CENTER-RIGHT;");
                }
            }
        });
    }
}
