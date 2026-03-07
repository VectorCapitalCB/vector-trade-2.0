package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.scene.control.ListView;
import javafx.scene.layout.Region;
import javafx.stage.Popup;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class PrestamosController {

    @FXML private TableView<BlotterMessage.Prestamos> tableView;

    @FXML private TableColumn<BlotterMessage.Prestamos, String> nemotecnico;
    @FXML private TableColumn<BlotterMessage.Prestamos, Number> cantidad_vigente;
    @FXML private TableColumn<BlotterMessage.Prestamos, Number> precio_ayer;
    @FXML private TableColumn<BlotterMessage.Prestamos, Number> monto;
    @FXML private TableColumn<BlotterMessage.Prestamos, String> fecha_ingreso;
    @FXML private TableColumn<BlotterMessage.Prestamos, String> plazo_pimo;
    @FXML private TableColumn<BlotterMessage.Prestamos, String> fecha_vencimiento;

    // AHORA ES TEXTFIELD (como en PositionHistoricalController)
    @FXML private TextField accountFilter;

    private final DecimalFormat money0 = new DecimalFormat("#,##0");
    private final DecimalFormat money4 = new DecimalFormat("#,##0.0000");

    // cuenta -> lista de préstamos (snapshot)
    private final Map<String, List<BlotterMessage.Prestamos>> dataPerAccount = new HashMap<>();
    // items visibles en la tabla
    private final ObservableList<BlotterMessage.Prestamos> backing = FXCollections.observableArrayList();

    // === Sugerencias como en PositionHistoricalController ===
    private Popup accountSuggestionsPopup;
    private ListView<String> accountSuggestionsList;
    private ObservableList<String> allAccounts = FXCollections.observableArrayList();
    private FilteredList<String> filteredAccountList;
    private boolean isUpdatingFromAccount = false;
    private static final int MAX_SUGGESTIONS = 10;

    @FXML
    private void initialize() {
        // registrar controller para recibir snapshots
        Repository.getPrestamosControllerList().add(this);

        // columnas
        nemotecnico.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().getNemotecnico()));
        cantidad_vigente.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().getCantidadVigente()));
        precio_ayer.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().getPrecioAyer()));
        monto.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().getMonto()));
        fecha_ingreso.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().getFechaIngreso()));
        plazo_pimo.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().getPlazoPimo()));
        fecha_vencimiento.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().getFechaVto()));

        // formateo opcional
        cantidad_vigente.setCellFactory(col -> new javafx.scene.control.TableCell<>() {
            @Override protected void updateItem(Number v, boolean empty) {
                super.updateItem(v, empty);
                setText(empty || v == null ? null : money0.format(v.doubleValue()));
            }
        });
        precio_ayer.setCellFactory(col -> new javafx.scene.control.TableCell<>() {
            @Override protected void updateItem(Number v, boolean empty) {
                super.updateItem(v, empty);
                setText(empty || v == null ? null : money4.format(v.doubleValue()));
            }
        });
        monto.setCellFactory(col -> new javafx.scene.control.TableCell<>() {
            @Override protected void updateItem(Number v, boolean empty) {
                super.updateItem(v, empty);
                setText(empty || v == null ? null : money0.format(v.doubleValue()));
            }
        });

        tableView.setItems(backing);

        // === cuentas iniciales (si ya hay user cargado) ===
        if (Repository.getUser() != null) {
            allAccounts.setAll(Repository.getUser().getAccountList());
        }
        filteredAccountList = new FilteredList<>(allAccounts, p -> true);

        // popup de sugerencias
        setupAccountSuggestionsPopup();

        // listeners como en PositionHistoricalController
        accountFilter.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!isUpdatingFromAccount) {
                if (newVal != null && !newVal.isEmpty()) {
                    updateAccountSuggestions(newVal);
                } else {
                    accountSuggestionsPopup.hide();
                }
                // refrescar tabla según texto
                refreshFromFilter();
            }
        });

        // uppercase
        accountFilter.textProperty().addListener((obs, o, n) -> {
            if (n != null && !n.isEmpty()) {
                isUpdatingFromAccount = true;
                accountFilter.setText(n.toUpperCase());
                accountFilter.positionCaret(n.length());
                isUpdatingFromAccount = false;
            }
        });

        // primera carga (sin filtro => todas)
        refreshFromFilter();
    }

    // === llamado por el actor cuando llega el User ===
    public void updateAccounts(BlotterMessage.User user) {
        if (user == null) return;
        allAccounts.setAll(user.getAccountList());
        // también puedes refrescar sugerencias si el usuario está escribiendo
        if (accountFilter.getText() != null && !accountFilter.getText().isEmpty()) {
            updateAccountSuggestions(accountFilter.getText());
        }
    }

    // === llamado por el actor cuando llega SnapshotPrestamos ===
    public void updateTableView(BlotterMessage.SnapshotPrestamos snapshot) {
        if (snapshot == null) return;

        // guardamos por cuenta
        dataPerAccount.put(snapshot.getAccount(), snapshot.getPrestamosList());

        // asegurar que la cuenta esté disponible para sugerencias
        if (snapshot.getAccount() != null && !snapshot.getAccount().isEmpty()
                && !allAccounts.contains(snapshot.getAccount())) {
            allAccounts.add(snapshot.getAccount());
        }

        // refrescamos vista según filtro actual
        refreshFromFilter();

        // si el usuario está escribiendo, actualiza sugerencias
        if (accountFilter.getText() != null && !accountFilter.getText().isEmpty()) {
            updateAccountSuggestions(accountFilter.getText());
        }
    }

    // Une listas por cuentas que “contienen” el texto del filtro; si vacío => todas
    private void refreshFromFilter() {
        final String text = accountFilter.getText();
        List<BlotterMessage.Prestamos> result;

        if (text == null || text.isEmpty()) {
            result = dataPerAccount.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        } else {
            String t = text.toLowerCase(Locale.ROOT);
            result = dataPerAccount.entrySet().stream()
                    .filter(e -> e.getKey() != null && e.getKey().toLowerCase(Locale.ROOT).contains(t))
                    .flatMap(e -> e.getValue().stream())
                    .collect(Collectors.toList());
        }

        backing.setAll(result);
        tableView.refresh();
    }

    // ==== Popup de sugerencias (idéntico al patrón del otro controller) ====
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
                refreshFromFilter();
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
                default:
                    break;
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
                        refreshFromFilter();
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
                default:
                    break;
            }
        });
    }

    private void updateAccountSuggestions(String text) {
        if (text == null || text.isEmpty()) {
            accountSuggestionsPopup.hide();
            return;
        }
        filteredAccountList.setPredicate(acc -> acc != null && acc.toLowerCase(Locale.ROOT).contains(text.toLowerCase(Locale.ROOT)));
        var limited = filteredAccountList.stream().limit(MAX_SUGGESTIONS).collect(Collectors.toList());
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

    // Limpieza opcional cuando cierres la vista
    public void dispose() {
        Repository.getPrestamosControllerList().remove(this);
    }
}
