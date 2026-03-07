package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.layout.Region;
import javafx.stage.Popup;
import javafx.util.converter.DoubleStringConverter;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Data
public class PositionHistoricalController {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#,###,###,##0.0");

    private static final int MAX_SUGGESTIONS = 10;

    @FXML
    private TextField accountFilter;
    @FXML
    private TextField symbolFilter;
    @FXML
    private TableView<BlotterMessage.PositionHistory> tableView;

    @FXML
    private TableColumn<BlotterMessage.PositionHistory, String> instrumentColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, String> accountColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> availableQuantityColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> phColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> pmColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> averagePurchasePriceColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> purchaseAmountColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> marketPriceColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> marketValueColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> priceVariationColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, String> qtygarantia;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, Double> qtyplazo;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, String> simultaneousColumn;
    @FXML
    private TableColumn<BlotterMessage.PositionHistory, String> guaranteeColumn;
    @FXML
    private TextField totalAvailableField;
    @FXML
    private TextField totalPhField;
    @FXML
    private TextField totalPmField;
    @FXML
    private TextField totalOperationsField;
    @FXML
    private TextField totalSimultaneousField;
    @FXML
    private TextField totalGuaranteesField;

    private ObservableList<BlotterMessage.PositionHistory> data;
    private FilteredList<BlotterMessage.PositionHistory> filteredPositionsList;
    private SortedList<BlotterMessage.PositionHistory> sortedData;


    private Popup accountSuggestionsPopup;
    private ListView<String> accountSuggestionsList;
    private FilteredList<String> filteredAccountList;
    private ObservableList<String> allAccounts;
    private boolean isUpdatingFromAccount = false;

    @FXML
    public void initialize() {


        allAccounts = FXCollections.observableArrayList(Repository.getAllAccounts());
        filteredAccountList = new FilteredList<>(allAccounts, p -> true);


        setupAccountSuggestionsPopup();


        accountFilter.textProperty().addListener((observable, oldValue, newValue) -> {
            if (!isUpdatingFromAccount) {
                if (!newValue.isEmpty()) {
                    updateAccountSuggestions(newValue);
                } else {
                    accountSuggestionsPopup.hide();
                }
            }
        });

        accountFilter.textProperty().addListener((observable, oldValue, newValue) -> {
            if (!newValue.equals("")) {
                isUpdatingFromAccount = true;
                accountFilter.setText(newValue.toUpperCase());
                isUpdatingFromAccount = false;
            }
        });


        data = FXCollections.observableArrayList(new ArrayList<>());
        data.addListener((ListChangeListener<BlotterMessage.PositionHistory>) c -> calculateTotals());

        this.filteredPositionsList = new FilteredList<>(this.data);
        this.sortedData = new SortedList<>(this.filteredPositionsList);
        this.sortedData.comparatorProperty().bind(this.tableView.comparatorProperty());
        tableView.setItems(sortedData);

        tableView.setEditable(false);


        setupTableColumns();


        Repository.getPositionHistoricalControllerList().add(this);


        tableView.setOnMouseClicked(event -> {
            try {
                BlotterMessage.PositionHistory value = tableView.getSelectionModel().getSelectedItem();
                if (value == null) return;

                var md = Repository.getMarketDataController();
                if (md == null) {
                    log.warn("MarketDataController == null (aún no inicializado). No puedo setLauncherFromHistorical.");
                    return;
                }

                md.setLauncherFromHistorical(value);

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });



        this.filteredPositionsList.predicateProperty().bind(Bindings.createObjectBinding(() -> position -> {
                    try {
                        boolean accountFilters = findByFilterAccount(position, accountFilter.getText());
                        boolean symbolFilters = findByFilterSymbol(position, symbolFilter.getText());
                        return accountFilters && symbolFilters;
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                    return false;
                },
                accountFilter.textProperty(),
                symbolFilter.textProperty()));
    }

    private void setupTableColumns() {
        accountColumn.setCellValueFactory(new PropertyValueFactory<>("account"));
        instrumentColumn.setCellValueFactory(new PropertyValueFactory<>("instrument"));
        availableQuantityColumn.setCellValueFactory(new PropertyValueFactory<>("availableQuantity"));
        phColumn.setCellValueFactory(new PropertyValueFactory<>("ph"));
        pmColumn.setCellValueFactory(new PropertyValueFactory<>("pm"));
        averagePurchasePriceColumn.setCellValueFactory(new PropertyValueFactory<>("averagePurchasePrice"));
        purchaseAmountColumn.setCellValueFactory(new PropertyValueFactory<>("purchaseAmount"));
        marketPriceColumn.setCellValueFactory(new PropertyValueFactory<>("marketPrice"));
        marketValueColumn.setCellValueFactory(new PropertyValueFactory<>("marketValue"));
        priceVariationColumn.setCellValueFactory(new PropertyValueFactory<>("priceVariation"));
        guaranteeColumn.setCellValueFactory(cd -> {
            Double v = cd.getValue().getGuarantee();
            String s = v == null ? "" : DECIMAL_FORMAT.format(v);
            return new SimpleStringProperty(s);
        });
        qtygarantia.setCellValueFactory(cd -> {
            Double v = cd.getValue().getGarantia();
            String s = v == null ? "" : DECIMAL_FORMAT.format(v);
            return new SimpleStringProperty(s);
        });
        qtyplazo.setCellValueFactory(new PropertyValueFactory<>("compraPlazo"));

        simultaneousColumn.setCellValueFactory(cellData -> {
            Boolean isSimultaneous = cellData.getValue().getSimultaneous();
            return new SimpleStringProperty(isSimultaneous != null && isSimultaneous ? "Sí" : "No");
        });


        setupColumn(accountColumn, "account", String.class);
        setupColumn(instrumentColumn, "instrument", String.class);
        setupColumn(availableQuantityColumn, "availableQuantity", Double.class);
        setupColumn(phColumn, "ph", Double.class);
        setupColumn(pmColumn, "pm", Double.class);
        setupColumn(averagePurchasePriceColumn, "averagePurchasePrice", Double.class);
        setupColumn(purchaseAmountColumn, "purchaseAmount", Double.class);
        setupColumn(marketPriceColumn, "marketPrice", Double.class);
        setupColumn(marketValueColumn, "marketValue", Double.class);
        setupColumn(priceVariationColumn, "priceVariation", Double.class);
        //setupColumn(guaranteeColumn, "guarantee", Double.class);
        //setupColumn(qtygarantia, "garantia", Double.class);
        setupColumn(qtyplazo, "compraPlazo", Double.class);
        setupColumn(simultaneousColumn, "simultaneous", String.class);


    }

    private <T> void setupColumn(TableColumn<BlotterMessage.PositionHistory, T> column, String property, Class<T> type) {
        column.setEditable(false);
        column.setCellFactory(col -> new TableCell<BlotterMessage.PositionHistory, T>() {
            @Override
            protected void updateItem(T item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                } else {
                    if (type == Double.class) {
                        setText(DECIMAL_FORMAT.format(item));
                    } else {
                        setText(item.toString());
                    }
                }
            }
        });
    }

    private void setupAccountSuggestionsPopup() {
        accountSuggestionsList = new ListView<>();
        accountSuggestionsPopup = new Popup();
        accountSuggestionsPopup.getContent().add(accountSuggestionsList);
        accountSuggestionsPopup.setAutoHide(true);

        // Manejar clics en las sugerencias
        accountSuggestionsList.setOnMouseClicked(event -> {
            String selectedItem = accountSuggestionsList.getSelectionModel().getSelectedItem();
            if (selectedItem != null) {
                isUpdatingFromAccount = true;
                accountFilter.setText(selectedItem);
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
                default:
                    break;
            }
        });


        accountSuggestionsList.setOnKeyPressed(event -> {
            switch (event.getCode()) {
                case ENTER:
                    String selectedItem = accountSuggestionsList.getSelectionModel().getSelectedItem();
                    if (selectedItem != null) {
                        isUpdatingFromAccount = true;
                        accountFilter.setText(selectedItem);
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
                default:
                    break;
            }
        });
    }

    private void updateAccountSuggestions(String text) {
        if (text == null || text.isEmpty()) {
            accountSuggestionsPopup.hide();
        } else {
            filteredAccountList.setPredicate(account -> account.toLowerCase().contains(text.toLowerCase()));

            List<String> limitedSuggestions = filteredAccountList.stream()
                    .limit(MAX_SUGGESTIONS)
                    .collect(Collectors.toList());
            accountSuggestionsList.setItems(FXCollections.observableArrayList(limitedSuggestions));

            if (!limitedSuggestions.isEmpty()) {
                showAccountSuggestionsPopup();
            } else {
                accountSuggestionsPopup.hide();
            }
        }
    }

    private void showAccountSuggestionsPopup() {
        if (!accountSuggestionsPopup.isShowing()) {
            // Obtener las coordenadas del TextField para posicionar el Popup
            double x = accountFilter.localToScreen(accountFilter.getBoundsInLocal()).getMinX();
            double y = accountFilter.localToScreen(accountFilter.getBoundsInLocal()).getMaxY();

            // Configurar el tamaño del ListView
            accountSuggestionsList.setPrefWidth(accountFilter.getWidth());
            accountSuggestionsList.setPrefHeight(Region.USE_COMPUTED_SIZE);

            accountSuggestionsPopup.show(accountFilter, x, y);
        }
    }

    public boolean findByFilterSymbol(BlotterMessage.PositionHistory position, String symbol) {
        return symbol.isEmpty() || position.getInstrument().toLowerCase().contains(symbol.toLowerCase());
    }

    public boolean findByFilterAccount(BlotterMessage.PositionHistory position, String accountInput) {
        if (accountInput == null || accountInput.isEmpty()) {
            return true;
        }
        return position.getAccount().toLowerCase().contains(accountInput.toLowerCase());
    }

    private void calculateTotals() {
        double totalAvailableQuantity = 0;
        double totalPh = 0;
        double totalPm = 0;
        double totalAveragePurchasePrice = 0;
        double totalPurchaseAmount = 0;
        double totalMarketPrice = 0;
        double totalMarketValue = 0;
        double totalPriceVariation = 0;
        double totalGuarantee = 0;
        double totalSimultaneousQty = 0;

        for (BlotterMessage.PositionHistory position : data) {
            totalAvailableQuantity += position.getAvailableQuantity();
            totalPh += position.getPh();
            totalPm += position.getPm();
            totalAveragePurchasePrice += position.getAveragePurchasePrice();
            totalPurchaseAmount += position.getPurchaseAmount();
            totalMarketPrice += position.getMarketPrice();
            totalMarketValue += position.getMarketValue();
            totalPriceVariation += position.getPriceVariation();
            totalGuarantee += position.getGuarantee();

        }

        totalAvailableField.setText(DECIMAL_FORMAT.format(totalAvailableQuantity));
        totalPhField.setText(DECIMAL_FORMAT.format(totalPh));
        totalPmField.setText(DECIMAL_FORMAT.format(totalPm));
        totalOperationsField.setText(DECIMAL_FORMAT.format(totalAveragePurchasePrice));
        totalSimultaneousField.setText(DECIMAL_FORMAT.format(totalSimultaneousQty));
        totalGuaranteesField.setText(DECIMAL_FORMAT.format(totalGuarantee));
    }

    public void addPositions(BlotterMessage.SnapshotPositionHistory snapshotPositions) {
        Platform.runLater(() -> {
            for (BlotterMessage.PositionHistory newPosition : snapshotPositions.getPositionsHistoryList()) {
                boolean exists = false;
                for (int i = 0; i < data.size(); i++) {
                    BlotterMessage.PositionHistory existingPosition = data.get(i);
                    if (existingPosition.getInstrument().equals(newPosition.getInstrument()) &&
                            existingPosition.getAccount().equals(newPosition.getAccount())) {
                        data.set(i, newPosition);
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    if (newPosition.getInstrument() != null && !newPosition.getInstrument().isEmpty()) {
                        data.add(newPosition);
                    } else {
                        log.warn("Position with missing instrument: {}", newPosition);
                    }
                }
            }
            tableView.refresh();
            calculateTotals();
        });
    }

    public void clear() {
        Platform.runLater(() -> {
            data.clear();
            tableView.refresh();
            calculateTotals();
        });
    }

    public void setUser(BlotterMessage.User user) {
        boolean isAdmin = user.getRoles().getAccessList().contains("admin");
        tableView.setEditable(isAdmin);

        if (isAdmin) {
            makeColumnsEditable();
        } else {
            makeColumnsNonEditable();
        }
    }

    private void makeColumnsEditable() {
        instrumentColumn.setEditable(true);
        instrumentColumn.setCellFactory(TextFieldTableCell.forTableColumn());
        instrumentColumn.setOnEditCommit(event -> {
            BlotterMessage.PositionHistory oldPosition = event.getRowValue();
            BlotterMessage.PositionHistory newPosition = oldPosition.toBuilder()
                    .setInstrument(event.getNewValue())
                    .build();
            int index = data.indexOf(oldPosition);
            data.set(index, newPosition);
            tableView.refresh();
            sendUpdatedPosition(newPosition);
        });

        // Available Quantity Column
        setupEditableColumn(availableQuantityColumn, "availableQuantity");

        // PH Column
        setupEditableColumn(phColumn, "ph");

        // PM Column
        setupEditableColumn(pmColumn, "pm");

        // Average Purchase Price Column
        setupEditableColumn(averagePurchasePriceColumn, "averagePurchasePrice");

        // Purchase Amount Column
        setupEditableColumn(purchaseAmountColumn, "purchaseAmount");

        // Market Price Column
        setupEditableColumn(marketPriceColumn, "marketPrice");

        // Market Value Column
        setupEditableColumn(marketValueColumn, "marketValue");

        // Price Variation Column
        setupEditableColumn(priceVariationColumn, "priceVariation");

        // Qty Plazo Column
        setupEditableColumn(qtyplazo, "compraPlazo");

        // Asegurarse de que simultaneousColumn no sea editable
        simultaneousColumn.setEditable(false);
        simultaneousColumn.setOnEditCommit(null);
    }

    private void setupEditableColumn(TableColumn<BlotterMessage.PositionHistory, Double> column, String propertyName) {
        column.setEditable(true);
        column.setCellFactory(TextFieldTableCell.forTableColumn(new DoubleStringConverter()));
        column.setOnEditCommit(event -> {
            BlotterMessage.PositionHistory oldPosition = event.getRowValue();
            double newValue = event.getNewValue() != null ? event.getNewValue() : 0.0;
            BlotterMessage.PositionHistory newPosition = null;

            switch (propertyName) {
                case "availableQuantity":
                    newPosition = oldPosition.toBuilder().setAvailableQuantity(newValue).build();
                    break;
                case "ph":
                    newPosition = oldPosition.toBuilder().setPh(newValue).build();
                    break;
                case "pm":
                    newPosition = oldPosition.toBuilder().setPm(newValue).build();
                    break;
                case "averagePurchasePrice":
                    newPosition = oldPosition.toBuilder().setAveragePurchasePrice(newValue).build();
                    break;
                case "purchaseAmount":
                    newPosition = oldPosition.toBuilder().setPurchaseAmount(newValue).build();
                    break;
                case "marketPrice":
                    newPosition = oldPosition.toBuilder().setMarketPrice(newValue).build();
                    break;
                case "marketValue":
                    newPosition = oldPosition.toBuilder().setMarketValue(newValue).build();
                    break;
                case "priceVariation":
                    newPosition = oldPosition.toBuilder().setPriceVariation(newValue).build();
                    break;
                case "compraPlazo":
                    newPosition = oldPosition.toBuilder().setCompraPlazo(newValue).build();
                    break;
                default:
                    break;
            }

            if (newPosition != null) {
                int index = data.indexOf(oldPosition);
                data.set(index, newPosition);
                tableView.refresh();
                sendUpdatedPosition(newPosition);
            }
        });
    }

    private void makeColumnsNonEditable() {
        // Instrument Column
        instrumentColumn.setEditable(false);
        instrumentColumn.setCellFactory(column -> new TableCell<BlotterMessage.PositionHistory, String>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : item);
            }
        });
        instrumentColumn.setOnEditCommit(null);

        // Available Quantity Column
        availableQuantityColumn.setEditable(false);
        availableQuantityColumn.setCellFactory(column -> new TableCell<BlotterMessage.PositionHistory, Double>() {
            @Override
            protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : DECIMAL_FORMAT.format(item));
            }
        });
        availableQuantityColumn.setOnEditCommit(null);

        // Repetir este patrón para las demás columnas
        setColumnNonEditable(phColumn);
        setColumnNonEditable(pmColumn);
        setColumnNonEditable(averagePurchasePriceColumn);
        setColumnNonEditable(purchaseAmountColumn);
        setColumnNonEditable(marketPriceColumn);
        setColumnNonEditable(marketValueColumn);
        setColumnNonEditable(priceVariationColumn);
        setColumnNonEditable(qtyplazo);

        // Asegurarse de que simultaneousColumn no sea editable
        simultaneousColumn.setEditable(false);
        simultaneousColumn.setOnEditCommit(null);
    }

    private void setColumnNonEditable(TableColumn<BlotterMessage.PositionHistory, Double> column) {
        column.setEditable(false);
        column.setCellFactory(col -> new TableCell<BlotterMessage.PositionHistory, Double>() {
            @Override
            protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : DECIMAL_FORMAT.format(item));
            }
        });
        column.setOnEditCommit(null);
    }

    private void sendUpdatedPosition(BlotterMessage.PositionHistory updatedPosition) {
        try {
            Repository.getClientService().sendMessage(updatedPosition);

        } catch (Exception e) {

        }
    }

    public void updateAllAccounts(List<String> newAccounts) {
        Platform.runLater(() -> {
            this.allAccounts.setAll(newAccounts);
        });
    }
}
