package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.ScrollEvent;
import javafx.scene.layout.Region;
import javafx.stage.Popup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class PositionsController implements Initializable {

    @FXML public TableView<BlotterMessage.Position> tablePositions;
    private final HashMap<String, BlotterMessage.SnapshotPositions> portafolioSnpashot = new HashMap<>();
    @FXML private TableColumn<BlotterMessage.Position, Double> buy;
    @FXML private TableColumn<BlotterMessage.Position, Double> sell;
    @FXML private TableColumn<BlotterMessage.Position, Double> net;
    @FXML private TableColumn<BlotterMessage.Position, String> symbol;
    @FXML private TableColumn<BlotterMessage.Position, RoutingMessage.SecurityExchangeRouting> market;
    @FXML private TableColumn<BlotterMessage.Position, String> account;
    @FXML private TableColumn<BlotterMessage.Position, Double> buySent;
    @FXML private TableColumn<BlotterMessage.Position, Double> buyWorking;
    @FXML private TableColumn<BlotterMessage.Position, Double> buyTrade;
    @FXML private TableColumn<BlotterMessage.Position, Double> buyPx;
    @FXML private TableColumn<BlotterMessage.Position, Double> buyCashBought;
    @FXML private TableColumn<BlotterMessage.Position, Double> sellSent;
    @FXML private TableColumn<BlotterMessage.Position, Double> sellWorking;
    @FXML private TableColumn<BlotterMessage.Position, Double> sellTrade;
    @FXML private TableColumn<BlotterMessage.Position, Double> sellPx;
    @FXML private TableColumn<BlotterMessage.Position, Double> sellCash;
    @FXML private TableColumn<BlotterMessage.Position, Double> netQty;
    @FXML private TableColumn<BlotterMessage.Position, Double> netPx;
    @FXML private TableColumn<BlotterMessage.Position, Double> netAmount;
    private ObservableList<BlotterMessage.Position> positionsObsList;
    private FilteredList<BlotterMessage.Position> filteredPositionsList;
    private SortedList<BlotterMessage.Position> sortedData;
    @FXML private TableView<BlotterMessage.PositionsTotals> tvTotals;
    @FXML private TableColumn<BlotterMessage.PositionsTotals, String> tcTotal;
    @FXML private TableColumn<BlotterMessage.PositionsTotals, String> tcTotalMarket;
    @FXML private TableColumn<BlotterMessage.PositionsTotals, Double> tcTotalBuy;
    @FXML private TableColumn<BlotterMessage.PositionsTotals, Double> tcTotalSell;
    @FXML private TableColumn<BlotterMessage.PositionsTotals, Double> tcTotalNet;
    private ObservableList<BlotterMessage.PositionsTotals> positionsTotalsObsList;
    private SortedList<BlotterMessage.PositionsTotals> totalsSortedData;
    private final DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.00000");
    @Getter @Setter private String accountSelected;
    @Getter @Setter private String strategyConfigIdSelected;
    @Getter @Setter private String textSymbol;
    private boolean isLoadingAccounts = false;
    private boolean suppressAccountPopup = false;
    @FXML private TextField accountFilter; // TextField visible (FXML)
    @FXML private ComboBox<RoutingMessage.SecurityExchangeRouting> securityExchangeFilter;
    @FXML private TextField symbolFilter;
    private final ComboBox<String> accountFilterShim = new ComboBox<>();
    private boolean ignoreFirstAccountSelect = true;
    private static final int MAX_SUGGESTIONS = 10;
    private final ObservableList<String> allAccounts = FXCollections.observableArrayList();
    private FilteredList<String> filteredAccountList;
    private Popup accountSuggestionsPopup;
    private ListView<String> accountSuggestionsList;
    private boolean isUpdatingFromAccount = false;
    private final javafx.event.EventHandler<ScrollEvent> scrollOnTableEvent = e -> tablePositions.refresh();
    private final javafx.event.EventHandler<MouseEvent>  clickOnTableEvent  = e -> tablePositions.refresh();

    public ComboBox<String> getAccountFilter() {
        return accountFilterShim;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {

        symbol.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getSymbol()));
        market.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getSecurityexchange()));
        account.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getAccount()));
        buySent.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getSentBuy()));
        buyWorking.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getWorkingBuy()));
        buyTrade.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getTradeBuy()));
        buyPx.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getPxBuy()));
        buyCashBought.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getCashBoughtBuy()));
        sellSent.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getSentSell()));
        sellWorking.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getWorkingSell()));
        sellTrade.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getTradeSell()));
        sellPx.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getPxSell()));
        sellCash.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getCashSell()));
        netQty.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getQtyNet()));
        netPx.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getPxNet()));
        netAmount.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getAmountNet()));

        setCellFactoryBuy(buySent);
        setCellFactoryBuy(buyWorking);
        setCellFactoryBuy(buyTrade);
        setCellFactoryBuyPx(buyPx);
        setCellFactoryBuyPx(buyCashBought);
        setCellFactorySell(sellSent);
        setCellFactorySell(sellWorking);
        setCellFactorySell(sellTrade);
        setCellFactorySellPx(sellPx);
        setCellFactorySellPx(sellCash);
        setCellFactoryNet(netQty);
        setCellFactoryNetPx(netPx);
        setCellFactoryNet(netAmount);

        positionsObsList      = FXCollections.observableArrayList(new ArrayList<>());
        filteredPositionsList = new FilteredList<>(positionsObsList);
        sortedData            = new SortedList<>(filteredPositionsList);
        sortedData.comparatorProperty().bind(tablePositions.comparatorProperty());

        tablePositions.setItems(sortedData);
        tablePositions.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        tablePositions.getSortOrder().add(symbol);
        tablePositions.setCache(false);

        positionsTotalsObsList = FXCollections.observableArrayList(new ArrayList<>());
        totalsSortedData       = new SortedList<>(positionsTotalsObsList);
        tvTotals.setItems(totalsSortedData);
        totalsSortedData.comparatorProperty().bind(tvTotals.comparatorProperty());
        tvTotals.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        tvTotals.setSelectionModel(null);

        tcTotal.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getTotal()));
        tcTotalBuy.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getBuy()));
        setCellFactoryBuyNet(tcTotalBuy);
        tcTotalSell.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getSell()));
        setCellFactorySellTotal(tcTotalSell);
        tcTotalNet.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().getNet()));
        setCellFactoryNetTotal(tcTotalNet);

        BlotterMessage.PositionsTotals.Builder total = BlotterMessage.PositionsTotals.newBuilder();
        total.setBuy(0d).setSell(0d).setNet(0d);
        positionsTotalsObsList.add(total.build());

        tvTotals.setStyle("-fx-table-cell-border-color: transparent;");

        if (Repository.getUser() != null) {
            allAccounts.setAll(Repository.getUser().getAccountList());
        }
        filteredAccountList = new FilteredList<>(allAccounts, a -> true);
        setupAccountSuggestionsPopup();

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
            updateAccountSuggestions(u);
        });

        accountFilter.focusedProperty().addListener((o, was, focused) -> {
            if (!focused) {
                suppressAccountPopup = true;
                if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();
            } else {
                suppressAccountPopup = false;

            }
        });


        accountFilter.sceneProperty().addListener((o, oldScene, newScene) -> {
            if (oldScene != null && oldScene.getWindow() != null) {
                oldScene.getWindow().showingProperty().removeListener((o2, ov, nv) -> {});
            }
            if (newScene != null) {
                newScene.windowProperty().addListener((o2, oldW, newW) -> {
                    if (newW != null) {
                        newW.showingProperty().addListener((o3, wasShowing, showingNow) -> {
                            if (!showingNow && accountSuggestionsPopup != null) {
                                accountSuggestionsPopup.hide();
                            }
                        });
                    }
                });
            }
        });

        accountFilterShim.itemsProperty().addListener((obs, old, newItems) -> {
            isLoadingAccounts = true;
            suppressAccountPopup = true;
            if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();

            if (newItems != null) {
                allAccounts.setAll(newItems);
            } else {
                allAccounts.clear();
            }

            accountFilterShim.setDisable(true);
            Platform.runLater(() -> {
                isLoadingAccounts = false;
                accountFilterShim.setDisable(false);
            });
        });


        accountFilterShim.getSelectionModel().selectedItemProperty().addListener((o, old, sel) -> {
            if (isLoadingAccounts) return;

            if (ignoreFirstAccountSelect) {
                ignoreFirstAccountSelect = false;
                Platform.runLater(() -> {
                    isUpdatingFromAccount = true;
                    accountFilter.clear();
                    accountFilterShim.getSelectionModel().clearSelection();
                    isUpdatingFromAccount = false;
                    if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();
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


        filteredPositionsList.predicateProperty().bind(
                Bindings.createObjectBinding(
                        () -> (BlotterMessage.Position p) -> {
                            try {
                                boolean seOk  = (securityExchangeFilter.getSelectionModel().getSelectedItem() == null)
                                        || findByFilterSE(p, securityExchangeFilter.getSelectionModel().getSelectedItem());
                                boolean accOk = findByFilterAccountText(p, accountFilter.getText());
                                boolean symOk = findByFilterSymbol(p, symbolFilter.getText());
                                return seOk && accOk && symOk;
                            } catch (Exception e) {
                                log.error("Predicate error", e);
                                return true;
                            }
                        },
                        securityExchangeFilter.valueProperty(),
                        accountFilter.textProperty(),
                        symbolFilter.textProperty()
                )
        );

        filteredPositionsList.addListener((ListChangeListener<BlotterMessage.Position>) c -> {
            while (c.next()) calculateTotal();
        });

        tablePositions.addEventFilter(ScrollEvent.ANY, scrollOnTableEvent);
        tablePositions.addEventFilter(MouseEvent.MOUSE_CLICKED, clickOnTableEvent);
    }


    private boolean findByFilterSE(BlotterMessage.Position p, RoutingMessage.SecurityExchangeRouting se) {
        return se == null
                || se == RoutingMessage.SecurityExchangeRouting.ALL_SECURITY_EXCHANGE
                || p.getSecurityexchange() == se;
    }

    private boolean findByFilterSymbol(BlotterMessage.Position p, String symbol) {
        if (symbol == null || symbol.isEmpty()) return true;
        String s = p.getSymbol() == null ? "" : p.getSymbol();
        return s.toLowerCase(Locale.ROOT).contains(symbol.toLowerCase(Locale.ROOT));
    }


    private boolean findByFilterAccountText(BlotterMessage.Position p, String accountInput) {
        if (accountInput == null || accountInput.isEmpty()) return true;
        String acc = p.getAccount() == null ? "" : p.getAccount();
        return acc.toLowerCase(Locale.ROOT).contains(accountInput.toLowerCase(Locale.ROOT));
    }

    public void enableAccountFilter(boolean enable) {
        accountFilter.setDisable(!enable);
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
                case ESCAPE -> {
                    accountSuggestionsPopup.hide();
                    accountFilter.requestFocus();
                }
            }
        });
    }

    private void updateAccountSuggestions(String text) {

        if (text == null || text.isEmpty()) {
            if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();
            return;
        }

        filteredAccountList.setPredicate(acc ->
                acc != null && acc.toUpperCase(Locale.ROOT).contains(text.toUpperCase(Locale.ROOT)));

        var limited = filteredAccountList.stream().limit(MAX_SUGGESTIONS).collect(Collectors.toList());
        accountSuggestionsList.setItems(FXCollections.observableArrayList(limited));

        boolean canShow =
                !limited.isEmpty()
                        && accountFilter.isFocused()
                        && !suppressAccountPopup
                        && accountFilter.getScene() != null
                        && accountFilter.getScene().getWindow() != null
                        && accountFilter.getScene().getWindow().isShowing();

        if (canShow) {
            showAccountSuggestionsPopup();
        } else {
            if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();
        }
    }

    private void showAccountSuggestionsPopup() {
        if (!accountSuggestionsPopup.isShowing() && accountFilter.getScene() != null) {
            var b = accountFilter.localToScreen(accountFilter.getBoundsInLocal());
            accountSuggestionsList.setPrefWidth(accountFilter.getWidth());
            accountSuggestionsList.setPrefHeight(Region.USE_COMPUTED_SIZE);
            accountSuggestionsPopup.show(accountFilter, b.getMinX(), b.getMaxY());
        }
    }

    public void calculateTotal() {
        BigDecimal totalBuy  = BigDecimal.valueOf(filteredPositionsList.stream().map(BlotterMessage.Position::getCashBoughtBuy).reduce(0.0, Double::sum));
        BigDecimal totalSell = BigDecimal.valueOf(filteredPositionsList.stream().map(BlotterMessage.Position::getCashSell).reduce(0.0, Double::sum));
        BigDecimal totalNet  = BigDecimal.valueOf(filteredPositionsList.stream().map(BlotterMessage.Position::getAmountNet).reduce(0.0, Double::sum));
        loadTotal(totalBuy, totalSell, totalNet);
    }

    private void loadTotal(BigDecimal totalBuy, BigDecimal totalSell, BigDecimal totalNet) {
        try {
            BlotterMessage.PositionsTotals.Builder total = BlotterMessage.PositionsTotals.newBuilder()
                    .setTotal("Totales")
                    .setBuy(totalBuy.doubleValue())
                    .setSell(totalSell.doubleValue())
                    .setNet(totalNet.doubleValue());

            Platform.runLater(() -> {
                if (positionsTotalsObsList.isEmpty()) positionsTotalsObsList.add(total.build());
                else positionsTotalsObsList.set(0, total.build());
                tvTotals.refresh();
            });
        } catch (Exception ex) {
            log.error("Error calculando totales", ex);
        }
    }

    private void setCellFactoryBuyNet(TableColumn<BlotterMessage.PositionsTotals, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); setStyle(null); getStyleClass().clear(); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                if (!getStyleClass().contains("buyOrder")) getStyleClass().add("buyOrder");
                setStyle("-fx-alignment: CENTER-RIGHT; -fx-padding: 5,20,20,20;");
                getTableRow().getStyleClass().add("genericOrder");
            }
        });
    }

    private void setCellFactoryBuy(TableColumn<BlotterMessage.Position, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); setStyle(null); getStyleClass().clear(); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                if (!getStyleClass().contains("buyOrder")) getStyleClass().add("buyOrder");
                setStyle("-fx-alignment: CENTER-RIGHT; -fx-padding: 5,20,20,20;");
                getTableRow().getStyleClass().add("genericOrder");
            }
        });
    }

    private void setCellFactoryBuyPx(TableColumn<BlotterMessage.Position, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); setStyle(null); getStyleClass().clear(); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                if (!getStyleClass().contains("buyOrder")) getStyleClass().add("buyOrder");
                setStyle("-fx-alignment: CENTER-RIGHT; -fx-padding: 5,20,20,20;");
            }
        });
    }

    private void setCellFactorySell(TableColumn<BlotterMessage.Position, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); setStyle(null); getStyleClass().clear(); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                if (!getStyleClass().contains("sellOrder")) getStyleClass().add("sellOrder");
                setStyle("-fx-alignment: CENTER-RIGHT; -fx-padding: 5,20,20,20;");
            }
        });
    }

    private void setCellFactorySellPx(TableColumn<BlotterMessage.Position, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); setStyle(null); getStyleClass().clear(); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                if (!getStyleClass().contains("sellOrder")) getStyleClass().add("sellOrder");
                setStyle("-fx-alignment: CENTER-RIGHT; -fx-padding: 5,20,20,20;");
            }
        });
    }

    private void setCellFactorySellTotal(TableColumn<BlotterMessage.PositionsTotals, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); setStyle(null); getStyleClass().clear(); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                if (!getStyleClass().contains("sellOrder")) getStyleClass().add("sellOrder");
                setStyle("-fx-alignment: CENTER-RIGHT; -fx-padding: 5,20,20,20;");
            }
        });
    }

    private void setCellFactoryNetTotal(TableColumn<BlotterMessage.PositionsTotals, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); setStyle(null); getStyleClass().clear(); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                setStyle("-fx-alignment: CENTER-RIGHT;");
            }
        });
    }

    private void setCellFactoryNet(TableColumn<BlotterMessage.Position, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                setStyle("-fx-alignment: CENTER-RIGHT;");
            }
        });
    }

    private void setCellFactoryNetPx(TableColumn<BlotterMessage.Position, Double> col) {
        col.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(Double item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(""); return; }
                setText(NumberGenerator.getFormatNumberMilDec().format(item));
                setStyle("-fx-alignment: CENTER-RIGHT;");
            }
        });
    }

    public void clear() {
        Platform.runLater(() -> positionsObsList.clear());
    }

    public void addSnpashot(BlotterMessage.SnapshotPositions snapshot) {
        portafolioSnpashot.put(snapshot.getId(), snapshot);

        Platform.runLater(() -> {

            suppressAccountPopup = true;
            if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();

            positionsObsList.clear();
            portafolioSnpashot.values().forEach(s -> positionsObsList.addAll(s.getPositionsList()));
            tablePositions.refresh();

        });
    }

    public void updateAllAccounts(List<String> newAccounts) {
        Platform.runLater(() -> {
            isLoadingAccounts = true;
            suppressAccountPopup = true;
            if (accountSuggestionsPopup != null) accountSuggestionsPopup.hide();

            allAccounts.setAll(newAccounts != null ? newAccounts : Collections.emptyList());

            isLoadingAccounts = false;
        });
    }

    public void blockDestinos() {
        securityExchangeFilter.getItems().clear();
        securityExchangeFilter.getItems().add(RoutingMessage.SecurityExchangeRouting.ALL_SECURITY_EXCHANGE);
        securityExchangeFilter.getItems().add(RoutingMessage.SecurityExchangeRouting.XSGO);
        securityExchangeFilter.getSelectionModel().select(RoutingMessage.SecurityExchangeRouting.XSGO);
    }

    public ComboBox<RoutingMessage.SecurityExchangeRouting> getSecurityExchangeFilter() {
        return securityExchangeFilter;
    }
}
