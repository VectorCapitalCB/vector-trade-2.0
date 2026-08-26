package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.model.OrderBookEntry;
import cl.vc.blotter.utils.Sparkline;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.animation.KeyFrame;
import javafx.animation.PauseTransition;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.canvas.Canvas;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.HBox;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Popup;
import javafx.stage.Stage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.ResourceBundle;
import javafx.util.Duration;

@Slf4j
@Data
public class LibroEmergenteController implements Initializable {

    static final double COMPACT_TICKET_WIDTH = 64;
    static final double BCS_TICKET_WIDTH = 120;
    static final double BOOK_ROW_HEIGHT = 22;
    static final double BOOK_TABLE_HEADER_HEIGHT = 26;
    private static final double BOOK_BASE_HEIGHT = 50;
    private static final double STATISTICS_HEIGHT = 30;
    private static final double TREND_HEIGHT = 18;
    private static final double SUPPLEMENTARY_GAP = 6;
    private static final String OWN_LIVE_ORDER_STYLE =
            "-fx-text-fill: #ffffff; -fx-background-color: #8a6a12;"
                    + " -fx-border-color: transparent transparent transparent #ffd45c;"
                    + " -fx-border-width: 0 0 0 3; -fx-font-weight: bold;";

    @FXML
    public ChoiceBox<MarketDataMessage.SecurityExchangeMarketData> cbMarket;

    @FXML
    public ChoiceBox<RoutingMessage.SettlType> settlType;

    @FXML
    public ChoiceBox<RoutingMessage.SecurityType> securityType;

    public Boolean isStart = false;

    public String idSubscribeBook = "";

    public String idController = IDGenerator.getID();

    public int positions;

    private MarketDataMessage.Subscribe subscribe;

    @FXML
    private TextField ticket;
    @FXML
    private TableColumn<OrderBookEntry, String> quantityBid;
    @FXML
    private TableColumn<OrderBookEntry, String> priceBid;
    @FXML
    private TableColumn<OrderBookEntry, String> priceOffer;
    @FXML
    private TableColumn<OrderBookEntry, String> quantityOffer;
    @FXML
    private TableView<OrderBookEntry> bidViewTable;
    @FXML
    private TableView<OrderBookEntry> offerViewTable;
    @FXML
    private Label closepriceGen;
    @FXML
    private Label imbalanceGen;
    @FXML
    private Label volumeGen;
    @FXML
    private Label lowpriceGen;
    @FXML
    private Label previusClose;
    @FXML
    private Label medioGen;
    @FXML
    private Label highpriceGen;
    @FXML
    private Canvas tendencia;
    @FXML
    private HBox statisticsBar;
    @FXML
    private Button multibookSettingsButton;
    @FXML
    private AnchorPane bookRoot;
    @FXML
    private VBox supplementaryBox;

    private Runnable multibookSettingsAction;
    private int visibleDepth = 5;
    private boolean statisticsVisible = true;
    private boolean trendVisible = true;

    private ObservableList<String> allSymbols;

    private FilteredList<String> filteredList;

    private Popup suggestionsPopup;

    private TextField activeTextField;

    private Stage stage;
    private PauseTransition subscriptionHealthCheck;
    private int subscriptionRetryCount = 0;
    private boolean updatingSecurityTypeProgrammatically;
    private boolean securityTypeManuallySelected;
    private static final int MAX_SUBSCRIPTION_RETRIES = 2;


    @FXML
    public void initialize() {

    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

        try {

            setupSuggestionsPopup();

            securityType.setItems(FXCollections.observableArrayList(RoutingMessage.SecurityType.values()));
            securityType.getSelectionModel().selectFirst();
            securityType.getSelectionModel().selectedItemProperty().addListener((obs, oldValue, newValue) -> {
                if (!updatingSecurityTypeProgrammatically && newValue != null) {
                    securityTypeManuallySelected = true;
                }
            });

            settlType.setItems(FXCollections.observableArrayList(RoutingMessage.SettlType.values()));
            settlType.getItems().removeAll(RoutingMessage.SettlType.UNRECOGNIZED, RoutingMessage.SettlType.REGULAR);
            settlType.getSelectionModel().select(RoutingMessage.SettlType.T2);


            ObservableList<MarketDataMessage.SecurityExchangeMarketData> x = FXCollections.observableArrayList();
            x.addAll(Repository.getUser().getRoles().getDestinoMKDList());
            x.remove(MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL);

            cbMarket.setItems(x);
            cbMarket.getSelectionModel().select(MarketDataMessage.SecurityExchangeMarketData.BCS);
            refreshSettlementVisibility(cbMarket.getSelectionModel().getSelectedItem());

            cbMarket.getSelectionModel().selectedItemProperty().addListener((obs, oldValue, newValue) -> {
                refreshSettlementVisibility(newValue);
                String currentSymbol = ticket.getText();
                if (currentSymbol != null && !currentSymbol.isBlank()) {
                    securityTypeManuallySelected = false;
                    updateSecurityTypeComboBox(currentSymbol.trim().toUpperCase(Locale.ROOT), newValue);
                }
            });

            ticket.textProperty().addListener((obs, oldValue, newValue) -> {
                if (newValue == null || newValue.isBlank()) {
                    return;
                }
                securityTypeManuallySelected = false;
                updateSecurityTypeComboBox(newValue.trim().toUpperCase(Locale.ROOT),
                        cbMarket.getSelectionModel().getSelectedItem());
            });


            addTableClickListener(bidViewTable);
            addTableClickListener(offerViewTable);


            quantityOffer.setCellValueFactory(new PropertyValueFactory<>("size"));

            quantityOffer.setCellFactory(column -> {

                TableCell<OrderBookEntry, String> cell = new TableCell<>() {
                    @Override
                    protected void updateItem(String item, boolean empty) {
                        super.updateItem(item, empty);
                        if (empty || item == null) {
                            setText(null);
                            setStyle("");
                        } else {

                            OrderBookEntry data = getTableRow().getItem();

                            if (data == null) {
                                return;
                            }

                            setText(item);

                            if (isOwnLiveOrder(data)) {
                                setStyle(OWN_LIVE_ORDER_STYLE);
                            } else if (!data.getAccount().isEmpty() && Repository.getUser().getAccountList().contains(data.getAccount())) {
                                setStyle("-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");

                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                setStyle("-fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");

                            } else {
                                setStyle("-fx-text-fill: red;");
                            }
                        }
                    }
                };

                cell.setMouseTransparent(true);

                return cell;
            });


            quantityBid.setCellValueFactory(new PropertyValueFactory<>("size"));

            quantityBid.setCellFactory(column -> {

                TableCell<OrderBookEntry, String> cell = new TableCell<>() {
                    @Override
                    protected void updateItem(String item, boolean empty) {
                        super.updateItem(item, empty);
                        if (empty || item == null) {
                            setText(null);
                            setStyle("");
                        } else {

                            OrderBookEntry data = getTableRow().getItem();

                            if (data == null) {
                                return;
                            }

                            setText(item);

                            if (isOwnLiveOrder(data)) {
                                setStyle(OWN_LIVE_ORDER_STYLE);
                            } else if (!data.getAccount().isEmpty() && Repository.getUser().getAccountList().contains(data.getAccount())) {
                                setStyle("-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");

                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                setStyle("-fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");

                            } else {
                                setStyle("-fx-text-fill: green;");
                            }



                        }
                    }
                };

                cell.setMouseTransparent(true);

                return cell;
            });

            priceBid.setCellValueFactory(new PropertyValueFactory<>("price"));

            priceBid.setCellFactory(column -> {

                TableCell<OrderBookEntry, String> cell = new TableCell<>() {
                    private String baseStyle = "";
                    private String displayedPrice;
                    private long displayedPriceChangeSequence;
                    private final Timeline flash = new Timeline(
                            new KeyFrame(Duration.millis(800), e -> setStyle(baseStyle))
                    );

                    @Override
                    protected void updateItem(String item, boolean empty) {
                        super.updateItem(item, empty);
                        if (empty || item == null) {
                            flash.stop();
                            displayedPrice = null;
                            setText(null);
                            baseStyle = "";
                            setStyle("");
                        } else {
                            OrderBookEntry data = getTableRow().getItem();
                            if (data == null || data.getDecimalFormat() == null) return;

                            boolean firstDisplay = displayedPrice == null;
                            boolean visiblePriceChanged = !firstDisplay && !item.equals(displayedPrice);
                            displayedPrice = item;
                            long changeSequence = data.getPriceChangeSequence();
                            boolean changed = changeSequence > 0
                                    && changeSequence != displayedPriceChangeSequence;
                            if (changed) {
                                displayedPriceChangeSequence = changeSequence;
                            }
                            setText(item);

                            if (isOwnLiveOrder(data)) {
                                baseStyle = OWN_LIVE_ORDER_STYLE;
                            } else if (Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                                baseStyle = "-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;";
                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                baseStyle = "";
                            } else {
                                baseStyle = "-fx-text-fill: green;";
                            }

                            if (changed) {
                                flash.stop();
                                setStyle(baseStyle + "; -fx-background-color: #2e7d327a; -fx-text-fill: #effff1; -fx-font-weight: bold; -fx-border-color: transparent transparent transparent #69f0ae; -fx-border-width: 0 0 0 3;");
                                flash.playFromStart();
                            } else if (visiblePriceChanged) {
                                flash.stop();
                                setStyle(baseStyle);
                            } else if (flash.getStatus() == Timeline.Status.STOPPED) {
                                setStyle(baseStyle);
                            }
                        }
                    }
                };

                cell.setMouseTransparent(true);

                return cell;
            });

            priceOffer.setCellValueFactory(new PropertyValueFactory<>("price"));

            priceOffer.setCellFactory(column -> {

                TableCell<OrderBookEntry, String> cell = new TableCell<>() {
                    private String baseStyle = "";
                    private String displayedPrice;
                    private long displayedPriceChangeSequence;
                    private final Timeline flash = new Timeline(
                            new KeyFrame(Duration.millis(800), e -> setStyle(baseStyle))
                    );

                    @Override
                    protected void updateItem(String item, boolean empty) {
                        super.updateItem(item, empty);
                        if (empty || item == null) {
                            flash.stop();
                            displayedPrice = null;
                            setText(null);
                            baseStyle = "";
                            setStyle("");
                        } else {
                            OrderBookEntry data = getTableRow().getItem();
                            if (data == null || data.getDecimalFormat() == null) return;

                            boolean firstDisplay = displayedPrice == null;
                            boolean visiblePriceChanged = !firstDisplay && !item.equals(displayedPrice);
                            displayedPrice = item;
                            long changeSequence = data.getPriceChangeSequence();
                            boolean changed = changeSequence > 0
                                    && changeSequence != displayedPriceChangeSequence;
                            if (changed) {
                                displayedPriceChangeSequence = changeSequence;
                            }
                            setText(item);

                            if (isOwnLiveOrder(data)) {
                                baseStyle = OWN_LIVE_ORDER_STYLE;
                            } else if (Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                                baseStyle = "-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;";
                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                baseStyle = "-fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;";
                            } else {
                                baseStyle = "-fx-text-fill: red;";
                            }

                            if (changed) {
                                flash.stop();
                                setStyle(baseStyle + "; -fx-background-color: #b3263e7a; -fx-text-fill: #fff3f5; -fx-font-weight: bold; -fx-border-color: transparent transparent transparent #ff6b7a; -fx-border-width: 0 0 0 3;");
                                flash.playFromStart();
                            } else if (visiblePriceChanged) {
                                flash.stop();
                                setStyle(baseStyle);
                            } else if (flash.getStatus() == Timeline.Status.STOPPED) {
                                setStyle(baseStyle);
                            }
                        }
                    }
                };

                cell.setMouseTransparent(true);

                return cell;
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private void addTableClickListener(TableView<OrderBookEntry> tableView) {

        tableView.setRowFactory(tv -> {
            TableRow<OrderBookEntry> row = new TableRow<>();
            row.setOnMouseClicked(event -> {
                if (!Repository.getPrincipalController().isLightMode() && !row.isEmpty() && event.getClickCount() == 2) {
                    OrderBookEntry rowData = row.getItem();
                    boolean isCompra = (tableView == bidViewTable);
                    openOrderLauncher(rowData, isCompra ? "Venta" : "Compra");
                }
            });
            return row;
        });
    }

    static boolean shouldShowSettlement(MarketDataMessage.SecurityExchangeMarketData market) {
        return market != null && market != MarketDataMessage.SecurityExchangeMarketData.BCS;
    }

    static double ticketWidthForMarket(MarketDataMessage.SecurityExchangeMarketData market) {
        return market == MarketDataMessage.SecurityExchangeMarketData.BCS
                ? BCS_TICKET_WIDTH
                : COMPACT_TICKET_WIDTH;
    }

    private void refreshSettlementVisibility(MarketDataMessage.SecurityExchangeMarketData market) {
        boolean visible = shouldShowSettlement(market);
        settlType.setVisible(visible);
        settlType.setManaged(visible);

        double ticketWidth = ticketWidthForMarket(market);
        ticket.setMinWidth(ticketWidth);
        ticket.setPrefWidth(ticketWidth);
        ticket.setMaxWidth(ticketWidth);
    }

    public void setVisibleDepth(int depth) {
        visibleDepth = normalizeVisibleDepth(depth);
        double tableHeight = tableHeightForDepth(visibleDepth);
        bidViewTable.setFixedCellSize(BOOK_ROW_HEIGHT);
        offerViewTable.setFixedCellSize(BOOK_ROW_HEIGHT);
        setFixedHeight(bidViewTable, tableHeight);
        setFixedHeight(offerViewTable, tableHeight);
        updateCardHeight();
    }

    static int normalizeVisibleDepth(int depth) {
        return switch (depth) {
            case 3, 5, 10, 15 -> depth;
            default -> 5;
        };
    }

    static double tableHeightForDepth(int depth) {
        return BOOK_TABLE_HEADER_HEIGHT + normalizeVisibleDepth(depth) * BOOK_ROW_HEIGHT;
    }

    private void updateCardHeight() {
        boolean supplementaryVisible = statisticsVisible || trendVisible;
        double supplementaryHeight = (statisticsVisible ? STATISTICS_HEIGHT : 0)
                + (trendVisible ? TREND_HEIGHT : 0)
                + (supplementaryVisible ? SUPPLEMENTARY_GAP : 0);
        double cardHeight = BOOK_BASE_HEIGHT + tableHeightForDepth(visibleDepth) + supplementaryHeight;
        bookRoot.setMinHeight(cardHeight);
        bookRoot.setPrefHeight(cardHeight);
        bookRoot.setMaxHeight(cardHeight);
    }

    private void setFixedHeight(Control control, double height) {
        control.setMinHeight(height);
        control.setPrefHeight(height);
        control.setMaxHeight(height);
    }

    public void setSupplementaryVisibility(boolean statisticsVisible, boolean trendVisible) {
        this.statisticsVisible = statisticsVisible;
        this.trendVisible = trendVisible;
        statisticsBar.setVisible(statisticsVisible);
        statisticsBar.setManaged(statisticsVisible);
        tendencia.setVisible(trendVisible);
        tendencia.setManaged(trendVisible);
        supplementaryBox.setVisible(statisticsVisible || trendVisible);
        supplementaryBox.setManaged(statisticsVisible || trendVisible);
        updateCardHeight();
    }

    public void setMultibookSettingsAction(Runnable action) {
        multibookSettingsAction = action;
        boolean available = action != null;
        multibookSettingsButton.setVisible(available);
        multibookSettingsButton.setManaged(available);
    }

    @FXML
    private void openMultibookSettings() {
        if (multibookSettingsAction != null) {
            multibookSettingsAction.run();
        }
    }

    private void openOrderLauncher(OrderBookEntry dataBook, String sideOrderValue) {
        Repository.getPrincipalController().openLauncherFromBook(dataBook, sideOrderValue,
                settlType.getSelectionModel().getSelectedItem(), this);
    }

    String resolveLauncherSymbol(String levelSymbol) {
        if (levelSymbol != null && !levelSymbol.isBlank()) {
            return levelSymbol.trim().toUpperCase(Locale.ROOT);
        }
        if (subscribe != null && subscribe.getSymbol() != null && !subscribe.getSymbol().isBlank()) {
            return subscribe.getSymbol().trim().toUpperCase(Locale.ROOT);
        }
        String selectedSymbol = ticket.getText();
        return selectedSymbol == null ? "" : selectedSymbol.trim().toUpperCase(Locale.ROOT);
    }

    private boolean isOwnLiveOrder(OrderBookEntry entry) {
        if (entry == null) return false;
        TableView<OrderBookEntry> table = entry.getSide() == RoutingMessage.Side.BUY
                ? bidViewTable : offerViewTable;
        boolean repeatedPrice = table.getItems().stream()
                .filter(item -> item != null
                        && item.getSide() == entry.getSide()
                        && Math.abs(item.getPriceValue() - entry.getPriceValue()) < 0.0001d)
                .limit(2)
                .count() > 1;
        if (!repeatedPrice) {
            return Repository.tieneOrdenVivaEn(
                    resolveLauncherSymbol(entry.getSymbol()), entry.getSide(), entry.getPriceValue(),
                    entry.getSecurityExchangeRouting(), settlType.getSelectionModel().getSelectedItem());
        }
        return Repository.tienePosturaVivaEn(
                resolveLauncherSymbol(entry.getSymbol()), entry.getSide(), entry.getPriceValue(),
                entry.getSizeValue(), entry.getAccount(), entry.getOperator(),
                entry.getSecurityExchangeRouting(), settlType.getSelectionModel().getSelectedItem());
    }

    public void refreshOwnOrderMarker(String orderSymbol) {
        if (orderSymbol == null || !resolveLauncherSymbol("").equalsIgnoreCase(orderSymbol.trim())) {
            return;
        }
        bidViewTable.refresh();
        offerViewTable.refresh();
    }

    RoutingMessage.SecurityType resolveLauncherSecurityType(String symbol) {
        if (subscribe != null && subscribe.getSecurityType() != null
                && subscribe.getSecurityType() != RoutingMessage.SecurityType.UNRECOGNIZED) {
            return subscribe.getSecurityType();
        }
        RoutingMessage.SecurityType selected = securityType.getSelectionModel().getSelectedItem();
        return Repository.resolveSecurityType(
                symbol,
                cbMarket.getSelectionModel().getSelectedItem(),
                selected
        );
    }





    private void setupSuggestionsPopup() {
        try {

            suggestionsPopup = new Popup();
            ListView<String> suggestionsList = new ListView<>();
            suggestionsPopup.getContent().add(suggestionsList);
            suggestionsPopup.setAutoHide(true);

            suggestionsList.setOnMouseClicked(event -> {
                String selectedItem = suggestionsList.getSelectionModel().getSelectedItem();
                if (selectedItem != null && activeTextField != null) {
                    activeTextField.setText(selectedItem);
                    updateSecurityTypeComboBox(selectedItem.trim().toUpperCase(Locale.ROOT),
                            cbMarket.getSelectionModel().getSelectedItem());
                    suggestionsPopup.hide();
                }
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    @FXML
    public void subscribeSymbol() {

        try {
            String symbol = ticket.getText() == null ? "" : ticket.getText().trim().toUpperCase(Locale.ROOT);
            if (symbol.isEmpty()) {
                return;
            }

            boolean manualSecurityType = securityTypeManuallySelected;
            RoutingMessage.SecurityType selectedSecurityType = securityType.getSelectionModel().getSelectedItem();
            ticket.setText(symbol);
            if (manualSecurityType) {
                selectSecurityTypeProgrammatically(selectedSecurityType);
                securityTypeManuallySelected = true;
            }

            if (!Repository.getLibroEmergenteMap().containsKey(positions)) {
                Repository.getLibroEmergenteMap().put(positions, this);
            }

            if (!idSubscribeBook.isEmpty()) {
                Repository.unSuscripcion(idSubscribeBook);
                idSubscribeBook = "";
            }


            RoutingMessage.SecurityType suggestedSecurityType = Repository.resolveSecurityType(
                    symbol,
                    cbMarket.getSelectionModel().getSelectedItem(),
                    securityType.getSelectionModel().getSelectedItem()
            );
            RoutingMessage.SecurityType resolvedSecurityType = chooseSubscriptionSecurityType(
                    securityTypeManuallySelected,
                    securityType.getSelectionModel().getSelectedItem(),
                    suggestedSecurityType
            );
            selectSecurityTypeProgrammatically(resolvedSecurityType);

            idSubscribeBook = Repository.createSuscripcion(symbol,
                    cbMarket.getSelectionModel().getSelectedItem(),
                    settlType.getSelectionModel().getSelectedItem(),
                    resolvedSecurityType);

            subscribe = MarketDataMessage.Subscribe.newBuilder()
                    .setId(idSubscribeBook)
                    .setSymbol(symbol)
                    .setSecurityExchange(cbMarket.getSelectionModel().getSelectedItem())
                    .setSettlType(settlType.getSelectionModel().getSelectedItem())
                    .setSecurityType(resolvedSecurityType).build();

            log.info("MULTIBOOK subscribe position={} id={} symbol={} market={} settl={} securityType={}",
                    positions,
                    idSubscribeBook,
                    symbol,
                    cbMarket.getSelectionModel().getSelectedItem(),
                    settlType.getSelectionModel().getSelectedItem(),
                    resolvedSecurityType);


            bidViewTable.setItems(FXCollections.observableArrayList());
            offerViewTable.setItems(FXCollections.observableArrayList());

            bidViewTable.refresh();
            offerViewTable.refresh();

            Sparkline.pintar(tendencia, null);
            solicitarSerieIntradia(symbol);


            if (Repository.getBookPortMaps().containsKey(idSubscribeBook)) {


                BookVO bookVO = Repository.getBookPortMaps().get(idSubscribeBook);

                bidViewTable.setItems(bookVO.getBidBook());
                offerViewTable.setItems(bookVO.getAskBook());

                bidViewTable.refresh();
                offerViewTable.refresh();

                update(bookVO);

            }


            MultibookController.bookChanged(positions);

            Repository.getClientService().sendMessage(subscribe);
            subscriptionRetryCount = 0;
            scheduleSubscriptionHealthCheck();



        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Tendencia intradia del papel. Primero la serie que calcula el backend, que llega completa
     * apenas se suscribe; si todavia no llego, el buffer local que acumula StatisticVO con los ticks.
     */
    private void pintarTendencia(BookVO bookVO) {
        try {
            double[] serie = Repository.getSerieIntradia(
                    TopicGenerator.getTopicMKD(bookVO.getStatisticVO().getStatistic()));
            if (serie == null || serie.length < 2) {
                serie = bookVO.getStatisticVO().getSerieIntradia();
            }
            Sparkline.pintar(tendencia, serie,
                    bookVO.getStatisticVO().getStatistic().getPreviusClose());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /** La serie del dia la calcula el candle-service; sin esto el mini grafico parte vacio. */
    private void solicitarSerieIntradia(String symbol) {
        try {
            if (Repository.getCandleClientService() == null || symbol == null || symbol.isBlank()) {
                return;
            }
            Repository.getCandleClientService().sendMessage(new org.json.JSONObject()
                    .put("action", "load_intraday_series")
                    .put("symbols", new org.json.JSONArray().put(symbol))
                    .toString());
        } catch (Exception e) {
            log.error("No se pudo pedir la serie intradia de {}", symbol, e);
        }
    }

    public void update(BookVO bookVO){

        try {

            if(!bookVO.getId().equals(idSubscribeBook)){
                return;
            }

            if (hasLiveData(bookVO)) {
                cancelSubscriptionHealthCheck();
                subscriptionRetryCount = 0;
            }

            Platform.runLater(() ->{
                    closepriceGen.setText(bookVO.getStatisticVO().getLast());
                    volumeGen.setText(Repository.getFormatter2dec().format(bookVO.getStatisticVO().getVolume()));
                    lowpriceGen.setText(Repository.getFormatter2dec().format(bookVO.getStatisticVO().getLow()));
                    medioGen.setText(bookVO.getStatisticVO().getMid());
                    previusClose.setText(bookVO.getStatisticVO().getPreviusClose());
                    highpriceGen.setText(Repository.getFormatter2dec().format(bookVO.getStatisticVO().getHigh()));
                    imbalanceGen.setText(Repository.getFormatter2dec().format(bookVO.getStatisticVO().getImbalance()));
                    pintarTendencia(bookVO);
            });



        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public void startSubscribe(MarketDataMessage.Subscribe subscribe) {

        try {

            if (isStart) {
                return;
            }

            isStart = true;

            idSubscribeBook = TopicGenerator.getTopicMKD(subscribe);

            ticket.setText(subscribe.getSymbol());

            cbMarket.getSelectionModel().select(subscribe.getSecurityExchange());
            settlType.getSelectionModel().select(subscribe.getSettlType());
            selectSecurityTypeProgrammatically(subscribe.getSecurityType());
            // La configuracion restaurada viene del documento persistido del usuario en Redis.
            // Debe prevalecer incluso si el catalogo recomienda otro SecurityType para el simbolo.
            securityTypeManuallySelected = true;

            Repository.createBook(subscribe);
            subscribeSymbol();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private synchronized void updateSecurityTypeComboBox(String ticket, MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData) {
        try {
            if (securityExchangeMarketData == null) {
                log.warn("securityExchangeMarketData es null para el ticket: {}", ticket);
                return;
            }

            if (!Repository.getSecurityListMaps().contains(ticket, securityExchangeMarketData.name())) {
                log.info("No se encontró el ticket {} en el mercado {}", ticket, securityExchangeMarketData.name());
                return;
            }



                try {
                    MarketDataMessage.Security security = Repository.getSecurityListMaps().get(ticket, securityExchangeMarketData.name());

                    if (security != null) {
                        String securityTypeString = security.getSecurityType();
                        RoutingMessage.SecurityType securityTypeValue = RoutingMessage.SecurityType.valueOf(securityTypeString);
                        selectSecurityTypeProgrammatically(Repository.resolveSecurityType(
                                ticket,
                                securityExchangeMarketData,
                                securityTypeValue
                        ));
                    }
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(e);
                }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void selectSecurityTypeProgrammatically(RoutingMessage.SecurityType value) {
        updatingSecurityTypeProgrammatically = true;
        try {
            securityType.getSelectionModel().select(value);
        } finally {
            updatingSecurityTypeProgrammatically = false;
        }
    }

    static RoutingMessage.SecurityType chooseSubscriptionSecurityType(
            boolean manuallySelected,
            RoutingMessage.SecurityType selected,
            RoutingMessage.SecurityType suggested) {
        if (manuallySelected && selected != null && selected != RoutingMessage.SecurityType.UNRECOGNIZED) {
            return selected;
        }
        if (suggested != null && suggested != RoutingMessage.SecurityType.UNRECOGNIZED) {
            return suggested;
        }
        return selected == null || selected == RoutingMessage.SecurityType.UNRECOGNIZED
                ? RoutingMessage.SecurityType.CS
                : selected;
    }

    public void unsubscribe() {
        cancelSubscriptionHealthCheck();
        Repository.unSuscripcion(idSubscribeBook);
    }

    public void close() {
        if (stage != null && stage.isShowing()) {
            stage.close();
        }

    }

    private void scheduleSubscriptionHealthCheck() {
        cancelSubscriptionHealthCheck();

        subscriptionHealthCheck = new PauseTransition(Duration.seconds(2));
        subscriptionHealthCheck.setOnFinished(event -> {
            if (subscribe == null || idSubscribeBook == null || idSubscribeBook.isBlank()) {
                return;
            }

            BookVO bookVO = Repository.getBookPortMaps().get(idSubscribeBook);
            if (hasLiveData(bookVO)) {
                subscriptionRetryCount = 0;
                return;
            }

            if (subscriptionRetryCount >= MAX_SUBSCRIPTION_RETRIES) {
                log.warn("MULTIBOOK sin data tras reintentos position={} id={} symbol={}",
                        positions,
                        idSubscribeBook,
                        subscribe.getSymbol());
                return;
            }

            subscriptionRetryCount++;
            log.warn("MULTIBOOK retry {}/{} position={} id={} symbol={} market={} settl={} securityType={}",
                    subscriptionRetryCount,
                    MAX_SUBSCRIPTION_RETRIES,
                    positions,
                    idSubscribeBook,
                    subscribe.getSymbol(),
                    subscribe.getSecurityExchange(),
                    subscribe.getSettlType(),
                    subscribe.getSecurityType());
            Repository.refreshSubscription(subscribe, "multibook-zero-data-retry-" + subscriptionRetryCount);
            scheduleSubscriptionHealthCheck();
        });
        subscriptionHealthCheck.playFromStart();
    }

    private void cancelSubscriptionHealthCheck() {
        if (subscriptionHealthCheck != null) {
            subscriptionHealthCheck.stop();
            subscriptionHealthCheck = null;
        }
    }

    private boolean hasLiveData(BookVO bookVO) {
        if (bookVO == null) {
            return false;
        }

        if (!bookVO.getBidBook().isEmpty() || !bookVO.getAskBook().isEmpty()) {
            return true;
        }

        if (bookVO.getStatisticVO() == null || bookVO.getStatisticVO().getStatistic() == null) {
            return false;
        }

        MarketDataMessage.Statistic statistic = bookVO.getStatisticVO().getStatistic();
        return statistic.getBidPx() > 0d
                || statistic.getAskPx() > 0d
                || statistic.getLast() > 0d
                || statistic.getPreviusClose() > 0d
                || statistic.getTradeVolume() > 0d
                || statistic.getIndicativeOpening() > 0d
                || statistic.getReferencialPrice() > 0d;
    }
}
