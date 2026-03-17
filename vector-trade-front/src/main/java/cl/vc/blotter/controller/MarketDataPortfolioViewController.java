package cl.vc.blotter.controller;

import akka.actor.ActorRef;
import cl.vc.blotter.MainApp;
import cl.vc.blotter.Repository;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.model.StatisticVO;
import cl.vc.blotter.utils.Notifier;
// === ADDED
import cl.vc.blotter.utils.ColumnConfig;
// === END ADDED
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;
import javafx.util.Duration;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

@Data
@Slf4j
public class MarketDataPortfolioViewController {
    private static final Duration UI_REFRESH_DELAY = Duration.millis(80);

    @FXML
    public ChoiceBox<MarketDataMessage.SecurityExchangeMarketData> cbMarket;
    @FXML
    public ChoiceBox<RoutingMessage.SettlType> settlType;
    @FXML
    public ChoiceBox<RoutingMessage.SecurityType> securityType;

    private final Set<String> loadedKeys = new HashSet<>();

    @FXML
    public TextField txtSymbol;

    private String idController;

    private String portfolioName;

    private StatisticVO selectedItem = null;

    private ObservableList<StatisticVO> data;

    private RoutingMessage.Order orderSelected;

    @FXML
    private Label lbNews;
    @FXML
    private Button removeSymbol;
    @FXML
    private TitledPane tbNews;
    @FXML
    private TableView<StatisticVO> marketDataStatisticsTable;
    @FXML
    private TableColumn<StatisticVO, String> symbol;
    @FXML
    private TableColumn<StatisticVO, String> settlTypeCol;
    @FXML
    private TableColumn<StatisticVO, String> market;
    @FXML
    private TableColumn<StatisticVO, Double> openpriceGen;
    @FXML
    private TableColumn<StatisticVO, Double> closepriceGen;
    @FXML
    private TableColumn<StatisticVO, String> previusCloseGen;
    @FXML
    private TableColumn<StatisticVO, String> ohlcvCloseGen;
    @FXML
    private TableColumn<StatisticVO, String> bidpriceGen;
    @FXML
    private TableColumn<StatisticVO, Double> bidQtyGen;
    @FXML
    private TableColumn<StatisticVO, String> offerpriceGen;
    @FXML
    private TableColumn<StatisticVO, Double> offerQtyGen;
    @FXML
    private TableColumn<StatisticVO, Double> highpriceGen;
    @FXML
    private TableColumn<StatisticVO, Double> lowpriceGen;
    @FXML
    private TableColumn<StatisticVO, Double> imbalanceGen;
    @FXML
    private TableColumn<StatisticVO, Double> amountGen;
    @FXML
    private TableColumn<StatisticVO, Double> volumeGen;
    @FXML
    private TableColumn<StatisticVO, Double> vwapGen;
    @FXML
    private TableColumn<StatisticVO, String> priceTheoric;
    @FXML
    private TableColumn<StatisticVO, String> amountTheoric;
    @FXML
    private TableColumn<StatisticVO, String> desbalancetheoric;
    @FXML
    private HBox newsHBox;
    private String id = IDGenerator.getID();
    private final Timeline statisticsRefreshTimeline = new Timeline(new KeyFrame(UI_REFRESH_DELAY, e -> marketDataStatisticsTable.refresh()));


    @FXML
    private void initialize() {
        try {

            Repository.setMarketDataPortfolioViewController(this);

            ContextMenu columnMenu = new ContextMenu();

            ColumnConfig cfg = Repository.getColumnConfig();
            try {
                symbol.setVisible(cfg.isSymbol());
                settlTypeCol.setVisible(cfg.isSettlTypeCol());
                imbalanceGen.setVisible(cfg.isImbalanceGen());
                market.setVisible(cfg.isMarket());
                bidQtyGen.setVisible(cfg.isBidQtyGen());
                bidpriceGen.setVisible(cfg.isBidpriceGen());
                offerpriceGen.setVisible(cfg.isOfferpriceGen());
                offerQtyGen.setVisible(cfg.isOfferQtyGen());
                openpriceGen.setVisible(cfg.isOpenpriceGen());
                closepriceGen.setVisible(cfg.isClosepriceGen());
                previusCloseGen.setVisible(cfg.isPreviusCloseGen());
                ohlcvCloseGen.setVisible(cfg.isOhlcvCloseGen());
                highpriceGen.setVisible(cfg.isHighpriceGen());
                lowpriceGen.setVisible(cfg.isLowpriceGen());
                amountGen.setVisible(cfg.isAmountGen());
                volumeGen.setVisible(cfg.isVolumeGen());
                vwapGen.setVisible(cfg.isVwapGen());
                desbalancetheoric.setVisible(cfg.isDesbalancetheoric());
                priceTheoric.setVisible(cfg.isPriceTheoric());
                amountTheoric.setVisible(cfg.isAmountTheoric());
            } catch (Exception e) {
                log.error("Error aplicando visibilidades iniciales de columnas", e);
            }

            for (TableColumn<StatisticVO, ?> column : marketDataStatisticsTable.getColumns()) {

                CheckMenuItem checkMenuItem = new CheckMenuItem(column.getText());
                checkMenuItem.setSelected(column.isVisible());

                checkMenuItem.selectedProperty().addListener((obs, wasSelected, isSelected) -> {
                    column.setVisible(isSelected);

                    try {
                        if (column == symbol) cfg.setSymbol(isSelected);
                        else if (column == settlTypeCol) cfg.setSettlTypeCol(isSelected);
                        else if (column == imbalanceGen) cfg.setImbalanceGen(isSelected);
                        else if (column == market) cfg.setMarket(isSelected);
                        else if (column == bidQtyGen) cfg.setBidQtyGen(isSelected);
                        else if (column == bidpriceGen) cfg.setBidpriceGen(isSelected);
                        else if (column == offerpriceGen) cfg.setOfferpriceGen(isSelected);
                        else if (column == offerQtyGen) cfg.setOfferQtyGen(isSelected);
                        else if (column == openpriceGen) cfg.setOpenpriceGen(isSelected);
                        else if (column == closepriceGen) cfg.setClosepriceGen(isSelected);
                        else if (column == previusCloseGen) cfg.setPreviusCloseGen(isSelected);
                        else if (column == ohlcvCloseGen) cfg.setOhlcvCloseGen(isSelected);
                        else if (column == highpriceGen) cfg.setHighpriceGen(isSelected);
                        else if (column == lowpriceGen) cfg.setLowpriceGen(isSelected);
                        else if (column == amountGen) cfg.setAmountGen(isSelected);
                        else if (column == volumeGen) cfg.setVolumeGen(isSelected);
                        else if (column == vwapGen) cfg.setVwapGen(isSelected);
                        else if (column == desbalancetheoric) cfg.setDesbalancetheoric(isSelected);
                        else if (column == priceTheoric) cfg.setPriceTheoric(isSelected);
                        else if (column == amountTheoric) cfg.setAmountTheoric(isSelected);

                        Repository.saveColumnConfig();
                    } catch (Exception ex) {
                        log.error("Error guardando configuración de columnas", ex);
                    }

                });

                column.visibleProperty().addListener((o, oldV, newV) -> {
                    if (checkMenuItem.isSelected() != newV) {
                        checkMenuItem.setSelected(newV);
                    }
                    try {
                        if (column == symbol) cfg.setSymbol(newV);
                        else if (column == settlTypeCol) cfg.setSettlTypeCol(newV);
                        else if (column == imbalanceGen) cfg.setImbalanceGen(newV);
                        else if (column == market) cfg.setMarket(newV);
                        else if (column == bidQtyGen) cfg.setBidQtyGen(newV);
                        else if (column == bidpriceGen) cfg.setBidpriceGen(newV);
                        else if (column == offerpriceGen) cfg.setOfferpriceGen(newV);
                        else if (column == offerQtyGen) cfg.setOfferQtyGen(newV);
                        else if (column == openpriceGen) cfg.setOpenpriceGen(newV);
                        else if (column == closepriceGen) cfg.setClosepriceGen(newV);
                        else if (column == previusCloseGen) cfg.setPreviusCloseGen(newV);
                        else if (column == ohlcvCloseGen) cfg.setOhlcvCloseGen(newV);
                        else if (column == highpriceGen) cfg.setHighpriceGen(newV);
                        else if (column == lowpriceGen) cfg.setLowpriceGen(newV);
                        else if (column == amountGen) cfg.setAmountGen(newV);
                        else if (column == volumeGen) cfg.setVolumeGen(newV);
                        else if (column == vwapGen) cfg.setVwapGen(newV);
                        else if (column == desbalancetheoric) cfg.setDesbalancetheoric(newV);
                        else if (column == priceTheoric) cfg.setPriceTheoric(newV);
                        else if (column == amountTheoric) cfg.setAmountTheoric(newV);

                        Repository.saveColumnConfig();
                    } catch (Exception ex) {
                        log.error("Error guardando configuración (cambio programático)", ex);
                    }
                });

                columnMenu.getItems().add(checkMenuItem);
            }

            marketDataStatisticsTable.setOnContextMenuRequested(event -> {
                columnMenu.show(marketDataStatisticsTable, event.getScreenX(), event.getScreenY());
            });

            marketDataStatisticsTable.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
            cbMarket.getSelectionModel().select(MarketDataMessage.SecurityExchangeMarketData.BCS);

            cbMarket.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
                if (newValue != null && newValue.toString().equals("DATATEC_XBCL")) {
                    txtSymbol.setText("USD/CLP");
                    txtSymbol.setDisable(true);
                } else {
                    txtSymbol.setDisable(false);
                }
            });

            settlType.setItems(FXCollections.observableArrayList(RoutingMessage.SettlType.values()));
            settlType.getItems().remove(RoutingMessage.SettlType.UNRECOGNIZED);
            settlType.getItems().remove(RoutingMessage.SettlType.T3);
            settlType.getItems().remove(RoutingMessage.SettlType.T5);
            settlType.getItems().remove(RoutingMessage.SettlType.REGULAR);

            settlType.getSelectionModel().select(RoutingMessage.SettlType.T2);

            securityType.setItems(FXCollections.observableArrayList(RoutingMessage.SecurityType.values()));
            securityType.getItems().remove(RoutingMessage.SecurityType.UNRECOGNIZED);
            securityType.getItems().remove(RoutingMessage.SecurityType.PAXOS);
            securityType.getItems().remove(RoutingMessage.SecurityType.OPT);
            securityType.getItems().remove(RoutingMessage.SecurityType.FUT);
            securityType.getSelectionModel().selectFirst();

            txtSymbol.textProperty().addListener((ov, oldValue, newValue) -> txtSymbol.setText(newValue.toUpperCase()));

            data = FXCollections.observableArrayList();

            marketDataStatisticsTable.setItems(data);
            marketDataStatisticsTable.setFixedCellSize(26);
            this.marketDataStatisticsTable.setEditable(true);
            this.marketDataStatisticsTable.getSortOrder().add(this.symbol);
            this.marketDataStatisticsTable.setRowFactory(tv -> new RatioAwareTableRow());

            this.market.setCellValueFactory(new PropertyValueFactory<>("securityExchange"));
            this.symbol.setCellValueFactory(new PropertyValueFactory<>("symbol"));
            this.openpriceGen.setCellValueFactory(new PropertyValueFactory<>("open"));
            openpriceGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        try {
                            setText(Repository.getFormatter2dec().format(item));
                        } catch (Exception e) {
                            log.error("Error formateando bidQty", e);
                            setText("");
                        }
                    }
                }
            });


            this.settlTypeCol.setCellValueFactory(new PropertyValueFactory<>("settlType"));

            this.highpriceGen.setCellValueFactory(new PropertyValueFactory<>("high"));

            highpriceGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        try {
                            setText(Repository.getFormatter4dec().format(item));
                        } catch (Exception e) {
                            log.error("Error formateando bidQty", e);
                            setText("");
                        }
                    }
                }
            });

            this.priceTheoric.setCellValueFactory(new PropertyValueFactory<>("priceTheoric"));
            this.desbalancetheoric.setCellValueFactory(new PropertyValueFactory<>("desbalTheoric"));
            this.amountTheoric.setCellValueFactory(new PropertyValueFactory<>("amountTheoric"));
            this.bidpriceGen.setCellValueFactory(new PropertyValueFactory<>("bidPx"));

            this.amountGen.setCellValueFactory(new PropertyValueFactory<>("amount"));
            amountGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        try {
                            setText(Repository.getFormatter2dec().format(item));
                        } catch (Exception e) {
                            log.error("Error formateando bidQty", e);
                            setText("");
                        }
                    }
                }
            });

            this.volumeGen.setCellValueFactory(new PropertyValueFactory<>("volume"));

            volumeGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        try {
                            setText(Repository.getFormatter0dec().format(item));
                        } catch (Exception e) {
                            log.error("Error formateando bidQty", e);
                            setText("");
                        }
                    }
                }
            });

            this.bidpriceGen.setCellValueFactory(new PropertyValueFactory<>("bidPx"));
            this.bidpriceGen.getStyleClass().add("buyOrder");

            bidpriceGen.setCellFactory(column -> new TableCell<StatisticVO, String>() {
                private String prevItem = null;
                private final Timeline flash = new Timeline(
                        new KeyFrame(Duration.millis(400), e -> setStyle(""))
                );

                @Override
                public void updateIndex(int i) {
                    super.updateIndex(i);
                    prevItem = null;
                }

                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    flash.stop();
                    if (empty || item == null || item.isEmpty()) {
                        setText(null);
                        setStyle("");
                        prevItem = null;
                    } else {
                        boolean changed = prevItem != null && !item.equals(prevItem);
                        prevItem = item;
                        setText(item);
                        if (changed) {
                            setStyle("-fx-background-color: #69f0ae26; -fx-font-weight: bold; -fx-border-color: transparent transparent transparent #69f0ae; -fx-border-width: 0 0 0 3;");
                            flash.playFromStart();
                        } else {
                            setStyle("");
                        }
                    }
                }
            });

            this.bidQtyGen.setCellValueFactory(new PropertyValueFactory<>("bidQty"));

            bidQtyGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        try {
                            setText(Repository.getFormatter0dec().format(item));
                        } catch (Exception e) {
                            log.error("Error formateando bidQty", e);
                            setText("");
                        }
                    }
                }
            });


            this.offerpriceGen.setCellValueFactory(new PropertyValueFactory<>("askPx"));
            this.offerpriceGen.getStyleClass().add("sellOrder");

            offerpriceGen.setCellFactory(column -> new TableCell<StatisticVO, String>() {
                private String prevItem = null;
                private final Timeline flash = new Timeline(
                        new KeyFrame(Duration.millis(400), e -> setStyle(""))
                );

                @Override
                public void updateIndex(int i) {
                    super.updateIndex(i);
                    prevItem = null;
                }

                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    flash.stop();
                    if (empty || item == null || item.isEmpty()) {
                        setText(null);
                        setStyle("");
                        prevItem = null;
                    } else {
                        boolean changed = prevItem != null && !item.equals(prevItem);
                        prevItem = item;
                        setText(item);
                        if (changed) {
                            setStyle("-fx-background-color: #ff525226; -fx-font-weight: bold; -fx-border-color: transparent transparent transparent #ff5252; -fx-border-width: 0 0 0 3;");
                            flash.playFromStart();
                        } else {
                            setStyle("");
                        }
                    }
                }
            });

            this.offerQtyGen.setCellValueFactory(new PropertyValueFactory<>("askQty"));

            offerQtyGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        try {
                            setText(Repository.getFormatter0dec().format(item));
                        } catch (Exception e) {
                            log.error("Error formateando bidQty", e);
                            setText("");
                        }
                    }
                }
            });


            this.imbalanceGen.setCellValueFactory(new PropertyValueFactory<>("imbalance"));
            imbalanceGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        setText(Repository.getFormatter2dec().format(item));
                        if (item < 0) {
                            setStyle("-fx-text-fill: #de292c;");
                        } else if (item > 0) {
                            setStyle("-fx-text-fill: #23a126;");
                        } else {
                            setStyle("-fx-text-fill: white;");
                        }
                    }
                }
            });
            this.lowpriceGen.setCellValueFactory(new PropertyValueFactory<>("low"));
            lowpriceGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        try {
                            setText(Repository.getFormatter4dec().format(item));
                        } catch (Exception e) {
                            log.error("Error formateando bidQty", e);
                            setText("");
                        }
                    }
                }
            });

            this.vwapGen.setCellValueFactory(new PropertyValueFactory<>("vwap"));
            vwapGen.setCellFactory(column -> new TableCell<StatisticVO, Double>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        try {
                            setText(Repository.getFormatter4dec().format(item));
                        } catch (Exception e) {
                            log.error("Error formateando bidQty", e);
                            setText("");
                        }
                    }
                }
            });


            this.closepriceGen.setCellValueFactory(new PropertyValueFactory<>("close"));

            this.previusCloseGen.setCellValueFactory(new PropertyValueFactory<>("previusClose"));

            this.ohlcvCloseGen.setCellValueFactory(new PropertyValueFactory<>("ohlcvClose"));




            tbNews.addEventHandler(MouseEvent.MOUSE_CLICKED, e -> {
                if (e.getButton() == MouseButton.PRIMARY && e.getClickCount() == 2) {

                    FXMLLoader fxmlLoader = new FXMLLoader();
                    fxmlLoader.setLocation(MainApp.class.getResource("/view/marketdata/NewsView.fxml"));
                    AnchorPane newsPane = null;
                    try {
                        newsPane = fxmlLoader.load();
                    } catch (IOException e1) {
                        log.error(e1.getMessage(), e);
                    }

                    Repository.setNotificationController(fxmlLoader.getController());
                    Stage newsStage = new Stage();
                    newsStage.initOwner(Repository.getPrincipal());
                    Scene scene = new Scene(newsPane);
                    newsStage.setScene(scene);
                    newsStage.show();
                }
            });

            marketDataStatisticsTable.setOnMouseClicked(event -> {
                try {
                    StatisticVO sel = marketDataStatisticsTable.getSelectionModel().getSelectedItem();
                    if (sel == null) {
                        return;
                    }
                    selectedItem = sel;

                    BookVO bookVO = Repository.getBookPortMaps().get(sel.getId());
                    if (bookVO != null) {
                        Repository.getClientActor().tell(bookVO.getStatisticVO().getStatistic(), ActorRef.noSender());
                    }
                    onClick(sel);

                } catch (Exception ex) {
                    log.error("Click en tabla: error procesando selección", ex);
                }
            });



            closepriceGen.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {

                        StatisticVO data = getTableRow().getItem();

                        if (data == null) {
                            return;
                        }

                        try {
                            setText(Repository.getFormatter2dec().format(data.getClose()));
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }


                    }
                }
            });


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onClick(StatisticVO selectedItem) {

        try {

            removeSymbol.setDisable(false);

            if (selectedItem != null) {
                setValueSubscribe(selectedItem.getStatistic());
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void setValueSubscribe(MarketDataMessage.Statistic selectedItem) {

        String id = TopicGenerator.getTopicMKD(selectedItem);


        orderSelected = null;
        Repository.getPrincipalController().getLanzadorController().setFormByStatistic(selectedItem);
        Repository.setLastSelectedStatistic(selectedItem);

        if (Repository.getBookPortMaps().containsKey(id)) {

            Platform.runLater(() -> {

                BookVO bookVO = Repository.getBookPortMaps().get(id);
                ensureLiveSubscription(bookVO, selectedItem, "portfolio-click");

                Repository.getPrincipalController().getTableViewBookVController().getBidViewTable().setItems(bookVO.getBidBook());
                Repository.getPrincipalController().getTableViewBookVController().getOfferViewTable().setItems(bookVO.getAskBook());

                Repository.getPrincipalController().getTableViewBookVController().getBidViewTable().refresh();
                Repository.getPrincipalController().getTableViewBookVController().getOfferViewTable().refresh();

                Repository.getPrincipalController().getTableViewBookHController().getBidViewTable().setItems(bookVO.getBidBook());
                Repository.getPrincipalController().getTableViewBookHController().getOfferViewTable().setItems(bookVO.getAskBook());

                Repository.getPrincipalController().getTableViewBookHController().getBidViewTable().refresh();
                Repository.getPrincipalController().getTableViewBookHController().getOfferViewTable().refresh();


                Repository.getPrincipalController().getTradeController()
                        .getMarketDataTradeTable().getSortOrder()
                        .add(Repository.getPrincipalController().getTradeController().getTime());

                Repository.getPrincipalController().getTradeController().getMarketDataTradeTable().refresh();

                SortedList<MarketDataMessage.Trade> sortedData = new SortedList<>(bookVO.getTradesVO());
                sortedData.comparatorProperty().bind( Repository.getPrincipalController().getTradeController()
                        .getMarketDataTradeTable().comparatorProperty());

                Repository.getPrincipalController().getTradeController()
                        .getMarketDataTradeTable().setItems(sortedData);




                Repository.getPrincipalController().getTabTrade().setText("Últimas Operaciones Nemo " + bookVO.getSymbol() + " (" + bookVO.getTradesVO().size() + ")");


            });

        } else {
            log.error("objeto bookVO no existe, muy raro por que está en el portafolio {}", id);
        }

    }

    private void ensureLiveSubscription(BookVO bookVO, MarketDataMessage.Statistic selectedItem, String reason) {
        try {
            if (selectedItem == null) {
                return;
            }

            boolean emptyBook = bookVO == null
                    || (bookVO.getBidBook().isEmpty() && bookVO.getAskBook().isEmpty());

            boolean zeroStatistic = selectedItem.getBidPx() <= 0d
                    && selectedItem.getAskPx() <= 0d
                    && selectedItem.getLast() <= 0d
                    && selectedItem.getPreviusClose() <= 0d
                    && selectedItem.getTradeVolume() <= 0d
                    && selectedItem.getIndicativeOpening() <= 0d
                    && selectedItem.getReferencialPrice() <= 0d;

            if (!emptyBook && !zeroStatistic) {
                return;
            }

            String id = TopicGenerator.getTopicMKD(selectedItem);
            MarketDataMessage.Subscribe subscribe = Repository.getSubscribeIdsMaps().get(id);
            if (subscribe == null) {
                subscribe = MarketDataMessage.Subscribe.newBuilder()
                        .setId(id)
                        .setSymbol(selectedItem.getSymbol())
                        .setSecurityExchange(selectedItem.getSecurityExchange())
                        .setSettlType(selectedItem.getSettlType())
                        .setSecurityType(selectedItem.getSecurityType())
                        .setBook(true)
                        .setStatistic(true)
                        .setTrade(true)
                        .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                        .build();
            }

            log.warn("PORTFOLIO zero-data click symbol={} id={} market={} settl={} securityType={} emptyBook={} zeroStatistic={}",
                    selectedItem.getSymbol(),
                    id,
                    selectedItem.getSecurityExchange(),
                    selectedItem.getSettlType(),
                    selectedItem.getSecurityType(),
                    emptyBook,
                    zeroStatistic);
            Repository.refreshSubscription(subscribe, reason);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private String safeStatSymbol(MarketDataMessage.Statistic s) {
        if (s == null) return null;
        // ajusta al campo real de tu proto:
        // return s.hasSymbol() ? s.getSymbol() : null;
        return s.getSymbol();
    }


    @FXML
    private void cleanPortfolio() {

    }

    @FXML
    private void addSymbol() {


        boolean existeObjeto = data.stream()
                .anyMatch(stock -> txtSymbol.getText().equals(stock.getSymbol())
                        && settlType.getSelectionModel().getSelectedItem().equals(RoutingMessage.SettlType.valueOf(stock.getSettlType()))
                        && cbMarket.getSelectionModel().getSelectedItem().equals(MarketDataMessage.SecurityExchangeMarketData.valueOf(stock.getSecurityExchange())));

        if (!existeObjeto) {

            RoutingMessage.SecurityType securityType1;

            if(Repository.getStaticSecurityType().containsKey(txtSymbol.getText().trim())){
                securityType1 = Repository.getStaticSecurityType().get(txtSymbol.getText().trim());
            } else {
                securityType1 = securityType.getSelectionModel().getSelectedItem();
            }

            MarketDataMessage.Statistic statistic1 = MarketDataMessage.Statistic
                    .newBuilder()
                    .setSymbol(txtSymbol.getText().trim())
                    .setId(IDGenerator.getID())
                    .setSecurityExchange(cbMarket.getSelectionModel().getSelectedItem())
                    .setSettlType(settlType.getSelectionModel().getSelectedItem())
                    .setSecurityType(securityType1)
                    .build();

            BlotterMessage.Asset asset = BlotterMessage.Asset.newBuilder()
                    .setSymbol(txtSymbol.getText().trim())
                    .setStatistic(statistic1)
                    .setSecurityexchange(cbMarket.getSelectionModel().getSelectedItem())
                    .build();

            BlotterMessage.PortfolioRequest addSymbol = BlotterMessage.PortfolioRequest.newBuilder()
                    .setStatusPortfolio(BlotterMessage.StatusPortfolio.ADD_ASSET)
                    .setNamePortfolio(portfolioName)
                    .setAsset(asset)
                    .setUsername(Repository.username).build();

            Repository.getClientService().sendMessage(addSymbol);

        } else {

            Notifier.INSTANCE.notifyError("Error", "Symbol exists");
        }
    }

    @FXML
    private void removeSymbol() {
        try {

            if (selectedItem == null) {
                Notifier.INSTANCE.notifyError("Error", "Symbolo no selecionado");
                return;
            }

            BlotterMessage.PortfolioRequest addSymbol = BlotterMessage.PortfolioRequest.newBuilder()
                    .setStatusPortfolio(BlotterMessage.StatusPortfolio.REMOVE_ASSET)
                    .setNamePortfolio(portfolioName)
                    .setAsset(BlotterMessage.Asset.newBuilder().setStatistic(selectedItem.getStatistic())
                            .setSymbol(selectedItem.getSymbol()).setSecurityexchange(selectedItem.getStatistic().getSecurityExchange()))
                    .setUsername(Repository.username).build();

            Repository.getClientService().sendMessage(addSymbol);

            selectedItem = null;

        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public void addModelVo(BookVO bookVO) {
        try {
            if (bookVO == null || bookVO.getStatisticVO() == null || bookVO.getStatisticVO().getStatistic() == null) return;

            String key = TopicGenerator.getTopicMKD(bookVO.getStatisticVO().getStatistic());

            if (!loadedKeys.add(key)) {
                return;
            }
            data.add(bookVO.getStatisticVO());
            scheduleStatisticsRefresh();

        } catch (Exception e) {
            log.error("addModelVo error", e);
        }
    }

    public void removeModelVo(StatisticVO vo) {
        try {
            if (vo == null || vo.getStatistic() == null) return;
            String key = TopicGenerator.getTopicMKD(vo.getStatistic());
            loadedKeys.remove(key);
            data.remove(vo);
            scheduleStatisticsRefresh();
        } catch (Exception e) {
            log.error("removeModelVo error", e);
        }
    }

    @FXML
    private void deletePorfolio() {
        try {
            // Validaciones básicas
            if (portfolioName == null || portfolioName.isBlank()) {
                Notifier.INSTANCE.notifyError("Error", "Portafolio inválido.");
                return;
            }
            if ("IPSA".equalsIgnoreCase(portfolioName) || "IGPA".equalsIgnoreCase(portfolioName)) {
                Notifier.INSTANCE.notifyError("No permitido", "No puedes eliminar el portafolio " + portfolioName + ".");
                return;
            }

            // Confirmación
            Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
            confirm.setTitle("Eliminar portafolio");
            confirm.setHeaderText("¿Eliminar portafolio \"" + portfolioName + "\"?");
            confirm.setContentText("Esta acción removerá la pestaña del portafolio.");
            confirm.getDialogPane().getScene().getStylesheets()
                    .add(getClass().getResource(Repository.getSTYLE()).toExternalForm());

            var res = confirm.showAndWait();
            if (res.isEmpty() || res.get() != ButtonType.OK) return;

            // IMPORTANTE:
            // Enviar también el marketdataControllerId (id) para que el backend
            // correlacione correctamente (igual que en SNAPSHOT/NEW_PORTFOLIO).
            BlotterMessage.PortfolioRequest.Builder builder =
                    BlotterMessage.PortfolioRequest.newBuilder()
                            .setStatusPortfolio(BlotterMessage.StatusPortfolio.DELETE_PORTFOLIO)
                            .setUsername(Repository.getUsername())
                            .setNamePortfolio(portfolioName);

            // Si este controller tiene id (lo generas con IDGenerator.getID()), inclúyelo:
            if (id != null && !id.isBlank()) {
                builder.setMarketdataControllerId(id);
            }

            // Si dispones de un ID estable del portafolio (idController) y tu .proto
            // tiene un campo para eso (p.ej. setIdPortfolio / setPortfolioId / setId),
            // descomenta y usa el setter correcto:
            // if (idController != null && !idController.isBlank()) {
            //     builder.setIdPortfolio(idController); // <-- AJUSTA al nombre real de tu proto
            // }

            Repository.getClientService().sendMessage(builder.build());

            // No toques la UI aquí: espera la respuesta del servidor.
            // PrincipalController.addDatosDeMercado() ya maneja DELETE_PORTFOLIO
            // y quitará la pestaña, moverá el "+", re-seleccionará Principal, etc.

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Notifier.INSTANCE.notifyError("Error", "No se pudo eliminar el portafolio.");
        }
    }


    private void runFx(Runnable r) {
        if (Platform.isFxApplicationThread()) r.run();
        else Platform.runLater(r);
    }

    private void scheduleStatisticsRefresh() {
        runFx(() -> {
            statisticsRefreshTimeline.stop();
            statisticsRefreshTimeline.playFromStart();
        });
    }

    private static double parseRatioValue(StatisticVO item) {
        if (item == null || item.getRatio() == null) {
            return 0d;
        }
        String raw = item.getRatio().trim();
        if (raw.isBlank()) {
            return 0d;
        }

        String normalized = raw
                .replace("%", "")
                .replace("−", "-")
                .replaceAll("[^0-9,.-]", "");

        if (normalized.indexOf(',') >= 0 && normalized.indexOf('.') >= 0) {
            normalized = normalized.replace(".", "").replace(",", ".");
        } else if (normalized.indexOf(',') >= 0) {
            normalized = normalized.replace(",", ".");
        }

        try {
            return Double.parseDouble(normalized);
        } catch (Exception e) {
            return 0d;
        }
    }

    private static final class RatioAwareTableRow extends TableRow<StatisticVO> {
        private static final String BASE_STYLE = "-fx-background-insets: 0; -fx-background-radius: 0;";
        private static final String POSITIVE_STYLE = BASE_STYLE + " -fx-background-color: rgba(22, 163, 74, 0.12);";
        private static final String NEGATIVE_STYLE = BASE_STYLE + " -fx-background-color: rgba(220, 38, 38, 0.12);";
        private static final String NEUTRAL_STYLE = BASE_STYLE;
        private static final String POSITIVE_BLINK_STYLE = BASE_STYLE + " -fx-background-color: linear-gradient(to right, rgba(34,197,94,0.55), rgba(34,197,94,0.18));";
        private static final String NEGATIVE_BLINK_STYLE = BASE_STYLE + " -fx-background-color: linear-gradient(to right, rgba(239,68,68,0.55), rgba(239,68,68,0.18));";

        private final Timeline blinkTimeline;
        private final ChangeListener<String> ratioListener = (obs, oldValue, newValue) -> {
            StatisticVO current = getItem();
            if (current == null) {
                return;
            }
            applyTrendStyle(current, false);
            if (oldValue == null || oldValue.equals(newValue)) {
                return;
            }
            triggerBlink(current);
        };

        private StatisticVO observedItem;

        private RatioAwareTableRow() {
            blinkTimeline = new Timeline(
                    new KeyFrame(Duration.ZERO, evt -> applyBlinkFrame()),
                    new KeyFrame(Duration.millis(180), evt -> applyTrendStyle(getItem(), false)),
                    new KeyFrame(Duration.millis(360), evt -> applyBlinkFrame()),
                    new KeyFrame(Duration.millis(540), evt -> applyTrendStyle(getItem(), false))
            );
            blinkTimeline.setCycleCount(2);
        }

        @Override
        protected void updateItem(StatisticVO item, boolean empty) {
            if (observedItem != null) {
                observedItem.ratioProperty().removeListener(ratioListener);
                observedItem = null;
            }

            super.updateItem(item, empty);

            if (empty || item == null) {
                blinkTimeline.stop();
                setStyle("");
                return;
            }

            observedItem = item;
            observedItem.ratioProperty().addListener(ratioListener);
            applyTrendStyle(item, false);
        }

        private void triggerBlink(StatisticVO item) {
            if (item == null) {
                return;
            }
            double ratio = parseRatioValue(item);
            if (Double.compare(ratio, 0d) == 0) {
                return;
            }
            blinkTimeline.stop();
            blinkTimeline.playFromStart();
        }

        private void applyBlinkFrame() {
            StatisticVO item = getItem();
            if (item == null) {
                setStyle("");
                return;
            }
            double ratio = parseRatioValue(item);
            if (ratio > 0d) {
                setStyle(POSITIVE_BLINK_STYLE);
            } else if (ratio < 0d) {
                setStyle(NEGATIVE_BLINK_STYLE);
            } else {
                setStyle(NEUTRAL_STYLE);
            }
        }

        private void applyTrendStyle(StatisticVO item, boolean selected) {
            if (item == null) {
                setStyle("");
                return;
            }
            if (isSelected() || selected) {
                setStyle("");
                return;
            }

            double ratio = parseRatioValue(item);
            if (ratio > 0d) {
                setStyle(POSITIVE_STYLE);
            } else if (ratio < 0d) {
                setStyle(NEGATIVE_STYLE);
            } else {
                setStyle(NEUTRAL_STYLE);
            }
        }
    }

    public void requestPortfolio() {
        try {

            Repository.getClientService().sendMessage(BlotterMessage.PortfolioRequest.newBuilder()
                    .setStatusPortfolio(BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO)
                    .setMarketdataControllerId(id)
                    .setUsername(Repository.username).build());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
