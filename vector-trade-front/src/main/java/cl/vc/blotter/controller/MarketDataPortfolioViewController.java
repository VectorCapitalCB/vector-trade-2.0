package cl.vc.blotter.controller;

import akka.actor.ActorRef;
import cl.vc.blotter.MainApp;
import cl.vc.blotter.Repository;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.model.StatisticVO;
import cl.vc.blotter.utils.Notifier;
import cl.vc.blotter.utils.CandleWindow;
import cl.vc.blotter.utils.Sparkline;
// === ADDED
import cl.vc.blotter.utils.ColumnConfig;
// === END ADDED
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.animation.KeyFrame;
import javafx.animation.PauseTransition;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.scene.canvas.Canvas;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;
import javafx.util.Duration;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.HashSet;

@Data
@Slf4j
public class MarketDataPortfolioViewController {
    private static final Duration UI_REFRESH_DELAY = Duration.millis(80);
    private static final Duration INTRADAY_REQUEST_DEBOUNCE = Duration.millis(250);
    private static final KeyCodeCombination COPIAR = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);
    private final DecimalFormat dfCopia = new DecimalFormat("#,##0.####");

    @FXML
    public ChoiceBox<MarketDataMessage.SecurityExchangeMarketData> cbMarket;
    @FXML
    public ChoiceBox<RoutingMessage.SettlType> settlType;
    @FXML
    public ChoiceBox<RoutingMessage.SecurityType> securityType;
    @FXML
    private HBox settlementControls;

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

    /** Mini grafico intradia del ultimo precio. */
    @FXML
    private TableColumn<StatisticVO, Number> sparklineGen;
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
    private final PauseTransition intradayRequestDebounce = new PauseTransition(INTRADAY_REQUEST_DEBOUNCE);


    /**
     * Copia en TSV las columnas VISIBLES de las filas seleccionadas (o de toda la tabla),
     * para pegarlas en una planilla o en un script de validacion. Se excluye "Tendencia":
     * es un canvas, no tiene texto que copiar.
     */
    private void copiarFilas(boolean soloSeleccion) {
        List<StatisticVO> filas = soloSeleccion && !marketDataStatisticsTable.getSelectionModel().getSelectedItems().isEmpty()
                ? new ArrayList<>(marketDataStatisticsTable.getSelectionModel().getSelectedItems())
                : new ArrayList<>(marketDataStatisticsTable.getItems());
        if (filas.isEmpty()) {
            return;
        }

        List<TableColumn<StatisticVO, ?>> columnas = marketDataStatisticsTable.getColumns().stream()
                .filter(TableColumn::isVisible)
                .filter(columna -> columna != sparklineGen)
                .toList();

        StringBuilder sb = new StringBuilder();
        agregarLinea(sb, columnas.stream().map(TableColumn::getText).toList());
        for (StatisticVO fila : filas) {
            agregarLinea(sb, columnas.stream().map(columna -> textoCelda(columna.getCellData(fila))).toList());
        }

        ClipboardContent contenido = new ClipboardContent();
        contenido.putString(sb.toString());
        Clipboard.getSystemClipboard().setContent(contenido);
    }

    private void agregarLinea(StringBuilder sb, List<String> celdas) {
        sb.append(String.join("\t", celdas)).append('\n');
    }

    private String textoCelda(Object valor) {
        if (valor == null) {
            return "";
        }
        return valor instanceof Number numero ? dfCopia.format(numero.doubleValue()) : valor.toString();
    }

    @FXML
    private void initialize() {
        try {

            Repository.setMarketDataPortfolioViewController(this);

            ContextMenu columnMenu = new ContextMenu();

            // Los valores de esta tabla se contrastan contra Mongo y contra la pestana Estadisticas,
            // asi que tienen que poder copiarse en vez de transcribirse a mano.
            MenuItem copiarInstrumento = new MenuItem("Copiar instrumento");
            copiarInstrumento.setOnAction(e -> copiarFilas(true));
            MenuItem copiarTablaCompleta = new MenuItem("Copiar tabla");
            copiarTablaCompleta.setOnAction(e -> copiarFilas(false));
            columnMenu.getItems().addAll(copiarInstrumento, copiarTablaCompleta, new SeparatorMenuItem());

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

            // MULTIPLE para poder copiar varios instrumentos de una; el resto de la vista sigue
            // usando getSelectedItem(), que devuelve el ultimo seleccionado.
            marketDataStatisticsTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
            marketDataStatisticsTable.setOnKeyPressed(event -> {
                if (COPIAR.match(event)) {
                    copiarFilas(true);
                    event.consume();
                }
            });
            cbMarket.getSelectionModel().select(MarketDataMessage.SecurityExchangeMarketData.BCS);

            cbMarket.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
                updateSettlementVisibility(newValue);
                if (newValue != null && newValue.toString().equals("DATATEC_XBCL")) {
                    txtSymbol.setText("USD/CLP");
                    txtSymbol.setDisable(true);
                } else {
                    txtSymbol.setDisable(false);
                    selectRegisteredSecurityType(txtSymbol.getText(), newValue);
                }
            });

            settlType.setItems(FXCollections.observableArrayList(RoutingMessage.SettlType.values()));
            settlType.getItems().remove(RoutingMessage.SettlType.UNRECOGNIZED);
            settlType.getItems().remove(RoutingMessage.SettlType.T3);
            settlType.getItems().remove(RoutingMessage.SettlType.T5);
            settlType.getItems().remove(RoutingMessage.SettlType.REGULAR);

            settlType.getSelectionModel().select(RoutingMessage.SettlType.T2);
            updateSettlementVisibility(cbMarket.getSelectionModel().getSelectedItem());

            securityType.setItems(FXCollections.observableArrayList(RoutingMessage.SecurityType.values()));
            securityType.getItems().remove(RoutingMessage.SecurityType.UNRECOGNIZED);
            securityType.getItems().remove(RoutingMessage.SecurityType.PAXOS);
            securityType.getItems().remove(RoutingMessage.SecurityType.OPT);
            securityType.getItems().remove(RoutingMessage.SecurityType.FUT);
            securityType.getSelectionModel().selectFirst();

            data = FXCollections.observableArrayList();
            intradayRequestDebounce.setOnFinished(e -> solicitarSeriesIntradia());
            data.addListener((ListChangeListener<StatisticVO>) change -> programarSolicitudSeriesIntradia());
            Repository.candleConnectedProperty().addListener((obs, oldValue, connected) -> {
                if (connected) {
                    programarSolicitudSeriesIntradia();
                }
            });

            // El campo de simbolo tambien filtra la tabla mientras se escribe. data sigue
            // recibiendo los updates de mercado; la vista filtrada se recalcula sola.
            FilteredList<StatisticVO> datosFiltrados = new FilteredList<>(data, vo -> true);
            // SortedList es obligatorio: sin el, ordenar por una cabecera lanza
            // UnsupportedOperationException porque FilteredList es inmutable.
            SortedList<StatisticVO> datosOrdenados = new SortedList<>(datosFiltrados);
            datosOrdenados.comparatorProperty().bind(marketDataStatisticsTable.comparatorProperty());

            txtSymbol.textProperty().addListener((ov, oldValue, newValue) -> {
                txtSymbol.setText(newValue.toUpperCase());
                String filtro = txtSymbol.getText().trim();
                selectRegisteredSecurityType(filtro, cbMarket.getSelectionModel().getSelectedItem());
                datosFiltrados.setPredicate(filtro.isEmpty()
                        ? vo -> true
                        : vo -> vo.getSymbol() != null && vo.getSymbol().contains(filtro));
            });

            marketDataStatisticsTable.setItems(datosOrdenados);
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
            configurarColumnaTendencia();

            // El arranque lo dispara la llegada real de simbolos. Este timer es solo respaldo.
            Timeline seriesIntradia = new Timeline(
                    new KeyFrame(Duration.seconds(60), e -> solicitarSeriesIntradia()));
            seriesIntradia.setCycleCount(Timeline.INDEFINITE);
            seriesIntradia.play();
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
        String symbolToAdd = txtSymbol.getText().trim().toUpperCase(Locale.ROOT);
        if (symbolToAdd.isEmpty()) {
            Notifier.INSTANCE.notifyError("Error", "Ingrese un instrumento");
            return;
        }

        MarketDataMessage.SecurityExchangeMarketData selectedMarket =
                cbMarket.getSelectionModel().getSelectedItem();
        MarketDataMessage.Security registeredSecurity =
                getRegisteredSecurity(symbolToAdd, selectedMarket);
        if (registeredSecurity == null) {
            Notifier.INSTANCE.notifyError(
                    "Instrumento no encontrado",
                    symbolToAdd + " no está registrado en " + selectedMarket
            );
            return;
        }
        selectRegisteredSecurityType(symbolToAdd, selectedMarket);

        boolean existeObjeto = data.stream()
                .anyMatch(stock -> symbolToAdd.equalsIgnoreCase(stock.getSymbol())
                        && settlType.getSelectionModel().getSelectedItem().equals(RoutingMessage.SettlType.valueOf(stock.getSettlType()))
                        && cbMarket.getSelectionModel().getSelectedItem().equals(MarketDataMessage.SecurityExchangeMarketData.valueOf(stock.getSecurityExchange())));

        if (!existeObjeto) {

            // Manda la Clase que el usuario eligio. El mapa staticSecurityType forzaba a CS
            // los simbolos CFI*, asi que un CFI nunca se podia suscribir como CFI; ese mapa
            // sigue aplicando al Lanzador (ordenes), donde esos papeles si operan como CS.
            RoutingMessage.SecurityType securityType1 = securityType.getSelectionModel().getSelectedItem();

            MarketDataMessage.Statistic statistic1 = MarketDataMessage.Statistic
                    .newBuilder()
                    .setSymbol(symbolToAdd)
                    .setId(IDGenerator.getID())
                    .setSecurityExchange(cbMarket.getSelectionModel().getSelectedItem())
                    .setSettlType(settlType.getSelectionModel().getSelectedItem())
                    .setSecurityType(securityType1)
                    .build();

            BlotterMessage.Asset asset = BlotterMessage.Asset.newBuilder()
                    .setSymbol(symbolToAdd)
                    .setStatistic(statistic1)
                    .setSecurityexchange(cbMarket.getSelectionModel().getSelectedItem())
                    .build();

            BlotterMessage.PortfolioRequest addSymbol = BlotterMessage.PortfolioRequest.newBuilder()
                    .setStatusPortfolio(BlotterMessage.StatusPortfolio.ADD_ASSET)
                    .setNamePortfolio(portfolioName)
                    .setAsset(asset)
                    .setUsername(Repository.username).build();

            Repository.getClientService().sendMessage(addSymbol);
            txtSymbol.clear();

        } else {

            Notifier.INSTANCE.notifyError("Error", "Symbol exists");
        }
    }

    private void selectRegisteredSecurityType(
            String symbol,
            MarketDataMessage.SecurityExchangeMarketData market) {
        MarketDataMessage.Security registeredSecurity = getRegisteredSecurity(symbol, market);
        if (registeredSecurity == null || securityType == null || securityType.getItems() == null) {
            return;
        }

        try {
            RoutingMessage.SecurityType registeredType =
                    RoutingMessage.SecurityType.valueOf(registeredSecurity.getSecurityType());
            if (securityType.getItems().contains(registeredType)) {
                securityType.getSelectionModel().select(registeredType);
            }
        } catch (IllegalArgumentException e) {
            log.warn("Clase desconocida para instrumento {} en {}: {}",
                    symbol, market, registeredSecurity.getSecurityType());
        }
    }

    private MarketDataMessage.Security getRegisteredSecurity(
            String symbol,
            MarketDataMessage.SecurityExchangeMarketData market) {
        if (symbol == null || symbol.isBlank() || market == null) {
            return null;
        }
        String normalizedSymbol = symbol.trim().toUpperCase(Locale.ROOT);
        MarketDataMessage.Security catalogSecurity = Repository.getSecurityListMaps()
                .get(normalizedSymbol, market.name());
        if (catalogSecurity != null) {
            return catalogSecurity;
        }

        // La SecurityList puede llegar despues del portfolio inicial. Una suscripcion MKD
        // activa ya fue validada por el core y es una fuente segura para completar esa ventana.
        return findSubscribedSecurity(
                normalizedSymbol,
                market,
                Repository.getSubscribeIdsMaps().values());
    }

    static MarketDataMessage.Security findSubscribedSecurity(
            String symbol,
            MarketDataMessage.SecurityExchangeMarketData market,
            Collection<MarketDataMessage.Subscribe> subscriptions) {
        if (symbol == null || symbol.isBlank() || market == null || subscriptions == null) {
            return null;
        }

        String normalizedSymbol = symbol.trim().toUpperCase(Locale.ROOT);
        return subscriptions.stream()
                .filter(subscription -> subscription != null
                        && normalizedSymbol.equalsIgnoreCase(subscription.getSymbol())
                        && market == subscription.getSecurityExchange())
                .findFirst()
                .map(subscription -> MarketDataMessage.Security.newBuilder()
                        .setSymbol(normalizedSymbol)
                        .setSecurityExchange(market)
                        .setSecurityType(subscription.getSecurityType().name())
                        .build())
                .orElse(null);
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

    /**
     * Columna "Tendencia": dibuja la serie intradia del ultimo precio de cada papel.
     *
     * El cellValueFactory devuelve la propiedad 'close', asi que la celda se repinta
     * exactamente cuando cambia el precio de ESA fila, sin listeners extra ni timers.
     * TableView solo instancia celdas para las filas visibles, asi que el costo esta
     * acotado a lo que se ve, no al total de instrumentos suscritos.
     */
    /**
     * Pide al candle-service la serie intradia de los papeles de esta tabla. El backend la
     * calcula desde los trades del dia, asi que el mini grafico se ve completo apenas carga
     * el portafolio, sin esperar a acumular ticks en el cliente.
     *
     * Se repite cada 60 s porque la cola no se puede apoyar en el stream incremental de
     * trades: ese cursorea por _id, que con los ids String del productor actual ordena
     * alfabeticamente y se saltea papeles.
     */
    private void solicitarSeriesIntradia() {
        try {
            if (Repository.getCandleClientService() == null || data == null || data.isEmpty()) {
                return;
            }
            org.json.JSONArray simbolos = new org.json.JSONArray();
            data.stream()
                    .map(StatisticVO::getSymbol)
                    .filter(s -> s != null && !s.isBlank())
                    .distinct()
                    .forEach(simbolos::put);
            if (simbolos.isEmpty()) return;

            org.json.JSONObject peticion = new org.json.JSONObject()
                    .put("action", "load_intraday_series")
                    .put("symbols", simbolos);
            Repository.getCandleClientService().sendMessage(peticion.toString());
        } catch (Exception e) {
            log.error("No se pudo pedir la serie intradia", e);
        }
    }

    private void programarSolicitudSeriesIntradia() {
        if (!Platform.isFxApplicationThread()) {
            Platform.runLater(this::programarSolicitudSeriesIntradia);
            return;
        }
        intradayRequestDebounce.playFromStart();
    }

    public void solicitarSerieIntradia(String symbol) {
        try {
            if (Repository.getCandleClientService() == null || symbol == null || symbol.isBlank()) {
                return;
            }
            org.json.JSONObject peticion = new org.json.JSONObject()
                    .put("action", "load_intraday_series")
                    .put("symbols", new org.json.JSONArray().put(symbol.trim().toUpperCase(Locale.ROOT)));
            Repository.getCandleClientService().sendMessage(peticion.toString());
        } catch (Exception e) {
            log.error("No se pudo pedir la serie intradia de {}", symbol, e);
        }
    }

    public void refreshTrendColumn() {
        runFx(() -> {
            if (marketDataStatisticsTable != null) {
                marketDataStatisticsTable.refresh();
            }
        });
    }

    private void configurarColumnaTendencia() {
        if (sparklineGen == null) return;

        sparklineGen.setCellValueFactory(cd -> cd.getValue().tendenciaVersionProperty());

        sparklineGen.setCellFactory(col -> new TableCell<>() {
            private final Canvas lienzo = new Canvas(62, 18);

            {
                // Doble click sobre la tendencia abre el grafico de velas del papel de esa fila.
                // El handler va en la CELDA y no en la tabla para que solo dispare en esta columna.
                // No se consume el evento: el setOnMouseClicked de la tabla sigue haciendo su
                // seleccion y su tell al ClientActor como hasta ahora.
                setOnMouseClicked(e -> {
                    if (e.getButton() != MouseButton.PRIMARY || e.getClickCount() != 2) return;
                    StatisticVO fila = (getTableRow() == null) ? null : (StatisticVO) getTableRow().getItem();
                    if (fila == null || fila.getStatistic() == null) return;
                    String symbol = fila.getStatistic().getSymbol();
                    if (symbol == null || symbol.isBlank()) return;
                    CandleWindow.open(symbol);
                });
                setTooltip(new Tooltip("Doble click: abrir grafico de velas"));
            }

            @Override
            protected void updateItem(Number valor, boolean vacio) {
                super.updateItem(valor, vacio);
                StatisticVO vo = (getTableRow() == null) ? null : (StatisticVO) getTableRow().getItem();
                if (vacio || vo == null) {
                    setGraphic(null);
                    return;
                }
                // Los trades del core son la fuente viva. Mongo solo gana cuando trae una
                // serie mas completa y termina cerca del ultimo precio actual.
                double[] serie = vo.getSerieIntradia();
                double[] backend = Repository.getSerieIntradia(TopicGenerator.getTopicMKD(vo.getStatistic()));
                if (contarPuntosValidos(backend) > contarPuntosValidos(serie)
                        && serieBackendCompatible(backend, vo)) {
                    serie = backend;
                }
                Sparkline.pintar(lienzo, serie, vo.getReferenciaTendencia());
                setGraphic(lienzo);
            }
        });
    }

    private static int contarPuntosValidos(double[] serie) {
        if (serie == null) return 0;
        int validos = 0;
        for (double value : serie) {
            if (Double.isFinite(value) && value > 0d) validos++;
        }
        return validos;
    }

    private static boolean serieBackendCompatible(double[] serie, StatisticVO vo) {
        if (serie == null || vo == null || vo.getStatistic() == null) return false;
        double actual = vo.getStatistic().getLast();
        if (actual <= 0d) actual = vo.getClose();
        if (actual <= 0d) return true;

        double ultimo = 0d;
        for (double value : serie) {
            if (Double.isFinite(value) && value > 0d) ultimo = value;
        }
        return ultimo > 0d && Math.abs(ultimo - actual) / actual <= 0.01d;
    }

    public void addModelVo(BookVO bookVO) {
        if (bookVO == null) return;
        addStatisticVo(bookVO.getStatisticVO());
    }

    /** Agrega el VO vivo tal cual: comparte properties con la pestaña de origen, no lo clona. */
    public void addStatisticVo(StatisticVO vo) {
        try {
            if (vo == null || vo.getStatistic() == null) return;

            if (!loadedKeys.add(TopicGenerator.getTopicMKD(vo.getStatistic()))) {
                return;
            }
            data.add(vo);
            scheduleStatisticsRefresh();

        } catch (Exception e) {
            log.error("addStatisticVo error", e);
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

    private void updateSettlementVisibility(MarketDataMessage.SecurityExchangeMarketData selectedMarket) {
        boolean showSettlement = selectedMarket != MarketDataMessage.SecurityExchangeMarketData.BCS;
        settlementControls.setVisible(showSettlement);
        settlementControls.setManaged(showSettlement);
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
