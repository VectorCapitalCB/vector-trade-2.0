package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.adaptor.ClientActor;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.model.OrderBookEntry;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.module.protocolbuff.utils.Corredoras;
import cl.vc.module.protocolbuff.utils.ProtoConverter;
import com.google.protobuf.Timestamp;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.geometry.Side;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.*;
import javafx.stage.Popup;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.math.BigDecimal;
import java.net.URL;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Data
@Slf4j
public class MarketDataViewerController implements Initializable {

    public static final int QTY_DATA_TRADE = 200;

    private static final Set<RoutingMessage.OrderStatus> nonCancelableStatuses = EnumSet.of(RoutingMessage.OrderStatus.FILLED, RoutingMessage.OrderStatus.REJECTED, RoutingMessage.OrderStatus.CANCELED);

    @FXML
    public Tab tabRuteo;

    @FXML
    public Tab tabmkd;

    @FXML
    public Tab tabmkd2;

    @FXML
    public CheckBox chkIndivisible;

    @FXML
    public Tab simbol;

    @FXML
    public TitledPane titelpaneMarketData;

    public VBox vboxBooks;

    public VBox vboxBooksOffer;

    public VBox vboxBooksBid;

    @FXML
    public TitledPane titelpaneMarketDataMercadoGeneral;

    String symbol = "BCS:ENELCHILE";

    private String id = IDGenerator.getID();

    private String subscribeBook = "";

    private MarketDataMessage.Subscribe.Builder subscribe = MarketDataMessage.Subscribe.newBuilder();

    private MarketDataMessage.Unsubscribe.Builder unSubscribe = MarketDataMessage.Unsubscribe.newBuilder();

    private boolean isLightMode = false;

    @FXML
    private SplitPane Splid;

    @FXML
    private HBox containerbajo;

    @FXML
    private Label lblquantity;

    @FXML
    private Label lblpriceOrder2;

    private TableView<OrderBookEntry> bidViewTable;

    private TableColumn<OrderBookEntry, String> quantityBid;

    private TableColumn<OrderBookEntry, String> priceBid;

    private TableView<OrderBookEntry> offerViewTable;

    private TableColumn<OrderBookEntry, String> priceOffer;

    private TableColumn<OrderBookEntry, String> quantityOffer;

    @FXML
    private TableView<MarketDataMessage.Trade> marketDataTradeTable;

    @FXML
    private TableColumn<MarketDataMessage.Trade, Timestamp> time;

    @FXML
    private TableColumn<MarketDataMessage.Trade, String> symboltrades;

    @FXML
    private TableColumn<MarketDataMessage.Trade, Double> priceTrade;

    @FXML
    private TableColumn<MarketDataMessage.Trade, Double> qtyTrade;

    @FXML
    private TableColumn<MarketDataMessage.Trade, String> buyer;

    @FXML
    private TableColumn<MarketDataMessage.Trade, String> seller;

    @FXML
    private TableColumn<MarketDataMessage.Trade, String> idgenerado;

    @FXML
    private TableView<MarketDataMessage.TradeGeneral> marketDataTradeTableG;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, Timestamp> timeG;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> symboltradesG;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, Double> priceTradeG;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, Double> qtyTradeG;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> buyerG;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> selleG;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> idgeneradoG;

    @Getter
    private SortedList<MarketDataMessage.TradeGeneral> sortedDataG;

    @FXML
    private ClientActor clientActor;

    @FXML
    private ComboBox<RoutingMessage.SecurityExchangeRouting> securityExchangeFilter;

    @FXML
    private ComboBox<RoutingMessage.OrderStatus> statusFilter;

    @FXML
    private TabPane tabpanel;

    @FXML
    private ComboBox<String> sideFilter;

    @FXML
    private TextField symbolFilter;

    private RoutingMessage.Order orderSelected;

    @FXML
    private TitledPane webViewHBox;

    @FXML
    private TabPane tpMkData;

    @FXML
    private Tab add;

    @FXML
    private AnchorPane mkdcontroller;

    @FXML
    private TextField quantity2;

    @FXML
    private TextField priceOrder2;

    @FXML
    private VBox orderLaunchermd;

    @FXML
    private Button addpreselect;

    @FXML
    private Button StopAll;

    @FXML
    private ComboBox<RoutingMessage.StrategyOrder> estrategia2;

    @FXML
    private Button addBookButton;

    @FXML
    private TextField spread2;

    @FXML
    private TextField limit2;

    @FXML
    private Label visible;

    @FXML
    private TextField visibleid;

    @FXML
    private Button best;

    @FXML
    private Button hit;

    @FXML
    private CheckBox hideIDs;

    @FXML
    private Button clean;

    @FXML
    private Button unlock;

    @FXML
    private Button routeOrder;

    @FXML
    private Button tickMas;

    @FXML
    private Button tickMenos;

    @FXML
    private Tab orderlauncher;

    @FXML
    private GridPane gpLauncher;

    @FXML
    private Button replaceOrder;
    @FXML
    private TitledPane titelpanelLauncher;
    @FXML
    private TitledPane tpWorking;


    @FXML
    private TitledPane TPexecutionsOrder;
    @FXML
    private ButtonBar ButtonbarFiltros;
    @FXML
    private Label tipo;
    @FXML
    private Label labelBroker;
    @FXML
    private Pane pane8;
    @FXML
    private Pane pane9;
    @FXML
    private Label lse;
    @FXML
    private Label currencylb;
    @FXML
    private Label cOperadorlb;
    @FXML
    private Label spreadlb;
    @FXML
    private Label strategylb;
    @FXML
    private Label limitlb;
    @FXML
    private Label secLb;
    @FXML
    private Pane pane1;
    @FXML
    private Pane pane2;
    @FXML
    private Pane pane3;
    @FXML
    private Pane pane4;
    @FXML
    private Pane pane5;
    @FXML
    private ListView<String> suggestionsList;

    @FXML
    private TableView<RoutingMessage.Order> tableExecutionReports;

    @FXML
    private Button cancelOrder;

    private FilteredList<RoutingMessage.Order> filteredData;

    @FXML
    private Button paste;
    @FXML
    private TitledPane preselect;
    @FXML
    private ExecutionsController preselectOrdersController;
    @FXML
    private HBox hboxMKD;

    @FXML
    private Label lblsettltypeOrder;

    @FXML
    private Label LblacAccount;

    @FXML
    private Label LblsecurityType;
    @FXML
    private Label estrategiaLabel;
    @FXML
    private Label spreadLabel;
    @FXML
    private Label limitLabel;
    @FXML
    private GraficoController graficoController;

    @FXML
    private PositionsController positionsViewController;

    private Map<String, List<String>> deaultoForm = new HashMap<>();

    private HashMap<String, String> allbrokercode = Corredoras.getAll();

    private boolean isVertical = true;

    @FXML
    private VBox myHBox;
    @FXML
    private VBox vboxTipoOrden;
    @FXML
    private VBox vboxNemo;
    @FXML
    private VBox vboxCantidad;
    @FXML
    private VBox vboxPrecio;
    @FXML
    private SplitPane mainSplitPane;

    private boolean isProgrammaticChange = false;

    private ObservableList<String> allSymbols;

    private FilteredList<String> filteredList;

    private Popup suggestionsPopup;

    @FXML
    private LanzadorController lanzadorController;

    private boolean isUpdatingFromAccount = false;
    private boolean ignoreFirstAccountSelect = true;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

        try {
            lanzadorController.setupTicketTextField();
            Repository.setLanzadorController(lanzadorController);
            Repository.getMapLanzadores().put("Principal", lanzadorController);


            estrategiaLabel.setVisible(false);
            estrategia2.setVisible(false);
            spreadLabel.setVisible(false);
            spread2.setVisible(false);
            limitLabel.setVisible(false);
            limit2.setVisible(false);
            best.setVisible(false);
            hit.setVisible(false);
            tickMas.setVisible(false);
            tickMenos.setVisible(false);
            replaceOrder.setVisible(false);
            quantity2.setVisible(false);
            priceOrder2.setVisible(false);
            cancelOrder.setVisible(false);
            lblquantity.setVisible(false);
            lblpriceOrder2.setVisible(false);
            visible.setVisible(false);
            visibleid.setVisible(false);
            replaceOrder.setDisable(true);

            setConfByuser(true);


            replaceOrder.getStyleClass().clear();
            replaceOrder.getStyleClass().add("btn-replaceOrder-style");

            priceBid.setCellValueFactory(new PropertyValueFactory<>("price"));

            priceBid.setCellFactory(column -> new TableCell<>() {
                {
                    if (!getStyleClass().contains("book-num")) getStyleClass().add("book-num");
                }

                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                        return;
                    }

                    setText(item);
                    OrderBookEntry data = getTableRow() != null ? getTableRow().getItem() : null;
                    if (data == null) {
                        setStyle("");
                        return;
                    }

                    final boolean alignRight = MarketDataViewerController.this.isLightMode && !MarketDataViewerController.this.isVertical;
                    final String BASE = alignRight ? "-fx-alignment: CENTER-RIGHT;" : "";

                    if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                        setStyle(BASE + " -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #056774;");
                    } else if (Repository.getUser() != null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                        setStyle(BASE + " -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");
                    } else if ("041".equals(data.getOperator()) && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                        setStyle(BASE + " -fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");
                    } else {
                        setStyle(BASE + " -fx-text-fill: green;");
                    }
                }
            });

            quantityBid.setCellValueFactory(new PropertyValueFactory<>("size"));

            quantityBid.setCellFactory(column -> new TableCell<>() {
                {
                    if (!getStyleClass().contains("book-num")) getStyleClass().add("book-num");
                }

                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                        return;
                    }

                    setText(item);
                    OrderBookEntry data = getTableRow() != null ? getTableRow().getItem() : null;
                    if (data == null) {
                        setStyle("");
                        return;
                    }

                    // SOLO alinear a la derecha cuando es LIGHT + HORIZONTAL
                    final boolean alignRight = MarketDataViewerController.this.isLightMode && !MarketDataViewerController.this.isVertical;
                    final String BASE = alignRight ? "-fx-alignment: CENTER-RIGHT;" : "";

                    if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                        setStyle(BASE + " -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #056774;");
                    } else if (Repository.getUser() != null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                        setStyle(BASE + " -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");
                    } else if ("041".equals(data.getOperator()) && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                        setStyle(BASE + " -fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");
                    } else {
                        setStyle(BASE + " -fx-text-fill: green;");
                    }
                }
            });

            priceOffer.setCellValueFactory(new PropertyValueFactory<>("price"));

            priceOffer.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {

                        if (!getStyleClass().contains("book-num")) getStyleClass().add("book-num");

                        try {

                            OrderBookEntry data = getTableRow().getItem();

                            if (data == null || data.getDecimalFormat() == null) {
                                return;
                            }

                            setText(item);

                            final boolean alignRight = MarketDataViewerController.this.isLightMode && !MarketDataViewerController.this.isVertical;
                            final String BASE = alignRight ? "-fx-alignment: CENTER-RIGHT;" : "";

                            if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                                setStyle(BASE + " -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #450574;");
                            } else if (Repository.getUser() != null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                                setStyle(getStyle() + "-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #8B3A3A; ");
                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                setStyle(getStyle() + "-fx-border-color: #2b3178; -fx-text-fill: #ffffff; -fx-background-color: #e01919;");
                            } else {
                                setStyle("-fx-text-fill: #db292b;");
                            }

                            double rot = getTableView().getRotate(); // 0 o 180
                            setRotate(rot);
                            //setAlignment(rot == 180 ? Pos.CENTER_LEFT : Pos.CENTER_RIGHT);

                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
            });

            quantityOffer.setCellValueFactory(new PropertyValueFactory<>("size"));

            quantityOffer.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {


                        if (!getStyleClass().contains("book-num")) getStyleClass().add("book-num");

                        OrderBookEntry data = getTableRow().getItem();

                        if (data == null) return;

                        try {

                            setText(item);

                            final boolean alignRight = MarketDataViewerController.this.isLightMode && !MarketDataViewerController.this.isVertical;
                            final String BASE = alignRight ? "-fx-alignment: CENTER-RIGHT;" : "";

                            if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                                setStyle(BASE + " -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #450574;");

                            } else if (Repository.getUser() != null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                                setStyle(getStyle() + "-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #8B3A3A; ");
                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                setStyle(getStyle() + "-fx-border-color: #2b3178; -fx-text-fill: #ffffff; -fx-background-color: #e01919;");
                            } else {
                                setStyle("-fx-text-fill: #db282c;");
                            }


                            double rot = getTableView().getRotate(); // 0 o 180
                            setRotate(rot);

                            if (rot == 0) {
                                if (offerViewTable.getItems().size() <= 12) {
                                    priceOffer.setMaxWidth(130);
                                    priceOffer.setMinWidth(130);
                                    quantityOffer.setMaxWidth(109);
                                    quantityOffer.setMinWidth(109);
                                } else {
                                    priceOffer.setMaxWidth(130);
                                    priceOffer.setMinWidth(130);
                                    quantityOffer.setMaxWidth(107);
                                    quantityOffer.setMinWidth(107);
                                }
                            }

                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
            });

            priceTrade.setCellValueFactory(new PropertyValueFactory<>("price"));
            qtyTrade.setCellValueFactory(new PropertyValueFactory<>("qty"));
            time.setCellValueFactory(new PropertyValueFactory<>("t"));

            time.setComparator((Timestamp t1, Timestamp t2) -> {
                Instant instant1 = Instant.ofEpochSecond(t1.getSeconds(), t1.getNanos());
                Instant instant2 = Instant.ofEpochSecond(t2.getSeconds(), t2.getNanos());
                return instant1.compareTo(instant2);
            });

            timeG.setComparator((Timestamp t1, Timestamp t2) -> {
                Instant instant1 = Instant.ofEpochSecond(t1.getSeconds(), t1.getNanos());
                Instant instant2 = Instant.ofEpochSecond(t2.getSeconds(), t2.getNanos());
                return instant1.compareTo(instant2);
            });

            time.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(Timestamp item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        Instant instant = Instant.ofEpochSecond(item.getSeconds(), item.getNanos());
                        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
                        ZonedDateTime zonedDateTimeChile = zonedDateTime.withZoneSameInstant(Repository.getZoneID());
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                        String formattedDateTime = zonedDateTimeChile.format(formatter);
                        setText(formattedDateTime);
                    }
                }
            });

            priceTrade.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(Double item, boolean empty) {

                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        MarketDataMessage.Trade data = getTableRow().getItem();

                        if (data == null) {
                            return;
                        }

                        BigDecimal tick = Ticks.conversorExdestination(data.getSecurityExchange(), BigDecimal.valueOf(item));
                        DecimalFormat decimalFormat = NumberGenerator.formetByticks(tick);
                        setText(decimalFormat.format(item));
                    }
                }
            });


            qtyTrade.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        MarketDataMessage.Trade data = getTableRow().getItem();

                        if (data == null) {
                            return;
                        }

                        DecimalFormat decimalFormat = NumberGenerator.getFormatNumberMilDec(data.getSecurityExchange());
                        setText(decimalFormat.format(item));
                    }
                }
            });

            buyer.setCellValueFactory(new PropertyValueFactory<>("buyer"));
            seller.setCellValueFactory(new PropertyValueFactory<>("seller"));
            symboltrades.setCellValueFactory(new PropertyValueFactory<>("symbol"));
            idgenerado.setCellValueFactory(new PropertyValueFactory<>("idGenerico"));
            idgeneradoG.setCellValueFactory(new PropertyValueFactory<>("idGenerico"));

            timeG.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(Timestamp item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        Instant instant = Instant.ofEpochSecond(item.getSeconds(), item.getNanos());
                        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
                        ZonedDateTime zonedDateTimeChile = zonedDateTime.withZoneSameInstant(Repository.getZoneID());
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                        String formattedDateTime = zonedDateTimeChile.format(formatter);
                        setText(formattedDateTime);
                    }
                }
            });

            priceTradeG.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        MarketDataMessage.TradeGeneral data = getTableRow().getItem();

                        if (data == null) {
                            return;
                        }

                        BigDecimal tick = Ticks.conversorExdestination(data.getSecurityExchange(), BigDecimal.valueOf(item));
                        DecimalFormat decimalFormat = NumberGenerator.formetByticks(tick);
                        setText(decimalFormat.format(item));
                    }
                }
            });

            qtyTradeG.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(Double item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        MarketDataMessage.TradeGeneral data = getTableRow().getItem();

                        if (data == null) {
                            return;
                        }

                        DecimalFormat decimalFormat = NumberGenerator.getFormatNumberMilDec(data.getSecurityExchange());
                        setText(decimalFormat.format(item));
                    }
                }
            });

            buyerG.setCellValueFactory(new PropertyValueFactory<>("buyer"));
            selleG.setCellValueFactory(new PropertyValueFactory<>("seller"));
            symboltradesG.setCellValueFactory(new PropertyValueFactory<>("symbol"));
            priceTradeG.setCellValueFactory(new PropertyValueFactory<>("price"));
            qtyTradeG.setCellValueFactory(new PropertyValueFactory<>("qty"));
            timeG.setCellValueFactory(new PropertyValueFactory<>("t"));

            buyer.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        if (allbrokercode.containsKey(item)) {

                            getStyleClass().clear();

                            setText(allbrokercode.get(item));

                            if (item.equals("041")) {
                                getStyleClass().add("vc");
                                marketDataTradeTable.refresh();
                            } else {
                                getStyleClass().add("notvc");
                            }

                        } else {
                            setText(item);
                        }
                    }
                }
            });

            buyerG.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        getStyleClass().clear();
                        if (allbrokercode.containsKey(item)) {
                            setText(allbrokercode.get(item));

                            if (item.equals("041")) {
                                getStyleClass().add("vc");
                                marketDataTradeTableG.refresh();
                            } else {
                                getStyleClass().add("notvc");

                            }
                        } else {
                            setText(item);
                        }
                    }
                }
            });

            seller.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        if (allbrokercode.containsKey(item)) {
                            setText(allbrokercode.get(item));

                            getStyleClass().clear();

                            if (item.equals("041")) {
                                getStyleClass().add("vc");
                                marketDataTradeTable.refresh();
                            } else {
                                getStyleClass().add("notvc");
                            }
                        } else {
                            setText(item);
                        }
                    }
                }
            });

            selleG.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(String item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        if (allbrokercode.containsKey(item)) {
                            setText(allbrokercode.get(item));
                        } else {
                            setText(item);
                            getStyleClass().clear();
                            if (item.equals("041")) {
                                getStyleClass().add("vc");
                                marketDataTradeTableG.refresh();
                            } else {
                                getStyleClass().add("notvc");
                            }
                        }
                    }
                }
            });

            tabpanel.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
                if (newValue != null) {

                    if (newValue.getText().contains("Pre")) {
                        BlotterMessage.PreselectRequest reques = BlotterMessage.PreselectRequest.newBuilder()
                                .setStatusPreselect(BlotterMessage.StatusPreselect.SNAPSHOT_PRESELECT)
                                .setUsername(Repository.username).build();
                        Repository.getClientService().sendMessage(reques);
                    }
                }
            });


            offerViewTable.setOnMouseClicked(event -> {
                try {

                    OrderBookEntry value = offerViewTable.getSelectionModel().getSelectedItem();

                    if (value != null) {
                        lanzadorController.getPriceOrder().setText(String.valueOf(value.getPrice()));
                        lanzadorController.getQuantity().setText(String.valueOf(value.getSize()));
                        lanzadorController.getSideOrder().getSelectionModel().select(ProtoConverter.routingDecryptStatus(RoutingMessage.Side.BUY.name()));
                        lanzadorController.getSecExchOrder().getSelectionModel().select(value.getSecurityExchangeRouting());
                        lanzadorController.getIceberg().setText("");

                    } else {
                        log.warn("Selected item is null.");
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });

            bidViewTable.setOnMouseClicked(event -> {
                try {

                    OrderBookEntry value = bidViewTable.getSelectionModel().getSelectedItem();

                    if (value != null) {
                        lanzadorController.getPriceOrder().setText(String.valueOf(value.getPrice()));
                        lanzadorController.getQuantity().setText(String.valueOf(value.getSize()));
                        lanzadorController.getSideOrder().getSelectionModel().select(ProtoConverter.routingDecryptStatus(RoutingMessage.Side.SELL.name()));
                        lanzadorController.getSecExchOrder().getSelectionModel().select(value.getSecurityExchangeRouting());
                        lanzadorController.getIceberg().setText("");

                    } else {
                        log.warn("Selected item is null.");
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });

            hideTableHeader(bidViewTable);
            hideTableHeader(offerViewTable);

            bidViewTable.refresh();
            offerViewTable.refresh();

            if (Repository.getIsLight()) {
                isLight();
            }

            positionsViewController.blockDestinos();

            this.sortedDataG = new SortedList<>(Repository.getTradeGenerales());
            sortedDataG.comparatorProperty().bind(marketDataTradeTableG.comparatorProperty());
            marketDataTradeTableG.setItems(sortedDataG);

            marketDataTradeTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
            marketDataTradeTableG.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

            this.marketDataTradeTableG.getSortOrder().add(this.timeG);

            marketDataTradeTableG.sort();

            marketDataTradeTable.getItems().addListener((ListChangeListener<MarketDataMessage.Trade>) change -> {
                while (change.next()) {
                    if (change.wasAdded()) {

                    }
                }
            });

            marketDataTradeTableG.getItems().addListener((ListChangeListener<MarketDataMessage.TradeGeneral>) change -> {
                while (change.next()) {
                    if (change.wasAdded()) {
                        titelpaneMarketDataMercadoGeneral.setText("Últimas Operaciones (" + marketDataTradeTableG.getItems().size() + ")");
                        titelpaneMarketData.setText("Últimas Operaciones Nemo (" + marketDataTradeTable.getItems().size() + ")");
                    }
                }
            });


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    @FXML
    public void bestAction2(ActionEvent actionEvent) {
        lanzadorController.bestActions(actionEvent);
    }

    @FXML
    public void hitAction2(ActionEvent actionEvent) {
        lanzadorController.hitActions(actionEvent);
    }

    private void hideVerticalScrollBar(TableView<?> table) {
        table.widthProperty().addListener((source, oldWidth, newWidth) -> {
            Platform.runLater(() -> {
                for (Node node : table.lookupAll(".scroll-bar")) {
                    if (node instanceof ScrollBar sb && sb.getOrientation() == Orientation.VERTICAL) {
                        sb.setVisible(false);
                        sb.setManaged(false);
                        sb.setOpacity(0);
                        sb.setPrefWidth(0); // Forzar ancho cero
                        sb.setMinWidth(0);
                        sb.setMaxWidth(0);
                    }
                }
            });
        });

        // Ajustar el TableView para no reservar espacio
        table.setStyle("-fx-background-color: transparent; -fx-control-inner-background: transparent; -fx-padding: 0; -fx-spacing: 0;");
        table.setMaxWidth(Double.MAX_VALUE);
        table.setPrefWidth(240); // Igualar al ancho de vboxBooksOffer
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        // Reaplicar ocultación en cambios de skin
        table.skinProperty().addListener((obs, oldSkin, newSkin) -> {
            if (newSkin != null) {
                Platform.runLater(() -> {
                    for (Node node : table.lookupAll(".scroll-bar")) {
                        if (node instanceof ScrollBar sb && sb.getOrientation() == Orientation.VERTICAL) {
                            sb.setVisible(false);
                            sb.setManaged(false);
                            sb.setOpacity(0);
                            sb.setPrefWidth(0);
                            sb.setMinWidth(0);
                            sb.setMaxWidth(0);
                        }
                    }
                });
            }
        });
    }

    private void applyHeaderVisibility(TableView<?> table, boolean visible) {
        if (table == null) return;

        // Limpia cualquier rastro de ocultamiento por id/clase
        table.setId(null);
        table.getStyleClass().remove("hide-table-header");

        Runnable run = () -> {
            Region header = (Region) table.lookup("TableHeaderRow");
            if (header != null) {
                header.setVisible(visible);
                header.setManaged(visible);
                if (visible) {
                    header.setMinHeight(Region.USE_COMPUTED_SIZE);
                    header.setPrefHeight(Region.USE_COMPUTED_SIZE);
                    header.setMaxHeight(Region.USE_COMPUTED_SIZE);
                } else {
                    header.setMinHeight(0);
                    header.setPrefHeight(0);
                    header.setMaxHeight(0);
                }
            }
        };

        // Aplica ahora y cada vez que se recrea el skin
        Platform.runLater(run);
        table.skinProperty().addListener((obs, oldSkin, newSkin) -> {
            if (newSkin != null) Platform.runLater(run);
        });
    }

    private void hideTableHeader(TableView<?> table) {
        applyHeaderVisibility(table, false);
    }

    private void showTableHeader(TableView<?> table) {
        applyHeaderVisibility(table, true);
    }

    public void isLight() {
        try {
            isLightMode = true;

            if (bidViewTable != null) {
                bidViewTable.setId(null); // quita "tableviewWithoutHead"
                bidViewTable.getStyleClass().remove("hide-table-header");
                Platform.runLater(() -> {
                    Pane header = (Pane) bidViewTable.lookup("TableHeaderRow");
                    if (header != null) {
                        header.setVisible(true);
                        header.setManaged(true);
                    }
                });
            }
            if (offerViewTable != null) {
                offerViewTable.setId(null);
                offerViewTable.getStyleClass().remove("hide-table-header");
                Platform.runLater(() -> {
                    Pane header = (Pane) offerViewTable.lookup("TableHeaderRow");
                    if (header != null) {
                        header.setVisible(true);
                        header.setManaged(true);
                    }
                });
            }

            lanzadorController.getLabelBroker().setVisible(false);
            lanzadorController.getLabelBroker().setManaged(false);

            lanzadorController.getPane9().setVisible(false);
            lanzadorController.getPane9().setManaged(false);

            lanzadorController.getPane8().setVisible(false);
            lanzadorController.getPane8().setManaged(false);

            lanzadorController.getBrokerOrder().setVisible(false);
            lanzadorController.getBrokerOrder().setManaged(false);

            lanzadorController.getVboxTmoneda().setVisible(false);
            lanzadorController.getVboxTmoneda().setManaged(false);

            if (Repository.isPremiumLiquidationUser()) {
                lanzadorController.getVboxLiquidacion().setVisible(true);
                lanzadorController.getVboxLiquidacion().setManaged(true);
                lanzadorController.getSettltypeOrder().setDisable(false);
                log.info("[LIQUIDACION][PREMIUM] user={} mode=light scope=marketDataViewer action=enable", Repository.getUsername());
            } else {
                lanzadorController.getVboxLiquidacion().setVisible(false);
                lanzadorController.getVboxLiquidacion().setManaged(false);
            }

            lanzadorController.getVboxSecurityType().setVisible(false);
            lanzadorController.getVboxSecurityType().setManaged(false);

            lanzadorController.getVboxEstrategia().setVisible(false);
            lanzadorController.getVboxEstrategia().setManaged(false);

            lanzadorController.getVboxCodOperador().setVisible(false);
            lanzadorController.getVboxCodOperador().setManaged(false);

            lanzadorController.getVboxTipo().setVisible(false);
            lanzadorController.getVboxTipo().setManaged(false);

            lanzadorController.getVboxSpread().setVisible(false);
            lanzadorController.getVboxSpread().setManaged(false);

            lanzadorController.getVboxLimit().setVisible(false);
            lanzadorController.getVboxLimit().setManaged(false);

            lanzadorController.getVboxSecExc().setVisible(false);
            lanzadorController.getVboxSecExc().setManaged(false);

            lanzadorController.getVboxHandInst().setVisible(false);
            lanzadorController.getVboxHandInst().setManaged(false);

            lanzadorController.getVboxTIF().setVisible(false);
            lanzadorController.getVboxTIF().setManaged(false);

            lanzadorController.getVboxOPCI().setVisible(false);
            lanzadorController.getVboxOPCI().setManaged(false);

            lanzadorController.getVboxBroker().setVisible(false);
            lanzadorController.getVboxBroker().setManaged(false);


            lanzadorController.getPaste().setVisible(false);
            lanzadorController.getPaste().setManaged(false);


            Splid.setDividerPositions(0.35);


            lanzadorController.getAnchopane().setPrefWidth(400);
            lanzadorController.getAnchopane().setPrefHeight(280);

            lanzadorController.getTitelpanelLauncher().setPrefWidth(400);
            lanzadorController.getTitelpanelLauncher().setPrefHeight(280);

            lanzadorController.getMyHBox().setPrefWidth(300);
            lanzadorController.getVboxNemo().setPrefWidth(400);

            lanzadorController.getRouteOrder().setPrefWidth(75);
            lanzadorController.getBestR().setPrefWidth(60);
            lanzadorController.getHitR().setPrefWidth(60);

            lanzadorController.getClean().setPrefWidth(80);
            lanzadorController.getAddpreselect().setPrefWidth(98);


            GridPane.setRowIndex(lanzadorController.getVboxCash(), 2);
            GridPane.setColumnIndex(lanzadorController.getVboxCash(), 2);

            lanzadorController.getGpLauncher().setAlignment(Pos.TOP_LEFT);
            for (Node node : lanzadorController.getGpLauncher().getChildren()) {
                if (node instanceof VBox vbox) {
                    vbox.setAlignment(Pos.TOP_LEFT);
                }
            }

            setConfByuser(false);

            Platform.runLater(() -> {
                showTableHeader(bidViewTable);
                showTableHeader(offerViewTable);
            });

            tabpanel.setSide(Side.TOP);


            lanzadorController.getTypeOrder().getSelectionModel().select(RoutingMessage.OrdType.LIMIT);
            lanzadorController.getTifOrder().getSelectionModel().select(RoutingMessage.Tif.DAY);
            lanzadorController.getBrokerOrder().getSelectionModel().select(RoutingMessage.ExecBroker.VC);
            lanzadorController.getCurrency().getSelectionModel().select(RoutingMessage.Currency.CLP);
            lanzadorController.getSecExchOrder().getSelectionModel().select(RoutingMessage.SecurityExchangeRouting.XSGO);

            ObservableList<RoutingMessage.SettlType> filteredSettlTypes = FXCollections.observableArrayList(
                    Arrays.stream(RoutingMessage.SettlType.values())
                            .filter(type -> !type.equals(RoutingMessage.SettlType.REGULAR) &&
                                    !type.equals(RoutingMessage.SettlType.T3) &&
                                    !type.equals(RoutingMessage.SettlType.T5))
                            .toArray(RoutingMessage.SettlType[]::new)
            );

            lanzadorController.getSettltypeOrder().setItems(filteredSettlTypes);
            lanzadorController.getSettltypeOrder().getSelectionModel().select(RoutingMessage.SettlType.T2);

            lanzadorController.getPaste().setVisible(false);
            lanzadorController.getPaste().setManaged(false);

            lanzadorController.getVboxNemo().setPrefWidth(390);

            if (lanzadorController.getCOperador().getItems().size() > 1) {
                lanzadorController.getCOperador().setVisible(true);
                lanzadorController.getCOperador().setManaged(true);
                lanzadorController.getCOperadorlb().setVisible(true);
                lanzadorController.getCOperadorlb().setManaged(true);
            } else {
                lanzadorController.getCOperador().setVisible(false);
                lanzadorController.getCOperador().setManaged(false);
                lanzadorController.getCOperadorlb().setVisible(false);
                lanzadorController.getCOperadorlb().setManaged(false);
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void syncOfferHeaderRotation() {
        Runnable r = () -> {
            Region header = (Region) offerViewTable.lookup("TableHeaderRow");
            if (header != null) {
                // Si la tabla está a 180, la cabecera también a 180 (compensa y queda derecha).
                header.setRotate(offerViewTable.getRotate());
            }
        };
        // Ejecuta ahora y cada vez que cambie el skin / tamaño / items
        Platform.runLater(r);
        offerViewTable.skinProperty().addListener((obs, o, n) -> Platform.runLater(r));
        offerViewTable.widthProperty().addListener((obs, ov, nv) -> Platform.runLater(r));
        offerViewTable.itemsProperty().addListener((obs, o, n) -> Platform.runLater(r));
    }

    private void relaxMKDWidthForHorizontal() {

        double totalWidth = 482;
        hboxMKD.setMinWidth(totalWidth);
        hboxMKD.setPrefWidth(totalWidth);
        hboxMKD.setMaxWidth(Region.USE_PREF_SIZE);
        HBox.setHgrow(hboxMKD, Priority.NEVER);
        VBox.setVgrow(hboxMKD, Priority.NEVER);


        if (hboxMKD.getParent() instanceof AnchorPane ap) {
            AnchorPane.setLeftAnchor(hboxMKD, 0.0);
            AnchorPane.setRightAnchor(hboxMKD, null);
        }

        double bidW = totalWidth / 2.0;
        double offW = totalWidth / 2.0;

        bidViewTable.setMinWidth(bidW);
        bidViewTable.setPrefWidth(bidW);
        bidViewTable.setMaxWidth(Region.USE_PREF_SIZE);
        HBox.setHgrow(bidViewTable, Priority.NEVER);

        offerViewTable.setMinWidth(offW);
        offerViewTable.setPrefWidth(offW);
        offerViewTable.setMaxWidth(Region.USE_PREF_SIZE);
        HBox.setHgrow(offerViewTable, Priority.NEVER);
    }

    public void setConfByuser(boolean isVertical) {
        this.isVertical = isVertical;

        if (bidViewTable == null || offerViewTable == null) {
            bidViewTable = new TableView<>();
            quantityBid = new TableColumn<>("Cantidad");
            priceBid = new TableColumn<>("Compra");

            offerViewTable = new TableView<>();
            priceOffer = new TableColumn<>("Venta");
            quantityOffer = new TableColumn<>("Cantidad");

            bidViewTable.getColumns().addAll(priceBid, quantityBid);
            offerViewTable.getColumns().setAll(priceOffer, quantityOffer);
        }

        if (!bidViewTable.getStyleClass().contains("book-table")) bidViewTable.getStyleClass().add("book-table");
        if (!offerViewTable.getStyleClass().contains("book-table")) offerViewTable.getStyleClass().add("book-table");

        if (isVertical) {
            bidViewTable.setId("tableviewWithoutHead");

            hideVerticalScrollBar(bidViewTable);

            bidViewTable.setMaxWidth(Double.MAX_VALUE);
            bidViewTable.setPrefWidth(240);

            vboxBooksBid = new VBox(bidViewTable);
            vboxBooksBid.setMaxWidth(240);
            vboxBooksBid.setPrefWidth(240);
            vboxBooksBid.setPadding(new Insets(0));

            vboxBooksOffer = new VBox(offerViewTable);
            vboxBooksOffer.setMaxWidth(240);

            vboxBooks = new VBox(vboxBooksOffer, vboxBooksBid);
            hboxMKD.getChildren().setAll(vboxBooks);

        } else {
            bidViewTable.setId(null);
            offerViewTable.setId(null);
            bidViewTable.getStyleClass().remove("hide-table-header");
            offerViewTable.getStyleClass().remove("hide-table-header");

            relaxMKDWidthForHorizontal();

            if (Repository.getIsLight()) {
                TitledPane titledPane = new TitledPane();
                titledPane.setText("Libro de mercado");
                titledPane.setId("libroDeMercadoPane");
                titledPane.setCollapsible(false);

                HBox content = new HBox(bidViewTable, offerViewTable);
                content.setSpacing(0);
                content.setPadding(new Insets(0));
                content.setFillHeight(true);

                HBox.setHgrow(bidViewTable, Priority.ALWAYS);
                HBox.setHgrow(offerViewTable, Priority.ALWAYS);
                bidViewTable.setMaxWidth(Double.MAX_VALUE);
                offerViewTable.setMaxWidth(Double.MAX_VALUE);
                bidViewTable.setMinWidth(220);
                offerViewTable.setMinWidth(240);

                titledPane.setContent(content);
                titledPane.setMaxWidth(Double.MAX_VALUE);
                HBox.setHgrow(titledPane, Priority.ALWAYS);

                hboxMKD.getChildren().setAll(titledPane);

            } else {
                HBox hBox = new HBox(bidViewTable, offerViewTable);
                hBox.setSpacing(0);
                hBox.setPadding(new Insets(0));
                hBox.setFillHeight(true);

                HBox.setHgrow(bidViewTable, Priority.ALWAYS);
                HBox.setHgrow(offerViewTable, Priority.ALWAYS);
                bidViewTable.setMaxWidth(Double.MAX_VALUE);
                offerViewTable.setMaxWidth(Double.MAX_VALUE);
                bidViewTable.setMinWidth(220);
                offerViewTable.setMinWidth(240);

                hboxMKD.getChildren().setAll(hBox);
            }
        }
    }

    public void setLauncherFromHistorical(BlotterMessage.PositionHistory value) {

        Platform.runLater(() -> {

            lanzadorController.getTicket().setText(value.getInstrument());
            lanzadorController.getQuantity().setText(String.valueOf(value.getAvailableQuantity()));

            Repository.getPrincipalController().getSubscribe();

            if (suggestionsPopup != null && suggestionsPopup.isShowing()) {
                suggestionsPopup.hide();
            }
            String id = value.getInstrument() + "BCST2CS";

            if (Repository.getBookPortMaps().containsKey(id)) {
                BookVO symbolBook = Repository.getBookPortMaps().get(id);
                if (!symbolBook.getBidBook().isEmpty()) {
                    OrderBookEntry data = symbolBook.getBidBook().get(0);
                    lanzadorController.getPriceOrder().setText(String.valueOf(data.getPrice()));
                } else {
                    lanzadorController.getPriceOrder().setText("0.0");
                }
            }
        });
    }
}

