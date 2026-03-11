package cl.vc.blotter.controller;

import akka.actor.ActorRef;
import cl.vc.blotter.controller.PreDigitadosController;
import cl.vc.blotter.Repository;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.model.StatisticVO;
import cl.vc.blotter.utils.Notifier;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.SortedList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Point2D;
import javafx.geometry.Pos;
import javafx.geometry.Side;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.TransferMode;
import javafx.scene.layout.*;
import javafx.stage.Modality;
import javafx.stage.Stage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Data
@Slf4j
public class PrincipalController {

    @FXML
    private RoutingController routingViewController;

    @FXML
    private PreDigitadosController preselectOrdersController;


    @FXML private FooterController footerController;

    @FXML
    private LanzadorController lanzadorController;

    @FXML
    private TabPane TabPaneLanzador;

    @FXML
    private BookHorizontalController tableViewBookHController;

    @FXML
    private BookVerticalController tableViewBookVController;

    @FXML
    private Region tableViewBookH;

    @FXML
    private TabPane tabPaneBook;

    @FXML
    private Region tableViewBookV;

    @FXML
    private PositionsController positionsViewController;

    @FXML
    private MarketDataPortfolioViewController marketDataPortfolioViewController;

    @FXML
    private TradeMKDController tradeController;

    @FXML
    private TradeGeneralesController tradeGeneralesController;

    @FXML
    private Tab tabGenerales;

    @FXML
    private Tab tabTrade;

    private MarketDataMessage.Subscribe.Builder subscribe = MarketDataMessage.Subscribe.newBuilder();

    private MarketDataMessage.Unsubscribe.Builder unSubscribe = MarketDataMessage.Unsubscribe.newBuilder();


    private RoutingMessage.Order orderSelected;

    private static final Map<String, List> account = new HashMap<>();

    private boolean isDayMode = true;

    @FXML
    private MarketDataViewerController marketDataController;

    private HashMap<String, MarketDataPortfolioViewController> marketDataPortfolioViewControllers = new HashMap<>();

    @FXML
    private TabPane tpMkData;

    @FXML
    private Tab add;

    private String subscribeBook = "";

    @FXML
    private AnchorPane simultaneousView;
    @FXML
    private TabPane tpOrders;
    @FXML
    private Label lbSymbol;
    @FXML
    private ComboBox cmbTMCnx;
    @FXML
    private Button btnMonitor;
    @FXML
    private Button btnBasket;
    @FXML
    private Button btnEnviroment;
    @FXML
    private HBox hbRight;
    @FXML
    private Slider fontSlider;
    @FXML
    private HBox box;
    @FXML
    private TabPane tabPrincipa;
    @FXML
    private VBox mainVbox;
    @FXML
    private Tab applauncher;
    @FXML
    private Tab mkd;
    @FXML
    private Tab basket;
    @FXML
    private Tab positions;
    @FXML
    private AnchorPane principal;
    @FXML
    private ToolBar toolBarContainer;
    @FXML
    private HBox hoverArea;
    private ComboBox<String> cmbAccount;
    private Stage simultaneousStage = new Stage();
    private Alert a;
    private ObservableList<String> accountList;
    private BorderPane rootLayout;
    private Point2D dragAnchor;
    private Tab tabDragged;
    private String id = IDGenerator.getID();
    private boolean suppressAddDialog = false;

    @FXML private TabPane TabPaneTrade;
    @FXML private TabPane tabPaneTradeGeneral;
    @FXML private Tab tabPreDigitadas;
    private boolean isLightMode = false;
    private MarketDataPortfolioViewController floatingMDTable;

    @FXML
    private SplitPane splid;


    @FXML
    private void initialize() {
        try {
            Repository.setLanzadorController(lanzadorController);
            Repository.getControllerStageMap().put("principal", marketDataController);
            Repository.setPrincipalController(this);

            // ===== Baseline neutral para que HBox reparta 50/50 desde el primer frame =====
            // (asegura que ninguno "se coma" el ancho al inicio)
            TabPaneTrade.setMinWidth(0);
            tabPaneTradeGeneral.setMinWidth(0);
            TabPaneTrade.setPrefWidth(0);
            tabPaneTradeGeneral.setPrefWidth(0);
            HBox.setHgrow(TabPaneTrade, Priority.ALWAYS);
            HBox.setHgrow(tabPaneTradeGeneral, Priority.ALWAYS);
            // =================================================================================

            // ==== LAYOUT PRE-DIGITADOS: listeners ====
            tabPaneTradeGeneral.getSelectionModel().selectedItemProperty().addListener((obs, oldTab, newTab) -> {
                applyPreDigitadosLayout();
            });

            tabPreDigitadas.selectedProperty().addListener((obs, was, isSel) -> {
                applyPreDigitadosLayout();
            });

            // Forzar estado correcto apenas termine de crear la UI
            Platform.runLater(() -> {
                applyPreDigitadosLayout();
                Region parent = (Region) TabPaneTrade.getParent();
                if (parent != null) parent.requestLayout();
            });

            // Y también cuando la escena/ventana ya exista (al montarse el Stage)
            principal.sceneProperty().addListener((obs, oldScene, newScene) -> {
                if (newScene != null) {
                    newScene.windowProperty().addListener((o, ow, w) -> {
                        if (w != null) {
                            Platform.runLater(() -> {
                                applyPreDigitadosLayout();
                                Region parent = (Region) TabPaneTrade.getParent();
                                if (parent != null) parent.requestLayout();
                            });
                        }
                    });
                }
            });
            // ==== FIN LAYOUT PRE-DIGITADOS ====

            tabPrincipa.setOnDragOver(event -> {
                if (event.getGestureSource() != tabPrincipa && event.getDragboard().hasString()) {
                    event.acceptTransferModes(TransferMode.MOVE);
                    event.consume();
                }
            });

            tabPrincipa.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
                if (newValue != null) {
                    Tab selectedTab = (Tab) newValue;
                    if (selectedTab.getText().equals("Launcher")) {
                        BlotterMessage.PreselectRequest reques = BlotterMessage.PreselectRequest.newBuilder()
                                .setStatusPreselect(BlotterMessage.StatusPreselect.SNAPSHOT_PRESELECT)
                                .setUsername(Repository.username).build();
                        Repository.getClientService().sendMessage(reques);
                    }
                }
            });

            Repository.setMarketDataController(marketDataController);
            requestPortfolio();

            lanzadorController.getSettltypeOrder().setOnAction(event -> {
                subscribeSymbol();
            });

            tpMkData.getSelectionModel().selectedItemProperty().addListener((obs, oldTab, newTab) -> {
                if (newTab == null || !"+".equals(newTab.getText())) return;
                runFx(() -> {
                    Alert alert = new Alert(Alert.AlertType.INFORMATION);
                    alert.setTitle("Nuevo Portfolio");
                    alert.setHeaderText("Agregar nuevo portafolio");

                    TextInputDialog dialog = new TextInputDialog();
                    dialog.setTitle("Nuevo Portafolio");
                    dialog.setHeaderText("Agregar nuevo portafolio");
                    dialog.setContentText("Ingrese el nombre del portafolio:");

                    DialogPane dp = dialog.getDialogPane();
                    var css = getClass().getResource(Repository.getSTYLE());
                    if (css != null) dp.getStylesheets().add(css.toExternalForm());

                    dialog.showAndWait().ifPresent(portfolioName -> {
                        for (Tab t : tpMkData.getTabs()) {
                            if (portfolioName.equals(t.getText())) {
                                Notifier.INSTANCE.notifyError("Error", "El nombre del portafolio ya existe");
                                return;
                            }
                        }
                        Repository.getClientService().sendMessage(BlotterMessage.PortfolioRequest.newBuilder()
                                .setStatusPortfolio(BlotterMessage.StatusPortfolio.NEW_PORTFOLIO)
                                .setUsername(Repository.username)
                                .setMarketdataControllerId(id)
                                .setNamePortfolio(portfolioName)
                                .build());
                    });
                });
            });


            SessionsMessage.Connect connect = SessionsMessage.Connect.newBuilder().setUsername(Repository.getUsername()).build();
            Repository.getClientService().sendMessage(connect);

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            Runnable gcTask = System::gc;
            scheduler.scheduleAtFixedRate(gcTask, 0, 10, TimeUnit.SECONDS);

            Repository.getFormatter0dec().setDecimalFormatSymbols(DecimalFormatSymbols.getInstance(Locale.US));

            if (Repository.isDayMode()) {
                setDayMode();
            } else {
                setNightMode();
            }

            preselectOrdersController.getTableExecutionReports()
                    .getSelectionModel()
                    .selectedItemProperty()
                    .addListener((obs, oldV, newV) -> {
                        if (newV != null) {
                            try {
                                lanzadorController.presele(newV);
                                Repository.getRoutingController().getHit().setDisable(false);
                                Repository.getRoutingController().getBest().setDisable(false);
                                subscribeSymbol();
                                lanzadorController.getRouteOrder().setDisable(false);
                                Repository.getRoutingController().getReplaceOrder().setDisable(true);
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                            }
                        }
                    });

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void applyPreDigitadosLayout() {
        if (tabPaneTradeGeneral == null || TabPaneTrade == null || tabPreDigitadas == null) return;

        boolean showingPre = (tabPaneTradeGeneral.getSelectionModel().getSelectedItem() == tabPreDigitadas);

        // Nunca estorben los min/pref al repartir
        TabPaneTrade.setMinWidth(0);
        tabPaneTradeGeneral.setMinWidth(0);

        if (showingPre) {
            // Ocultamos Trade completamente
            TabPaneTrade.setVisible(false);
            TabPaneTrade.setManaged(false);

            // Dejamos TradeGeneral crecer a todo el ancho disponible
            HBox.setHgrow(tabPaneTradeGeneral, Priority.ALWAYS);
            HBox.setHgrow(TabPaneTrade, Priority.NEVER);

            tabPaneTradeGeneral.setPrefWidth(Region.USE_COMPUTED_SIZE);
            tabPaneTradeGeneral.setMaxWidth(Double.MAX_VALUE);

            TabPaneTrade.setPrefWidth(0);
            TabPaneTrade.setMaxWidth(Region.USE_COMPUTED_SIZE);
        } else {
            // Mostrar ambos y repartir equitativamente desde el arranque
            TabPaneTrade.setVisible(true);
            TabPaneTrade.setManaged(true);

            HBox.setHgrow(TabPaneTrade, Priority.ALWAYS);
            HBox.setHgrow(tabPaneTradeGeneral, Priority.ALWAYS);

            // Pref=0 + min=0 permite que el HBox reparta 50/50 de forma natural
            TabPaneTrade.setPrefWidth(0);
            tabPaneTradeGeneral.setPrefWidth(0);
            TabPaneTrade.setMaxWidth(Double.MAX_VALUE);
            tabPaneTradeGeneral.setMaxWidth(Double.MAX_VALUE);
        }

        // El contenido de Pre-Digitados no debe ocupar espacio si no está seleccionado
        var content = tabPreDigitadas.getContent();
        if (content != null) {
            boolean isSel = tabPreDigitadas.isSelected();
            content.setVisible(isSel);
            content.setManaged(isSel);
        }

        // Forzar un ciclo de layout del contenedor
        Region parent = (Region) TabPaneTrade.getParent();
        if (parent != null) parent.requestLayout();
    }

    public void setDayMode() {

        URL dayModeUrl = getClass().getResource("/blotter/css/daymode.css");
        if (dayModeUrl == null) {
            log.error("No se pudo cargar el archivo CSS para el modo día.");
        } else {
            Repository.getPrincipalController().getPrincipal().getStylesheets().clear();
            Repository.getPrincipalController().getPrincipal().getStylesheets().add(dayModeUrl.toExternalForm());
            Image image = new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/moon.png")));
            ImageView imageView = new ImageView(image);
            imageView.setFitWidth(30);
            imageView.setFitHeight(30);
            Repository.getFooterController().getModo().setGraphic(imageView);
            isDayMode = true;
        }
    }

    public void setNightMode() {
        URL nightModeUrl = getClass().getResource(Repository.getSTYLE());
        if (nightModeUrl == null) {
            log.error("No se pudo cargar el archivo CSS para el modo noche.");
        } else {
            Repository.getPrincipalController().getPrincipal().getStylesheets().clear();
            Repository.getPrincipalController().getPrincipal().getStylesheets().add(nightModeUrl.toExternalForm());
            Image image = new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/sun.png")));
            ImageView imageView = new ImageView(image);
            imageView.setFitWidth(30);
            imageView.setFitHeight(30);
            Repository.getFooterController().getModo().setGraphic(imageView);
            isDayMode = false;
        }
    }

    public void requestPortfolio() {
        try {
            Repository.getClientService().sendMessage(BlotterMessage.PortfolioRequest.newBuilder()
                    .setStatusPortfolio(BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO)
                    .setMarketdataControllerId(IDGenerator.getID())
                    .setUsername(Repository.username).build());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void abrirTradeGeneral(ActionEvent actionEvent) {

        try {

            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/TradeGenerales.fxml"));
            AnchorPane root = loader.load();
            TradeGeneralesController tradeGeneralesController1 = loader.getController();


            SortedList<MarketDataMessage.TradeGeneral> sortedData = new SortedList<>(Repository.getTradeGenerales());
            sortedData.comparatorProperty().bind(tradeGeneralesController1.getMarketDataTradeTable().comparatorProperty());
            tradeGeneralesController1.getMarketDataTradeTable().setItems(sortedData);
            tradeGeneralesController1.getMarketDataTradeTable().refresh();


            URL cssResource = getClass().getResource(Repository.getSTYLE());
            Scene scene = new Scene(root);
            scene.getStylesheets().add(cssResource.toExternalForm());
            Stage stage = new Stage();
            stage.setTitle("Trade General");
            stage.setScene(scene);
            stage.initModality(Modality.NONE);
            stage.show();

        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }


    }

    public void abrirTradeMKD(ActionEvent actionEvent) {
        try {
            String id = TopicGenerator.getTopicMKD(Repository.getLastSelectedStatistic());
            BookVO bookVO = Repository.getBookPortMaps().get(id);

            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/TradeMkd.fxml"));
            AnchorPane root = loader.load();

            TradeMKDController ctrl = loader.getController();

            // ✅ CLAVE: usar el binder del controller (para que el sort funcione siempre)
            ctrl.bindTrades(bookVO.getTradesVO());
            ctrl.sortByTimeDesc(); // opcional, pero recomendado

            URL cssResource = getClass().getResource(Repository.getSTYLE());
            Scene scene = new Scene(root);
            if (cssResource != null) scene.getStylesheets().add(cssResource.toExternalForm());

            Stage stage = new Stage();
            stage.setTitle("Trade " + bookVO.getSymbol());
            stage.setScene(scene);
            stage.initModality(Modality.NONE);
            stage.show();

        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }


    public void abrirBoook(ActionEvent actionEvent) {


        try {

            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/BookVerticalView.fxml"));
            AnchorPane root = loader.load();
            root.setMaxWidth(360);

            BookVerticalController bookControler = loader.getController();


            String id = TopicGenerator.getTopicMKD(Repository.getLastSelectedStatistic());
            BookVO bookVO = Repository.getBookPortMaps().get(id);

            bookControler.getBidViewTable().setItems(bookVO.getBidBook());
            bookControler.getOfferViewTable().setItems(bookVO.getAskBook());

            URL cssResource = getClass().getResource(Repository.getSTYLE());
            Scene scene = new Scene(root);
            assert cssResource != null;
            scene.getStylesheets().add(cssResource.toExternalForm());
            Stage stage = new Stage();
            stage.setMaxWidth(265);
            stage.setMinWidth(265);
            stage.setTitle("Book " + bookVO.getSymbol());
            stage.setScene(scene);
            stage.initModality(Modality.NONE);
            stage.show();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void subscribeSymbol() {

        try {

            if (lanzadorController.getTicket().getText().isEmpty() || lanzadorController.getSecExchOrder().getSelectionModel().getSelectedItem() == null || lanzadorController.getTicket().getText().isEmpty()) {
                return;
            }

            RoutingMessage.SecurityType selectedSecurityType = lanzadorController.getSecurityType().getSelectionModel().getSelectedItem();
            if (selectedSecurityType == null) {
                log.error("No security type selected.");
                return;
            }

            RoutingMessage.SecurityType securityType1 = selectedSecurityType.equals(RoutingMessage.SecurityType.CFI)
                    ? RoutingMessage.SecurityType.CS : selectedSecurityType;

            MarketDataMessage.Subscribe.Builder subscribeAux = MarketDataMessage.Subscribe.newBuilder();

            if (orderSelected != null) {

                subscribeAux.setSettlType(orderSelected.getSettlType())
                        .setSecurityType(orderSelected.getSecurityType())
                        .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                        .setBook(true)
                        .setStatistic(true)
                        .setTrade(true)
                        .setSymbol(orderSelected.getSymbol())
                        .setSecurityExchange(IDGenerator.conversorExdestination(orderSelected.getSecurityExchange()));
            } else {

                RoutingMessage.SecurityExchangeRouting selectedSecurityExchange = lanzadorController.getSecExchOrder().getSelectionModel().getSelectedItem();
                RoutingMessage.SettlType selectedSettlType = lanzadorController.getSettltypeOrder().getSelectionModel().getSelectedItem();

                if (selectedSecurityExchange == null || selectedSettlType == null) {
                    log.error("No security exchange or settlement type selected.");
                    return;
                }

                MarketDataMessage.SecurityExchangeMarketData seMarketData = IDGenerator.conversorExdestination(selectedSecurityExchange);


                if (seMarketData == null) {
                    log.error("Security Exchange conversion failed for: {}", selectedSecurityExchange);
                    return;
                }

                subscribeAux.setSettlType(selectedSettlType)
                        .setSecurityType(securityType1)
                        .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                        .setBook(true)
                        .setStatistic(true)
                        .setTrade(true)
                        .setSymbol(lanzadorController.getTicket().getText())
                        .setSecurityExchange(seMarketData);
            }

            subscribeAux.setId(TopicGenerator.getTopicMKD(subscribeAux.build()));

            if (!subscribeAux.getId().equals(subscribe.getId())) {

                if (!subscribe.getId().isEmpty()) {
                    unSubscribe.setId(subscribe.getId());
                    Repository.unSuscripcion(unSubscribe.getId());
                }

                Repository.getPrincipalController().getTableViewBookVController().getOfferViewTable().setItems(FXCollections.observableArrayList());
                Repository.getPrincipalController().getTableViewBookVController().getBidViewTable().setItems(FXCollections.observableArrayList());

                Repository.getPrincipalController().getTableViewBookHController().getOfferViewTable().setItems(FXCollections.observableArrayList());
                Repository.getPrincipalController().getTableViewBookHController().getBidViewTable().setItems(FXCollections.observableArrayList());

                Repository.getPrincipalController().getTradeController().getMarketDataTradeTable().setItems(FXCollections.observableArrayList());

                subscribeBook = subscribeAux.getId();
                subscribe = subscribeAux.clone();

                Repository.createSuscripcion(subscribe.getSymbol(),
                        subscribe.getSecurityExchange(),
                        subscribe.getSettlType(),
                        subscribe.getSecurityType());

                bindCurrentBookSubscription();
            } else {
                // Si el usuario vuelve a seleccionar el mismo símbolo, asegurar el binding de tablas.
                bindCurrentBookSubscription();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void bindCurrentBookSubscription() {
        try {
            if (subscribe == null || subscribe.getId().isEmpty()) {
                return;
            }

            BookVO bo = Repository.getBookPortMaps().get(subscribe.getId());
            if (bo == null) {
                return;
            }

            Repository.getPrincipalController().getTableViewBookVController().getOfferViewTable().setItems(bo.getAskBook());
            Repository.getPrincipalController().getTableViewBookHController().getOfferViewTable().setItems(bo.getAskBook());

            Repository.getPrincipalController().getTableViewBookVController().getBidViewTable().setItems(bo.getBidBook());
            Repository.getPrincipalController().getTableViewBookHController().getBidViewTable().setItems(bo.getBidBook());

            Repository.getPrincipalController().getTradeController().getMarketDataTradeTable().setItems(bo.getTradesVO());

            Repository.getPrincipalController().getTabTrade().setText(
                    "Últimas Operaciones Nemo " + bo.getSymbol() + " (" + bo.getTradesVO().size() + ")"
            );
        } catch (Exception e) {
            log.error("Error vinculando book/trades de la suscripción actual", e);
        }
    }

    public void datosMercadoFlotante(ActionEvent actionEvent) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/MarketDataPortfolioView.fxml"));
            AnchorPane root = loader.load();
            MarketDataPortfolioViewController c = loader.getController();

            VBox vbox = (VBox) root.getChildren().get(0);
            Node topBar = vbox.getChildren().get(0);
            topBar.setVisible(false);
            topBar.setManaged(false);
            AnchorPane center = (AnchorPane) vbox.getChildren().get(1);
            Node news = vbox.getChildren().get(2);
            news.setVisible(false);
            news.setManaged(false);

            TableView<?> table = c.getMarketDataStatisticsTable();
            AnchorPane.setTopAnchor(table, 0.0);
            AnchorPane.setRightAnchor(table, 0.0);
            AnchorPane.setBottomAnchor(table, 0.0);
            AnchorPane.setLeftAnchor(table, 0.0);
            table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
            table.setPlaceholder(new Label("Cargando…"));


            c.setPortfolioName("Principal");
            c.getData().clear();

            MarketDataPortfolioViewController principalCtrl =
                    marketDataPortfolioViewControllers.get("Principal");

            if (principalCtrl != null) {
                // Copiamos lo que ya está en la pestaña Principal (sin re-suscribir)
                var snapshot = new ArrayList<>(principalCtrl.getData());
                for (StatisticVO svo : snapshot) {
                    try {
                        String topicId = TopicGenerator.getTopicMKD(svo.getStatistic());
                        BookVO book = Repository.getBookPortMaps().get(topicId);
                        if (book != null) {
                            c.addModelVo(book);
                        }
                    } catch (Exception ex) {
                        log.error("Error copiando símbolo al flotante", ex);
                    }
                }
            } else {
                log.warn("No se encontró el controller del portafolio 'Principal'; el flotante quedará vacío.");
            }

            Scene scene = new Scene(root);
            var css = getClass().getResource(Repository.getSTYLE());
            if (css != null) scene.getStylesheets().add(css.toExternalForm());

            Stage stage = new Stage();
            stage.setTitle("Datos de Mercado");
            stage.setScene(scene);
            stage.setWidth(1500);
            stage.setHeight(800);
            stage.centerOnScreen();


            this.floatingMDTable = c;
            stage.setOnCloseRequest(ev -> this.floatingMDTable = null);

            stage.show();

            if (!table.getItems().isEmpty()) {
                table.getSelectionModel().selectFirst();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }



    private void addToFloating(BookVO book) {
        if (floatingMDTable != null && book != null) {
            Platform.runLater(() -> floatingMDTable.addModelVo(book));
        }
    }

    private void removeFromFloating(String topicId) {
        if (floatingMDTable != null && topicId != null) {
            Platform.runLater(() -> {
                var data = new ArrayList<>(floatingMDTable.getData());
                data.stream()
                        .filter(svo -> TopicGenerator.getTopicMKD(svo.getStatistic()).equals(topicId))
                        .findFirst()
                        .ifPresent(floatingMDTable::removeModelVo);
            });
        }
    }

    public void addDatosDeMercado() {

        try {

            Platform.runLater(() -> {

                try {

                    AtomicReference<Tab> principal = new AtomicReference<>();

                    if (Repository.getPortfolioResponse().getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO)) {

                        List<BlotterMessage.Portfolio> orderedPortfolios =
                                new ArrayList<>(Repository.getPortfolioResponse().getPostfolioList());
                        orderedPortfolios.sort(Comparator
                                .comparingInt(this::portfolioOrderPriority)
                                .thenComparing(BlotterMessage.Portfolio::getNamePortfolio, String.CASE_INSENSITIVE_ORDER));

                        orderedPortfolios.forEach(s -> {

                            try {

                                Tab tab = new Tab();
                                tab.setText(s.getNamePortfolio());
                                tab.setId(s.getId());
                                FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/MarketDataPortfolioView.fxml"));
                                Pane root = loader.load();
                                MarketDataPortfolioViewController statisticsViewController = loader.getController();
                                statisticsViewController.setPortfolioName(s.getNamePortfolio());
                                statisticsViewController.setIdController(s.getId());
                                tab.setContent(root);

                                if (s.getNamePortfolio().equals("Principal")) {
                                    principal.set(tab);
                                }

                                marketDataPortfolioViewControllers.put(s.getNamePortfolio(), statisticsViewController);

                                tpMkData.getTabs().add(tab);

                                s.getAssetList().forEach(r -> {

                                    try {

                                        BookVO bookVO = Repository.createBook(r.getStatistic());
                                        if (bookVO != null) {
                                            statisticsViewController.addModelVo(bookVO);
                                            addToFloating(bookVO); // NUEVO
                                            Repository.createSuscripcion(
                                                    r.getStatistic().getSymbol(),
                                                    r.getStatistic().getSecurityExchange(),
                                                    r.getStatistic().getSettlType(),
                                                    r.getStatistic().getSecurityType()
                                            );
                                        }

                                    } catch (Exception e) {
                                        log.error(e.getMessage(), e);
                                    }
                                });

                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                            }
                        });

                        MarketDataPortfolioViewController marketDataPortfolioViewController =
                                Repository.getPrincipalController().getMarketDataPortfolioViewControllers()
                                        .get(Repository.getPortfolioResponse().getNamePortfolio());

                        if (marketDataPortfolioViewController != null) {

                            if (Repository.getBookPortMaps().containsKey(Repository.getPortfolioResponse().getAsset().getStatistic().getId())) {

                                BookVO statisticVO = Repository.getBookPortMaps().get(Repository.getPortfolioResponse().getAsset().getStatistic().getId());
                                marketDataPortfolioViewController.addModelVo(statisticVO);
                                marketDataPortfolioViewController.getMarketDataStatisticsTable().refresh();

                            } else {

                                BookVO bookVO = Repository.createBook(Repository.getPortfolioResponse().getAsset().getStatistic());
                                marketDataPortfolioViewController.addModelVo(bookVO);

                            }

                            Repository.createSuscripcion(
                                    Repository.getPortfolioResponse().getAsset().getStatistic().getSymbol(),
                                    Repository.getPortfolioResponse().getAsset().getStatistic().getSecurityExchange(),
                                    Repository.getPortfolioResponse().getAsset().getStatistic().getSettlType(),
                                    Repository.getPortfolioResponse().getAsset().getStatistic().getSecurityType()
                            );

                        }

                        Repository.getPrincipalController().getMarketDataPortfolioViewControllers()
                                .get("Principal").getMarketDataStatisticsTable().getSelectionModel().selectFirst();

                        StatisticVO statistic = Repository.getPrincipalController().getMarketDataPortfolioViewControllers()
                                .get("Principal").getMarketDataStatisticsTable().getSelectionModel().getSelectedItem();

                        Repository.getPrincipalController().getMarketDataPortfolioViewControllers()
                                .get("Principal").onClick(statistic);
                        Repository.getClientActor().tell(statistic, ActorRef.noSender());

                        tpMkData.getSelectionModel().select(principal.get());

                    } else if (Repository.getPortfolioResponse().getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.REMOVE_ASSET)) {

                        String id = TopicGenerator.getTopicMKD(Repository.getPortfolioResponse().getAsset().getStatistic());
                        MarketDataPortfolioViewController s =
                                Repository.getPrincipalController().getMarketDataPortfolioViewControllers()
                                        .get(Repository.getPortfolioResponse().getNamePortfolio());

                        if (s != null) {
                            List<StatisticVO> statistics = s.getData().stream()
                                    .filter(statisticss -> TopicGenerator.getTopicMKD(statisticss.getStatistic()).equals(id))
                                    .toList();

                            statistics.forEach(ss -> {
                                s.removeModelVo(ss);
                                removeFromFloating(id);
                                Repository.unSuscripcion(ss.getId());
                            });
                        }

                    } else if (Repository.getPortfolioResponse().getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.NEW_PORTFOLIO)) {

                        try {

                            Tab tab = new Tab();
                            tab.setText(Repository.getPortfolioResponse().getNamePortfolio());
                            tab.setId(Repository.getPortfolioResponse().getNamePortfolio());
                            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/MarketDataPortfolioView.fxml"));
                            Pane root = loader.load();
                            MarketDataPortfolioViewController statisticsViewController = loader.getController();
                            statisticsViewController.setPortfolioName(Repository.getPortfolioResponse().getNamePortfolio());
                            tab.setContent(root);

                            Repository.getPrincipalController().getTpMkData().getTabs().add(tab);
                            Repository.getPrincipalController().getMarketDataPortfolioViewControllers()
                                    .put(statisticsViewController.getPortfolioName(), statisticsViewController);
                            Repository.getPrincipalController().getTpMkData().getSelectionModel().select(tab);
                            statisticsViewController.getMarketDataStatisticsTable().refresh();

                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }

                    } else if (Repository.getPortfolioResponse().getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.ADD_ASSET)) {

                        MarketDataPortfolioViewController sss =
                                Repository.getPrincipalController().getMarketDataPortfolioViewControllers()
                                        .get(Repository.getPortfolioResponse().getNamePortfolio());
                        BookVO bookVO = Repository.createBook(Repository.getPortfolioResponse().getAsset().getStatistic());
                        if (sss != null && bookVO != null) {
                            sss.addModelVo(bookVO);
                            addToFloating(bookVO);
                            Repository.createSuscripcion(
                                    Repository.getPortfolioResponse().getAsset().getStatistic().getSymbol(),
                                    Repository.getPortfolioResponse().getAsset().getStatistic().getSecurityExchange(),
                                    Repository.getPortfolioResponse().getAsset().getStatistic().getSettlType(),
                                    Repository.getPortfolioResponse().getAsset().getStatistic().getSecurityType()
                            );
                        }

                    } else if (Repository.getPortfolioResponse().getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.DELETE_PORTFOLIO)) {
                        try {
                            // Si el server te manda ID, úsalo; si no, queda en null y se borrará por name
                            String portfolioId = Repository.getPortfolioResponse().getNamePortfolio(); // <- si no existe este getter, deja null
                            String name       = Repository.getPortfolioResponse().getNamePortfolio();

                            // Quitar del mapa y desuscribirse de sus assets (si lo tenemos cargado)
                            MarketDataPortfolioViewController ctrl =
                                    Repository.getPrincipalController()
                                            .getMarketDataPortfolioViewControllers()
                                            .remove(name);

                            if (ctrl != null) {
                                var copy = new ArrayList<>(ctrl.getData());
                                copy.forEach(svo -> {
                                    String topicId = TopicGenerator.getTopicMKD(svo.getStatistic());
                                    removeFromFloating(topicId);
                                    Repository.unSuscripcion(topicId);
                                });
                            }

                            // Eliminar el tab de forma robusta (por id y/o nombre)
                            removePortfolioTab(portfolioId, name);

                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }






                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });



        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private Tab findTabByText(String text) {
        for (Tab t : tpMkData.getTabs()) {
            if (text.equals(t.getText())) return t;
        }
        return null;
    }

    private int portfolioOrderPriority(BlotterMessage.Portfolio portfolio) {
        if (portfolio == null || portfolio.getNamePortfolio() == null) {
            return 99;
        }

        String name = portfolio.getNamePortfolio().trim();
        if ("IPSA".equalsIgnoreCase(name)) {
            return 0;
        }
        if ("Principal".equalsIgnoreCase(name)) {
            return 1;
        }
        return 2;
    }

    private void selectPrincipalOrFirst() {
        runFx(() -> {
            if (tpMkData == null) return;
            Tab principal = findTabByText("Principal");
            if (principal != null) {
                tpMkData.getSelectionModel().select(principal);
            } else {
                tpMkData.getTabs().stream()
                        .filter(t -> !"+".equals(t.getText()))
                        .findFirst()
                        .ifPresent(t -> tpMkData.getSelectionModel().select(t));
            }
        });
    }

    private void movePlusToEnd() {
        runFx(() -> {
            if (tpMkData == null) return;
            Tab plus = findTabByText("+");
            if (plus != null) {
                tpMkData.getTabs().remove(plus);
                tpMkData.getTabs().add(plus);
            }
        });
    }

    // Intenta eliminar el tab de un portafolio por id y/o nombre de forma robusta.
    private void removePortfolioTab(String portfolioIdOrNull, String nameOrNull) {
        runFx(() -> {
            if (tpMkData == null) return;

            String idKey = portfolioIdOrNull == null ? null : portfolioIdOrNull.trim();
            String nameKey = nameOrNull == null ? null : nameOrNull.trim();

            // 1) Intentar por ID (Tab.getId())
            if (idKey != null && !idKey.isEmpty()) {
                tpMkData.getTabs().removeIf(t -> idKey.equalsIgnoreCase(String.valueOf(t.getId())));
            }

            // 2) Intentar por nombre visible (Tab.getText())
            if (nameKey != null && !nameKey.isEmpty()) {
                tpMkData.getTabs().removeIf(t -> {
                    String txt = t.getText() == null ? "" : t.getText().trim();
                    return nameKey.equalsIgnoreCase(txt);
                });
            }

            // Mantener UX consistente
            movePlusToEnd();
            selectPrincipalOrFirst();
        });
    }

    // Asegura que el "+" exista y quede al final (idempotente). Úsalo si lo necesitas.
    private void ensurePlusAtEnd() {
        runFx(() -> {
            if (tpMkData == null) return;
            Tab plus = findTabByText("+");
            if (plus == null) {
                Tab t = new Tab("+");
                tpMkData.getTabs().add(t);
            }
            movePlusToEnd();
        });
    }



    private void runFx(Runnable r) {
        if (Platform.isFxApplicationThread()) r.run();
        else Platform.runLater(r);
    }


    public void isLight() {
        try {
            isLightMode = true;
            LanzadorController lanzadorController = Repository.getLanzadorController();

            TabPane tabPaneLanzador = TabPaneLanzador;
            if (tabPaneLanzador != null) {
                tabPaneLanzador.setPrefWidth(410);
            }

            if (tableViewBookH != null) {
                tableViewBookH.setVisible(true);
                tableViewBookH.setManaged(true);
            }
            if (tableViewBookV != null) {
                tableViewBookV.setVisible(false);
                tableViewBookV.setManaged(false);
            }

            if (tabPaneBook != null) {
                tabPaneBook.setVisible(true);
                tabPaneBook.setManaged(true);
                tabPaneBook.setMinWidth(500);
                tabPaneBook.setPrefWidth(500);
                tabPaneBook.setMaxWidth(Region.USE_PREF_SIZE);
                HBox.setHgrow(tabPaneBook, Priority.NEVER);
            }

            footerController.btnAdminUser.setVisible(false);
            footerController.btnAdminUser.setManaged(false);


            splid.setDividerPositions(0.35);

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
                log.info("[LIQUIDACION][PREMIUM] user={} mode=light scope=principal action=enable", Repository.getUsername());
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


            lanzadorController.getPaste().setVisible(false);
            lanzadorController.getPaste().setManaged(false);


            lanzadorController.getChkIndivisible().setVisible(false);
            lanzadorController.getChkIndivisible().setManaged(false);

            lanzadorController.getLblAgreement().setVisible(false);
            lanzadorController.getLblAgreement().setManaged(false);

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



}
