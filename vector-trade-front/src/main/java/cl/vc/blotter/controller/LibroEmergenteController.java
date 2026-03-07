package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.model.OrderBookEntry;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
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

@Slf4j
@Data
public class LibroEmergenteController implements Initializable {

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

    private ObservableList<String> allSymbols;

    private FilteredList<String> filteredList;

    private Popup suggestionsPopup;

    private TextField activeTextField;

    private Stage stage;


    @FXML
    public void initialize() {

    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

        try {

            setupSuggestionsPopup();

            securityType.setItems(FXCollections.observableArrayList(RoutingMessage.SecurityType.values()));
            securityType.getSelectionModel().selectFirst();

            settlType.setItems(FXCollections.observableArrayList(RoutingMessage.SettlType.values()));
            settlType.getItems().removeAll(RoutingMessage.SettlType.UNRECOGNIZED, RoutingMessage.SettlType.REGULAR);
            settlType.getSelectionModel().select(RoutingMessage.SettlType.T2);


            ObservableList<MarketDataMessage.SecurityExchangeMarketData> x = FXCollections.observableArrayList();
            x.addAll(Repository.getUser().getRoles().getDestinoMKDList());
            x.remove(MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL);

            cbMarket.setItems(x);
            cbMarket.getSelectionModel().select(MarketDataMessage.SecurityExchangeMarketData.BCS);


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

                            if (!data.getAccount().isEmpty() && Repository.getUser().getAccountList().contains(data.getAccount())) {
                                setStyle(getStyle() + "-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");

                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                setStyle(getStyle() + "-fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");

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

                            if (!data.getAccount().isEmpty() && Repository.getUser().getAccountList().contains(data.getAccount())) {
                                setStyle(getStyle() + "-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");

                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                setStyle(getStyle() + "-fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");

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
                    @Override
                    protected void updateItem(String item, boolean empty) {
                        super.updateItem(item, empty);
                        if (empty || item == null) {
                            setText(null);
                            setStyle("");

                        } else {

                            OrderBookEntry data = getTableRow().getItem();

                            if (data == null || data.getDecimalFormat() == null) {
                                return;
                            }

                            setText(item);

                            if (Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                                setStyle(getStyle() + "-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");

                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {

                            } else {
                                setStyle("-fx-text-fill: green;");
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
                    @Override
                    protected void updateItem(String item, boolean empty) {
                        super.updateItem(item, empty);
                        if (empty || item == null) {
                            setText(null);
                            setStyle("");

                        } else {

                            OrderBookEntry data = getTableRow().getItem();

                            if (data == null || data.getDecimalFormat() == null) {
                                return;
                            }

                            setText(item);

                            if (Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                                setStyle(getStyle() + "-fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");
                            } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                                setStyle(getStyle() + "-fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");
                            } else {
                                setStyle("-fx-text-fill: red;");
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

    private void openOrderLauncher(OrderBookEntry dataBook, String sideOrderValue) {
        try {


            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/Lanzador.fxml"));
            Parent root = loader.load();

            LanzadorController lanzadorController = loader.getController();
            lanzadorController.setIslibrazo(true);
            lanzadorController.setIdLibrazo(dataBook.getId());
            lanzadorController.setSkipConfirmationAlert(Repository.getUserEnable().contains(Repository.getUser().getUsername()));



            if (Repository.getUser().getUsername().contains("fricci")) {
                lanzadorController.getAcAccount().getItems().clear();
                lanzadorController.getAcAccount().setItems(FXCollections.observableArrayList("47024924/0"));
                Platform.runLater(() -> lanzadorController.getAcAccount().getSelectionModel().select("47024924/0"));
            }

            if (Repository.getStaticSecurityType().containsKey(dataBook.getSymbol())) {
                lanzadorController.getSecurityType().getSelectionModel().select(RoutingMessage.SecurityType.CS);
            } else if (dataBook.getSymbol().contains("CFI")) {
                lanzadorController.getSecurityType().getSelectionModel().select(RoutingMessage.SecurityType.CFI);
            } else {
                lanzadorController.getSecurityType().getSelectionModel().select(RoutingMessage.SecurityType.CS);
            }


            RoutingMessage.SettlType selectedSettlType = settlType.getSelectionModel().getSelectedItem();
            lanzadorController.getSettltypeOrder().setValue(selectedSettlType);

            lanzadorController.getTicket().setText(dataBook.getSymbol());
            lanzadorController.getPriceOrder().setText(dataBook.getPrice());
            lanzadorController.getQuantity().setText(dataBook.getSize());

            lanzadorController.getSideOrder().setValue(sideOrderValue);

            lanzadorController.setLibrazoController(this);


            double px = Double.parseDouble(dataBook.getPrice().replace(",", ""));
            double spread = px * (10 / 10_000d);

            DecimalFormatSymbols usSymbols = DecimalFormatSymbols.getInstance(Locale.US);
            DecimalFormat formatter4dec = new DecimalFormat("0.0000", usSymbols);

            lanzadorController.getSpread().setText(formatter4dec.format(spread));

            lanzadorController.setValues(Repository.getUser());
            lanzadorController.getIceberg().setText("10");


            Scene scene = new Scene(root);


            String cssPath = "/blotter/css/style.css";
            URL cssResource = getClass().getResource(cssPath);

            if (cssResource != null) {
                scene.getStylesheets().add(cssResource.toExternalForm());
            } else {
                log.error("Recurso CSS no encontrado: " + cssPath);
            }


            stage = new Stage();
            stage.setTitle("Lanzador de Órdenes");
            stage.setScene(scene);
            stage.initModality(Modality.APPLICATION_MODAL);
            stage.setWidth(700);
            stage.setHeight(440);
            stage.setResizable(true);
            stage.show();

        } catch (IOException e) {
            log.error("Error al abrir el lanzador", e);
        }
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

            if (!Repository.getLibroEmergenteMap().containsKey(positions)) {
                Repository.getLibroEmergenteMap().put(positions, this);
            }

            if (!idSubscribeBook.isEmpty()) {
                Repository.unSuscripcion(idSubscribeBook);
                idSubscribeBook = "";
            }


            if(Repository.getStaticSecurityType().containsKey(ticket.getText())){
                securityType.getSelectionModel().select(RoutingMessage.SecurityType.CS);
            }

            idSubscribeBook = Repository.createSuscripcion(ticket.getText(),
                    cbMarket.getSelectionModel().getSelectedItem(),
                    settlType.getSelectionModel().getSelectedItem(),
                    securityType.getSelectionModel().getSelectedItem());

            subscribe = MarketDataMessage.Subscribe.newBuilder()
                    .setId(idSubscribeBook)
                    .setSymbol(ticket.getText())
                    .setSecurityExchange(cbMarket.getSelectionModel().getSelectedItem())
                    .setSettlType(settlType.getSelectionModel().getSelectedItem())
                    .setSecurityType(securityType.getSelectionModel().getSelectedItem()).build();


            bidViewTable.setItems(FXCollections.observableArrayList());
            offerViewTable.setItems(FXCollections.observableArrayList());

            bidViewTable.refresh();
            offerViewTable.refresh();


            if (Repository.getBookPortMaps().containsKey(idSubscribeBook)) {


                BookVO bookVO = Repository.getBookPortMaps().get(idSubscribeBook);

                bidViewTable.setItems(bookVO.getBidBook());
                offerViewTable.setItems(bookVO.getAskBook());

                bidViewTable.refresh();
                offerViewTable.refresh();

                update(bookVO);

            }


            BlotterMessage.Multibook.Builder multibook = BlotterMessage.Multibook.newBuilder();
            multibook.setUsername(Repository.getUsername());

            Repository.getLibroEmergenteMap().forEach((key, value) -> {
                try {
                    if (value.getSubscribe() != null) {
                        BlotterMessage.SubMultibook subMultibook = BlotterMessage.SubMultibook.newBuilder()
                                .setPositions(value.getPositions())
                                .setSubscribeBook(value.getSubscribe())
                                .build();
                        multibook.addSubmultibook(subMultibook);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });

            Repository.getClientService().sendMessage(multibook.build());
            Repository.getClientService().sendMessage(subscribe);



        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void update(BookVO bookVO){

        try {

            if(!bookVO.getId().equals(idSubscribeBook)){
                return;
            }

            Platform.runLater(() ->{
                    closepriceGen.setText(bookVO.getStatisticVO().getLast());
                    volumeGen.setText(Repository.getFormatter2dec().format(bookVO.getStatisticVO().getVolume()));
                    lowpriceGen.setText(Repository.getFormatter2dec().format(bookVO.getStatisticVO().getLow()));
                    medioGen.setText(bookVO.getStatisticVO().getMid());
                    previusClose.setText(bookVO.getStatisticVO().getPreviusClose());
                    highpriceGen.setText(Repository.getFormatter2dec().format(bookVO.getStatisticVO().getHigh()));
                    imbalanceGen.setText(Repository.getFormatter2dec().format(bookVO.getStatisticVO().getImbalance()));
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
            securityType.getSelectionModel().select(subscribe.getSecurityType());

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
                        securityType.getSelectionModel().select(securityTypeValue);
                    }
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(e);
                }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void unsubscribe() {
        Repository.unSuscripcion(idSubscribeBook);
    }

    public void close() {
        if(stage.isShowing()){
            stage.close();
        }

    }
}