package cl.vc.blotter.controller;


import cl.vc.blotter.Repository;
import cl.vc.blotter.model.OrderBookEntry;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.utils.ProtoConverter;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.Region;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class BookVerticalController {

    @FXML
    private TableView<OrderBookEntry> bidViewTable;
    @FXML
    private TableColumn<OrderBookEntry, String> quantityBid;
    @FXML
    private TableColumn<OrderBookEntry, String> priceBid;

    @FXML
    private TableView<OrderBookEntry> offerViewTable;
    @FXML
    private TableColumn<OrderBookEntry, String> priceOffer;
    @FXML
    private TableColumn<OrderBookEntry, String> quantityOffer;
    @FXML
    private SplitPane bookSplit;

    @FXML
    private void initialize() {

        offerViewTable.setMinHeight(170);  // Altura mínima para la tabla de oferta
        bidViewTable.setMinHeight(170);    // Altura mínima para la tabla de demanda

        // Establecemos el tamaño máximo de las tablas para que no se agranden más allá de un cierto límite
        offerViewTable.setMaxHeight(170);  // Limitar la altura máxima
        bidViewTable.setMaxHeight(170);    // Limitar la altura máxima

        // Permitir que el SplitPane sea redimensionable
        SplitPane.setResizableWithParent(offerViewTable, true);
        SplitPane.setResizableWithParent(bidViewTable, true);

        // Configuración del SplitPane
        bookSplit.setDividerPositions(0.5);  // Mantener el divisor en el 50% inicial

        // Permitir el ajuste de las columnas en la tabla
        offerViewTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        bidViewTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        // Configurar las celdas de las tablas
        priceBid.setCellValueFactory(new PropertyValueFactory<>("price"));
        quantityBid.setCellValueFactory(new PropertyValueFactory<>("size"));
        priceOffer.setCellValueFactory(new PropertyValueFactory<>("price"));
        quantityOffer.setCellValueFactory(new PropertyValueFactory<>("size"));

        priceBid.setCellFactory(column -> new TableCell<>() { /* Código de la celda */ });
        quantityBid.setCellFactory(column -> new TableCell<>() { /* Código de la celda */ });
        priceOffer.setCellFactory(column -> new TableCell<>() { /* Código de la celda */ });
        quantityOffer.setCellFactory(column -> new TableCell<>() { /* Código de la celda */ });

        // Escuchar los cambios en la escena y mantener el divisor en la posición 50% al cargar
        bookSplit.sceneProperty().addListener((obs, oldScene, newScene) -> {
            if (newScene != null) {
                Platform.runLater(() -> bookSplit.setDividerPositions(0.5));  // Establecer el divisor al 50% al cargar
            }
        });

        // Hacer lo mismo cuando el SplitPane tenga un nuevo skin (primer layout real)
        bookSplit.skinProperty().addListener((obs, oldSkin, newSkin) -> {
            if (newSkin != null) {
                Platform.runLater(() -> bookSplit.setDividerPositions(0.5));  // Establecer el divisor al 50%
            }
        });

        // Asegurar que el divisor se mantenga en su lugar después de cargar los elementos
        offerViewTable.itemsProperty().addListener((o, old, val) ->
                Platform.runLater(() -> bookSplit.setDividerPositions(0.5)));
        bidViewTable.itemsProperty().addListener((o, old, val) ->
                Platform.runLater(() -> bookSplit.setDividerPositions(0.5)));

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

                if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #056774;");
                } else if (Repository.getUser() != null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");
                } else if ("041".equals(data.getOperator()) && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");
                } else {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-text-fill: green;");
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

                if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #056774;");
                } else if (Repository.getUser() != null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");
                } else if ("041".equals(data.getOperator()) && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");
                } else {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-text-fill: green;");
                }
            }
        });

        priceOffer.setCellValueFactory(new PropertyValueFactory<>("price"));
        offerViewTable.setRotate(180);

        priceOffer.setCellFactory(column -> new TableCell<>() {

            public static final String DB_292_B = "-fx-alignment: CENTER-RIGHT; -fx-text-fill: #db292b;";
            public static final String STRING1 = "-fx-alignment: CENTER-RIGHT; -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #450574;";
            public static final String STRING = "-fx-alignment: CENTER-RIGHT; -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #8B3A3A; ";
            public static final String stile = "-fx-alignment: CENTER-RIGHT; -fx-border-color: #2b3178; -fx-text-fill: #ffffff; -fx-background-color: #e01919;";

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
                        setScaleY(-1);

                        if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                            setStyle(STRING1);
                        } else if (Repository.getUser()!= null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                            setStyle(STRING);
                        } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                            setStyle(stile);
                        } else {
                            setStyle(DB_292_B);
                        }



                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        });

        quantityOffer.setCellValueFactory(new PropertyValueFactory<>("size"));
        quantityOffer.setStyle("-fx-alignment: CENTER-RIGHT;");

        quantityOffer.setCellFactory(column -> new TableCell<>() {

            public static final String CENTER_RIGHT_FX_TEXT_FILL_DB_282_C = "-fx-alignment: CENTER-RIGHT; -fx-text-fill: #db282c;";
            public static final String COLOR_2_B_3178_FX_TEXT_FILL_FFFFFF_FX_BACKGROUND_COLOR_E_01919 = "-fx-alignment: CENTER-RIGHT; -fx-border-color: #2b3178; -fx-text-fill: #ffffff; -fx-background-color: #e01919;";
            public static final String x = "-fx-alignment: CENTER-RIGHT; -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #8B3A3A; ";
            public static final String y = "-fx-alignment: CENTER-RIGHT; -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #450574;";

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
                        setScaleY(-1);

                        if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                            setStyle(y);
                        } else if (Repository.getUser() != null &&  Repository.getUser().getAccountList().contains(data.getAccount())
                                && !data.getAccount().isEmpty()) {
                            setStyle(x);
                        } else if (data.getOperator().equals("041") && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                            setStyle(COLOR_2_B_3178_FX_TEXT_FILL_FFFFFF_FX_BACKGROUND_COLOR_E_01919);
                        } else {
                            setStyle(CENTER_RIGHT_FX_TEXT_FILL_DB_282_C);
                        }



                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        });




        bidViewTable.setOnMouseClicked(event -> {
            try {

                OrderBookEntry value = bidViewTable.getSelectionModel().getSelectedItem();

                if (value != null) {

                    Repository.getPrincipalController().getLanzadorController().getPriceOrder().setText(String.valueOf(value.getPrice()));
                    Repository.getPrincipalController().getLanzadorController().getQuantity().setText(String.valueOf(value.getSize()));
                    Repository.getPrincipalController().getLanzadorController().getSideOrder().getSelectionModel().select(ProtoConverter.routingDecryptStatus(RoutingMessage.Side.SELL.name()));
                    Repository.getPrincipalController().getLanzadorController().getSecExchOrder().getSelectionModel().select(value.getSecurityExchangeRouting());
                    Repository.getPrincipalController().getLanzadorController().getIceberg().setText("");

                } else {
                    log.warn("Selected item is null.");
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });

        offerViewTable.setOnMouseClicked(event -> {
            try {

                OrderBookEntry value = offerViewTable.getSelectionModel().getSelectedItem();

                if (value != null) {
                    Repository.getPrincipalController().getLanzadorController().getPriceOrder().setText(String.valueOf(value.getPrice()));
                    Repository.getPrincipalController().getLanzadorController().getQuantity().setText(String.valueOf(value.getSize()));
                    Repository.getPrincipalController().getLanzadorController().getSideOrder().getSelectionModel().select(ProtoConverter.routingDecryptStatus(RoutingMessage.Side.BUY.name()));
                    Repository.getPrincipalController().getLanzadorController().getSecExchOrder().getSelectionModel().select(value.getSecurityExchangeRouting());
                    Repository.getPrincipalController().getLanzadorController().getIceberg().setText("");

                } else {
                    log.warn("Selected item is null.");
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });

        bidViewTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        offerViewTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);


        hideTableHeader(bidViewTable);
        hideTableHeader(offerViewTable);


    }

    private void hideTableHeader(TableView<?> table) {
        applyHeaderVisibility(table, false);
    }

    private void applyHeaderVisibility(TableView<?> table, boolean visible) {
        if (table == null) return;


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


}
