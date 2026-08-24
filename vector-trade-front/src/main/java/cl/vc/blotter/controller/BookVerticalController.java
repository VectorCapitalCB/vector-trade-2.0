package cl.vc.blotter.controller;


import cl.vc.blotter.Repository;
import cl.vc.blotter.model.OrderBookEntry;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.utils.ProtoConverter;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.geometry.Orientation;
import javafx.scene.control.ScrollBar;
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

    private static final double BOOK_SCROLLBAR_GUTTER = 16.0;

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
    private ScrollBar bidBookScroll;
    @FXML
    private ScrollBar offerBookScroll;
    @FXML
    private SplitPane bookSplit;

    @FXML
    private void initialize() {

        // El libro vive dentro del SplitPane principal. Debe poder comprimirse con el panel
        // superior; un alto fijo hace que desborde sobre la seccion inferior al moverlo.
        offerViewTable.setMinHeight(0);
        bidViewTable.setMinHeight(0);
        offerViewTable.setMaxHeight(Double.MAX_VALUE);
        bidViewTable.setMaxHeight(Double.MAX_VALUE);

        // Permitir que el SplitPane sea redimensionable
        SplitPane.setResizableWithParent(offerViewTable, true);
        SplitPane.setResizableWithParent(bidViewTable, true);

        // Configuración del SplitPane
        bookSplit.setDividerPositions(0.5);  // Mantener el divisor en el 50% inicial

        // Ambas tablas deben reservar el mismo espacio para el scroll vertical. Si solo uno
        // de los lados tiene suficientes posturas para mostrarlo, CONSTRAINED_RESIZE_POLICY
        // entrega anchos distintos y desalinea precio/cantidad entre compra y venta.
        offerViewTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        bidViewTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        installSynchronizedBookColumns();
        installPermanentScrollBar(bidViewTable, bidBookScroll, false);
        installPermanentScrollBar(offerViewTable, offerBookScroll, true);

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

                // Orden propia viva en este nivel: se evalua ANTES que las reglas por cuenta,
                // que son mas gruesas (marcan cualquier orden de tus cuentas, no la tuya).
                if (isOwnLiveOrder(bidViewTable, data)) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-text-fill: #ffffff; -fx-background-color: #b8860b;"
                           + " -fx-border-color: #ffd700; -fx-border-width: 0 0 0 3;");
                } else if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
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

                // Orden propia viva en este nivel: se evalua ANTES que las reglas por cuenta,
                // que son mas gruesas (marcan cualquier orden de tus cuentas, no la tuya).
                if (isOwnLiveOrder(bidViewTable, data)) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-text-fill: #ffffff; -fx-background-color: #b8860b;"
                           + " -fx-border-color: #ffd700; -fx-border-width: 0 0 0 3;");
                } else if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
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

                        // Orden propia viva en este nivel: se evalua ANTES que las reglas por cuenta,
                // que son mas gruesas (marcan cualquier orden de tus cuentas, no la tuya).
                if (isOwnLiveOrder(offerViewTable, data)) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-text-fill: #ffffff; -fx-background-color: #b8860b;"
                           + " -fx-border-color: #ffd700; -fx-border-width: 0 0 0 3;");
                } else if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
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

                        // Orden propia viva en este nivel: se evalua ANTES que las reglas por cuenta,
                // que son mas gruesas (marcan cualquier orden de tus cuentas, no la tuya).
                if (isOwnLiveOrder(offerViewTable, data)) {
                    setStyle("-fx-alignment: CENTER-RIGHT; -fx-text-fill: #ffffff; -fx-background-color: #b8860b;"
                           + " -fx-border-color: #ffd700; -fx-border-width: 0 0 0 3;");
                } else if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
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

        hideTableHeader(bidViewTable);
        hideTableHeader(offerViewTable);


    }

    private boolean isOwnLiveOrder(TableView<OrderBookEntry> table, OrderBookEntry entry) {
        if (entry == null) return false;
        boolean repeatedPrice = table.getItems().stream()
                .filter(item -> item != null
                        && item.getSide() == entry.getSide()
                        && Math.abs(item.getPriceValue() - entry.getPriceValue()) < 0.0001d)
                .limit(2)
                .count() > 1;
        if (!repeatedPrice) {
            return Repository.tieneOrdenVivaEn(
                    entry.getSymbol(), entry.getSide(), entry.getPriceValue(),
                    entry.getSecurityExchangeRouting(), null);
        }
        return Repository.tienePosturaVivaEn(
                entry.getSymbol(), entry.getSide(), entry.getPriceValue(), entry.getSizeValue(),
                entry.getAccount(), entry.getOperator(), entry.getSecurityExchangeRouting(), null);
    }

    private void installSynchronizedBookColumns() {
        Runnable resize = this::resizeBookColumns;
        bidViewTable.widthProperty().addListener((obs, oldWidth, newWidth) -> resize.run());
        offerViewTable.widthProperty().addListener((obs, oldWidth, newWidth) -> resize.run());
        bidViewTable.skinProperty().addListener((obs, oldSkin, newSkin) -> {
            if (newSkin != null) Platform.runLater(resize);
        });
        offerViewTable.skinProperty().addListener((obs, oldSkin, newSkin) -> {
            if (newSkin != null) Platform.runLater(resize);
        });
        Platform.runLater(resize);
    }

    private void resizeBookColumns() {
        double tableWidth = Math.min(bidViewTable.getWidth(), offerViewTable.getWidth());
        if (tableWidth <= BOOK_SCROLLBAR_GUTTER) return;

        double columnWidth = bookColumnWidth(tableWidth);
        setColumnWidth(priceBid, columnWidth);
        setColumnWidth(quantityBid, columnWidth);
        setColumnWidth(priceOffer, columnWidth);
        setColumnWidth(quantityOffer, columnWidth);
    }

    static double bookColumnWidth(double tableWidth) {
        return Math.max(1.0, (tableWidth - BOOK_SCROLLBAR_GUTTER) / 2.0);
    }

    private static void setColumnWidth(TableColumn<?, ?> column, double width) {
        column.setMinWidth(width);
        column.setPrefWidth(width);
        column.setMaxWidth(width);
    }

    private void installPermanentScrollBar(TableView<?> table, ScrollBar externalBar, boolean inverted) {
        Runnable connect = () -> connectScrollBars(table, externalBar, inverted);
        table.skinProperty().addListener((obs, oldSkin, newSkin) -> {
            if (newSkin != null) Platform.runLater(connect);
        });
        Platform.runLater(connect);
    }

    private void connectScrollBars(TableView<?> table, ScrollBar externalBar, boolean inverted) {
        ScrollBar internalBar = table.lookupAll(".scroll-bar").stream()
                .filter(ScrollBar.class::isInstance)
                .map(ScrollBar.class::cast)
                .filter(bar -> bar.getOrientation() == Orientation.VERTICAL)
                .findFirst()
                .orElse(null);
        if (internalBar == null || externalBar.getProperties().get("book-scroll-source") == internalBar) return;

        externalBar.getProperties().put("book-scroll-source", internalBar);
        internalBar.setOpacity(0);
        internalBar.setMouseTransparent(true);

        Runnable sync = () -> {
            externalBar.setMin(internalBar.getMin());
            externalBar.setMax(internalBar.getMax());
            externalBar.setVisibleAmount(internalBar.getVisibleAmount());
            externalBar.setUnitIncrement(internalBar.getUnitIncrement());
            externalBar.setBlockIncrement(internalBar.getBlockIncrement());
            double externalValue = inverted
                    ? internalBar.getMax() + internalBar.getMin() - internalBar.getValue()
                    : internalBar.getValue();
            if (Math.abs(externalBar.getValue() - externalValue) > 0.0001) {
                externalBar.setValue(externalValue);
            }
        };
        internalBar.minProperty().addListener((obs, oldValue, newValue) -> sync.run());
        internalBar.maxProperty().addListener((obs, oldValue, newValue) -> sync.run());
        internalBar.visibleAmountProperty().addListener((obs, oldValue, newValue) -> sync.run());
        internalBar.valueProperty().addListener((obs, oldValue, newValue) -> sync.run());
        externalBar.valueProperty().addListener((obs, oldValue, newValue) -> {
            double internalValue = inverted
                    ? internalBar.getMax() + internalBar.getMin() - newValue.doubleValue()
                    : newValue.doubleValue();
            if (Math.abs(internalBar.getValue() - internalValue) > 0.0001) {
                internalBar.setValue(internalValue);
            }
        });
        sync.run();
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
