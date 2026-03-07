package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.model.OrderBookEntry;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.utils.ProtoConverter;
import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class BookHorizontalController {

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
    private void initialize() {

        quantityBid.setCellValueFactory(new PropertyValueFactory<>("size"));
        priceBid.setCellValueFactory(new PropertyValueFactory<>("price"));
        priceOffer.setCellValueFactory(new PropertyValueFactory<>("price"));
        quantityOffer.setCellValueFactory(new PropertyValueFactory<>("size"));

        priceBid.setCellFactory(col -> new TableCell<>() {
            { setAlignment(Pos.CENTER); }
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setStyle(""); return; }
                setText(item);
                OrderBookEntry data = getTableRow() != null ? getTableRow().getItem() : null;
                if (data == null) { setStyle(""); return; }

                if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #056774;");
                } else if (Repository.getUser() != null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");
                } else if ("041".equals(data.getOperator()) && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");
                } else {
                    setStyle("-fx-alignment: CENTER; -fx-text-fill: green;");
                }
            }
        });

        quantityBid.setCellFactory(col -> new TableCell<>() {
            { setAlignment(Pos.CENTER); }
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setStyle(""); return; }
                setText(item);
                OrderBookEntry data = getTableRow() != null ? getTableRow().getItem() : null;
                if (data == null) { setStyle(""); return; }

                if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #056774;");
                } else if (Repository.getUser() != null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #3e782b;");
                } else if ("041".equals(data.getOperator()) && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #e01919; -fx-text-fill: #ffffff; -fx-background-color: #2b3178;");
                } else {
                    setStyle("-fx-alignment: CENTER; -fx-text-fill: green;");
                }
            }
        });

        priceOffer.setCellFactory(col -> new TableCell<>() {
            { setAlignment(Pos.CENTER); }
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setStyle(""); return; }
                setText(item);
                OrderBookEntry data = getTableRow() != null ? getTableRow().getItem() : null;
                if (data == null) { setStyle(""); return; }

                if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #450574;");
                } else if (Repository.getUser()!= null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #8B3A3A;");
                } else if ("041".equals(data.getOperator()) && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #2b3178; -fx-text-fill: #ffffff; -fx-background-color: #e01919;");
                } else {
                    setStyle("-fx-alignment: CENTER; -fx-text-fill: #db292b;");
                }
            }
        });

        quantityOffer.setCellFactory(col -> new TableCell<>() {
            { setAlignment(Pos.CENTER); }
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setStyle(""); return; }
                setText(item);
                OrderBookEntry data = getTableRow() != null ? getTableRow().getItem() : null;
                if (data == null) { setStyle(""); return; }

                if (Repository.getUser() != null && "16138017/0".equals(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #14e8cf; -fx-text-fill: #ffffff; -fx-background-color: #450574;");
                } else if (Repository.getUser()!= null && Repository.getUser().getAccountList().contains(data.getAccount()) && !data.getAccount().isEmpty()) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #856714; -fx-text-fill: #ffffff; -fx-background-color: #8B3A3A;");
                } else if ("041".equals(data.getOperator()) && Repository.getUserEnable().contains(Repository.getUser().getUsername())) {
                    setStyle("-fx-alignment: CENTER; -fx-border-color: #2b3178; -fx-text-fill: #ffffff; -fx-background-color: #e01919;");
                } else {
                    setStyle("-fx-alignment: CENTER; -fx-text-fill: #db282c;");
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
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });

        bidViewTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        offerViewTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);

        quantityBid.setPrefWidth(121);
        priceBid.setPrefWidth(121);
        quantityOffer.setPrefWidth(121);
        priceOffer.setPrefWidth(121);

        bidViewTable.setPrefWidth(quantityBid.getPrefWidth() + priceBid.getPrefWidth());
        bidViewTable.setMinWidth(bidViewTable.getPrefWidth());
        bidViewTable.setMaxWidth(Region.USE_PREF_SIZE);

        HBox.setHgrow(bidViewTable, Priority.NEVER);
    }
}
