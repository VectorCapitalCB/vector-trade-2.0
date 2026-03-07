package cl.vc.blotter.controller;


import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import lombok.Data;


@Data
public class NewsController {

    @FXML
    private TableView<MarketDataMessage.News> newsTableView;


    @FXML
    private TableColumn<MarketDataMessage.News, com.google.protobuf.Timestamp> horaColumn;

    @FXML
    private TableColumn<MarketDataMessage.News, String> textoColumn;

    @FXML
    private TableColumn<MarketDataMessage.News, String> lineoftextColumn;

    @FXML
    private TableColumn<MarketDataMessage.News, String> securityExchangeColumn;


    private ObservableList<MarketDataMessage.News> newsList = FXCollections.observableArrayList();

    public void initialize() {

        textoColumn.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getTexto()));
        lineoftextColumn.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getLineoftext()));
        securityExchangeColumn.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getSecurityExchange().name()));
        horaColumn.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getT()));
        newsTableView.setItems(newsList);

        horaColumn.setCellFactory(column -> new javafx.scene.control.TableCell<MarketDataMessage.News, com.google.protobuf.Timestamp>() {
            @Override
            protected void updateItem(com.google.protobuf.Timestamp item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                } else {
                    java.time.Instant instant = java.time.Instant.ofEpochSecond(item.getSeconds(), item.getNanos());
                    java.time.ZonedDateTime zonedDateTime = java.time.ZonedDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
                    java.time.ZonedDateTime zonedDateTimeChile = zonedDateTime.withZoneSameInstant(cl.vc.blotter.Repository.getZoneID());
                    java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                    String formattedDateTime = zonedDateTimeChile.format(formatter);
                    setText(formattedDateTime);
                }
            }
        });


    }
}
