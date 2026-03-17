package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.adaptor.ClientActor;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.module.protocolbuff.utils.Corredoras;
import com.google.protobuf.Timestamp;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import lombok.Data;
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
public class TradeGeneralesController implements Initializable {

    @FXML
    private TableView<MarketDataMessage.TradeGeneral> marketDataTradeTable;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, Timestamp> time;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> symboltrades;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, Double> priceTrade;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, Double> qtyTrade;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> buyer;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> seller;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> idgenerado;

    @FXML
    private TableColumn<MarketDataMessage.TradeGeneral, String> liquidacion;

    @FXML
    private ClientActor clientActor;


    private HashMap<String, String> allbrokercode = Corredoras.getAll();


    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

        try {


            priceTrade.setCellValueFactory(new PropertyValueFactory<>("price"));
            qtyTrade.setCellValueFactory(new PropertyValueFactory<>("qty"));
            time.setCellValueFactory(new PropertyValueFactory<>("t"));

            time.setComparator((Timestamp t1, Timestamp t2) -> {
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


            qtyTrade.setCellFactory(column -> new TableCell<>() {
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

            buyer.setCellValueFactory(new PropertyValueFactory<>("buyer"));
            seller.setCellValueFactory(new PropertyValueFactory<>("seller"));
            symboltrades.setCellValueFactory(new PropertyValueFactory<>("symbol"));
            idgenerado.setCellValueFactory(new PropertyValueFactory<>("idGenerico"));
            liquidacion.setCellValueFactory(new PropertyValueFactory<>("settlType"));


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
                            } else {
                                getStyleClass().add("notvc");
                            }
                        } else {
                            setText(item);
                        }
                    }
                }
            });

            marketDataTradeTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
            marketDataTradeTable.setFixedCellSize(24);
            marketDataTradeTable.getSortOrder().add(time);
            SortedList<MarketDataMessage.TradeGeneral> sortedData = new SortedList<>(Repository.getTradeGenerales());
            sortedData.comparatorProperty().bind(marketDataTradeTable.comparatorProperty());
            marketDataTradeTable.setItems(sortedData);
            marketDataTradeTable.sort();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}


