package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.adaptor.ClientActor;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.module.protocolbuff.utils.Corredoras;
import com.google.protobuf.Timestamp;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.ListChangeListener;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
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
import java.util.Locale;
import java.util.ResourceBundle;

@Data
@Slf4j
public class TradeMKDController implements Initializable {

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
    private ClientActor clientActor;

    private final java.util.HashMap<String, String> allbrokercode = Corredoras.getAll();
    private ObservableList<MarketDataMessage.Trade> tradesBacking = FXCollections.observableArrayList();
    private SortedList<MarketDataMessage.Trade> sortedTrades = new SortedList<>(tradesBacking);

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        try {

            try {
                var m = Repository.class.getMethod("setTradeMKDController", TradeMKDController.class);
                m.invoke(null, this);
            } catch (Throwable ignored) {}

            priceTrade.setCellValueFactory(new PropertyValueFactory<>("price"));
            qtyTrade.setCellValueFactory(new PropertyValueFactory<>("qty"));
            time.setCellValueFactory(new PropertyValueFactory<>("t"));


            time.setComparator((Timestamp t1, Timestamp t2) -> {
                if (t1 == t2) return 0;
                if (t1 == null) return -1;
                if (t2 == null) return 1;
                Instant i1 = Instant.ofEpochSecond(t1.getSeconds(), t1.getNanos());
                Instant i2 = Instant.ofEpochSecond(t2.getSeconds(), t2.getNanos());
                return i1.compareTo(i2);
            });


            time.setCellFactory(column -> new TableCell<>() {
                @Override
                protected void updateItem(Timestamp item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        Instant instant = Instant.ofEpochSecond(item.getSeconds(), item.getNanos());
                        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
                                .withZoneSameInstant(Repository.getZoneID());
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                        setText(zonedDateTime.format(formatter));
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
                        if (data == null) return;
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
                        if (data == null) return;
                        DecimalFormat decimalFormat = NumberGenerator.getFormatNumberMilDec(data.getSecurityExchange());
                        setText(decimalFormat.format(item));
                    }
                }
            });

            buyer.setCellValueFactory(new PropertyValueFactory<>("buyer"));
            seller.setCellValueFactory(new PropertyValueFactory<>("seller"));
            symboltrades.setCellValueFactory(new PropertyValueFactory<>("symbol"));
            idgenerado.setCellValueFactory(new PropertyValueFactory<>("idGenerico"));

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

            marketDataTradeTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
            sortedTrades.comparatorProperty().bind(marketDataTradeTable.comparatorProperty());
            marketDataTradeTable.setItems(sortedTrades);

            time.setSortType(TableColumn.SortType.DESCENDING);
            marketDataTradeTable.getSortOrder().setAll(time);
            marketDataTradeTable.sort();

            marketDataTradeTable.itemsProperty().addListener((obs, oldV, newV) -> {
                if (newV == null) {
                    tradesBacking = FXCollections.observableArrayList();
                    sortedTrades = new SortedList<>(tradesBacking);
                    sortedTrades.comparatorProperty().bind(marketDataTradeTable.comparatorProperty());
                    marketDataTradeTable.setItems(sortedTrades);
                    return;
                }
                if (!(newV instanceof SortedList)) {

                    tradesBacking = (newV instanceof ObservableList<?> ol)
                            ? (ObservableList<MarketDataMessage.Trade>) ol
                            : FXCollections.observableArrayList(newV);
                    sortedTrades = new SortedList<>(tradesBacking);
                    sortedTrades.comparatorProperty().bind(marketDataTradeTable.comparatorProperty());
                    marketDataTradeTable.setItems(sortedTrades);
                    if (!marketDataTradeTable.getSortOrder().contains(time)) {
                        marketDataTradeTable.getSortOrder().setAll(time);
                    }
                    time.setSortType(TableColumn.SortType.DESCENDING);
                    marketDataTradeTable.sort();
                }
            });

            tradesBacking.addListener((ListChangeListener<MarketDataMessage.Trade>) c -> {
                if (!marketDataTradeTable.getSortOrder().isEmpty()) {
                    marketDataTradeTable.sort();
                }
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void setTrades(java.util.Collection<MarketDataMessage.Trade> nuevos) {
        Platform.runLater(() -> {
            tradesBacking.setAll(nuevos);
            marketDataTradeTable.sort();
        });
    }

    public void addTrades(java.util.Collection<MarketDataMessage.Trade> nuevos) {
        Platform.runLater(() -> {
            tradesBacking.addAll(nuevos);
            marketDataTradeTable.sort();
        });
    }

    public void addTrade(MarketDataMessage.Trade t) {
        Platform.runLater(() -> {
            tradesBacking.add(t);
            marketDataTradeTable.sort();
        });
    }

    public void sortByTimeDesc() {
        Platform.runLater(() -> {
            if (!marketDataTradeTable.getSortOrder().contains(time)) {
                marketDataTradeTable.getSortOrder().setAll(time);
            }
            time.setSortType(TableColumn.SortType.DESCENDING);
            marketDataTradeTable.sort();
        });
    }

    public void bindTrades(ObservableList<MarketDataMessage.Trade> source) {
        Platform.runLater(() -> {
            tradesBacking = source;
            sortedTrades = new SortedList<>(tradesBacking);
            sortedTrades.comparatorProperty().bind(marketDataTradeTable.comparatorProperty());
            marketDataTradeTable.setItems(sortedTrades);

            time.setSortType(TableColumn.SortType.DESCENDING);
            marketDataTradeTable.getSortOrder().setAll(time);
            marketDataTradeTable.sort();

            tradesBacking.addListener((ListChangeListener<MarketDataMessage.Trade>) c -> marketDataTradeTable.sort());
        });
    }

}
