package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.adaptor.ClientActor;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.module.protocolbuff.utils.Corredoras;
import com.google.protobuf.Timestamp;
import javafx.animation.PauseTransition;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ResourceBundle;

@Data
@Slf4j
public class TradeMKDController implements Initializable {
    private static final int MAX_VISIBLE_TRADES = 200;

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
    private final PauseTransition sortDebouncer = new PauseTransition(javafx.util.Duration.millis(75));
    private final List<MarketDataMessage.Trade> pendingTrades = new ArrayList<>();
    private List<MarketDataMessage.Trade> pendingReplacement;
    private boolean flushScheduled;
    private ListChangeListener<MarketDataMessage.Trade> boundTradesListener;

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
            sortedTrades.comparatorProperty().bind(marketDataTradeTable.comparatorProperty());
            marketDataTradeTable.setItems(sortedTrades);
            sortDebouncer.setOnFinished(evt -> sortTableIfNeeded());

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

            attachTradesListener(tradesBacking);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void setTrades(Collection<MarketDataMessage.Trade> nuevos) {
        synchronized (pendingTrades) {
            pendingReplacement = new ArrayList<>(nuevos);
            pendingTrades.clear();
        }
        scheduleFlush();
    }

    public void addTrades(Collection<MarketDataMessage.Trade> nuevos) {
        synchronized (pendingTrades) {
            pendingTrades.addAll(nuevos);
        }
        scheduleFlush();
    }

    public void addTrade(MarketDataMessage.Trade t) {
        synchronized (pendingTrades) {
            pendingTrades.add(t);
        }
        scheduleFlush();
    }

    public void sortByTimeDesc() {
        runFx(this::sortTableIfNeeded);
    }

    public void bindTrades(ObservableList<MarketDataMessage.Trade> source) {
        runFx(() -> {
            if (tradesBacking == source) {
                return;
            }
            detachTradesListener();
            tradesBacking = source;
            sortedTrades = new SortedList<>(tradesBacking);
            sortedTrades.comparatorProperty().bind(marketDataTradeTable.comparatorProperty());
            marketDataTradeTable.setItems(sortedTrades);
            attachTradesListener(tradesBacking);

            time.setSortType(TableColumn.SortType.DESCENDING);
            marketDataTradeTable.getSortOrder().setAll(time);
            sortTableIfNeeded();
        });
    }

    private void scheduleFlush() {
        synchronized (pendingTrades) {
            if (flushScheduled) {
                return;
            }
            flushScheduled = true;
        }
        runFx(this::flushPendingTrades);
    }

    private void flushPendingTrades() {
        List<MarketDataMessage.Trade> replacement;
        List<MarketDataMessage.Trade> additions;
        synchronized (pendingTrades) {
            replacement = pendingReplacement;
            pendingReplacement = null;
            additions = new ArrayList<>(pendingTrades);
            pendingTrades.clear();
            flushScheduled = false;
        }

        if (replacement != null) {
            tradesBacking.setAll(trimToMaxVisible(replacement));
        }
        if (!additions.isEmpty()) {
            tradesBacking.addAll(additions);
            trimTradesBacking();
        }
        requestSort();
    }

    private void requestSort() {
        runFx(() -> {
            sortDebouncer.stop();
            sortDebouncer.playFromStart();
        });
    }

    private void sortTableIfNeeded() {
        if (!marketDataTradeTable.getSortOrder().contains(time)) {
            marketDataTradeTable.getSortOrder().setAll(time);
        }
        time.setSortType(TableColumn.SortType.DESCENDING);
        marketDataTradeTable.sort();
    }

    private void trimTradesBacking() {
        int overflow = tradesBacking.size() - MAX_VISIBLE_TRADES;
        if (overflow > 0) {
            tradesBacking.remove(0, overflow);
        }
    }

    private List<MarketDataMessage.Trade> trimToMaxVisible(Collection<MarketDataMessage.Trade> source) {
        List<MarketDataMessage.Trade> snapshot = new ArrayList<>(source);
        int fromIndex = Math.max(0, snapshot.size() - MAX_VISIBLE_TRADES);
        return new ArrayList<>(snapshot.subList(fromIndex, snapshot.size()));
    }

    private void attachTradesListener(ObservableList<MarketDataMessage.Trade> source) {
        detachTradesListener();
        boundTradesListener = c -> requestSort();
        source.addListener(boundTradesListener);
    }

    private void detachTradesListener() {
        if (boundTradesListener != null && tradesBacking != null) {
            tradesBacking.removeListener(boundTradesListener);
            boundTradesListener = null;
        }
    }

    private void runFx(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
        } else {
            Platform.runLater(action);
        }
    }

}
