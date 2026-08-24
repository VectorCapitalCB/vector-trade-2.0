package cl.vc.blotter.model;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Data
@Slf4j
public class BookVO {

    private static final int MAX_TRADES = 200;


    private String id;

    private BigDecimal tick = BigDecimal.ZERO;
    private DecimalFormat decimalFormat;

    private DecimalFormat decimalFormatBkp = new DecimalFormat("#,##0.0000");

    private StringProperty settlType = new SimpleStringProperty();
    private StringProperty securityType = new SimpleStringProperty();
    private StringProperty securityExchange = new SimpleStringProperty();

    private StringProperty symbol = new SimpleStringProperty();

    private ObservableList<OrderBookEntry> bidBook = FXCollections.observableArrayList();
    private ObservableList<OrderBookEntry> askBook = FXCollections.observableArrayList();

    private StatisticVO statisticVO;

    /** Cantidades de la mejor punta; NaN hasta recibir profundidad real. */
    private double topBidQty = Double.NaN;
    private double topAskQty = Double.NaN;

    private ObservableList<MarketDataMessage.Trade> tradesVO = FXCollections.observableArrayList();

    private Set<String> tradesListId = new LinkedHashSet<>();

    private MarketDataMessage.SecurityExchangeMarketData securityExchangeObj;

    public BookVO(MarketDataMessage.Statistic statistic) {

        try {

            id = TopicGenerator.getTopicMKD(statistic);
            this.settlType.set(statistic.getSettlType().name());
            this.securityExchange.set(statistic.getSecurityExchange().name());
            this.securityType.set(statistic.getSecurityType().name());
            this.symbol.set(statistic.getSymbol());
            this.securityExchangeObj = statistic.getSecurityExchange();

            statisticVO = new StatisticVO(statistic);

            if (decimalFormat == null) {
                if (statistic.getBidPx() > 0d) {
                    tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getBidPx()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (statistic.getAskPx() > 0d) {
                    tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getAskPx()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (statistic.getPreviusClose() > 0d) {
                    tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getPreviusClose()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (statistic.getOhlcv().getClose() > 0d) {
                    tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getOhlcv().getClose()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }

    public BookVO(MarketDataMessage.Snapshot snapshot) {

        try {

            id = TopicGenerator.getTopicMKD(snapshot);

            this.settlType.set(snapshot.getSettlType().name());
            this.securityExchange.set(snapshot.getSecurityExchange().name());
            this.securityType.set(snapshot.getSecurityType().name());
            this.symbol.set(snapshot.getStatistic().getSymbol());

            statisticVO = new StatisticVO(snapshot.getStatistic());

            this.securityExchangeObj = snapshot.getSecurityExchange();

            if (decimalFormat == null) {

                if (!snapshot.getBidsList().isEmpty() && snapshot.getBidsList().get(0).getPrice() > 0d) {
                    tick = Ticks.conversorExdestination(snapshot.getSecurityExchange(), BigDecimal.valueOf(snapshot.getBidsList().get(0).getPrice()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (!snapshot.getAsksList().isEmpty() && snapshot.getAsksList().get(0).getPrice() > 0d) {
                    tick = Ticks.conversorExdestination(snapshot.getSecurityExchange(), BigDecimal.valueOf(snapshot.getAsksList().get(0).getPrice()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (statisticVO.getStatistic().getBidPx() > 0d) {
                    tick = Ticks.conversorExdestination(snapshot.getSecurityExchange(), BigDecimal.valueOf(statisticVO.getStatistic().getBidPx()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (statisticVO.getStatistic().getAskPx() > 0d) {
                    tick = Ticks.conversorExdestination(statisticVO.getStatistic().getSecurityExchange(), BigDecimal.valueOf(statisticVO.getStatistic().getAskPx()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (statisticVO.getStatistic().getPreviusClose() > 0d) {
                    tick = Ticks.conversorExdestination(statisticVO.getStatistic().getSecurityExchange(), BigDecimal.valueOf(statisticVO.getStatistic().getPreviusClose()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (statisticVO.getStatistic().getOhlcv().getClose() > 0d) {
                    tick = Ticks.conversorExdestination(statisticVO.getStatistic().getSecurityExchange(), BigDecimal.valueOf(statisticVO.getStatistic().getOhlcv().getClose()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                }
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public BookVO(MarketDataMessage.Subscribe subscribe) {

        try {

            id = TopicGenerator.getTopicMKD(subscribe);

            this.settlType.set(subscribe.getSettlType().name());
            this.securityExchange.set(subscribe.getSecurityExchange().name());
            this.securityType.set(subscribe.getSecurityType().name());
            this.symbol.set(subscribe.getSymbol());

            this.securityExchangeObj = subscribe.getSecurityExchange();

            statisticVO = new StatisticVO(subscribe);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public void addtrade(MarketDataMessage.Trade trade) {
        if (trade == null) {
            return;
        }

        String tradeId = trade.getIdGenerico();
        if (tradeId == null || tradeId.isBlank() || !tradesListId.add(tradeId)) {
            return;
        }

        Runnable addTrade = () -> {
            tradesVO.add(trade);
            if (statisticVO != null && trade.getPrice() > 0d) {
                statisticVO.registrarTradeIntradia(trade.getPrice());
            }
            trimTrades();
        };

        if (Platform.isFxApplicationThread()) {
            addTrade.run();
        } else {
            Platform.runLater(addTrade);
        }
    }

    private void trimTrades() {
        while (tradesVO.size() > MAX_TRADES) {
            MarketDataMessage.Trade removed = tradesVO.remove(0);
            if (removed != null) {
                tradesListId.remove(removed.getIdGenerico());
            }
        }
    }


    public void update(MarketDataMessage.IncrementalBook incremental) {

        try {

            if (decimalFormat == null) {
                if (!incremental.getAsksList().isEmpty() && incremental.getAsksList().get(0).getPrice() > 0d) {
                    tick = Ticks.conversorExdestination(securityExchangeObj, BigDecimal.valueOf(incremental.getAsksList().get(0).getPrice()));
                    decimalFormat = NumberGenerator.formetByticks(tick);

                } else if (!incremental.getBidsList().isEmpty() && incremental.getBidsList().get(0).getPrice() > 0d) {
                    tick = Ticks.conversorExdestination(securityExchangeObj, BigDecimal.valueOf(incremental.getBidsList().get(0).getPrice()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                }

            }

            Runnable r = () -> {

                PriceSnapshot previousBidPrices = pricesOf(bidBook);
                PriceSnapshot previousAskPrices = pricesOf(askBook);

                topBidQty = incremental.getBidsList().isEmpty()
                        ? 0d : incremental.getBidsList().get(0).getSize();
                topAskQty = incremental.getAsksList().isEmpty()
                        ? 0d : incremental.getAsksList().get(0).getSize();

                List<OrderBookEntry> nextBidBook = new ArrayList<>(incremental.getBidsCount());
                List<OrderBookEntry> nextAskBook = new ArrayList<>(incremental.getAsksCount());

                for (int level = 0; level < incremental.getBidsCount(); level++) {
                    MarketDataMessage.DataBook bid = incremental.getBids(level);
                    OrderBookEntry entry = new OrderBookEntry(id,
                            bid.getPrice(),
                            bid.getSize(),
                            decimalFormat,
                            resolveEntrySymbol(bid.getSymbol()),
                            bid.getAccount(),
                            bid.getOperator(),
                            bid.getSecurityExchange());
                    markPriceChange(entry, previousBidPrices, level, bid.getPrice());
                    nextBidBook.add(entry);
                }

                for (int level = 0; level < incremental.getAsksCount(); level++) {
                    MarketDataMessage.DataBook ask = incremental.getAsks(level);
                    OrderBookEntry entry = new OrderBookEntry(id,
                            ask.getPrice(),
                            ask.getSize(),
                            decimalFormat,
                            resolveEntrySymbol(ask.getSymbol()),
                            ask.getAccount(),
                            ask.getOperator(),
                            ask.getSecurityExchange());
                    markPriceChange(entry, previousAskPrices, level, ask.getPrice());
                    nextAskBook.add(entry);
                }

                // El lado no viene en la entrada: se deduce de la lista a la que pertenece.
                // Lo necesita el libro para marcar los niveles donde el usuario tiene una
                // orden viva (Repository.tieneOrdenVivaEn).
                nextBidBook.forEach(e -> e.setSide(RoutingMessage.Side.BUY));
                nextAskBook.forEach(e -> e.setSide(RoutingMessage.Side.SELL));
                bidBook.setAll(nextBidBook);
                askBook.setAll(nextAskBook);
                applyTopOfBookQuantities();
            };

            if (Platform.isFxApplicationThread()) {
                r.run();
            } else {
                Platform.runLater(r);
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }


    public void updateStatistic(MarketDataMessage.Statistic statistic) {

        try {

            if (statistic == null) {
                return;
            }

            if ((getSymbol() == null || getSymbol().isBlank())
                    && statistic.getSymbol() != null && !statistic.getSymbol().isBlank()) {
                this.symbol.set(statistic.getSymbol());
            }

            if (decimalFormat == null) {
                if (statistic.getBidPx() > 0d) {
                    tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getBidPx()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                } else if (statistic.getAskPx() > 0d) {
                    tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getAskPx()));
                    decimalFormat = NumberGenerator.formetByticks(tick);
                }
            }

            if (this.statisticVO == null) {
                this.statisticVO = new StatisticVO(statistic);
            } else {
                this.statisticVO.update(statistic);
            }
            applyTopOfBookQuantities();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }


    public void update(MarketDataMessage.Snapshot snapshot) {


        try {

            snapshot.getTradesList().forEach(this::addtrade);

            // Ensure decimalFormat is initialized before creating OrderBookEntry instances
            if (decimalFormat == null) {
                try {
                    if (snapshot != null) {
                        if (!snapshot.getBidsList().isEmpty() && snapshot.getBidsList().get(0) != null && snapshot.getBidsList().get(0).getPrice() > 0d) {
                            tick = Ticks.conversorExdestination(snapshot.getSecurityExchange(), BigDecimal.valueOf(snapshot.getBidsList().get(0).getPrice()));
                            decimalFormat = NumberGenerator.formetByticks(tick);
                        } else if (!snapshot.getAsksList().isEmpty() && snapshot.getAsksList().get(0) != null && snapshot.getAsksList().get(0).getPrice() > 0d) {
                            tick = Ticks.conversorExdestination(snapshot.getSecurityExchange(), BigDecimal.valueOf(snapshot.getAsksList().get(0).getPrice()));
                            decimalFormat = NumberGenerator.formetByticks(tick);
                        } else if (statisticVO != null && snapshot.getStatistic() != null && snapshot.getStatistic().getBidPx() > 0d) {
                            tick = Ticks.conversorExdestination(snapshot.getSecurityExchange(), BigDecimal.valueOf(snapshot.getStatistic().getBidPx()));
                            decimalFormat = NumberGenerator.formetByticks(tick);
                        } else if (statisticVO != null && snapshot.getStatistic() != null && snapshot.getStatistic().getAskPx() > 0d) {
                            tick = Ticks.conversorExdestination(snapshot.getSecurityExchange(), BigDecimal.valueOf(snapshot.getStatistic().getAskPx()));
                            decimalFormat = NumberGenerator.formetByticks(tick);
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            Platform.runLater(()->{

                try {
                    if ((getSymbol() == null || getSymbol().isBlank())
                            && snapshot.getStatistic() != null
                            && snapshot.getStatistic().getSymbol() != null
                            && !snapshot.getStatistic().getSymbol().isBlank()) {
                        this.symbol.set(snapshot.getStatistic().getSymbol());
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                PriceSnapshot previousBidPrices = pricesOf(bidBook);
                PriceSnapshot previousAskPrices = pricesOf(askBook);
                List<OrderBookEntry> nextBidBook = new ArrayList<>(snapshot.getBidsCount());
                List<OrderBookEntry> nextAskBook = new ArrayList<>(snapshot.getAsksCount());

                topBidQty = snapshot.getBidsList().isEmpty()
                        ? 0d : snapshot.getBidsList().get(0).getSize();
                topAskQty = snapshot.getAsksList().isEmpty()
                        ? 0d : snapshot.getAsksList().get(0).getSize();

                for (int level = 0; level < snapshot.getBidsCount(); level++) {
                    MarketDataMessage.DataBook bid = snapshot.getBids(level);
                    try {
                        OrderBookEntry entry = new OrderBookEntry(id, bid.getPrice(), bid.getSize(), decimalFormat,
                                resolveEntrySymbol(bid.getSymbol()), bid.getAccount(), bid.getOperator(), bid.getSecurityExchange());
                        markPriceChange(entry, previousBidPrices, level, bid.getPrice());
                        nextBidBook.add(entry);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                for (int level = 0; level < snapshot.getAsksCount(); level++) {
                    MarketDataMessage.DataBook ask = snapshot.getAsks(level);
                    try {
                        OrderBookEntry entry = new OrderBookEntry(id, ask.getPrice(), ask.getSize(), decimalFormat,
                                resolveEntrySymbol(ask.getSymbol()), ask.getAccount(), ask.getOperator(), ask.getSecurityExchange());
                        markPriceChange(entry, previousAskPrices, level, ask.getPrice());
                        nextAskBook.add(entry);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                nextBidBook.forEach(e -> e.setSide(RoutingMessage.Side.BUY));
                nextAskBook.forEach(e -> e.setSide(RoutingMessage.Side.SELL));
                bidBook.setAll(nextBidBook);
                askBook.setAll(nextAskBook);

                // statistic may update UI-bound properties as well
                try {
                    if (snapshot.getStatistic() != null) {
                        if (statisticVO == null) {
                            statisticVO = new StatisticVO(snapshot.getStatistic());
                        } else {
                            statisticVO.update(snapshot.getStatistic());
                        }
                        applyTopOfBookQuantities();
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

            });


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private void applyTopOfBookQuantities() {
        Runnable apply = () -> {
            if (statisticVO == null) return;
            if (!Double.isNaN(topBidQty)) statisticVO.setBidQty(topBidQty);
            if (!Double.isNaN(topAskQty)) statisticVO.setAskQty(topAskQty);
        };
        if (Platform.isFxApplicationThread()) apply.run();
        else Platform.runLater(apply);
    }


    public String getSettlType() {
        return settlType.get();
    }

    public void setSettlType(String settlType) {
        this.settlType.set(settlType);
    }

    public StringProperty settlTypeProperty() {
        return settlType;
    }

    public String getSecurityType() {
        return securityType.get();
    }

    public void setSecurityType(String securityType) {
        this.securityType.set(securityType);
    }

    public StringProperty securityTypeProperty() {
        return securityType;
    }

    public String getSecurityExchange() {
        return securityExchange.get();
    }

    public void setSecurityExchange(String securityExchange) {
        this.securityExchange.set(securityExchange);
    }

    public StringProperty securityExchangeProperty() {
        return securityExchange;
    }

    public String getSymbol() {
        return symbol.get();
    }

    public StringProperty symbolProperty() {
        return symbol;

    }


    public void creanBook() {
        Platform.runLater(() -> {
            askBook.clear();
            bidBook.clear();
        });

    }

    static boolean shouldFlashPriceChange(List<Double> previousLevels, Set<Double> previousPrices,
                                          int level, double currentPrice) {
        if (previousLevels == null || previousLevels.isEmpty() || level < 0) {
            return false;
        }
        if (level == 0) {
            return Double.compare(previousLevels.get(0), currentPrice) != 0;
        }
        return previousPrices != null && !previousPrices.contains(currentPrice);
    }

    private static PriceSnapshot pricesOf(List<OrderBookEntry> entries) {
        List<Double> levels = new ArrayList<>(entries.size());
        entries.forEach(entry -> levels.add(entry.getPriceValue()));
        return new PriceSnapshot(levels, new LinkedHashSet<>(levels));
    }

    private static void markPriceChange(OrderBookEntry entry, PriceSnapshot previousPrices,
                                        int level, double currentPrice) {
        if (shouldFlashPriceChange(previousPrices.levels(), previousPrices.values(), level, currentPrice)) {
            entry.markPriceChanged();
        }
    }

    private record PriceSnapshot(List<Double> levels, Set<Double> values) {
    }

    private String resolveEntrySymbol(String levelSymbol) {
        return levelSymbol == null || levelSymbol.isBlank() ? getSymbol() : levelSymbol;
    }

    public void setSymbol(String value) {
        this.symbol.set(value);
    }

}
