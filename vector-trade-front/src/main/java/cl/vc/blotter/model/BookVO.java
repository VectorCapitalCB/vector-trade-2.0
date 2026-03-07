package cl.vc.blotter.model;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
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
import java.util.List;

@Data
@Slf4j
public class BookVO {


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

    private ObservableList<MarketDataMessage.Trade> tradesVO = FXCollections.observableArrayList();

    private List<String> tradesListId = new ArrayList<>();

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

            this.securityExchangeObj = subscribe.getSecurityExchange();

            statisticVO = new StatisticVO(subscribe);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public void addtrade(MarketDataMessage.Trade trade) {
        if (!tradesListId.contains(trade.getIdGenerico())) {
            tradesListId.add(trade.getIdGenerico());
            if (Platform.isFxApplicationThread()) {
                tradesVO.add(trade);
            } else {
                Platform.runLater(() -> tradesVO.add(trade));
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

                bidBook.clear();
                askBook.clear();

                incremental.getBidsList().forEach(bid -> {
                    bidBook.add(new OrderBookEntry(id,
                            bid.getPrice(),
                            bid.getSize(),
                            decimalFormat,
                            bid.getSymbol(),
                            bid.getAccount(),
                            bid.getOperator(),
                            bid.getSecurityExchange()));
                });

                incremental.getAsksList().forEach(ask -> {
                    askBook.add(new OrderBookEntry(id,
                            ask.getPrice(),
                            ask.getSize(),
                            decimalFormat,
                            ask.getSymbol(),
                            ask.getAccount(),
                            ask.getOperator(),
                            ask.getSecurityExchange()));
                });
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

                bidBook.clear();
                askBook.clear();

                snapshot.getBidsList().forEach(bid -> {
                    try {
                        bidBook.add(new OrderBookEntry(id, bid.getPrice(), bid.getSize(), decimalFormat, bid.getSymbol(), bid.getAccount(), bid.getOperator(), bid.getSecurityExchange()));
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });

                snapshot.getAsksList().forEach(ask -> {
                    try {
                        askBook.add(new OrderBookEntry(id, ask.getPrice(), ask.getSize(), decimalFormat, ask.getSymbol(), ask.getAccount(), ask.getOperator(), ask.getSecurityExchange()));
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });

                // statistic may update UI-bound properties as well
                try {
                    if (snapshot.getStatistic() != null) {
                        if (statisticVO == null) {
                            statisticVO = new StatisticVO(snapshot.getStatistic());
                        } else {
                            statisticVO.update(snapshot.getStatistic());
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

            });


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

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

    public void setSymbol(String value) {
        this.symbol.set(value);
    }

}