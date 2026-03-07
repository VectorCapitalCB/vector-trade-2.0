package cl.vc.service.util;

import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import lombok.Data;
import org.redisson.misc.Hash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

@Data
public class BookSnapshot {

    private final String id;
    private final String symbol;
    private final RoutingMessage.SettlType settlType;
    private final RoutingMessage.SecurityType securityType;
    private Boolean receivedSnapshot = false;
    private final MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData;
    private List<MarketDataMessage.DataBook> bid = new ArrayList<>();
    private List<MarketDataMessage.DataBook> ask = new ArrayList<>();

    private MarketDataMessage.Trade tradeBookEmpty;
    private MarketDataMessage.IncrementalBook incrementalBookEmpty;
    private MarketDataMessage.Statistic statisticBookEmpty;

    private HashMap<String, MarketDataMessage.Trade> trades = new HashMap<>();
    private LinkedList<MarketDataMessage.Trade> tradesList = new LinkedList<>();

    private Double maxClose = 0d;
    private Double minClose = Double.POSITIVE_INFINITY;;

    private MarketDataMessage.Statistic statistic = MarketDataMessage.Statistic.newBuilder().build();

    public BookSnapshot(String id, MarketDataMessage.Subscribe subscribe) {
        this.id = id;
        this.symbol = subscribe.getSymbol();
        this.securityExchangeMarketData = subscribe.getSecurityExchange();
        this.settlType = subscribe.getSettlType();
        this.securityType = subscribe.getSecurityType();

        incrementalBookEmpty = MarketDataMessage.IncrementalBook.newBuilder()
                .setId(id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        statisticBookEmpty = MarketDataMessage.Statistic.newBuilder()
                .setId(id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        tradeBookEmpty = MarketDataMessage.Trade.newBuilder()
                .setId(id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();


    }

    public BookSnapshot(MarketDataMessage.Snapshot snapshot) {
        this.id = TopicGenerator.getTopicMKD(snapshot);
        this.symbol = snapshot.getSymbol();
        this.securityType = snapshot.getSecurityType();
        this.securityExchangeMarketData = snapshot.getSecurityExchange();
        this.settlType = snapshot.getSettlType();
        incrementalBookEmpty = MarketDataMessage.IncrementalBook.newBuilder()
                .setId( this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        statisticBookEmpty = MarketDataMessage.Statistic.newBuilder()
                .setId( this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        tradeBookEmpty = MarketDataMessage.Trade.newBuilder()
                .setId( this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();

    }

    public BookSnapshot(MarketDataMessage.Trade trade, String id) {
        this.id = id;
        this.symbol = trade.getSymbol();
        this.securityType = trade.getSecurityType();
        this.securityExchangeMarketData = trade.getSecurityExchange();
        this.settlType = trade.getSettlType();
        incrementalBookEmpty = MarketDataMessage.IncrementalBook.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        statisticBookEmpty = MarketDataMessage.Statistic.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        tradeBookEmpty = MarketDataMessage.Trade.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
    }

    public BookSnapshot(MarketDataMessage.IncrementalBook incrementalBook, String id) {
        this.id = id;
        this.symbol = incrementalBook.getSymbol();
        this.securityExchangeMarketData = incrementalBook.getSecurityExchange();
        this.settlType = incrementalBook.getSettlType();
        this.securityType = incrementalBook.getSecurityType();
        incrementalBookEmpty = MarketDataMessage.IncrementalBook.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        statisticBookEmpty = MarketDataMessage.Statistic.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        tradeBookEmpty = MarketDataMessage.Trade.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();


    }

    public BookSnapshot(MarketDataMessage.Statistic statistic, String id) {
        this.id = id;
        this.symbol = statistic.getSymbol();
        this.securityType = statistic.getSecurityType();
        this.securityExchangeMarketData = statistic.getSecurityExchange();
        this.settlType = statistic.getSettlType();
        incrementalBookEmpty = MarketDataMessage.IncrementalBook.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        statisticBookEmpty = MarketDataMessage.Statistic.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
        tradeBookEmpty = MarketDataMessage.Trade.newBuilder()
                .setId(this.id)
                .setSymbol(symbol)
                .setSettlType(settlType)
                .setSecurityExchange(securityExchangeMarketData)
                .setSecurityType(securityType)
                .build();
    }

    public synchronized void updateTrades(MarketDataMessage.Trade trade) {

        synchronized (trades){
            if(!trades.containsKey(trade.getIdGenerico())){
                trades.put(trade.getIdGenerico(), trade);
                tradesList.addLast(trade);
                if (tradesList.size() > 5000) {
                    tradesList.removeFirst();
                }
            }
        }

    }
}
