package cl.vc.module.protocolbuff.generator;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;

public class TopicGenerator {

    public static synchronized String getTopicMKD(MarketDataMessage.Subscribe subscribe) {
        return subscribe.getSymbol() + subscribe.getSecurityExchange().name()
                + subscribe.getSettlType().name() + subscribe.getSecurityType().name();
    }

    public static synchronized String getTopicMKD(MarketDataMessage.Subscribe.Builder subscribe) {
        return subscribe.getSymbol() + subscribe.getSecurityExchange().name()
                + subscribe.getSettlType().name() + subscribe.getSecurityType().name();
    }

    public static synchronized String getTopicMKD(String symbol, MarketDataMessage.SecurityExchangeMarketData securityExchange,
                                                  RoutingMessage.SettlType settlType,
                                                  RoutingMessage.SecurityType securityType) {
        return symbol + securityExchange.name() + settlType.name() + securityType.name();
    }

    public static synchronized String getTopicMKD(MarketDataMessage.IncrementalBook incrementalBook) {
        return incrementalBook.getSymbol() + incrementalBook.getSecurityExchange().name() + incrementalBook.getSettlType().name() + incrementalBook.getSecurityType().name() ;
    }

    public static synchronized String getTopicMKD(MarketDataMessage.Snapshot snapshot) {
        return snapshot.getSymbol() + snapshot.getSecurityExchange().name() + snapshot.getSettlType().name() + snapshot.getStatistic().getSecurityType().name() ;
    }

    public static synchronized String getTopicMKD(MarketDataMessage.Trade snapshot) {
        return snapshot.getSymbol() + snapshot.getSecurityExchange().name() + snapshot.getSettlType().name() + snapshot.getSecurityType().name() ;
    }

    public static synchronized String getTopicMKD(MarketDataMessage.TradeGeneral snapshot) {
        return snapshot.getSymbol() + snapshot.getSecurityExchange().name() + snapshot.getSettlType().name() + snapshot.getSecurityType().name() ;
    }

    public static synchronized String getTopicMKD(MarketDataMessage.Statistic statistic) {
        return statistic.getSymbol() + statistic.getSecurityExchange().name() + statistic.getSettlType().name() + statistic.getSecurityType().name() ;
    }

    public static synchronized String getTopicMKD(MarketDataMessage.BolsaStats bolsaStats) {
        return bolsaStats.getSecurityExchange().name();
    }
}
