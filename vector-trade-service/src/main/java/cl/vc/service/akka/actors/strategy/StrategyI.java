package cl.vc.service.akka.actors.strategy;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.service.util.BookSnapshot;

public interface StrategyI {

    void onSnapshot(BookSnapshot snapshot);

    void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest);

    void onCancelRequest(RoutingMessage.OrderCancelRequest orderCancelRequest);

    void onOrders(RoutingMessage.Order order);

    void onRejected(RoutingMessage.OrderCancelReject rejected);

    void onIncrementalBook(MarketDataMessage.IncrementalBook statistic);

    void onStatistic(MarketDataMessage.Statistic statistic);

}
