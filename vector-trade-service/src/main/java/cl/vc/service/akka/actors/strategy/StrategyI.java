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

    /**
     * true si la estrategia sigue esperando el PRIMER libro usable (sin market data) y la orden
     * aún no salió al mercado. Lo consulta ActorStrategy tras un timeout para no dejar la orden
     * en el limbo (PENDING_NEW invisible para el operador).
     */
    default boolean awaitingFirstMarketData() {
        return false;
    }

    /**
     * Rechaza la orden hacia el front por falta de market data (libro vacío / sin suscripción).
     * Se invoca desde ActorStrategy cuando awaitingFirstMarketData() sigue true tras el timeout.
     */
    default void rejectNoMarketData() {
    }

    /** Indica que la estrategia está esperando una respuesta de alta/replace/cancel. */
    default boolean isTemporarilyBlocked() {
        return false;
    }

    /** Libera únicamente el candado operativo de esta estrategia. */
    default void resumeAfterTemporaryBlock() {
    }

    /** Reinicia el contador local legado; el umbral común lo administra ActorStrategy. */
    default void resetRejectRecovery() {
    }

    /** Cancela la orden viva después del umbral común de rechazos consecutivos. */
    default void cancelAfterConsecutiveRejects(String orderId) {
        onCancelRequest(RoutingMessage.OrderCancelRequest.newBuilder().setId(orderId).build());
    }

    /** true sólo cuando el precio vivo está detenido por el límite configurado. */
    default boolean isAtConfiguredLimit() {
        return false;
    }

}
