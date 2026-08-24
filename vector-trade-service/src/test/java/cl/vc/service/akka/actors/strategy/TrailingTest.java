package cl.vc.service.akka.actors.strategy;

import akka.actor.ActorRef;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.service.MainApp;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test de regresión de la estrategia Trailing (trailing stop): ajusta el stopLoss al moverse
 * el precio y, al alcanzarse, dispara la orden real vía NewOrderRequest.
 *
 * FIJA EL COMPORTAMIENTO ACTUAL, no el ideal. En particular documenta comportamiento hoy
 * inseguro/asimétrico:
 *  - BUY es un trailing-stop de compra correcto: stop = min(ask) + limit; dispara cuando el
 *    ask sube 'limit' por encima del mínimo ask visto.
 *  - SELL usa stop = max(bid) + limit (suma, no resta), que SIEMPRE queda por encima del bid;
 *    por eso la rama SELL dispara en la PRIMERA estadística (bidPx <= stop siempre verdadero).
 *  - El precio del stop se recotiza vía Envelope publicado en el MessageEventBus con
 *    ExecutionType.EXEC_REPLACED cada vez que cambia.
 */
class TrailingTest {

    private static final RoutingMessage.SecurityExchangeRouting EXCH =
            RoutingMessage.SecurityExchangeRouting.XSGO;

    private RoutingMessage.Order order(RoutingMessage.Side side, double limit,
                                       RoutingMessage.OrderStatus st) {
        return RoutingMessage.Order.newBuilder()
                .setId("trail1").setSymbol("CFMITNIPSA")
                .setSide(side)
                .setOrderQty(100_000).setMaxFloor(10_000).setLimit(limit)
                .setOrdStatus(st)
                .setSecurityExchange(EXCH).setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setPrice(0d).setLeaves(100_000).setCumQty(0)
                .setSpread(7d)
                .build();
    }

    private MarketDataMessage.Statistic stat(double bidPx, double askPx) {
        return MarketDataMessage.Statistic.newBuilder()
                .setId("trail1").setSymbol("CFMITNIPSA")
                .setBidPx(bidPx).setAskPx(askPx)
                .build();
    }

    private static RoutingMessage.Order asOrder(Object m) {
        assertInstanceOf(RoutingMessage.Order.class, m);
        return (RoutingMessage.Order) m;
    }

    private static class Harness implements AutoCloseable {
        final MockedStatic<MainApp> mainApp = mockStatic(MainApp.class);
        final NettyProtobufClient conn = mock(NettyProtobufClient.class);
        final MessageEventBus bus = mock(MessageEventBus.class);
        final ActorRef actorStrategy = mock(ActorRef.class);
        final ActorRef group = mock(ActorRef.class);
        final List<Message> sent = new ArrayList<>();          // enviados a OUCH vía conn
        final List<Object> published = new ArrayList<>();      // payloads publicados al bus

        Harness() {
            Map<RoutingMessage.SecurityExchangeRouting, NettyProtobufClient> conns =
                    new EnumMap<>(RoutingMessage.SecurityExchangeRouting.class);
            conns.put(EXCH, conn);
            mainApp.when(MainApp::getConnections).thenReturn(conns);
            mainApp.when(MainApp::getMessageEventBus).thenReturn(bus);
            doAnswer(i -> { sent.add(i.getArgument(0)); return null; })
                    .when(conn).sendMessage(any(Message.class));
            doAnswer(i -> { published.add(((Envelope) i.getArgument(0)).getPayload()); return null; })
                    .when(bus).publish(any(Envelope.class));
        }

        Trailing newTrailing(RoutingMessage.Order o) {
            return new Trailing(o, actorStrategy, "sub", mock(Logger.class), group);
        }

        Object lastPublished() { return published.get(published.size() - 1); }
        Message lastSent() { return sent.get(sent.size() - 1); }

        public void close() { mainApp.close(); }
    }

    // ---- Constructor -------------------------------------------------------

    @Test
    void pendingNewConLimiteCeroRechazaYMataElActor() {
        try (Harness h = new Harness()) {
            h.newTrailing(order(RoutingMessage.Side.BUY, 0d, RoutingMessage.OrderStatus.PENDING_NEW));

            // Se publica el rechazo con el texto y el precio == spread.
            RoutingMessage.Order rej = asOrder(h.lastPublished());
            assertEquals(RoutingMessage.OrderStatus.REJECTED, rej.getOrdStatus());
            assertEquals(RoutingMessage.ExecutionType.EXEC_REJECTED, rej.getExecType());
            assertEquals("el limite tiene que ser mayor que cero", rej.getText());
            assertEquals(7d, rej.getPrice(), 1e-9);

            // onOrders(REJECTED) desemboca en PoisonPill al actor y notificación al grupo.
            verify(h.actorStrategy).tell(any(akka.actor.PoisonPill.class), any());
            verify(h.group).tell(any(RoutingMessage.Order.class), any());
            assertTrue(h.sent.isEmpty(), "no debe salir nada a OUCH en el rechazo");
        }
    }

    @Test
    void pendingNewValidoConfirmaExecNewConPrecioCero() {
        try (Harness h = new Harness()) {
            h.newTrailing(order(RoutingMessage.Side.BUY, 10d, RoutingMessage.OrderStatus.PENDING_NEW));

            assertEquals(1, h.published.size());
            RoutingMessage.Order ack = asOrder(h.lastPublished());
            assertEquals(RoutingMessage.OrderStatus.PENDING_NEW, ack.getOrdStatus());
            assertEquals(RoutingMessage.ExecutionType.EXEC_NEW, ack.getExecType());
            assertEquals(0d, ack.getPrice(), 1e-9);
            assertTrue(h.sent.isEmpty());
        }
    }

    // ---- BUY: trailing-stop de compra --------------------------------------

    @Test
    void buyRecotizaElStopAlBajarElAskYNoDisparaAntesDeTiempo() {
        try (Harness h = new Harness()) {
            Trailing t = h.newTrailing(order(RoutingMessage.Side.BUY, 10d, RoutingMessage.OrderStatus.NEW));

            // Primer tick: minAsk=100 -> stop=110. Recotiza; ask(100) < stop(110), no dispara.
            t.onStatistic(stat(99d, 100d));
            assertEquals(110d, asOrder(h.lastPublished()).getPrice(), 1e-9);
            assertEquals(RoutingMessage.ExecutionType.EXEC_REPLACED, asOrder(h.lastPublished()).getExecType());
            assertTrue(h.sent.isEmpty(), "el ask aún no alcanzó el stop");

            // El ask baja a 95: minAsk=95 -> stop=105. Sigue sin disparar.
            t.onStatistic(stat(94d, 95d));
            assertEquals(105d, asOrder(h.lastPublished()).getPrice(), 1e-9);
            assertTrue(h.sent.isEmpty());
        }
    }

    @Test
    void buyDisparaCuandoElAskSubeLimitePorEncimaDelMinimo() {
        try (Harness h = new Harness()) {
            Trailing t = h.newTrailing(order(RoutingMessage.Side.BUY, 10d, RoutingMessage.OrderStatus.NEW));

            t.onStatistic(stat(94d, 95d));     // minAsk=95, stop=105
            assertTrue(h.sent.isEmpty());

            // El ask sube a 105 == stop -> dispara la compra real al precio del stop.
            t.onStatistic(stat(104d, 105d));
            assertEquals(1, h.sent.size());
            RoutingMessage.NewOrderRequest req = (RoutingMessage.NewOrderRequest) h.lastSent();
            assertEquals(105d, req.getOrder().getPrice(), 1e-9);

            // blockOrders queda activo: un nuevo tick por encima no reenvía.
            t.onStatistic(stat(110d, 111d));
            assertEquals(1, h.sent.size(), "blockOrders impide un segundo disparo");
        }
    }

    // ---- SELL: comportamiento actual (dispara de inmediato) ----------------

    @Test
    void sellDisparaEnLaPrimeraEstadistica_comportamientoActual() {
        try (Harness h = new Harness()) {
            Trailing t = h.newTrailing(order(RoutingMessage.Side.SELL, 10d, RoutingMessage.OrderStatus.NEW));

            // stop = maxBid(100) + limit(10) = 110, SIEMPRE > bid -> dispara ya en el 1er tick.
            t.onStatistic(stat(100d, 101d));

            assertEquals(1, h.sent.size(), "la rama SELL dispara de inmediato (stop = bid + limit)");
            RoutingMessage.NewOrderRequest req = (RoutingMessage.NewOrderRequest) h.lastSent();
            assertEquals(110d, req.getOrder().getPrice(), 1e-9);
            assertEquals(110d, asOrder(h.lastPublished()).getPrice(), 1e-9);
        }
    }

    // ---- Cancelaciones -----------------------------------------------------

    @Test
    void cancelEnPendingNewSeResuelveLocalmenteSinTocarOuch() {
        try (Harness h = new Harness()) {
            Trailing t = h.newTrailing(order(RoutingMessage.Side.BUY, 10d, RoutingMessage.OrderStatus.PENDING_NEW));
            h.published.clear();

            t.onCancelRequest(RoutingMessage.OrderCancelRequest.newBuilder().setId("trail1").build());

            RoutingMessage.Order c = asOrder(h.lastPublished());
            assertEquals(RoutingMessage.OrderStatus.CANCELED, c.getOrdStatus());
            assertEquals(RoutingMessage.ExecutionType.EXEC_CANCELED, c.getExecType());
            assertTrue(h.sent.isEmpty(), "PENDING_NEW se cancela sin enviar a OUCH");
        }
    }

    @Test
    void cancelVivaSeReenviaAOuch() {
        try (Harness h = new Harness()) {
            Trailing t = h.newTrailing(order(RoutingMessage.Side.BUY, 10d, RoutingMessage.OrderStatus.NEW));

            RoutingMessage.OrderCancelRequest cancel =
                    RoutingMessage.OrderCancelRequest.newBuilder().setId("trail1").build();
            t.onCancelRequest(cancel);

            assertEquals(1, h.sent.size());
            assertSame(cancel, h.lastSent(), "la cancelación de una orden viva se enruta a OUCH");
        }
    }

    // ---- Ciclo de vida / rechazos -----------------------------------------

    @Test
    void filledDesuscribeYMataElActor() {
        try (Harness h = new Harness()) {
            Trailing t = h.newTrailing(order(RoutingMessage.Side.BUY, 10d, RoutingMessage.OrderStatus.NEW));

            RoutingMessage.Order filled = order(
                    RoutingMessage.Side.BUY, 10d, RoutingMessage.OrderStatus.FILLED).toBuilder()
                    .setCumQty(100_000)
                    .setLeaves(0)
                    .build();
            t.onOrders(filled);

            verify(h.bus).unsubscribe(h.actorStrategy, "sub");
            verify(h.bus).unsubscribe(h.actorStrategy, "trail1");
            verify(h.actorStrategy).tell(any(akka.actor.PoisonPill.class), any());
            verify(h.group).tell(any(RoutingMessage.Order.class), any());
        }
    }

    @Test
    void cincoRechazosCancelanLaOrdenYMatanElActor() {
        try (Harness h = new Harness()) {
            Trailing t = h.newTrailing(order(RoutingMessage.Side.SELL, 10d, RoutingMessage.OrderStatus.NEW));

            RoutingMessage.OrderCancelReject rej =
                    RoutingMessage.OrderCancelReject.newBuilder().setId("trail1").build();

            for (int i = 0; i < 4; i++) t.onRejected(rej);
            assertTrue(h.sent.isEmpty(), "aún no llega al umbral de 5 rechazos");

            t.onRejected(rej); // quinto
            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.OrderCancelRequest.class, h.lastSent());
            assertEquals("trail1", ((RoutingMessage.OrderCancelRequest) h.lastSent()).getId());
            verify(h.bus).unsubscribe(h.actorStrategy, "sub");
            verify(h.actorStrategy).tell(any(akka.actor.PoisonPill.class), any());
        }
    }
}
