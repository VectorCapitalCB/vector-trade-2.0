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
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test de regresión de la estrategia OCO (one-cancels-other).
 *
 * OCO usa dos disparadores sobre la MISMA orden:
 *   - limit  = STOP LOSS
 *   - spread = TAKE PROFIT
 * BUY:  stopLoss cuando ask >= limit ; takeProfit cuando ask <= spread.
 * SELL: stopLoss cuando bid <= limit ; takeProfit cuando bid >= spread.
 * Al disparar UNA pata, blockOrders=true impide que la otra dispare (la "cancela").
 *
 * Estos tests FIJAN el comportamiento ACTUAL de src/.../strategy/Oco.java, incluidas
 * dos rarezas hoy vigentes:
 *   1) En el alta feliz (PENDING_NEW válida) se publica al front la orden ORIGINAL
 *      (no la versión EXEC_NEW/price=0 que se guarda en this.order).
 *
 * La rareza que existía en onReplace (BUY inválido publicaba la ORDEN y no el reject, a
 * diferencia de la rama SELL) se corrigió: ambas ramas publican el OrderCancelReject.
 */
class OcoTest {

    private static final RoutingMessage.SecurityExchangeRouting EXCH =
            RoutingMessage.SecurityExchangeRouting.XSGO;

    private MarketDataMessage.Statistic stat(double bid, double ask) {
        return MarketDataMessage.Statistic.newBuilder()
                .setSymbol("ITAUCL")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setBidPx(bid).setAskPx(ask).setLast(bid)
                .build();
    }

    private RoutingMessage.Order.Builder base(RoutingMessage.Side side) {
        return RoutingMessage.Order.newBuilder()
                .setId("oco1").setSymbol("ITAUCL")
                .setSide(side)
                .setOrderQty(100).setLimit(0).setSpread(0)
                .setSecurityExchange(EXCH).setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setLeaves(100).setCumQty(0);
    }

    /** Orden ya viva (NEW): el constructor no dispara nada, sólo reacciona a onStatistic. */
    private RoutingMessage.Order liveBuy(double limit, double spread) {
        return base(RoutingMessage.Side.BUY)
                .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                .setLimit(limit).setSpread(spread).setPrice(100).build();
    }

    private RoutingMessage.Order liveSell(double limit, double spread) {
        return base(RoutingMessage.Side.SELL)
                .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                .setLimit(limit).setSpread(spread).setPrice(100).build();
    }

    private double sentPrice(Message m) {
        if (m instanceof RoutingMessage.NewOrderRequest) {
            return ((RoutingMessage.NewOrderRequest) m).getOrder().getPrice();
        }
        return Double.NaN;
    }

    private String sentText(Message m) {
        return ((RoutingMessage.NewOrderRequest) m).getOrder().getText();
    }

    /** Estáticos (MainApp) mockeados + captura de envíos al exchange y publicaciones al bus. */
    private static class Harness implements AutoCloseable {
        final MockedStatic<MainApp> mainApp = mockStatic(MainApp.class);
        final NettyProtobufClient conn = mock(NettyProtobufClient.class);
        final MessageEventBus bus = mock(MessageEventBus.class);
        final ActorRef strategy = mock(ActorRef.class);
        final ActorRef session = mock(ActorRef.class);
        final ActorRef group = mock(ActorRef.class);
        final List<Message> sent = new ArrayList<>();

        Harness() {
            Map<RoutingMessage.SecurityExchangeRouting, NettyProtobufClient> conns =
                    new EnumMap<>(RoutingMessage.SecurityExchangeRouting.class);
            conns.put(EXCH, conn);
            mainApp.when(MainApp::getConnections).thenReturn(conns);
            mainApp.when(MainApp::getMessageEventBus).thenReturn(bus);
            doAnswer(i -> { sent.add(i.getArgument(0)); return null; })
                    .when(conn).sendMessage(any(Message.class));
        }

        Oco strat(RoutingMessage.Order order) {
            return new Oco(order, strategy, "sub", session, mock(Logger.class), group);
        }

        Message last() { return sent.get(sent.size() - 1); }

        public void close() { mainApp.close(); }
    }

    // ------------------------------------------------------------------
    // Alta / construcción
    // ------------------------------------------------------------------

    /** BUY con take profit (spread) por encima del stop loss (limit) -> REJECTED al alta. */
    @Test
    void altaBuyTakeProfitMayorQueStopLoss_rechaza() {
        try (Harness h = new Harness()) {
            RoutingMessage.Order o = base(RoutingMessage.Side.BUY)
                    .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                    .setLimit(90).setSpread(110).setPrice(0).build();
            h.strat(o);

            ArgumentCaptor<Envelope> cap = ArgumentCaptor.forClass(Envelope.class);
            verify(h.bus, atLeastOnce()).publish(cap.capture());
            RoutingMessage.Order pub = (RoutingMessage.Order) cap.getValue().getPayload();
            assertEquals(RoutingMessage.OrderStatus.REJECTED, pub.getOrdStatus());
            assertEquals(RoutingMessage.ExecutionType.EXEC_REJECTED, pub.getExecType());
            assertEquals("el take profit es menos que el stop loss", pub.getText());
            assertEquals(110.0, pub.getPrice(), 1e-9, "price=spread en el rechazo");
            assertTrue(h.sent.isEmpty(), "un alta rechazada no envía nada al exchange");
        }
    }

    /** SELL con take profit (spread) por debajo del stop loss (limit) -> REJECTED al alta. */
    @Test
    void altaSellTakeProfitMenorQueStopLoss_rechaza() {
        try (Harness h = new Harness()) {
            RoutingMessage.Order o = base(RoutingMessage.Side.SELL)
                    .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                    .setLimit(110).setSpread(90).setPrice(0).build();
            h.strat(o);

            ArgumentCaptor<Envelope> cap = ArgumentCaptor.forClass(Envelope.class);
            verify(h.bus, atLeastOnce()).publish(cap.capture());
            RoutingMessage.Order pub = (RoutingMessage.Order) cap.getValue().getPayload();
            assertEquals(RoutingMessage.OrderStatus.REJECTED, pub.getOrdStatus());
            assertEquals("el take profit es mayor que el stop loss", pub.getText());
        }
    }

    /**
     * Alta VÁLIDA: hoy this.order pasa a EXEC_NEW/price=0, pero al bus se publica la orden
     * ORIGINAL (price y status como llegaron). Se fija esta rareza como comportamiento actual.
     */
    @Test
    void altaValidaPublicaLaOrdenOriginalNoLaModificada() {
        try (Harness h = new Harness()) {
            RoutingMessage.Order o = base(RoutingMessage.Side.BUY)
                    .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                    .setLimit(110).setSpread(90).setPrice(123.45).build();
            h.strat(o);

            ArgumentCaptor<Envelope> cap = ArgumentCaptor.forClass(Envelope.class);
            verify(h.bus).publish(cap.capture());
            RoutingMessage.Order pub = (RoutingMessage.Order) cap.getValue().getPayload();
            assertEquals(123.45, pub.getPrice(), 1e-9,
                    "se publica el price ORIGINAL (123.45), no el price=0 de this.order");
            assertEquals(RoutingMessage.OrderStatus.PENDING_NEW, pub.getOrdStatus());
            assertTrue(h.sent.isEmpty());
        }
    }

    // ------------------------------------------------------------------
    // Núcleo OCO: una pata cancela la otra (blockOrders)
    // ------------------------------------------------------------------

    /** BUY: dispara TAKE PROFIT (ask<=spread) y luego el STOP LOSS ya NO puede disparar. */
    @Test
    void buyTakeProfitDisparaYCancelaStopLoss() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(110, 90)); // stopLoss@ask>=110, takeProfit@ask<=90

            s.onStatistic(stat(80, 85));       // ask=85 <= 90 -> take profit
            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.NewOrderRequest.class, h.last());
            assertEquals(85.0, sentPrice(h.last()), 1e-9);
            assertEquals("TakeProfit :D", sentText(h.last()));

            s.onStatistic(stat(115, 120));     // ask=120 >= 110 -> sería stop loss, pero está bloqueado
            assertEquals(1, h.sent.size(), "la otra pata quedó cancelada (blockOrders)");
        }
    }

    /** BUY: dispara STOP LOSS (ask>=limit) y luego el TAKE PROFIT ya NO puede disparar. */
    @Test
    void buyStopLossDisparaYCancelaTakeProfit() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(110, 90));

            s.onStatistic(stat(115, 118));     // ask=118 >= 110 -> stop loss
            assertEquals(1, h.sent.size());
            assertEquals(118.0, sentPrice(h.last()), 1e-9);
            assertEquals("StopLoss :(", sentText(h.last()));

            s.onStatistic(stat(80, 85));       // ask=85 <= 90 -> take profit bloqueado
            assertEquals(1, h.sent.size(), "la otra pata quedó cancelada");
        }
    }

    /** SELL: dispara TAKE PROFIT (bid>=spread) y cancela el STOP LOSS. */
    @Test
    void sellTakeProfitDisparaYCancelaStopLoss() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveSell(90, 110)); // stopLoss@bid<=90, takeProfit@bid>=110

            s.onStatistic(stat(115, 120));      // bid=115 >= 110 -> take profit
            assertEquals(1, h.sent.size());
            assertEquals(115.0, sentPrice(h.last()), 1e-9);
            assertEquals("TakeProfit :D", sentText(h.last()));

            s.onStatistic(stat(80, 85));        // bid=80 <= 90 -> stop loss bloqueado
            assertEquals(1, h.sent.size(), "la otra pata quedó cancelada");
        }
    }

    /** SELL: dispara STOP LOSS (bid<=limit) y cancela el TAKE PROFIT. */
    @Test
    void sellStopLossDisparaYCancelaTakeProfit() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveSell(90, 110));

            s.onStatistic(stat(85, 88));        // bid=85 <= 90 -> stop loss
            assertEquals(1, h.sent.size());
            assertEquals(85.0, sentPrice(h.last()), 1e-9);
            assertEquals("StopLoss :(", sentText(h.last()));

            s.onStatistic(stat(115, 120));      // bid=115 >= 110 -> take profit bloqueado
            assertEquals(1, h.sent.size());
        }
    }

    /** Con limit=0 y spread=0 no hay disparadores: nunca envía nada. */
    @Test
    void sinDisparadoresNoEnvia() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(0, 0));
            s.onStatistic(stat(80, 85));
            s.onStatistic(stat(200, 210));
            assertTrue(h.sent.isEmpty());
        }
    }

    // ------------------------------------------------------------------
    // Cancelaciones
    // ------------------------------------------------------------------

    /** Cancelar una orden en PENDING_NEW se resuelve localmente (CANCELED al bus, nada al exchange). */
    // ------------------------------------------------------------------
    // Replace
    // ------------------------------------------------------------------

    /**
     * BUY: un replace con take profit (spread) por encima del stop loss (limit) debe publicar
     * el OrderCancelReject con el motivo. Regresión: la rama BUY construía el reject y publicaba
     * la orden, así que el operador nunca veía por qué no tomaba el cambio (la rama SELL sí lo
     * publicaba). Ahora las dos ramas se comportan igual.
     */
    @Test
    void replaceBuyTakeProfitMenorQueStopLoss_publicaElReject() {
        try (Harness h = new Harness()) {
            RoutingMessage.Order o = base(RoutingMessage.Side.BUY)
                    .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                    .setLimit(110).setSpread(90).setPrice(100).build();
            Oco s = h.strat(o);
            clearInvocations(h.bus);

            s.onReplace(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId(o.getId()).setQuantity(o.getOrderQty())
                    .setLimit(90).setSpread(110)   // invertidos: take profit > stop loss en BUY
                    .build());

            ArgumentCaptor<Envelope> cap = ArgumentCaptor.forClass(Envelope.class);
            verify(h.bus).publish(cap.capture());
            Object payload = cap.getValue().getPayload();
            assertInstanceOf(RoutingMessage.OrderCancelReject.class, payload,
                    "la rama BUY debe publicar el reject, no la orden");
            RoutingMessage.OrderCancelReject rej = (RoutingMessage.OrderCancelReject) payload;
            assertEquals(o.getId(), rej.getId());
            assertEquals("el take profit es menos que el stop loss", rej.getText());
            assertTrue(h.sent.isEmpty(), "un replace rechazado localmente no llega al exchange");
        }
    }

    /** SELL: la rama simétrica ya publicaba el reject; se fija para que las dos queden iguales. */
    @Test
    void replaceSellTakeProfitMayorQueStopLoss_publicaElReject() {
        try (Harness h = new Harness()) {
            RoutingMessage.Order o = base(RoutingMessage.Side.SELL)
                    .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                    .setLimit(90).setSpread(110).setPrice(100).build();
            Oco s = h.strat(o);
            clearInvocations(h.bus);

            s.onReplace(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId(o.getId()).setQuantity(o.getOrderQty())
                    .setLimit(110).setSpread(90)   // invertidos: take profit < stop loss en SELL
                    .build());

            ArgumentCaptor<Envelope> cap = ArgumentCaptor.forClass(Envelope.class);
            verify(h.bus).publish(cap.capture());
            assertInstanceOf(RoutingMessage.OrderCancelReject.class, cap.getValue().getPayload());
            assertEquals("el take profit es mayor que el stop loss",
                    ((RoutingMessage.OrderCancelReject) cap.getValue().getPayload()).getText());
        }
    }

    @Test
    void cancelEnPendingNewPublicaCanceladoLocal() {
        try (Harness h = new Harness()) {
            RoutingMessage.Order o = base(RoutingMessage.Side.BUY)
                    .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                    .setLimit(110).setSpread(90).setPrice(0).build();
            Oco s = h.strat(o);
            clearInvocations(h.bus);

            s.onCancelRequest(RoutingMessage.OrderCancelRequest.newBuilder().setId("oco1").build());

            ArgumentCaptor<Envelope> cap = ArgumentCaptor.forClass(Envelope.class);
            verify(h.bus).publish(cap.capture());
            RoutingMessage.Order pub = (RoutingMessage.Order) cap.getValue().getPayload();
            assertEquals(RoutingMessage.OrderStatus.CANCELED, pub.getOrdStatus());
            assertEquals(RoutingMessage.ExecutionType.EXEC_CANCELED, pub.getExecType());
            assertTrue(h.sent.isEmpty(), "en PENDING_NEW no se envía cancel al exchange");
        }
    }

    /** Cancelar una orden viva (NEW) reenvía el cancel al exchange. */
    @Test
    void cancelEnVivaReenviaAlExchange() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(110, 90));
            RoutingMessage.OrderCancelRequest req =
                    RoutingMessage.OrderCancelRequest.newBuilder().setId("oco1").build();
            s.onCancelRequest(req);
            assertEquals(1, h.sent.size());
            assertSame(req, h.last());
        }
    }

    // ------------------------------------------------------------------
    // onRejected
    // ------------------------------------------------------------------

    /** Un rechazo desbloquea (blockOrders=false): la pata puede volver a dispararse. */
    @Test
    void rechazoDesbloqueaYPermiteReintentar() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(110, 90));

            s.onStatistic(stat(80, 85));   // take profit -> bloquea
            assertEquals(1, h.sent.size());

            s.onRejected(RoutingMessage.OrderCancelReject.newBuilder()
                    .setId("oco1").setText("rechazado").build());

            s.onStatistic(stat(80, 85));   // desbloqueado -> vuelve a enviar
            assertEquals(2, h.sent.size(), "tras un rechazo la pata vuelve a poder disparar");
        }
    }

    /** Al 5º rechazo se cancela la orden contra el exchange (OrderCancelRequest). */
    @Test
    void cincoRechazosCancelanLaOrden() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(110, 90));
            RoutingMessage.OrderCancelReject rej = RoutingMessage.OrderCancelReject.newBuilder()
                    .setId("oco1").setText("rechazado").build();

            for (int i = 0; i < 4; i++) {
                s.onRejected(rej);
                assertTrue(h.sent.isEmpty(), "aún no cancela en el rechazo " + (i + 1));
            }
            s.onRejected(rej); // 5º
            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.OrderCancelRequest.class, h.last());
            assertEquals("oco1", ((RoutingMessage.OrderCancelRequest) h.last()).getId());
            verify(h.strategy).tell(any(akka.actor.PoisonPill.class), any());
        }
    }

    // ------------------------------------------------------------------
    // onOrders (ciclo de vida)
    // ------------------------------------------------------------------

    /** FILLED cierra la estrategia (PoisonPill) y reenvía la orden al grupo. */
    @Test
    void filledCierraEstrategiaYNotificaGrupo() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(110, 90));
            RoutingMessage.Order filled = liveBuy(110, 90).toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.FILLED)
                    .setCumQty(100)
                    .setLeaves(0)
                    .build();
            s.onOrders(filled);

            verify(h.strategy).tell(any(akka.actor.PoisonPill.class), any());
            verify(h.bus).unsubscribe(h.strategy, "sub");
            verify(h.group).tell(eq(filled), any());
        }
    }

    @Test
    void filledInconsistenteNoCierraLaEstrategia() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(110, 90));
            RoutingMessage.Order inconsistentFilled = liveBuy(110, 90).toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.FILLED)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED)
                    .setCumQty(60)
                    .setLeaves(40)
                    .build();

            s.onOrders(inconsistentFilled);

            verify(h.strategy, never()).tell(any(akka.actor.PoisonPill.class), any());
            verify(h.bus, never()).unsubscribe(h.strategy, "sub");
            verify(h.group).tell(eq(inconsistentFilled), any());
        }
    }

    /** NEW/PARTIALLY/REPLACED sólo reenvían al grupo (sin PoisonPill). */
    @Test
    void newReenviaAlGrupoSinCerrar() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveBuy(110, 90));
            RoutingMessage.Order neworder = liveBuy(110, 90).toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.NEW).build();
            s.onOrders(neworder);
            verify(h.group).tell(eq(neworder), any());
            verify(h.strategy, never()).tell(any(akka.actor.PoisonPill.class), any());
        }
    }

    // ------------------------------------------------------------------
    // onReplace
    // ------------------------------------------------------------------

    /** SELL inválido (spread<limit) en replace: publica un OrderCancelReject al front. */
    @Test
    void replaceSellInvalidoPublicaReject() {
        try (Harness h = new Harness()) {
            Oco s = h.strat(liveSell(90, 110));
            clearInvocations(h.bus);

            s.onReplace(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId("oco1").setLimit(110).setSpread(90).setQuantity(100).build());

            ArgumentCaptor<Envelope> cap = ArgumentCaptor.forClass(Envelope.class);
            verify(h.bus).publish(cap.capture());
            assertInstanceOf(RoutingMessage.OrderCancelReject.class, cap.getValue().getPayload());
            assertEquals("el take profit es mayor que el stop loss",
                    ((RoutingMessage.OrderCancelReject) cap.getValue().getPayload()).getText());
        }
    }

}
