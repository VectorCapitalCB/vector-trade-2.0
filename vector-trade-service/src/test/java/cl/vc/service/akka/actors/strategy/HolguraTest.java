package cl.vc.service.akka.actors.strategy;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;

/**
 * Test de regresión de la estrategia HOLGURA.
 *
 * FIJA el comportamiento ACTUAL (no el ideal). Holgura mantiene la orden en su
 * precio original y sólo la MUEVE hasta "precioOriginal +/- spread" cuando la
 * mejor punta contraria entra dentro de esa banda de holgura; luego arma un
 * scheduler que la devuelve al precio original.
 *
 * OJO (comportamiento documentado, potencialmente inseguro): en onStatistic el
 * parámetro 'statistic' hace SHADOW del campo homónimo. La DECISIÓN de precio usa
 * el ask/bid del PARÁMETRO recibido, mientras que la lectura del mapa de snapshots
 * sólo sirve para refrescar el campo this.statistic. Si el snapshot del mapa es
 * null, el NPE se traga en el catch y NO se cotiza.
 */
class HolguraTest {

    private static final RoutingMessage.SecurityExchangeRouting EXCH =
            RoutingMessage.SecurityExchangeRouting.XSGO;

    private MarketDataMessage.Statistic stat(double bid, double ask) {
        return MarketDataMessage.Statistic.newBuilder()
                .setSymbol("HOLG")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setBidPx(bid).setAskPx(ask).setLast(bid)
                .build();
    }

    /** Orden base. price=precio original; spread=holgura; risk=spread/price*100. */
    private RoutingMessage.Order order(RoutingMessage.Side side, double price, double spread,
                                       double qty, RoutingMessage.OrderStatus st) {
        return RoutingMessage.Order.newBuilder()
                .setId("holg1").setSymbol("HOLG")
                .setSide(side)
                .setOrderQty(qty).setMaxFloor(0).setSpread(spread)
                .setOrdStatus(st)
                .setSecurityExchange(EXCH).setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setPrice(price).setLeaves(qty).setCumQty(0)
                .build();
    }

    private double priceOf(Message m) {
        if (m instanceof RoutingMessage.OrderReplaceRequest) return ((RoutingMessage.OrderReplaceRequest) m).getPrice();
        if (m instanceof RoutingMessage.NewOrderRequest) return ((RoutingMessage.NewOrderRequest) m).getOrder().getPrice();
        return Double.NaN;
    }

    /** Estáticos (MainApp, Ticks, TopicGenerator) mockeados + captura de envíos al exchange. */
    private static class Harness implements AutoCloseable {
        final MockedStatic<MainApp> mainApp = mockStatic(MainApp.class);
        final MockedStatic<Ticks> ticks = mockStatic(Ticks.class);
        final MockedStatic<TopicGenerator> topic = mockStatic(TopicGenerator.class);
        final NettyProtobufClient conn = mock(NettyProtobufClient.class);
        final BookSnapshot snap = mock(BookSnapshot.class);
        final MessageEventBus bus = mock(MessageEventBus.class);
        final List<Message> sent = new ArrayList<>();

        Harness() {
            Map<RoutingMessage.SecurityExchangeRouting, NettyProtobufClient> conns =
                    new EnumMap<>(RoutingMessage.SecurityExchangeRouting.class);
            conns.put(EXCH, conn);

            // El mapa siempre entrega nuestro snapshot (sin depender de la clave/TopicGenerator).
            HashMap<String, BookSnapshot> snapMap = new HashMap<>() {
                @Override
                public BookSnapshot get(Object key) {
                    return snap;
                }
            };

            mainApp.when(MainApp::getConnections).thenReturn(conns);
            mainApp.when(MainApp::getSnapshotHashMap).thenReturn(snapMap);
            mainApp.when(MainApp::getMessageEventBus).thenReturn(bus);

            // applyRulePrice = identidad (sin redondeo) para precios deterministas.
            ticks.when(() -> Ticks.applyRulePrice(any(), any())).thenAnswer(i -> i.getArgument(1));
            // getTopicMKD(Statistic) devuelve una clave fija; tolera null.
            topic.when(() -> TopicGenerator.getTopicMKD(nullable(MarketDataMessage.Statistic.class)))
                    .thenReturn("sub");

            // snapshot del mapa por defecto: libro vacío (no dispara nada al refrescar this.statistic).
            when(snap.getStatistic()).thenReturn(MarketDataMessage.Statistic.getDefaultInstance());

            doAnswer(i -> { sent.add(i.getArgument(0)); return null; })
                    .when(conn).sendMessage(any(Message.class));
        }

        Message last() { return sent.get(sent.size() - 1); }

        public void close() { topic.close(); ticks.close(); mainApp.close(); }
    }

    private Holgura build(Harness h, RoutingMessage.Order o, ActorRef strategy, ActorRef group) {
        return new Holgura(o, "sub", mock(Logger.class), strategy, group);
    }

    // ----------------------------------------------------------------------------------------
    // riskOrder() en el constructor
    // ----------------------------------------------------------------------------------------

    /** PENDING_NEW válido -> alta LIMIT al precio original. */
    @Test
    void altaPendingNewEnviaNewOrderLimitAlPrecioOriginal() {
        try (Harness h = new Harness()) {
            build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.PENDING_NEW),
                    mock(ActorRef.class), mock(ActorRef.class));

            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.NewOrderRequest.class, h.last());
            RoutingMessage.NewOrderRequest req = (RoutingMessage.NewOrderRequest) h.last();
            assertEquals(100.0, req.getOrder().getPrice(), 1e-9, "el alta va al precio original");
            assertEquals(RoutingMessage.OrdType.LIMIT, req.getOrder().getOrdType(), "Holgura fuerza LIMIT");
        }
    }

    /** price <= 0 -> rechazo "Price must not be Zero", unsubscribe y PoisonPill; sin alta. */
    @Test
    void precioCeroRechazaYNoEnviaAlta() {
        try (Harness h = new Harness()) {
            ActorRef strategy = mock(ActorRef.class);
            ActorRef group = mock(ActorRef.class);
            build(h, order(RoutingMessage.Side.BUY, 0, 1, 100, RoutingMessage.OrderStatus.PENDING_NEW),
                    strategy, group);

            assertTrue(h.sent.isEmpty(), "no debe salir alta al exchange");
            ArgumentCaptor<RoutingMessage.Order> cap = ArgumentCaptor.forClass(RoutingMessage.Order.class);
            verify(group).tell(cap.capture(), eq(ActorRef.noSender()));
            RoutingMessage.Order rej = cap.getValue();
            assertEquals(RoutingMessage.OrderStatus.REJECTED, rej.getOrdStatus());
            assertEquals(RoutingMessage.ExecutionType.EXEC_REJECTED, rej.getExecType());
            assertTrue(rej.getText().contains("Price must not be Zero"), rej.getText());
            verify(h.bus).unsubscribe(strategy, "sub");
            verify(strategy).tell(eq(PoisonPill.getInstance()), any());
        }
    }

    /** spread <= 0 -> rechazo "Spread must not be Zero". */
    @Test
    void spreadCeroRechaza() {
        try (Harness h = new Harness()) {
            ActorRef group = mock(ActorRef.class);
            build(h, order(RoutingMessage.Side.BUY, 100, 0, 100, RoutingMessage.OrderStatus.PENDING_NEW),
                    mock(ActorRef.class), group);

            assertTrue(h.sent.isEmpty());
            ArgumentCaptor<RoutingMessage.Order> cap = ArgumentCaptor.forClass(RoutingMessage.Order.class);
            verify(group).tell(cap.capture(), any());
            assertTrue(cap.getValue().getText().contains("Spread must not be Zero"), cap.getValue().getText());
            assertEquals(RoutingMessage.OrderStatus.REJECTED, cap.getValue().getOrdStatus());
        }
    }

    /** spread/price*100 > 1 -> rechazo por riesgo. */
    @Test
    void spreadSobreRangoDeRiesgoRechaza() {
        try (Harness h = new Harness()) {
            ActorRef group = mock(ActorRef.class);
            build(h, order(RoutingMessage.Side.BUY, 100, 2, 100, RoutingMessage.OrderStatus.PENDING_NEW),
                    mock(ActorRef.class), group);

            assertTrue(h.sent.isEmpty());
            ArgumentCaptor<RoutingMessage.Order> cap = ArgumentCaptor.forClass(RoutingMessage.Order.class);
            verify(group).tell(cap.capture(), any());
            assertTrue(cap.getValue().getText().contains("Spread sobre el rango de riesgo"), cap.getValue().getText());
        }
    }

    // ----------------------------------------------------------------------------------------
    // onStatistic() : banda de holgura
    // ----------------------------------------------------------------------------------------

    /** BUY: ask entra en la banda (orig < ask <= orig+spread) -> mueve HASTA orig+spread. */
    @Test
    void buyAskEnLaBandaMueveHastaOrigMasSpread() {
        try (Harness h = new Harness()) {
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    mock(ActorRef.class), mock(ActorRef.class));

            assertTrue(h.sent.isEmpty(), "NEW válido no dispara alta");
            s.onStatistic(stat(99, 100.5)); // ask=100.5 dentro de (100, 101]

            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.OrderReplaceRequest.class, h.last());
            assertEquals(101.0, priceOf(h.last()), 1e-9, "mueve hasta orig+spread = 101");
            assertEquals(100.0, ((RoutingMessage.OrderReplaceRequest) h.last()).getQuantity(), 1e-9);
        }
    }

    /** BUY: ask por debajo del original -> no se mueve. */
    @Test
    void buyAskBajoElOriginalNoMueve() {
        try (Harness h = new Harness()) {
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    mock(ActorRef.class), mock(ActorRef.class));

            s.onStatistic(stat(99, 99.9)); // ask <= original
            assertTrue(h.sent.isEmpty(), "sin cruzar la banda no re-cotiza");
        }
    }

    /** BUY: ask por encima de la banda (> orig+spread) -> no se mueve. */
    @Test
    void buyAskSobreLaBandaNoMueve() {
        try (Harness h = new Harness()) {
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    mock(ActorRef.class), mock(ActorRef.class));

            s.onStatistic(stat(99, 102)); // ask=102 > orig+spread=101
            assertTrue(h.sent.isEmpty());
        }
    }

    /** SELL: bid entra en la banda (orig-spread <= bid < orig) -> mueve HASTA orig-spread. */
    @Test
    void sellBidEnLaBandaMueveHastaOrigMenosSpread() {
        try (Harness h = new Harness()) {
            Holgura s = build(h, order(RoutingMessage.Side.SELL, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    mock(ActorRef.class), mock(ActorRef.class));

            s.onStatistic(stat(99.5, 101)); // bid=99.5 dentro de [99, 100)
            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.OrderReplaceRequest.class, h.last());
            assertEquals(99.0, priceOf(h.last()), 1e-9, "mueve hasta orig-spread = 99");
        }
    }

    // ----------------------------------------------------------------------------------------
    // onReplace()
    // ----------------------------------------------------------------------------------------

    /** onReplace con spread/price > 1 -> OrderCancelReject hacia el actorStrategy, sin salir al exchange. */
    @Test
    void replaceSobreRiesgoRechazaHaciaStrategy() {
        try (Harness h = new Harness()) {
            ActorRef strategy = mock(ActorRef.class);
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    strategy, mock(ActorRef.class));

            s.onReplace(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId("holg1").setPrice(100).setSpread(2).setQuantity(100).build());

            assertTrue(h.sent.isEmpty(), "el replace riesgoso no debe salir a OUCH");
            ArgumentCaptor<RoutingMessage.OrderCancelReject> cap =
                    ArgumentCaptor.forClass(RoutingMessage.OrderCancelReject.class);
            verify(strategy).tell(cap.capture(), any());
            assertEquals("holg1", cap.getValue().getId());
            assertTrue(cap.getValue().getText().contains("rango de riesgo"), cap.getValue().getText());
        }
    }

    /** onReplace normal (sin bloqueo) -> reenvía el replace efectivo al exchange. */
    @Test
    void replaceNormalSaleAlExchange() {
        try (Harness h = new Harness()) {
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    mock(ActorRef.class), mock(ActorRef.class));

            s.onReplace(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId("holg1").setPrice(100.5).setSpread(1).setQuantity(100).build());

            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.OrderReplaceRequest.class, h.last());
            assertEquals(100.5, priceOf(h.last()), 1e-9);
        }
    }

    // ----------------------------------------------------------------------------------------
    // onCancelRequest / onRejected / onOrders
    // ----------------------------------------------------------------------------------------

    /** onCancelRequest reenvía el cancel tal cual al exchange. */
    @Test
    void cancelRequestSeReenviaAlExchange() {
        try (Harness h = new Harness()) {
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    mock(ActorRef.class), mock(ActorRef.class));

            s.onCancelRequest(RoutingMessage.OrderCancelRequest.newBuilder().setId("holg1").build());

            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.OrderCancelRequest.class, h.last());
            assertEquals("holg1", ((RoutingMessage.OrderCancelRequest) h.last()).getId());
        }
    }

    /** onRejected: recién al 5º rechazo cancela la orden (unsubscribe + cancel + PoisonPill). */
    @Test
    void cincoRechazosCancelanLaOrden() {
        try (Harness h = new Harness()) {
            ActorRef strategy = mock(ActorRef.class);
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    strategy, mock(ActorRef.class));

            RoutingMessage.OrderCancelReject rej = RoutingMessage.OrderCancelReject.newBuilder()
                    .setId("holg1").setText("rechazo").build();

            for (int i = 0; i < 4; i++) {
                s.onRejected(rej);
                assertTrue(h.sent.isEmpty(), "antes del 5º rechazo no cancela (i=" + i + ")");
            }

            s.onRejected(rej); // 5º
            assertEquals(1, h.sent.size());
            assertInstanceOf(RoutingMessage.OrderCancelRequest.class, h.last());
            assertEquals("holg1", ((RoutingMessage.OrderCancelRequest) h.last()).getId());
            verify(h.bus).unsubscribe(strategy, "sub");
            verify(strategy).tell(eq(PoisonPill.getInstance()), any());
        }
    }

    /** onOrders FILLED -> PoisonPill al actorStrategy y reenvío al grupo. */
    @Test
    void ordenFilledEnviaPoisonPill() {
        try (Harness h = new Harness()) {
            ActorRef strategy = mock(ActorRef.class);
            ActorRef group = mock(ActorRef.class);
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    strategy, group);

            RoutingMessage.Order filled = order(RoutingMessage.Side.BUY, 100, 1, 100,
                    RoutingMessage.OrderStatus.FILLED).toBuilder()
                    .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                    .setLeaves(0).setCumQty(100).build();
            s.onOrders(filled);

            verify(strategy).tell(eq(PoisonPill.getInstance()), any());
            verify(group).tell(any(RoutingMessage.Order.class), any());
        }
    }

    /** onOrders con EXEC_PENDING_REPLACE es ignorado (early return): no toca al grupo. */
    @Test
    void ordenPendingReplaceSeIgnora() {
        try (Harness h = new Harness()) {
            ActorRef group = mock(ActorRef.class);
            Holgura s = build(h, order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW),
                    mock(ActorRef.class), group);

            s.onOrders(order(RoutingMessage.Side.BUY, 100, 1, 100, RoutingMessage.OrderStatus.NEW)
                    .toBuilder().setExecType(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE).build());

            verify(group, never()).tell(any(), any());
            assertTrue(h.sent.isEmpty());
        }
    }
}