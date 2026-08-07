package cl.vc.service.akka.actors.strategy;

import akka.actor.ActorRef;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;
import com.google.protobuf.Message;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test de la estrategia BEST centrado en el caso real CFMITNIPSA (04-jun-2026):
 * el operador SUBE el límite y la orden no se mueve ("para abajo funciona, para arriba no").
 *
 * Causa: en la rama SELL, cuando la orden ya es la mejor punta (order.price == best ask) y
 * el ask cae <= límite (operador sube el piso), el guard 'order.getPrice() != ask' impedía
 * re-cotizar. El fix re-cotiza HASTA el límite cuando order.getPrice() != order.getLimit().
 */
class BestTest {

    private static final RoutingMessage.SecurityExchangeRouting EXCH =
            RoutingMessage.SecurityExchangeRouting.XSGO;

    private MarketDataMessage.DataBook db(double price) {
        return MarketDataMessage.DataBook.newBuilder().setPrice(price).build();
    }

    private RoutingMessage.Order sell(double price, double limit, RoutingMessage.OrderStatus st) {
        return RoutingMessage.Order.newBuilder()
                .setId("best1").setSymbol("CFMITNIPSA")
                .setSide(RoutingMessage.Side.SELL)
                .setOrderQty(100000).setMaxFloor(10000).setLimit(limit)
                .setIcebergPercentage("10")
                .setOrdStatus(st)
                .setSecurityExchange(EXCH).setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CFI)
                .setPrice(price).setLeaves(100000).setCumQty(0)
                .build();
    }

    private double priceOf(Message m) {
        if (m instanceof RoutingMessage.OrderReplaceRequest) return ((RoutingMessage.OrderReplaceRequest) m).getPrice();
        if (m instanceof RoutingMessage.NewOrderRequest) return ((RoutingMessage.NewOrderRequest) m).getOrder().getPrice();
        return Double.NaN;
    }

    private RoutingMessage.Order incidentOrder() {
        return RoutingMessage.Order.newBuilder()
                .setId("let2orcnlov6").setAccount("47024924/0").setSymbol("CFIETFIPSA")
                .setSide(RoutingMessage.Side.SELL)
                .setOrderQty(300_000).setMaxFloor(30_000).setLimit(1328.0)
                .setIcebergPercentage("10")
                .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                .setExecType(RoutingMessage.ExecutionType.EXEC_NEW)
                .setSecurityExchange(EXCH).setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.ETF).setTif(RoutingMessage.Tif.DAY)
                .setPrice(1328.0).setLeaves(300_000).setCumQty(0)
                .build();
    }

    private RoutingMessage.Order incidentReport(RoutingMessage.OrderStatus status,
                                                RoutingMessage.ExecutionType execType,
                                                double reportedOrderQty,
                                                double cumQty,
                                                double leaves,
                                                double price) {
        return incidentOrder().toBuilder()
                .setOrdStatus(status)
                .setExecType(execType)
                .setOrderQty(reportedOrderQty)
                .setCumQty(cumQty)
                .setLeaves(leaves)
                .setPrice(price)
                .build();
    }

    private RoutingMessage.Order eclLogOrder() {
        return RoutingMessage.Order.newBuilder()
                .setId("-xaqfi0ujuxu8").setAccount("77193937/7").setSymbol("ECL")
                .setSide(RoutingMessage.Side.BUY)
                .setOrderQty(1_854).setMaxFloor(370).setLimit(2_000.0)
                .setIcebergPercentage("20")
                .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                .setExecType(RoutingMessage.ExecutionType.EXEC_NEW)
                .setSecurityExchange(EXCH).setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS).setTif(RoutingMessage.Tif.DAY)
                .setPrice(1_826.1).setLeaves(1_854).setCumQty(0)
                .build();
    }

    private RoutingMessage.Order eclLogReport(RoutingMessage.ExecutionType execType,
                                              double reportedOrderQty,
                                              double cumQty,
                                              double leaves,
                                              double price) {
        return eclLogOrder().toBuilder()
                .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                .setExecType(execType)
                .setOrderQty(reportedOrderQty)
                .setCumQty(cumQty)
                .setLeaves(leaves)
                .setPrice(price)
                .build();
    }

    private RoutingMessage.OrderReplaceRequest replace(Message message) {
        assertInstanceOf(RoutingMessage.OrderReplaceRequest.class, message);
        return (RoutingMessage.OrderReplaceRequest) message;
    }

    private static class Harness implements AutoCloseable {
        final MockedStatic<MainApp> mainApp = mockStatic(MainApp.class);
        final MockedStatic<Ticks> ticks = mockStatic(Ticks.class);
        final NettyProtobufClient conn = mock(NettyProtobufClient.class);
        final MessageEventBus bus = mock(MessageEventBus.class);
        final List<Message> sent = new ArrayList<>();

        Harness() {
            Map<RoutingMessage.SecurityExchangeRouting, NettyProtobufClient> conns =
                    new EnumMap<>(RoutingMessage.SecurityExchangeRouting.class);
            conns.put(EXCH, conn);
            mainApp.when(MainApp::getConnections).thenReturn(conns);
            mainApp.when(MainApp::getMessageEventBus).thenReturn(bus);
            ticks.when(() -> Ticks.getTick(any(), any())).thenReturn(new BigDecimal("0.1"));
            doAnswer(i -> { sent.add(i.getArgument(0)); return null; })
                    .when(conn).sendMessage(any(Message.class));
        }

        Message last() { return sent.get(sent.size() - 1); }

        public void close() { ticks.close(); mainApp.close(); }
    }

    /** Libro simulado con su mejor bid/ask (2 niveles por lado). */
    private BookSnapshot book(double bestBid, double bestAsk) {
        BookSnapshot s = mock(BookSnapshot.class);
        when(s.getBid()).thenReturn(List.of(db(bestBid), db(bestBid - 1)));
        when(s.getAsk()).thenReturn(List.of(db(bestAsk), db(bestAsk + 1)));
        return s;
    }

    /**
     * Caso reportado: SELL viva a 4853.2 que ES la mejor punta de venta (ask == 4853.2).
     * El operador SUBE el límite a 4855. Antes NO re-cotizaba; ahora debe mover la orden HASTA 4855.
     */
    @Test
    void sellSube_siendoLaMejorPunta_reCotizaAlLimite() {
        try (Harness h = new Harness()) {
            Best s = new Best(sell(4853.2, 4855.0, RoutingMessage.OrderStatus.NEW), "sub",
                    mock(Logger.class), mock(ActorRef.class));

            // best ask = 4853.2 (= precio de la orden); límite subido a 4855 (> ask).
            s.onSnapshot(book(4852.0, 4853.2));

            assertFalse(h.sent.isEmpty(), "al subir el límite debe re-cotizar (antes no enviaba nada)");
            assertEquals(4855.0, priceOf(h.last()), 1e-9, "debe moverse HASTA el límite subido (4855)");
        }
    }

    /** Regresión: con ask > límite (operación normal) sigue cotizando a ask - tick. */
    @Test
    void sellNormal_sigueElAskMenosTick() {
        try (Harness h = new Harness()) {
            Best s = new Best(sell(4853.2, 4850.0, RoutingMessage.OrderStatus.NEW), "sub",
                    mock(Logger.class), mock(ActorRef.class));

            // best ask = 4854.6 (> límite 4850) -> debe cotizar a ask - tick = 4854.5.
            s.onSnapshot(book(4852.0, 4854.6));

            assertFalse(h.sent.isEmpty(), "debe re-cotizar siguiendo el ask");
            assertEquals(4854.5, priceOf(h.last()), 1e-9, "ask - tick = 4854.5");
        }
    }

    @Test
    void parcialMasDiezRepricingsConservaCantidadObjetivoOriginal() {
        try (Harness h = new Harness()) {
            Best strategy = new Best(incidentOrder(), "sub", mock(Logger.class), mock(ActorRef.class));

            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_TRADE,
                    300_000,
                    37_549,
                    262_451,
                    1334.9));

            double ask = 1335.2;
            for (int i = 0; i < 10; i++) {
                strategy.onSnapshot(book(ask - 2.0, ask));
                RoutingMessage.OrderReplaceRequest request = replace(h.last());

                assertEquals(300_000d, request.getQuantity(), 1e-9,
                        "el repricing " + i + " no debe convertir leaves en totalQty");
                assertEquals(30_000d, request.getMaxFloor(), 1e-9);

                // Reproduce el mapper OUCH defectuoso: OrderReplaced.qty trae leaves=262451.
                strategy.onOrders(incidentReport(
                        RoutingMessage.OrderStatus.REPLACED,
                        RoutingMessage.ExecutionType.EXEC_REPLACED,
                        262_451,
                        37_549,
                        262_451,
                        request.getPrice()));
                ask += 0.3;
            }

            assertEquals(10, h.sent.size());
        }
    }

    @Test
    void replayLogEclMantieneTotalYEvitaRechazoDeIceberg() {
        try (Harness h = new Harness()) {
            Best strategy = new Best(eclLogOrder(), "sub", mock(Logger.class), mock(ActorRef.class));

            // orb_ouch.log 9077-9082: leaves=1482 y luego ejecuciones de 370 y 2.
            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_REPLACED,
                    1_482, 372, 1_482, 1_826.1));
            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_TRADE,
                    1_482, 742, 1_112, 1_826.1));
            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_TRADE,
                    1_482, 744, 1_110, 1_826.1));

            strategy.onSnapshot(book(1_826.2, 1_827.0));
            assertReplace(h.last(), 1_854, 370, 1_826.3);

            // Con el total correcto, NUAM devolveria leaves=1110, no un nuevo total.
            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_REPLACED,
                    1_110, 744, 1_110, 1_826.3));
            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_TRADE,
                    1_110, 1_114, 740, 1_826.3));

            strategy.onSnapshot(book(1_826.4, 1_827.2));
            assertReplace(h.last(), 1_854, 370, 1_826.5);

            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_REPLACED,
                    740, 1_114, 740, 1_826.5));
            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_TRADE,
                    740, 1_482, 372, 1_826.5));

            strategy.onSnapshot(book(1_826.6, 1_827.4));
            assertReplace(h.last(), 1_854, 370, 1_826.7);

            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_REPLACED,
                    372, 1_482, 372, 1_826.7));
            strategy.onOrders(eclLogReport(RoutingMessage.ExecutionType.EXEC_TRADE,
                    372, 1_485, 369, 1_826.7));

            strategy.onSnapshot(book(1_826.8, 1_827.6));
            assertReplace(h.last(), 1_854, 369, 1_826.9);
            assertEquals(4, h.sent.size());
        }
    }

    private void assertReplace(Message message, double totalQty, double displayQty, double price) {
        RoutingMessage.OrderReplaceRequest request = replace(message);
        assertEquals(totalQty, request.getQuantity(), 1e-9);
        assertEquals(displayQty, request.getMaxFloor(), 1e-9);
        assertEquals(price, request.getPrice(), 1e-9);
        assertTrue(request.getMaxFloor() <= request.getQuantity());
    }

    @Test
    void ackReplaceParcialLiberaBloqueoYBestSigueLaPunta() {
        try (Harness h = new Harness()) {
            Best strategy = new Best(incidentOrder(), "sub", mock(Logger.class), mock(ActorRef.class));

            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_TRADE,
                    300_000,
                    39_718,
                    260_282,
                    1334.4));

            strategy.onSnapshot(book(1331.2, 1334.8));
            assertEquals(1, h.sent.size());
            assertEquals(1334.7, replace(h.last()).getPrice(), 1e-9);

            // NUAM confirma el replace manteniendo PARTIALLY_FILLED porque ya hubo ejecución.
            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_REPLACED,
                    300_000,
                    39_718,
                    260_282,
                    1334.7));

            strategy.onSnapshot(book(1330.4, 1333.7));

            assertEquals(2, h.sent.size(),
                    "BEST debe volver a cotizar después del ACK parcial");
            assertEquals(1333.6, replace(h.last()).getPrice(), 1e-9,
                    "la venta BEST debe seguir la mejor punta a ask menos un tick");
        }
    }

    @Test
    void icebergAcotaMaxFloorCuandoAlcanzaElSaldo() {
        try (Harness h = new Harness()) {
            Best strategy = new Best(incidentOrder(), "sub", mock(Logger.class), mock(ActorRef.class));
            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_TRADE,
                    300_000,
                    280_000,
                    20_000,
                    1328.0));

            strategy.onSnapshot(book(1328.0, 1330.0));
            RoutingMessage.OrderReplaceRequest request = replace(h.last());

            assertEquals(300_000d, request.getQuantity(), 1e-9);
            assertEquals(20_000d, request.getMaxFloor(), 1e-9,
                    "si maxFloor >= leaves se debe mostrar el saldo final, nunca cero");
        }
    }

    @Test
    void rechazoMantieneCantidadObjetivoYPermiteCancelar() {
        try (Harness h = new Harness()) {
            Best strategy = new Best(incidentOrder(), "sub", mock(Logger.class), mock(ActorRef.class));
            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_TRADE,
                    300_000,
                    37_549,
                    262_451,
                    1328.0));

            strategy.onSnapshot(book(1328.0, 1330.0));
            strategy.onRejected(RoutingMessage.OrderCancelReject.newBuilder()
                    .setId("let2orcnlov6").setText("replace rechazado").build());

            strategy.onSnapshot(book(1329.0, 1331.0));
            assertEquals(300_000d, replace(h.last()).getQuantity(), 1e-9);

            strategy.onRejected(RoutingMessage.OrderCancelReject.newBuilder()
                    .setId("let2orcnlov6").setText("replace rechazado").build());
            strategy.onCancelRequest(RoutingMessage.OrderCancelRequest.newBuilder()
                    .setId("let2orcnlov6").build());

            assertInstanceOf(RoutingMessage.OrderCancelRequest.class, h.last());
            assertEquals("let2orcnlov6", ((RoutingMessage.OrderCancelRequest) h.last()).getId());
        }
    }

    @Test
    void fillNoHabilitaSegundoReplaceMientrasHayUnoPendiente() {
        try (Harness h = new Harness()) {
            Best strategy = new Best(incidentOrder(), "sub", mock(Logger.class), mock(ActorRef.class));
            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_TRADE,
                    300_000,
                    37_549,
                    262_451,
                    1328.0));

            strategy.onSnapshot(book(1328.0, 1330.0));
            assertEquals(1, h.sent.size());

            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_TRADE,
                    300_000,
                    40_235,
                    259_765,
                    1328.0));
            strategy.onSnapshot(book(1329.0, 1331.0));

            assertEquals(1, h.sent.size(), "no debe existir más de un replace pendiente");
        }
    }

    @Test
    void segundoReplaceDelFrontSeRechazaSinSobrescribirElPendiente() {
        try (Harness h = new Harness()) {
            ActorRef group = mock(ActorRef.class);
            Best strategy = new Best(incidentOrder(), "sub", mock(Logger.class), group);

            strategy.onSnapshot(book(1328.0, 1330.0));
            assertEquals(1, h.sent.size());

            strategy.onReplace(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId("let2orcnlov6")
                    .setPrice(1331.0)
                    .setLimit(1328.0)
                    .setQuantity(250_000d)
                    .setMaxFloor(30_000d)
                    .setIcebergPercentage("10")
                    .build());

            assertEquals(1, h.sent.size(), "el segundo replace no debe salir hacia OUCH");
            ArgumentCaptor<RoutingMessage.OrderCancelReject> captor =
                    ArgumentCaptor.forClass(RoutingMessage.OrderCancelReject.class);
            verify(group).tell(captor.capture(), isNull());
            assertEquals("let2orcnlov6", captor.getValue().getId());
        }
    }

    @Test
    void cumQtyNoRetrocedeCuandoCambiaElReporte() {
        try (Harness h = new Harness()) {
            ActorRef group = mock(ActorRef.class);
            Best strategy = new Best(incidentOrder(), "sub", mock(Logger.class), group);

            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_TRADE,
                    300_000,
                    37_549,
                    262_451,
                    1334.9));
            strategy.onOrders(incidentReport(
                    RoutingMessage.OrderStatus.PARTIALLY_FILLED,
                    RoutingMessage.ExecutionType.EXEC_TRADE,
                    262_451,
                    2_686,
                    259_765,
                    1329.6));

            ArgumentCaptor<RoutingMessage.Order> captor = ArgumentCaptor.forClass(RoutingMessage.Order.class);
            verify(group, times(2)).tell(captor.capture(), isNull());
            RoutingMessage.Order normalized = captor.getAllValues().get(1);

            assertEquals(300_000d, normalized.getOrderQty(), 1e-9);
            assertEquals(37_549d, normalized.getCumQty(), 1e-9);
            assertEquals(262_451d, normalized.getLeaves(), 1e-9);
        }
    }
}
