package cl.vc.service.akka.actors.routing;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.service.MainApp;
import cl.vc.service.util.CalculatePosition;
import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Regresión de ActorGroupPerAccount (ruteo por cuenta). Fija el comportamiento ACTUAL:
 *
 *  - newOrder: una orden NONE_STRATEGY se rutea al destino vía
 *    MainApp.getConnections().get(exchange).sendMessage(NewOrderRequest).
 *  - onOrders: la orden se correlaciona por id en ordersMap; un cancel posterior
 *    con ese id encuentra la orden guardada y sale al mismo destino.
 *  - enrichAvgPx: recalcula el AvgPx (VWAP) sólo en fills con avgPrice=0.
 *  - calculateNotional: precio×cantidad en CLP; 0 si precio o cantidad no son positivos.
 *  - findOdConflict: detecta cruce contra la propia punta opuesta (protección OD).
 *  - tradeExecutionKey / normalizeNoneStrategyReplace: helpers deterministas.
 *
 * Se construye la cuenta con margen = -1 (sin límite): LogicaPosition deja pasar toda orden,
 * así el test aísla el ruteo del cálculo de custodia/saldo. Se siembran las colecciones
 * estáticas REALES de MainApp (no mockStatic) para que el actor, que corre en el dispatcher
 * de Akka, vea el mismo estado que el hilo de test.
 */
class ActorGroupPerAccountTest {

    private static final RoutingMessage.SecurityExchangeRouting EXCH =
            RoutingMessage.SecurityExchangeRouting.XSGO;
    private static final String ACCOUNT = "12345-6";

    private static ActorSystem system;
    private static boolean prevRequiereCreasys;

    @BeforeAll
    static void boot() {
        prevRequiereCreasys = MainApp.requiereCreasys;
        MainApp.requiereCreasys = false; // desactiva cualquier ruta Creasys/SQL
        system = ActorSystem.create("agpa-test");
    }

    @AfterAll
    static void shutdown() {
        MainApp.requiereCreasys = prevRequiereCreasys;
        if (system != null) {
            system.terminate();
        }
    }

    private RoutingMessage.Order.Builder baseOrder(String id, RoutingMessage.Side side) {
        return RoutingMessage.Order.newBuilder()
                .setId(id)
                .setAccount(ACCOUNT)
                .setSymbol("AAA")
                .setSide(side)
                .setStrategyOrder(RoutingMessage.StrategyOrder.NONE_STRATEGY)
                .setSecurityExchange(EXCH)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                .setExecType(RoutingMessage.ExecutionType.EXEC_NEW)
                .setPrice(100d)
                .setOrderQty(10d)
                .setLeaves(10d)
                .setCumQty(0d);
    }

    // ---------------------------------------------------------------------
    // newOrder: ruteo al destino vía connection.sendMessage
    // ---------------------------------------------------------------------
    @Test
    void newOrder_ruteaOrdenNoneStrategyAlDestino() throws Exception {
        NettyProtobufClient conn = mock(NettyProtobufClient.class);
        MainApp.getConnections().put(EXCH, conn);
        try {
            ActorRef actor = system.actorOf(
                    ActorGroupPerAccount.props(ACCOUNT, -1.0, 1.0), "newOrder-" + System.nanoTime());

            RoutingMessage.Order order = baseOrder("nord-1", RoutingMessage.Side.BUY).build();
            actor.tell(RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build(), ActorRef.noSender());

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(conn, timeout(4000)).sendMessage(captor.capture());

            Message sent = captor.getValue();
            assertInstanceOf(RoutingMessage.NewOrderRequest.class, sent);
            assertEquals("nord-1", ((RoutingMessage.NewOrderRequest) sent).getOrder().getId());
        } finally {
            MainApp.getConnections().remove(EXCH);
        }
    }

    // ---------------------------------------------------------------------
    // onOrders: correlación por id (ordersMap). El cancel encuentra la orden guardada.
    // ---------------------------------------------------------------------
    @Test
    void onOrders_guardaPorIdYElCancelEncuentraLaOrden() throws Exception {
        NettyProtobufClient conn = mock(NettyProtobufClient.class);
        MainApp.getConnections().put(EXCH, conn);
        try {
            ActorRef actor = system.actorOf(
                    ActorGroupPerAccount.props(ACCOUNT, -1.0, 1.0), "onOrders-" + System.nanoTime());

            // 1) llega el reporte de la orden -> se indexa en ordersMap por id
            actor.tell(baseOrder("ord-77", RoutingMessage.Side.BUY).build(), ActorRef.noSender());

            // 2) cancel del mismo id: onCancelRequest lee ordersMap.get("ord-77") y lo rutea al destino
            actor.tell(RoutingMessage.OrderCancelRequest.newBuilder().setId("ord-77").build(), ActorRef.noSender());

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(conn, timeout(4000)).sendMessage(captor.capture());

            Message sent = captor.getValue();
            assertInstanceOf(RoutingMessage.OrderCancelRequest.class, sent);
            assertEquals("ord-77", ((RoutingMessage.OrderCancelRequest) sent).getId());
        } finally {
            MainApp.getConnections().remove(EXCH);
        }
    }

    @Test
    void onOrders_cancelDeIdInexistente_noRutea() throws Exception {
        NettyProtobufClient conn = mock(NettyProtobufClient.class);
        MainApp.getConnections().put(EXCH, conn);
        try {
            ActorRef actor = system.actorOf(
                    ActorGroupPerAccount.props(ACCOUNT, -1.0, 1.0), "onOrdersMiss-" + System.nanoTime());

            actor.tell(RoutingMessage.OrderCancelRequest.newBuilder().setId("no-existe").build(), ActorRef.noSender());

            // ordersMap no tiene el id -> NPE interno atrapado, nunca sale al destino.
            verify(conn, after(1200).never()).sendMessage(any(Message.class));
        } finally {
            MainApp.getConnections().remove(EXCH);
        }
    }

    // ---------------------------------------------------------------------
    // enrichAvgPx (método privado, vía reflexión sobre instancia construida con `new`)
    // ---------------------------------------------------------------------
    /**
     * Obtiene la instancia real del actor (creado vía actorOf, como exige AbstractActor)
     * para invocar por reflexión sus métodos privados de lógica pura. Sin akka-testkit se
     * accede a la Cell del LocalActorRef y a su Actor subyacente.
     */
    private ActorGroupPerAccount rawActor() throws Exception {
        ActorRef ref = system.actorOf(
                ActorGroupPerAccount.props(ACCOUNT, -1.0, 1.0), "raw-" + System.nanoTime());
        ref.tell("__start__", ActorRef.noSender()); // fuerza el arranque (UnstartedCell -> ActorCell)
        Method underlyingM = ref.getClass().getMethod("underlying");
        Object actor = null;
        for (int i = 0; i < 300 && actor == null; i++) {
            Object cell = underlyingM.invoke(ref); // se re-consulta: la Cell cambia al arrancar
            try {
                Method actorM = cell.getClass().getMethod("actor");
                actor = actorM.invoke(cell);
            } catch (NoSuchMethodException stillUnstarted) {
                actor = null;
            }
            if (actor == null) {
                Thread.sleep(10);
            }
        }
        assertNotNull(actor, "no se pudo obtener la instancia del actor");
        return (ActorGroupPerAccount) actor;
    }

    @SuppressWarnings("unchecked")
    private RoutingMessage.Order enrichAvgPx(ActorGroupPerAccount actor, RoutingMessage.Order order) throws Exception {
        Method m = ActorGroupPerAccount.class.getDeclaredMethod("enrichAvgPx", RoutingMessage.Order.class);
        m.setAccessible(true);
        return (RoutingMessage.Order) m.invoke(actor, order);
    }

    @Test
    void enrichAvgPx_noEsFill_devuelveOrdenSinTocar() throws Exception {
        RoutingMessage.Order noFill = baseOrder("x", RoutingMessage.Side.BUY)
                .setExecType(RoutingMessage.ExecutionType.EXEC_NEW)
                .setAvgPrice(0d)
                .build();
        RoutingMessage.Order out = enrichAvgPx(rawActor(), noFill);
        assertEquals(0d, out.getAvgPrice(), 1e-9);
    }

    @Test
    void enrichAvgPx_fillConAvgYaPresente_noRecalcula() throws Exception {
        RoutingMessage.Order fill = baseOrder("x", RoutingMessage.Side.BUY)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setLastPx(200d).setLastQty(5d).setCumQty(5d)
                .setAvgPrice(999d)
                .build();
        RoutingMessage.Order out = enrichAvgPx(rawActor(), fill);
        assertEquals(999d, out.getAvgPrice(), 1e-9);
    }

    @Test
    void enrichAvgPx_fillUnico_calculaVwapDelFill() throws Exception {
        RoutingMessage.Order fill = baseOrder("id1", RoutingMessage.Side.BUY)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setExecId("e1")
                .setLastPx(100d).setLastQty(10d).setCumQty(10d)
                .setAvgPrice(0d)
                .build();
        RoutingMessage.Order out = enrichAvgPx(rawActor(), fill);
        assertEquals(100d, out.getAvgPrice(), 1e-9);
    }

    @Test
    @SuppressWarnings("unchecked")
    void enrichAvgPx_multiplesFills_promedioPonderado() throws Exception {
        ActorGroupPerAccount actor = rawActor();

        // fill previo de la MISMA orden en tradesMap (100 x 10)
        RoutingMessage.Order prev = baseOrder("id1", RoutingMessage.Side.BUY)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setExecId("e1")
                .setLastPx(100d).setLastQty(10d).setCumQty(10d)
                .build();

        Field f = ActorGroupPerAccount.class.getDeclaredField("tradesMap");
        f.setAccessible(true);
        HashMap<String, RoutingMessage.Order> trades =
                (HashMap<String, RoutingMessage.Order>) f.get(actor);
        trades.put("k1", prev);

        // fill actual (110 x 10), cumQty acumulado = 20
        RoutingMessage.Order cur = baseOrder("id1", RoutingMessage.Side.BUY)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setExecId("e2")
                .setLastPx(110d).setLastQty(10d).setCumQty(20d)
                .setAvgPrice(0d)
                .build();

        RoutingMessage.Order out = enrichAvgPx(actor, cur);
        // (100*10 + 110*10) / 20 = 105
        assertEquals(105d, out.getAvgPrice(), 1e-9);
    }

    // ---------------------------------------------------------------------
    // calculateNotional (privado, sin USD -> precio*cantidad)
    // ---------------------------------------------------------------------
    private double calculateNotional(ActorGroupPerAccount actor, double price, double qty) throws Exception {
        Method m = ActorGroupPerAccount.class.getDeclaredMethod(
                "calculateNotional", String.class, RoutingMessage.SecurityExchangeRouting.class,
                double.class, double.class);
        m.setAccessible(true);
        return (double) m.invoke(actor, "AAA", EXCH, price, qty);
    }

    @Test
    void calculateNotional_clp_precioPorCantidad() throws Exception {
        MainApp.getSecurityExchangeSymbolsMaps().clear(); // asegurar que no hay override USD
        assertEquals(500d, calculateNotional(rawActor(), 100d, 5d), 1e-9);
    }

    @Test
    void calculateNotional_precioOCantidadNoPositivos_devuelveCero() throws Exception {
        ActorGroupPerAccount actor = rawActor();
        assertEquals(0d, calculateNotional(actor, 0d, 5d), 1e-9);
        assertEquals(0d, calculateNotional(actor, 100d, 0d), 1e-9);
    }

    // ---------------------------------------------------------------------
    // findOdConflict (protección OD contra la propia punta opuesta)
    // ---------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private Optional<RoutingMessage.Order> findOdConflict(ActorGroupPerAccount actor,
                                                          RoutingMessage.Order existing,
                                                          RoutingMessage.Order incoming) throws Exception {
        Field f = ActorGroupPerAccount.class.getDeclaredField("ordersMap");
        f.setAccessible(true);
        HashMap<String, RoutingMessage.Order> map =
                (HashMap<String, RoutingMessage.Order>) f.get(actor);
        map.clear();
        map.put(existing.getId(), existing);

        Method m = ActorGroupPerAccount.class.getDeclaredMethod("findOdConflict", RoutingMessage.Order.class);
        m.setAccessible(true);
        return (Optional<RoutingMessage.Order>) m.invoke(actor, incoming);
    }

    @Test
    void findOdConflict_compraCruzaVentaPropia_detectaConflicto() throws Exception {
        RoutingMessage.Order sellVivo = baseOrder("s1", RoutingMessage.Side.SELL).setPrice(100d).build();
        RoutingMessage.Order compra = baseOrder("b1", RoutingMessage.Side.BUY).setPrice(100d).build();

        Optional<RoutingMessage.Order> conflict = findOdConflict(rawActor(), sellVivo, compra);
        assertTrue(conflict.isPresent());
        assertEquals("s1", conflict.get().getId());
    }

    @Test
    void findOdConflict_compraQueNoCruza_sinConflicto() throws Exception {
        RoutingMessage.Order sellVivo = baseOrder("s1", RoutingMessage.Side.SELL).setPrice(100d).build();
        // compra a 50 no cruza una venta a 100
        RoutingMessage.Order compra = baseOrder("b1", RoutingMessage.Side.BUY).setPrice(50d).build();

        Optional<RoutingMessage.Order> conflict = findOdConflict(rawActor(), sellVivo, compra);
        assertTrue(conflict.isEmpty());
    }

    @Test
    void findOdConflict_ordenCanceladaNoBloqueaUnaOrdenNueva() throws Exception {
        RoutingMessage.Order sellCancelada = baseOrder("s-cancel", RoutingMessage.Side.SELL)
                .setPrice(100d)
                .setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED)
                .setOrdStatus(RoutingMessage.OrderStatus.CANCELED)
                .setLeaves(0d)
                .build();
        RoutingMessage.Order compraNueva = baseOrder("b-new", RoutingMessage.Side.BUY)
                .setPrice(100d)
                .build();

        Optional<RoutingMessage.Order> conflict = findOdConflict(rawActor(), sellCancelada, compraNueva);
        assertTrue(conflict.isEmpty());
    }

    private boolean shouldIgnoreStaleOrderUpdate(ActorGroupPerAccount actor,
                                                  RoutingMessage.Order previous,
                                                  RoutingMessage.Order incoming) throws Exception {
        Method m = ActorGroupPerAccount.class.getDeclaredMethod(
                "shouldIgnoreStaleOrderUpdate", RoutingMessage.Order.class, RoutingMessage.Order.class);
        m.setAccessible(true);
        return (boolean) m.invoke(actor, previous, incoming);
    }

    private RoutingMessage.Order normalizeInconsistentFilledState(ActorGroupPerAccount actor,
                                                                   RoutingMessage.Order order) throws Exception {
        Method m = ActorGroupPerAccount.class.getDeclaredMethod(
                "normalizeInconsistentFilledState", RoutingMessage.Order.class);
        m.setAccessible(true);
        return (RoutingMessage.Order) m.invoke(actor, order);
    }

    private RoutingMessage.Order preserveFinalStateForLateTrade(ActorGroupPerAccount actor,
                                                                 RoutingMessage.Order previous,
                                                                 RoutingMessage.Order incoming) throws Exception {
        Method m = ActorGroupPerAccount.class.getDeclaredMethod(
                "preserveFinalStateForLateTrade", RoutingMessage.Order.class, RoutingMessage.Order.class);
        m.setAccessible(true);
        return (RoutingMessage.Order) m.invoke(actor, previous, incoming);
    }

    @Test
    void terminalCancelIgnoresPartialReportThatArrivesLate() throws Exception {
        RoutingMessage.Order canceled = baseOrder("od-stale", RoutingMessage.Side.SELL)
                .setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED)
                .setOrdStatus(RoutingMessage.OrderStatus.CANCELED)
                .setCumQty(5d)
                .setLeaves(0d)
                .build();
        RoutingMessage.Order stalePartial = canceled.toBuilder()
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                .setCumQty(5d)
                .setLeaves(5d)
                .build();

        assertTrue(shouldIgnoreStaleOrderUpdate(rawActor(), canceled, stalePartial));
    }

    @Test
    void terminalCancelAllowsOnlyAGenuinelyNewLateFill() throws Exception {
        RoutingMessage.Order canceled = baseOrder("late-fill", RoutingMessage.Side.BUY)
                .setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED)
                .setOrdStatus(RoutingMessage.OrderStatus.CANCELED)
                .setCumQty(4d)
                .setLeaves(0d)
                .build();
        RoutingMessage.Order lateFill = canceled.toBuilder()
                .setExecId("late-exec")
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                .setLastQty(2d)
                .setCumQty(6d)
                .setLeaves(4d)
                .build();

        assertFalse(shouldIgnoreStaleOrderUpdate(rawActor(), canceled, lateFill));
    }

    @Test
    void inconsistentFilledReplaceIsNormalizedWhileItStillHasLeaves() throws Exception {
        RoutingMessage.Order replaceAck = baseOrder("cencosud-real", RoutingMessage.Side.SELL)
                .setOrderQty(54_334d)
                .setCumQty(40_687d)
                .setLeaves(13_647d)
                .setExecId("61605")
                .setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED)
                .setOrdStatus(RoutingMessage.OrderStatus.FILLED)
                .build();

        RoutingMessage.Order normalized = normalizeInconsistentFilledState(rawActor(), replaceAck);

        assertEquals(RoutingMessage.OrderStatus.PARTIALLY_FILLED, normalized.getOrdStatus());
        assertEquals(40_687d, normalized.getCumQty(), 1e-9);
        assertEquals(13_647d, normalized.getLeaves(), 1e-9);
    }

    @Test
    void cencosudProductionBurstCountsEveryUniqueOutOfOrderFillWithoutRouting() throws Exception {
        ActorGroupPerAccount actor = rawActor();
        CalculatePosition calculator = new CalculatePosition(ACCOUNT);
        Map<String, BlotterMessage.Position> positions = new HashMap<>();
        String positionId = "CENCOSUD" + EXCH.name() + ACCOUNT;

        positions.put(positionId, BlotterMessage.Position.newBuilder()
                .setId(positionId)
                .setAccount(ACCOUNT)
                .setSymbol("CENCOSUD")
                .setSecurityexchange(EXCH)
                .setTradeSell(-40_687d)
                .setAuxBSell(79_749_411d)
                .setPxSell(1_960.071055d)
                .setCashSell(79_749_411d)
                .setQtyNet(-40_687d)
                .setPxNet(1_960.071055d)
                .setAmountNet(79_749_411d)
                .build());

        RoutingMessage.Order state = normalizeInconsistentFilledState(actor,
                cencosudEvent("61605", RoutingMessage.ExecutionType.EXEC_REPLACED,
                        RoutingMessage.OrderStatus.FILLED, 0d, 0d, 40_687d, 13_647d));

        String[] execIds = {"61606", "61616", "61607", "61617", "61608", "61618", "61609",
                "61619", "61610", "61611", "61612", "61613", "61614", "61615"};
        double[] lastQty = {302, 186, 2162, 986, 468, 468, 2174, 1789, 61, 186, 2162, 468, 2174, 61};
        double[] cumQty = {40989, 51091, 43151, 52077, 43619, 52545, 45793,
                54334, 45854, 46040, 48202, 48670, 50844, 50905};

        for (int i = 0; i < execIds.length; i++) {
            double leaves = 54_334d - cumQty[i];
            RoutingMessage.OrderStatus status = cumQty[i] == 54_334d
                    ? RoutingMessage.OrderStatus.FILLED
                    : RoutingMessage.OrderStatus.PARTIALLY_FILLED;
            RoutingMessage.Order fill = cencosudEvent(execIds[i], RoutingMessage.ExecutionType.EXEC_TRADE,
                    status, lastQty[i], 1_959.6d, cumQty[i], leaves);

            assertFalse(shouldIgnoreStaleOrderUpdate(actor, state, fill), "fill omitido: " + execIds[i]);
            double previousCumQty = state.getCumQty();
            RoutingMessage.Order accountingEvent = preserveFinalStateForLateTrade(actor, state, fill);
            assertTrue(accountingEvent.getCumQty() >= previousCumQty,
                    "CumQty retrocedió en fill: " + execIds[i]);
            BlotterMessage.Position position = calculator.onOrder(accountingEvent, positions);
            positions.put(position.getId(), position);
            state = accountingEvent;
        }

        BlotterMessage.Position result = positions.get(positionId);
        assertEquals(-54_334d, result.getTradeSell(), 1e-9);
        // El core redondea el precio promedio a 6 decimales antes de reconstruir el monto.
        assertEquals(106_492_072.2d, result.getCashSell(), 0.1d);
        assertEquals(1_959.952739d, result.getPxSell(), 1e-6);
        assertEquals(RoutingMessage.OrderStatus.FILLED, state.getOrdStatus());
        assertEquals(54_334d, state.getCumQty(), 1e-9);
        assertEquals(0d, state.getLeaves(), 1e-9);
    }

    private RoutingMessage.Order cencosudEvent(String execId,
                                                RoutingMessage.ExecutionType execType,
                                                RoutingMessage.OrderStatus status,
                                                double lastQty,
                                                double lastPx,
                                                double cumQty,
                                                double leaves) {
        return baseOrder("cencosud-real", RoutingMessage.Side.SELL)
                .setAccount(ACCOUNT)
                .setSymbol("CENCOSUD")
                .setOrderID("818140559209910661")
                .setClOrdId("NTY-BD7E697037C247E9")
                .setExecId(execId)
                .setOrderQty(54_334d)
                .setPrice(1_959.6d)
                .setLastQty(lastQty)
                .setLastPx(lastPx)
                .setCumQty(cumQty)
                .setLeaves(leaves)
                .setExecType(execType)
                .setOrdStatus(status)
                .build();
    }

}
