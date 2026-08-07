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
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test de ejecución de la estrategia BASKET_PASSIVE.
 *
 * Reproduce el escenario real de ITAUCL (01-jun-2026): se ingresa una orden,
 * se inyecta la MKD simulando el mercado y se valida que la estrategia reaccione
 * según lo acordado:
 *   1) NO cruza el spread (no cotiza al/por encima del ask en BUY)  -> arregla "sube y sube".
 *   2) El maxFloor (cantidad visible) se acota a la cantidad VIVA (leaves) -> arregla TE2369.
 */
class BasketPassiveTest {

    private static final RoutingMessage.SecurityExchangeRouting EXCH =
            RoutingMessage.SecurityExchangeRouting.XSGO;

    /** Statistic (libro) simulado. */
    private MarketDataMessage.Statistic stat(double bid, double ask, double last) {
        return MarketDataMessage.Statistic.newBuilder()
                .setSymbol("ITAUCL")
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setBidPx(bid).setAskPx(ask).setLast(last)
                .build();
    }

    /** Orden BUY base: qty 105, maxFloor 21 (iceberg 20%), limit 1%. */
    private RoutingMessage.Order baseBuy() {
        return RoutingMessage.Order.newBuilder()
                .setId("ord1").setSymbol("ITAUCL")
                .setSide(RoutingMessage.Side.BUY)
                .setOrderQty(105).setMaxFloor(21).setLimit(1)
                .setIcebergPercentage("20")
                .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                .setSecurityExchange(EXCH)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setPrice(100).setLeaves(105).setCumQty(0)
                .build();
    }

    /** Ack del exchange: devuelve la orden en un estado/leaves dados. */
    private RoutingMessage.Order ack(RoutingMessage.OrderStatus st, double price, double leaves, double cum) {
        return baseBuy().toBuilder()
                .setOrdStatus(st).setPrice(price).setLeaves(leaves).setCumQty(cum)
                .build();
    }

    private double priceOf(Message m) {
        if (m instanceof RoutingMessage.OrderReplaceRequest) {
            return ((RoutingMessage.OrderReplaceRequest) m).getPrice();
        }
        if (m instanceof RoutingMessage.NewOrderRequest) {
            return ((RoutingMessage.NewOrderRequest) m).getOrder().getPrice();
        }
        return Double.NaN;
    }

    /** Marco de prueba: estáticos (MainApp, Ticks) mockeados + captura de envíos al exchange. */
    private static class Harness implements AutoCloseable {
        final MockedStatic<MainApp> mainApp = mockStatic(MainApp.class);
        final MockedStatic<Ticks> ticks = mockStatic(Ticks.class);
        final NettyProtobufClient conn = mock(NettyProtobufClient.class);
        final BookSnapshot snap = mock(BookSnapshot.class);
        final MessageEventBus bus = mock(MessageEventBus.class);
        final List<Message> sent = new ArrayList<>();

        Harness() {
            Map<RoutingMessage.SecurityExchangeRouting, NettyProtobufClient> conns =
                    new EnumMap<>(RoutingMessage.SecurityExchangeRouting.class);
            conns.put(EXCH, conn);

            // getSnapshotHashMap() devuelve un HashMap concreto -> usamos un HashMap real
            // que siempre entrega nuestro BookSnapshot (sin depender de la clave/TopicGenerator).
            HashMap<String, BookSnapshot> snapMap = new HashMap<>() {
                @Override
                public BookSnapshot get(Object key) {
                    return snap;
                }
            };

            mainApp.when(MainApp::getConnections).thenReturn(conns);
            mainApp.when(MainApp::getSnapshotHashMap).thenReturn(snapMap);
            mainApp.when(MainApp::getMessageEventBus).thenReturn(bus);

            // tick = 1 ; applyRulePrice = identidad (sin redondeo) para precios deterministas
            ticks.when(() -> Ticks.getTick(any(), any())).thenReturn(BigDecimal.ONE);
            ticks.when(() -> Ticks.applyRulePrice(any(), any())).thenAnswer(i -> i.getArgument(1));

            doAnswer(i -> { sent.add(i.getArgument(0)); return null; })
                    .when(conn).sendMessage(any(Message.class));
        }

        /** Inyecta el libro actual. */
        void market(MarketDataMessage.Statistic s) {
            when(snap.getStatistic()).thenReturn(s);
        }

        Message last() { return sent.get(sent.size() - 1); }

        public void close() { ticks.close(); mainApp.close(); }
    }

    @Test
    void buyNuncaCruzaElSpread() {
        try (Harness h = new Harness()) {
            BasketPassive s = new BasketPassive(baseBuy(), "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            // (1) Alta con bid=100 ask=110 (last=200 deja el limit holgado).
            //     Debe cotizar bid+1 = 101, dentro del spread.
            h.market(stat(100, 110, 200));
            s.onStatistic(stat(100, 110, 200));
            assertEquals(101.0, priceOf(h.last()), 1e-9, "el alta debe ir a bid+1 = 101");

            // El exchange confirma NEW @101 (leaves 105).
            s.onOrders(ack(RoutingMessage.OrderStatus.NEW, 101, 105, 0));

            // (2) El bid sube hasta tocar el ask: bid=109 ask=110.
            //     bid+1 = 110 = ASK -> con el fix debe TOPAR en ask-1 = 109 y NO cruzar.
            h.market(stat(109, 110, 200));
            s.onStatistic(stat(109, 110, 200));

            assertEquals(109.0, priceOf(h.last()), 1e-9,
                    "debe quedarse en ask-1 = 109, sin cruzar a 110");

            // Invariante global: ningún precio enviado alcanza o supera el ask (no cruza nunca).
            for (Message m : h.sent) {
                assertTrue(priceOf(m) < 110.0,
                        "un precio cruzó/alcanzó el ask: " + priceOf(m));
            }
        }
    }

    /** Conduce la estrategia hasta un replace con la orden en PARTIALLY_FILLED y el leaves dado,
     *  y devuelve el maxFloor del replace enviado. orderQty=105, maxfloor configurado=21. */
    private double replaceMaxFloorConLeaves(double leaves) {
        try (Harness h = new Harness()) {
            BasketPassive s = new BasketPassive(baseBuy(), "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));
            h.market(stat(100, 200, 200));
            s.onStatistic(stat(100, 200, 200));
            s.onOrders(ack(RoutingMessage.OrderStatus.NEW, 101, 105, 0));
            s.onOrders(ack(RoutingMessage.OrderStatus.PARTIALLY_FILLED, 101, leaves, 105 - leaves));
            // mueve el bid -> dispara replace
            h.market(stat(150, 200, 200));
            s.onStatistic(stat(150, 200, 200));
            return ((RoutingMessage.OrderReplaceRequest) h.last()).getMaxFloor();
        }
    }

    @Test
    void maxFloorReglasIceberg() {
        // orderQty=105, maxFloor configurado=21. El remanente final queda visible,
        // pero el replace conserva un maxFloor positivo porque NUAM interpreta cero
        // como "sin cambios" para un iceberg existente.
        assertEquals(10.0, replaceMaxFloorConLeaves(10), 1e-9,
                "leaves(10) -> maxFloor=10");

        assertEquals(15.0, replaceMaxFloorConLeaves(15), 1e-9,
                "leaves(15) -> maxFloor=15");

        assertEquals(21.0, replaceMaxFloorConLeaves(50), 1e-9,
                "leaves(50) -> se mantiene maxFloor=21");
    }

    /**
     * Replica el escenario de producción (COLBUN/MALLPLAZA/QUINENCO/SMU, 1-jun): la estrategia
     * se (re)crea con la orden YA viva (PARTIALLY_FILLED) sin pasar por el alta, así que ticks
     * queda null. Antes: NPE en cada snapshot (cientos), 0 replaces -> orden congelada.
     * Ahora: recalcula ticks y re-cotiza normalmente.
     */
    @Test
    void reCreadaPartiallyFilled_noNpeYReCotiza() {
        try (Harness h = new Harness()) {
            RoutingMessage.Order viva = baseBuy().toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                    .setPrice(100).setLeaves(40).setCumQty(65)
                    .build();
            BasketPassive s = new BasketPassive(viva, "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            // Snapshot con bid distinto del precio de la orden -> fuerza la rama de replace
            // (que antes hacía ticks.doubleValue() con ticks=null -> NPE).
            h.market(stat(105, 110, 200));
            s.onStatistic(stat(105, 110, 200));

            assertFalse(h.sent.isEmpty(), "tras (re)crear viva debe poder re-cotizar (antes: NPE, 0 replaces)");
            Message m = h.last();
            assertTrue(m instanceof RoutingMessage.OrderReplaceRequest, "debe ser un replace");
            assertEquals(106.0, ((RoutingMessage.OrderReplaceRequest) m).getPrice(), 1e-9,
                    "px = bid+tick (clamp) = 106, sin NPE");
            assertTrue(((RoutingMessage.OrderReplaceRequest) m).getMaxFloor() <= 40.0,
                    "maxFloor acotado a leaves");
        }
    }

    /**
     * Replica el "sube y sube": cuando la orden YA es el mejor bid (bid == orderPx) no debe
     * re-cotizar (perseguirse a sí misma). Con el fix, guard y cálculo usan el mismo snapshot.
     */
    @Test
    void noSePersigueSiendoElMejorBid() {
        try (Harness h = new Harness()) {
            BasketPassive s = new BasketPassive(baseBuy(), "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            h.market(stat(100, 110, 200));
            s.onStatistic(stat(100, 110, 200));
            s.onOrders(ack(RoutingMessage.OrderStatus.NEW, 101, 105, 0));
            int trasAlta = h.sent.size();

            // La orden está @101 y es el mejor bid (bid=101). Varios ticks: NO debe re-cotizar.
            for (int i = 0; i < 5; i++) {
                h.market(stat(101, 110, 200));
                s.onStatistic(stat(101, 110, 200));
            }

            assertEquals(trasAlta, h.sent.size(),
                    "no debe perseguirse a sí misma cuando ya es la mejor punta");
        }
    }

    /**
     * Replica el bug "No price" (SQM-B 16l4v46dtzkg2): llega un onStatistic cuyo PARÁMETRO viene
     * vacío (bid=0/ask=0), pero el libro del mapa (this.statistic) está bien. Antes el cálculo usaba
     * el parámetro -> px=0 -> el exchange rechaza "No price". Ahora se usa el snapshot del mapa.
     */
    @Test
    void usaLibroDelMapaNoElParametroVacio() {
        try (Harness h = new Harness()) {
            BasketPassive s = new BasketPassive(baseBuy(), "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            // Alta + NEW @100 (la orden queda por debajo del bid del mapa).
            h.market(stat(100, 110, 200));
            s.onStatistic(stat(100, 110, 200));
            s.onOrders(ack(RoutingMessage.OrderStatus.NEW, 100, 105, 0));

            // Mapa: bid=105 (bueno). Parámetro: VACÍO (0,0,0).
            h.market(stat(105, 110, 200));
            s.onStatistic(stat(0, 0, 0));

            Message m = h.last();
            assertTrue(m instanceof RoutingMessage.OrderReplaceRequest, "debe re-cotizar siguiendo el bid del mapa");
            double px = ((RoutingMessage.OrderReplaceRequest) m).getPrice();
            assertTrue(px > 0.0, "jamás se debe enviar sin precio (px>0)");
            assertEquals(106.0, px, 1e-9,
                    "debe usar el bid del MAPA (105)+tick=106, no el parámetro vacío");
        }
    }

    /** Inyecta un tick de MKD (como lo haría el simulador): setea el libro y dispara onStatistic. */
    private void feed(Harness h, BasketPassive s, double bid, double ask) {
        h.market(stat(bid, ask, 200));
        s.onStatistic(stat(bid, ask, 200));
    }

    private double lastPx(Harness h) {
        return priceOf(h.last());
    }

    /**
     * Escenario "simulador": la punta compradora se mueve (sube, se mantiene, baja) y se valida que
     * la orden basket passive BUY:
     *   - siempre cotice en bid + 1 tick,
     *   - NO se auto-persiga al infinito cuando ella misma es la mejor punta (bid == orderPx),
     *   - siga la punta tanto hacia arriba como hacia abajo,
     *   - jamás envíe sin precio ni cruce el ask.
     */
    @Test
    void simuladorSiguePuntaMasTickSinAutoPersecucion() {
        try (Harness h = new Harness()) {
            BasketPassive s = new BasketPassive(baseBuy(), "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            // (1) Punta externa = 100. Alta -> cotiza 101 (bid+1).
            feed(h, s, 100, 110);
            s.onOrders(ack(RoutingMessage.OrderStatus.NEW, 101, 105, 0));
            assertEquals(101.0, lastPx(h), 1e-9, "alta = bid+1 = 101");

            // (2) La orden YA es la mejor punta (book bid = 101 = su propio precio).
            //     Aunque lleguen MUCHOS ticks, NO debe re-cotizar (no crece consigo misma).
            int n = h.sent.size();
            for (int i = 0; i < 20; i++) {
                feed(h, s, 101, 110);
            }
            assertEquals(n, h.sent.size(),
                    "NO debe auto-perseguirse: con bid==orderPx no re-cotiza (no crece al infinito)");

            // (3) La punta externa SUBE a 102 -> sigue a 103 (bid+1).
            feed(h, s, 102, 110);
            s.onOrders(ack(RoutingMessage.OrderStatus.REPLACED, 103, 105, 0));
            assertEquals(103.0, lastPx(h), 1e-9, "sigue la punta hacia ARRIBA: 102+1=103");

            // (4) La punta externa BAJA a 101 -> sigue a 102 (bid+1).
            feed(h, s, 101, 110);
            s.onOrders(ack(RoutingMessage.OrderStatus.REPLACED, 102, 105, 0));
            assertEquals(102.0, lastPx(h), 1e-9, "sigue la punta hacia ABAJO: 101+1=102");

            // (5) Invariantes globales: jamás sin precio, jamás cruzando el ask.
            for (Message m : h.sent) {
                assertTrue(priceOf(m) > 0.0, "jamás se envía sin precio: " + priceOf(m));
                assertTrue(priceOf(m) < 110.0, "jamás cruza el ask: " + priceOf(m));
            }
        }
    }

    /**
     * Regresión del incidente del 11-jun (CAP_-1lin5ftwke4lj / -okoi8xfl9mxq): el papel aún NO había
     * transado en el día -> last=0. La banda de límite se calculaba como last*(1+limit%) = 0, y el
     * guard (limBuy >= bid) nunca se cumplía -> "NEW_BUY skip: bid(6002) > limitBuy(0.0)" -> la orden
     * quedaba en PENDING_NEW y NUNCA salía al mercado.
     *
     * Con el fix (precio de referencia robusto: last -> cierre previo -> mid -> punta del lado) la
     * banda se calcula contra el bid (6002) y la orden SÍ sale a bid+tick.
     */
    @Test
    void papelSinTransar_last0_igualSaleAlMercado() {
        try (Harness h = new Harness()) {
            // Orden BUY, limit 1%, igual que en el log real.
            RoutingMessage.Order buy = baseBuy().toBuilder()
                    .setSymbol("CAP").setLimit(1).setPrice(0).build();
            BasketPassive s = new BasketPassive(buy, "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            // Libro EXACTO del incidente: bid=6002, SIN ask, last=0 (papel sin transar).
            h.market(stat(6002, 0, 0));
            s.onStatistic(stat(6002, 0, 0));

            assertFalse(h.sent.isEmpty(),
                    "con last=0 la orden DEBE salir igual (antes: NEW_BUY skip, nunca enviaba)");
            Message m = h.last();
            assertTrue(m instanceof RoutingMessage.NewOrderRequest, "debe ser el alta (NewOrderRequest)");
            double px = priceOf(m);
            // tick=1 (default Harness), sin ask -> sin clamp no-cross -> bid+tick = 6003.
            assertEquals(6003.0, px, 1e-9, "debe cotizar bid+tick = 6003 aunque last=0");
            assertTrue(px > 0.0, "jamás sin precio");
        }
    }

    /**
     * Incidente CCU (11-jun): el libro viene TOTALMENTE vacío (sin bid/ask/last, papel sin data
     * en el feed). Antes la orden quedaba en PENDING_NEW invisible ("no sé dónde quedó la orden").
     * Ahora: tras el timeout, ActorStrategy consulta awaitingFirstMarketData() y la estrategia
     * publica un REJECTED hacia el front explicando que NO HAY MARKET DATA.
     */
    @Test
    void sinMarketData_rechazaHaciaElFront() {
        try (Harness h = new Harness()) {
            RoutingMessage.Order sell = baseBuy().toBuilder()
                    .setSymbol("CCU").setSide(RoutingMessage.Side.SELL).setLimit(1).build();
            BasketPassive s = new BasketPassive(sell, "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            // Libro de CCU EXACTO al del incidente: vacío total.
            h.market(stat(0, 0, 0));
            s.onStatistic(stat(0, 0, 0));

            assertTrue(h.sent.isEmpty(), "con libro vacío no debe enviar nada al exchange");
            assertTrue(s.awaitingFirstMarketData(), "debe reportar que sigue SIN market data");

            // Simula el timeout de ActorStrategy -> rechazo hacia el front.
            s.rejectNoMarketData();

            org.mockito.ArgumentCaptor<cl.vc.module.protocolbuff.akka.Envelope> cap =
                    org.mockito.ArgumentCaptor.forClass(cl.vc.module.protocolbuff.akka.Envelope.class);
            verify(h.bus, atLeastOnce()).publish(cap.capture());
            RoutingMessage.Order rejected = (RoutingMessage.Order) cap.getValue().getPayload();
            assertEquals(RoutingMessage.OrderStatus.REJECTED, rejected.getOrdStatus(),
                    "la orden debe volver REJECTED al front");
            assertTrue(rejected.getText().contains("SIN MARKET DATA"),
                    "el texto debe explicar el motivo: " + rejected.getText());

            // Idempotente: un segundo timeout no debe duplicar el rechazo.
            s.rejectNoMarketData();
            verify(h.bus, times(1)).publish(any(cl.vc.module.protocolbuff.akka.Envelope.class));
        }
    }

    /** Con libro usable NO debe auto-rechazarse aunque el límite todavía no dé para cotizar. */
    @Test
    void conMarketData_noSeAutoRechaza() {
        try (Harness h = new Harness()) {
            BasketPassive s = new BasketPassive(baseBuy(), "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            // Libro usable (hay bid) -> aunque no cotice aún, ya NO está "sin data".
            h.market(stat(100, 110, 200));
            s.onStatistic(stat(100, 110, 200));

            assertFalse(s.awaitingFirstMarketData(), "con libro usable no debe reportarse sin data");
            int sent = h.sent.size();
            s.rejectNoMarketData(); // el timeout llega igual: NO debe rechazar nada
            verify(h.bus, never()).publish(argThat(e ->
                    ((cl.vc.module.protocolbuff.akka.Envelope) e).getPayload() instanceof RoutingMessage.Order o
                            && o.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)));
            assertEquals(sent, h.sent.size(), "no debe enviar nada nuevo al exchange");
        }
    }

    /** Orden REAL CAP (03-jun, vector-trade-ricci): BUY qty 97, maxFloor 19, limit 1%. */
    private RoutingMessage.Order capBuy() {
        return RoutingMessage.Order.newBuilder()
                .setId("CAP_ih1nvsz2mlhs").setSymbol("CAP")
                .setSide(RoutingMessage.Side.BUY)
                .setOrderQty(97).setMaxFloor(19).setLimit(1)
                .setIcebergPercentage("20")
                .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                .setSecurityExchange(EXCH).setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setPrice(0).setLeaves(97).setCumQty(0)
                .build();
    }

    private RoutingMessage.Order capAck(RoutingMessage.OrderStatus st, double price) {
        return capBuy().toBuilder().setOrdStatus(st).setPrice(price).setLeaves(97).setCumQty(0).build();
    }

    /**
     * Replay con MKD REAL capturada del servidor de producción
     * (logs/BASKET_PASSIVE/CAP_ih1nvsz2mlhs_20260603.log, vector-trade-ricci).
     *
     * Reproduce la MISMA orden que dio error en prod y valida que con el fix:
     *   1) JAMÁS se envía un alta/replace sin precio (px > 0 siempre),
     *   2) SIGUE el precio del mercado hacia arriba a medida que sube,
     *   3) respeta su LÍMITE: last_inicial * (1+limit%) = 6552.0*1.01 = 6617.52
     *      (nunca cotiza por encima),
     *   4) ante el snapshot VACÍO (lo que producía el px=0 "No price") re-cotiza
     *      usando el libro del MAPA, no se va a 0 ni se congela.
     */
    @Test
    void mkdRealCAP_siguePrecioHastaLimite_yNuncaSinPrecio() {
        // bid, ask, last  (secuencia real capturada del servidor)
        double[][] mkd = {
            {6552.3,6576.2,6552.0},{6586.7,6630.6,6632.6},{6578.7,6626.7,6628.1},
            {6629.9,6630.0,6630.0},{6694.0,6738.9,6698.0},{6700.0,6738.1,6700.0},
            {6739.5,6749.9,6739.4},{6750.0,6777.7,6750.0},{6782.7,6797.7,6797.7},
            {6843.3,6929.9,6930.0},{6852.1,6919.8,6919.1},{6856.8,6894.9,6894.9},
            {6858.5,6880.0,6880.0},{6866.1,6888.0,6889.9},{6888.2,6899.6,6888.0},
            {6876.4,6900.0,6900.0},{6851.7,6897.0,6854.3},{6854.3,6889.9,6854.3},
            {6860.4,6884.9,6884.9},{6855.4,6867.0,6867.0},{6850.0,6879.9,6850.0},
            {6823.6,6849.9,6849.9},{6818.6,6849.9,6849.9},{6825.6,6849.9,6825.5},
            {6814.5,6841.7,6819.0},{6762.8,6812.5,6775.0},{6770.9,6796.8,6770.9}
        };
        final double LIMIT = 6552.0 * 1.01; // = 6617.52

        try (Harness h = new Harness()) {
            // tick real de CAP = 0.1 (sobreescribe el tick=1 por defecto del Harness)
            h.ticks.when(() -> Ticks.getTick(any(), any())).thenReturn(new BigDecimal("0.1"));

            BasketPassive s = new BasketPassive(capBuy(), "sub",
                    mock(Logger.class), mock(ActorRef.class), mock(ActorRef.class));

            // (1) Alta con el primer snapshot real -> NEW a bid+tick.
            h.market(stat(mkd[0][0], mkd[0][1], mkd[0][2]));
            s.onStatistic(stat(mkd[0][0], mkd[0][1], mkd[0][2]));
            assertFalse(h.sent.isEmpty(), "debe enviar el alta");
            double altaPx = priceOf(h.last());
            assertTrue(altaPx > 0.0, "el alta debe llevar precio");
            s.onOrders(capAck(RoutingMessage.OrderStatus.NEW, altaPx));

            // (2) Replay del resto: cada re-cotización se ackea para desbloquear.
            for (int i = 1; i < mkd.length; i++) {
                int before = h.sent.size();
                h.market(stat(mkd[i][0], mkd[i][1], mkd[i][2]));
                s.onStatistic(stat(mkd[i][0], mkd[i][1], mkd[i][2]));
                if (h.sent.size() > before) {
                    double px = priceOf(h.last());
                    assertTrue(px > 0.0, "replace SIN precio en i=" + i + " (bid=" + mkd[i][0] + ")");
                    s.onOrders(capAck(RoutingMessage.OrderStatus.REPLACED, px));
                }
            }

            // (3) Invariantes sobre TODO lo enviado.
            double maxPx = 0;
            for (Message m : h.sent) {
                double px = priceOf(m);
                assertTrue(px > 0.0, "se envió una orden SIN precio: " + px);          // nunca px=0
                assertTrue(px <= LIMIT + 1e-6, "cotizó por encima del límite: " + px); // respeta límite
                maxPx = Math.max(maxPx, px);
            }
            assertTrue(maxPx > altaPx, "no siguió el precio hacia arriba (max=" + maxPx + " alta=" + altaPx + ")");
            assertTrue(maxPx >= 6586.0, "debió seguir el bid creciente hasta ~6586.8 (<= límite)");

            // (4) El snapshot VACÍO que producía el px=0 en prod: ahora re-cotiza con el libro del MAPA.
            int before = h.sent.size();
            h.market(stat(6586.7, 6630.6, 6632.6)); // libro real bueno en el mapa
            s.onStatistic(stat(0, 0, 0));           // parámetro VACÍO -> antes: px=0 "No price"
            assertTrue(h.sent.size() > before, "debió re-cotizar con el libro del mapa, no congelarse");
            double px = priceOf(h.last());
            assertEquals(6586.8, px, 1e-6, "usó el bid del MAPA (6586.7)+tick=6586.8, no el parámetro vacío");
            assertTrue(px > 0.0, "jamás px=0 (No price)");
        }
    }
}
