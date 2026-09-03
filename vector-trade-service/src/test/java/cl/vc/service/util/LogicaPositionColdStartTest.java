package cl.vc.service.util;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Arranque en frio: updates de orden que llegan mientras la cuenta todavia restaura desde
 * Redis/SQL y su mapa de posiciones historicas esta vacio.
 *
 * Incidente real del 2026-09-03 tras desplegar el core en RICCI con la rueda abierta:
 * <pre>
 * 13:36:43.779  [RedisRestore] cuenta ... positions=0     &lt;- restaurando
 * 13:36:43.783  NullPointerException: ... "positionHIstory" is null
 *               at LogicaPosition.orderUpdate(LogicaPosition.java:247)
 * </pre>
 * 10 ocurrencias en los primeros 90 segundos. Los cuatro accesos de los caminos de VENTA hacian
 * {@code snapshotPositionHistoryMaps.get(...).getAvailableQuantity()} sin null-check, y la
 * excepcion abortaba TODO el orderUpdate: se perdia tambien la actualizacion de saldo.
 *
 * DECISION DE DISENO que fijan estos tests: cuando la posicion no existe se OMITE el ajuste y se
 * deja un WARN; NO se crea la entrada en cero. Crearla desbalancearia la cuenta, porque el alta de
 * una venta RESTA la reserva y el cancel la SUMA de vuelta: sin la resta previa, el cancel
 * inflaria la cantidad disponible. Que no exista posicion significa que la cuenta no tiene el
 * papel, y entonces no hay nada que ajustar. El camino de COMPRA en EXEC_TRADE si crea la entrada,
 * porque una compra genera posicion, y ese comportamiento no se toca.
 */
class LogicaPositionColdStartTest {

    private static final String ACCOUNT = "12336718/9";

    private LogicaPosition logic(HashMap<String, BlotterMessage.PositionHistory.Builder> history,
                                 BlotterMessage.Balance.Builder balance) {
        // margin != -1 y operador que no contiene "voultech": asi orderUpdate no corta al entrar.
        return new LogicaPosition(0d, mock(ActorRef.class), balance, history, new HashMap<>());
    }

    private BlotterMessage.Balance.Builder balance() {
        return BlotterMessage.Balance.newBuilder()
                .setCuenta(ACCOUNT).setSaldoDisponible(1_000_000d).setCupo(1_000_000d);
    }

    private RoutingMessage.Order order(String id, RoutingMessage.Side side,
                                       RoutingMessage.ExecutionType execType,
                                       RoutingMessage.OrderStatus status,
                                       double orderQty, double leaves, double cumQty) {
        return RoutingMessage.Order.newBuilder()
                .setId(id).setAccount(ACCOUNT).setSymbol("SQM-B").setOperator("oper")
                .setSide(side)
                .setSecurityExchange(RoutingMessage.SecurityExchangeRouting.XSGO)
                .setExecType(execType).setOrdStatus(status)
                .setPrice(100d).setOrderQty(orderQty).setLeaves(leaves).setCumQty(cumQty)
                .build();
    }

    // ── el caso exacto de produccion: LogicaPosition:247, venta nueva sin posicion cargada ──
    @Test
    void ventaNuevaSinPosicionCargadaNoRevienta() {
        HashMap<String, BlotterMessage.PositionHistory.Builder> history = new HashMap<>();  // vacio: restaurando
        BlotterMessage.Balance.Builder balance = balance();

        RoutingMessage.Order venta = order("s-1", RoutingMessage.Side.SELL,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW, 1_000d, 1_000d, 0d);

        assertDoesNotThrow(() -> logic(history, balance).orderUpdate(venta, null),
                "antes lanzaba NullPointerException y abortaba todo el orderUpdate");
        assertTrue(history.isEmpty(), "no se inventa una posicion que la cuenta no tiene");
    }

    @Test
    void elReplaceSinPosicionCargadaNoRevienta() {
        HashMap<String, BlotterMessage.PositionHistory.Builder> history = new HashMap<>();
        BlotterMessage.Balance.Builder balance = balance();

        RoutingMessage.Order previa = order("s-2", RoutingMessage.Side.SELL,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW, 1_000d, 1_000d, 0d);
        RoutingMessage.Order replace = order("s-2", RoutingMessage.Side.SELL,
                RoutingMessage.ExecutionType.EXEC_REPLACED, RoutingMessage.OrderStatus.REPLACED, 2_000d, 2_000d, 0d);

        assertDoesNotThrow(() -> logic(history, balance).orderUpdate(replace, previa));
        assertTrue(history.isEmpty());
    }

    @Test
    void elCancelSinPosicionCargadaNoRevienta() {
        HashMap<String, BlotterMessage.PositionHistory.Builder> history = new HashMap<>();
        BlotterMessage.Balance.Builder balance = balance();

        RoutingMessage.Order cancel = order("s-3", RoutingMessage.Side.SELL,
                RoutingMessage.ExecutionType.EXEC_CANCELED, RoutingMessage.OrderStatus.CANCELED, 1_000d, 0d, 0d);

        assertDoesNotThrow(() -> logic(history, balance).orderUpdate(cancel, null));
        assertTrue(history.isEmpty(),
                "si nunca se resto la reserva, tampoco se suma de vuelta: eso inflaria la posicion");
    }

    // ── y con la posicion YA cargada, la matematica queda intacta ──
    @Test
    void conPosicionCargadaLaReservaSigueRestandoIgual() {
        HashMap<String, BlotterMessage.PositionHistory.Builder> history = new HashMap<>();
        history.put("SQM-B", BlotterMessage.PositionHistory.newBuilder()
                .setAccount(ACCOUNT).setInstrument("SQM-B").setAvailableQuantity(5_000d));
        BlotterMessage.Balance.Builder balance = balance();

        RoutingMessage.Order venta = order("s-4", RoutingMessage.Side.SELL,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW, 1_000d, 1_000d, 0d);
        logic(history, balance).orderUpdate(venta, null);

        assertEquals(4_000d, history.get("SQM-B").getAvailableQuantity(), 1e-9,
                "5.000 - 1.000: la reserva se aplica igual que antes del arreglo");
    }

    @Test
    void laCompraEnTradeSigueCreandoLaPosicion() {
        HashMap<String, BlotterMessage.PositionHistory.Builder> history = new HashMap<>();
        BlotterMessage.Balance.Builder balance = balance();

        RoutingMessage.Order compra = order("b-1", RoutingMessage.Side.BUY,
                RoutingMessage.ExecutionType.EXEC_TRADE, RoutingMessage.OrderStatus.PARTIALLY_FILLED, 1_000d, 700d, 300d)
                .toBuilder().setLastQty(300d).setLastPx(100d).build();

        logic(history, balance).orderUpdate(compra, null);

        assertNotNull(history.get("SQM-B"), "una compra SI genera posicion: ese camino no cambia");
        assertEquals(300d, history.get("SQM-B").getAvailableQuantity(), 1e-9);
    }
}
