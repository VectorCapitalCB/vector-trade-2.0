package cl.vc.service.akka.actors.routing;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduce el incidente real del 2026-09-02 en RICCI: la bolsa responde al replace/cancel con un
 * Order cuyo campo {@code id} viene VACIO. Solo trae {@code clOrdId} y el {@code orderID} externo.
 *
 * Secuencia literal del log XSGO_20260902 (cuenta y simbolo anonimizados):
 * <pre>
 * 11:28:01,148  OrderCancelRequest : {"id":"-db2r9igx0g6k"}
 * 11:28:01,154  Order              : {"id":"", "clOrdId":"servic17taetv3", "account":"",
 *                                     "symbol":"", "ordStatus":"REJECTED", ...}
 * </pre>
 *
 * 86 de los 6.708 Order de esa jornada (1,3%) llegaron asi. Al no poder correlacionarlos, la orden
 * quedaba viva en el blotter mientras en la bolsa ya no existia: el operador cancelaba y no pasaba
 * nada ("perdi el id y no puedo cancelarla"). Ni el 2.0 ni PROD resuelven por clOrdId: PROD no usa
 * getClOrdId() ni una vez en ActorGroupPerAccount.
 *
 * ALCANCE REAL, medido replayeando el log completo de esa jornada contra este codigo:
 * de los 86 casos solo 1 se reconcilia (por orderID). El clOrdId NO sirve para reconciliar porque
 * el service nunca lo genera ni lo conoce: lo asigna el gateway OUCH como token de sesion
 * (SessionTracker: sessionId=service-vector-trade-ricci token=servicXXXX) y cambia en cada replace.
 *
 * La causa raiz se corrigio AGUAS ARRIBA, en el gateway (XRO-OUCH-1.0-fat.jar recompilado el
 * 2026-09-02 19:18). Medicion: ayer 86 Order con id vacio y 4.248 "Unknown order"; hoy, con el
 * gateway nuevo, 0 y 2 respectivamente.
 *
 * Entonces el valor de resolveOrderId NO es la reconciliacion —es marginal— sino la GUARDA:
 * antes un id vacio se indexaba con ordersMap.put("", order), contaminando el mapa de ordenes de
 * la cuenta con una entrada basura bajo la clave vacia. Ahora se descarta con un WARN.
 */
class ActorGroupPerAccountEmptyIdTest {

    private static final String INTERNAL_ID = "-db2r9igx0g6k";
    private static final String CL_ORD_ID = "servic17taetv3";
    private static final String EXCHANGE_ID = "819288449349842345";

    /** Orden viva, tal como quedo indexada cuando salio al mercado. */
    private RoutingMessage.Order liveOrder() {
        return RoutingMessage.Order.newBuilder()
                .setId(INTERNAL_ID)
                .setClOrdId(CL_ORD_ID)
                .setOrderID(EXCHANGE_ID)
                .setAccount("CTA-TEST")
                .setSymbol("SQM-B")
                .setSide(RoutingMessage.Side.BUY)
                .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                .setExecType(RoutingMessage.ExecutionType.EXEC_NEW)
                .setOrderQty(1_000d)
                .setLeaves(1_000d)
                .setPrice(100d)
                .build();
    }

    /** Lo que efectivamente llego de la bolsa: id vacio, solo clOrdId y orderID. */
    private RoutingMessage.Order exchangeReplyWithoutId() {
        return RoutingMessage.Order.newBuilder()
                .setId("")
                .setClOrdId(CL_ORD_ID)
                .setOrderID(EXCHANGE_ID)
                .setAccount("")
                .setSymbol("")
                .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                .build();
    }

    @Test
    void elMensajeRealDeLaBolsaLlegaSinIdInterno() {
        RoutingMessage.Order reply = exchangeReplyWithoutId();
        assertTrue(reply.getId().isBlank(), "es el caso que rompia: id vacio");
        assertFalse(reply.getClOrdId().isBlank(), "pero el clOrdId si viene");
        assertFalse(reply.getOrderID().isBlank(), "y el orderID de la bolsa tambien");
    }

    @Test
    void resuelveElIdInternoPorClOrdId() {
        Map<String, RoutingMessage.Order> orders = new HashMap<>();
        orders.put(INTERNAL_ID, liveOrder());

        String resolved = ActorGroupPerAccount.resolveOrderId(exchangeReplyWithoutId(), orders);

        assertEquals(INTERNAL_ID, resolved,
                "un reply sin id debe reconciliarse contra la orden viva por su clOrdId");
    }

    @Test
    void resuelveTambienPorOrderIDDeLaBolsaSiNoHayClOrdId() {
        Map<String, RoutingMessage.Order> orders = new HashMap<>();
        orders.put(INTERNAL_ID, liveOrder());

        RoutingMessage.Order soloOrderId = exchangeReplyWithoutId().toBuilder().setClOrdId("").build();

        assertEquals(INTERNAL_ID, ActorGroupPerAccount.resolveOrderId(soloOrderId, orders));
    }

    @Test
    void unIdVacioIrreconciliableNoContaminaElMapaDeOrdenes() {
        Map<String, RoutingMessage.Order> orders = new HashMap<>();
        orders.put(INTERNAL_ID, liveOrder());

        RoutingMessage.Order huerfano = exchangeReplyWithoutId().toBuilder()
                .setClOrdId("servic-desconocido").setOrderID("").build();

        assertEquals("", ActorGroupPerAccount.resolveOrderId(huerfano, orders),
                "si no se puede reconciliar, se devuelve vacio y el llamador lo descarta");
        assertFalse(orders.containsKey(""), "jamas se indexa bajo la clave vacia");
    }

    @Test
    void unMensajeNormalConIdNoSeToca() {
        Map<String, RoutingMessage.Order> orders = new HashMap<>();
        orders.put(INTERNAL_ID, liveOrder());

        RoutingMessage.Order normal = liveOrder().toBuilder()
                .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .build();

        assertEquals(INTERNAL_ID, ActorGroupPerAccount.resolveOrderId(normal, orders));
    }

    @Test
    void trasReconciliarSePuedeCancelarLaOrden() {
        // El punto operativo: si el id se reconstruye, la orden sigue siendo cancelable por su id
        // interno. Antes el reply sin id la dejaba en un limbo y el cancel no encontraba nada.
        Map<String, RoutingMessage.Order> orders = new HashMap<>();
        orders.put(INTERNAL_ID, liveOrder());

        String resolved = ActorGroupPerAccount.resolveOrderId(exchangeReplyWithoutId(), orders);
        RoutingMessage.OrderCancelRequest cancel =
                RoutingMessage.OrderCancelRequest.newBuilder().setId(resolved).build();

        assertNotNull(orders.get(cancel.getId()),
                "el cancel debe encontrar la orden base: eso es lo que ayer no pasaba");
        assertEquals(INTERNAL_ID, orders.get(cancel.getId()).getId());
    }
}
