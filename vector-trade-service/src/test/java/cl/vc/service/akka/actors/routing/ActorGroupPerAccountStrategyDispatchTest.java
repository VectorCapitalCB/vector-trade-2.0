package cl.vc.service.akka.actors.routing;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.service.MainApp;
import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Despacho de replace y cancel hacia las estrategias en ActorGroupPerAccount.
 *
 * Fija dos correcciones de paridad operativa:
 *
 *  1) El criterio de "esta orden la maneja un ActorStrategy" es UNO solo,
 *     isStrategyManagedByActor, para alta, restauración, cancel y replace. Antes el replace
 *     usaba una lista literal que omitía VWAP: el replace de una VWAP salía a la bolsa con el
 *     id del padre (que en la bolsa no existe) y Vwap.onReplace era código muerto. Verificado
 *     en el bytecode de producción: cancel incluye VWAP, replace no.
 *
 *  2) Si la orden es de estrategia pero su ActorStrategy ya no existe, antes se producía un
 *     NullPointerException que el catch se comía: ni confirmación ni rechazo, la orden quedaba
 *     "viva" en pantalla ("cancelé y quedó viva"). Ahora se emite un OrderCancelReject a las
 *     sesiones, igual que los demás rechazos locales.
 *
 * Se siembra MainApp.getConnections() REAL (no mockStatic) porque el actor corre en el
 * dispatcher de Akka; una sesión-probe se registra vía NewActorSession para observar el reject.
 */
class ActorGroupPerAccountStrategyDispatchTest {

    private static final RoutingMessage.SecurityExchangeRouting EXCH =
            RoutingMessage.SecurityExchangeRouting.XSGO;
    private static final String ACCOUNT = "98765-4";

    private static ActorSystem system;
    private static boolean prevRequiereCreasys;

    @BeforeAll
    static void boot() {
        prevRequiereCreasys = MainApp.requiereCreasys;
        MainApp.requiereCreasys = false;
        system = ActorSystem.create("agpa-dispatch-test");
    }

    @AfterAll
    static void shutdown() {
        MainApp.requiereCreasys = prevRequiereCreasys;
        if (system != null) {
            system.terminate();
        }
    }

    /** Actor mínimo que encola todo lo que recibe, para observar lo que llega a la sesión. */
    static final class Probe extends AbstractActor {
        final BlockingQueue<Object> received;

        Probe(BlockingQueue<Object> received) {
            this.received = received;
        }

        static Props props(BlockingQueue<Object> q) {
            return Props.create(Probe.class, () -> new Probe(q));
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(received::add).build();
        }
    }

    private RoutingMessage.Order strategyOrder(String id, RoutingMessage.StrategyOrder strategy) {
        return RoutingMessage.Order.newBuilder()
                .setId(id)
                .setAccount(ACCOUNT)
                .setSymbol("SQM-B")
                .setSide(RoutingMessage.Side.BUY)
                .setStrategyOrder(strategy)
                .setSecurityExchange(EXCH)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                .setExecType(RoutingMessage.ExecutionType.EXEC_NEW)
                .setPrice(100d)   // nocional chico: MainApp.checkNotionalLimit es estado compartido entre tests
                .setOrderQty(1_000d)
                .setLeaves(1_000d)
                .build();
    }

    private static RoutingMessage.OrderCancelReject awaitReject(BlockingQueue<Object> q) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        while (System.nanoTime() < deadline) {
            Object o = q.poll(200, TimeUnit.MILLISECONDS);
            if (o instanceof RoutingMessage.OrderCancelReject r) {
                return r;
            }
        }
        return null;
    }

    private ActorRef accountActorWithSession(BlockingQueue<Object> sessionInbox, String name) {
        ActorRef actor = system.actorOf(ActorGroupPerAccount.props(ACCOUNT, -1.0, 1.0), name + System.nanoTime());
        ActorRef session = system.actorOf(Probe.props(sessionInbox), "session-" + System.nanoTime());
        actor.tell(new ActorGroupPerAccount.NewActorSession(session, "ses-1"), ActorRef.noSender());
        return actor;
    }

    // ---------------------------------------------------------------------
    // 1) VWAP: el replace ya no se manda a la bolsa con el id del padre
    // ---------------------------------------------------------------------
    @Test
    void replaceDeVwap_yaNoSaleALaBolsaConElIdDelPadre() throws Exception {
        NettyProtobufClient conn = mock(NettyProtobufClient.class);
        MainApp.getConnections().put(EXCH, conn);
        BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
        try {
            ActorRef actor = accountActorWithSession(inbox, "vwap-replace-");

            // La VWAP queda indexada en ordersMap sin ActorStrategy (el test aísla el despacho).
            actor.tell(strategyOrder("vwap-1", RoutingMessage.StrategyOrder.VWAP), ActorRef.noSender());
            actor.tell(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId("vwap-1").setQuantity(1_000d).setLimit(99d).build(), ActorRef.noSender());

            RoutingMessage.OrderCancelReject reject = awaitReject(inbox);
            assertNotNull(reject, "la VWAP debe tratarse como estrategia: sin actor, se rechaza localmente");
            assertEquals("vwap-1", reject.getId());
            assertTrue(reject.getText().contains("VWAP"), reject.getText());
            assertTrue(reject.getText().contains("replace"), reject.getText());
            // Lo esencial de la corrección: NADA salió hacia la bolsa.
            verify(conn, never()).sendMessage(any(Message.class));
        } finally {
            MainApp.getConnections().remove(EXCH);
        }
    }

    // ---------------------------------------------------------------------
    // 2) Actor de estrategia muerto: rechazo explícito en vez de NPE silencioso
    // ---------------------------------------------------------------------
    @Test
    void cancelDeEstrategiaSinActor_emiteRejectEnVezDeNpeSilencioso() throws Exception {
        NettyProtobufClient conn = mock(NettyProtobufClient.class);
        MainApp.getConnections().put(EXCH, conn);
        BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
        try {
            ActorRef actor = accountActorWithSession(inbox, "best-cancel-");

            actor.tell(strategyOrder("best-9", RoutingMessage.StrategyOrder.BEST), ActorRef.noSender());
            actor.tell(RoutingMessage.OrderCancelRequest.newBuilder().setId("best-9").build(), ActorRef.noSender());

            RoutingMessage.OrderCancelReject reject = awaitReject(inbox);
            assertNotNull(reject, "antes esto era un NPE tragado por el catch y el operador no veía nada");
            assertEquals("best-9", reject.getId());
            assertTrue(reject.getText().contains("BEST"), reject.getText());
            assertTrue(reject.getText().contains("cancel"), reject.getText());
            assertFalse(reject.getExecId().isBlank(), "el reject lleva execId como los demás rechazos locales");
            verify(conn, never()).sendMessage(any(Message.class));
        } finally {
            MainApp.getConnections().remove(EXCH);
        }
    }

    @Test
    void replaceDeEstrategiaSinActor_emiteReject() throws Exception {
        NettyProtobufClient conn = mock(NettyProtobufClient.class);
        MainApp.getConnections().put(EXCH, conn);
        BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
        try {
            ActorRef actor = accountActorWithSession(inbox, "holgura-replace-");

            actor.tell(strategyOrder("hol-3", RoutingMessage.StrategyOrder.HOLGURA), ActorRef.noSender());
            actor.tell(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId("hol-3").setQuantity(1_000d).setPrice(101d).build(), ActorRef.noSender());

            RoutingMessage.OrderCancelReject reject = awaitReject(inbox);
            assertNotNull(reject);
            assertEquals("hol-3", reject.getId());
            assertTrue(reject.getText().contains("HOLGURA"), reject.getText());
            verify(conn, never()).sendMessage(any(Message.class));
        } finally {
            MainApp.getConnections().remove(EXCH);
        }
    }

    // ---------------------------------------------------------------------
    // Control: una orden SIN estrategia sigue saliendo a la bolsa como siempre
    // ---------------------------------------------------------------------
    @Test
    void replaceSinEstrategia_sigueSaliendoALaBolsa() throws Exception {
        NettyProtobufClient conn = mock(NettyProtobufClient.class);
        MainApp.getConnections().put(EXCH, conn);
        BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
        try {
            ActorRef actor = accountActorWithSession(inbox, "none-replace-");

            actor.tell(strategyOrder("plain-5", RoutingMessage.StrategyOrder.NONE_STRATEGY), ActorRef.noSender());
            actor.tell(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId("plain-5").setQuantity(1_000d).setPrice(102d).build(), ActorRef.noSender());

            verify(conn, timeout(4000)).sendMessage(any(RoutingMessage.OrderReplaceRequest.class));
            Object late = inbox.poll(300, TimeUnit.MILLISECONDS);
            assertFalse(late instanceof RoutingMessage.OrderCancelReject,
                    "una orden sin estrategia no debe generar rechazo local");
        } finally {
            MainApp.getConnections().remove(EXCH);
        }
    }
}
