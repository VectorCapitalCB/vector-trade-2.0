package cl.vc.service.akka.actors;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.keycloak.KeycloakService;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.jupiter.api.Test;
import org.keycloak.representations.idm.UserRepresentation;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Regresión de la ENTRADA DE ÓRDENES por sesión en {@link ActorPerSession}:
 * onNewOrderRequest (path normal y passthrough voultech), onReplaceRequest, onCancelRequest,
 * el indexado en {@code MainApp.getIdOrders()} y el ruteo al actor de la cuenta.
 *
 * Fija el comportamiento ACTUAL (no el ideal). En particular documenta que:
 *  - el path voultech, además de rutear, MUTA la cuenta del usuario en Keycloak
 *    (updateUserAccount) a partir de una orden entrante;
 *  - new/replace están rate-limited por IP (auto-bloqueo), pero cancel NO;
 *  - replace/cancel sin orden base indexada se descartan silenciosamente.
 *
 * No usa Akka TestKit (no está en el classpath): se instancia el actor sembrando el
 * {@code ActorCell.contextStack} para saltar la guarda "no crear actores con new", y se invocan
 * los handlers directamente en el hilo del test para que {@code mockStatic(MainApp)} aplique.
 */
class ActorPerSessionOrderRoutingTest {

    /** Estáticos de MainApp mockeados + estado de ruteo observable. */
    private static class Env implements AutoCloseable {
        final MockedStatic<MainApp> mainApp = mockStatic(MainApp.class);
        final ConcurrentHashMap<String, ActorRef> accounts = new ConcurrentHashMap<>();
        final HashMap<String, RoutingMessage.Order> idOrders = new HashMap<>();
        final KeycloakService keycloak = mock(KeycloakService.class);

        Env() {
            mainApp.when(MainApp::getAccountGroupUser).thenReturn(accounts);
            mainApp.when(MainApp::getIdOrders).thenReturn(idOrders);
            mainApp.when(MainApp::getKeycloakService).thenReturn(keycloak);
            // Rate-limit apagado por defecto (se sobrescribe en el test de bloqueo).
            mainApp.when(() -> MainApp.recordIpOrderExceeded(anyString())).thenReturn(false);
        }

        public void close() { mainApp.close(); }
    }

    /**
     * Crea un ActorPerSession sin ActorSystem: siembra el contextStack de Akka para que el
     * constructor del trait Actor no lance "You cannot create an instance ... using new".
     * getSelf() queda en null (basta para construir NewActorSession y no se usa de otra forma).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private ActorPerSession newSession() throws Exception {
        ActorContext ctx = mock(ActorContext.class);
        ThreadLocal cs = akka.actor.ActorCell$.MODULE$.contextStack();
        cs.set(new scala.collection.immutable.$colon$colon(ctx, scala.collection.immutable.Nil$.MODULE$));
        try {
            Constructor<ActorPerSession> c = ActorPerSession.class.getDeclaredConstructor(Session.class);
            c.setAccessible(true);
            return c.newInstance(new Object[]{null});
        } finally {
            cs.set(scala.collection.immutable.Nil$.MODULE$);
        }
    }

    private RoutingMessage.Order order(String id, String account, String operator) {
        return RoutingMessage.Order.newBuilder()
                .setId(id).setAccount(account).setSymbol("SYM").setOperator(operator)
                .build();
    }

    private RoutingMessage.NewOrderRequest newReq(RoutingMessage.Order o) {
        return RoutingMessage.NewOrderRequest.newBuilder().setOrder(o).build();
    }

    // ── onNewOrderRequest: path normal ──────────────────────────────────────

    @Test
    void nuevaOrden_indexaEnIdOrders_yRuteaAlActorDeLaCuenta() throws Exception {
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);

            ActorPerSession s = newSession();
            RoutingMessage.NewOrderRequest req = newReq(order("O1", "ACC1", "trader"));
            s.onNewOrderRequest(req);

            assertEquals("O1", env.idOrders.get("O1").getId(), "la orden debe quedar indexada por id");
            verify(account).tell(eq(req), isNull());
        }
    }

    // ── onNewOrderRequest: passthrough voultech ─────────────────────────────

    @Test
    void nuevaOrden_voultech_mutaKeycloak_indexa_yRutea() throws Exception {
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC2", account);
            UserRepresentation user = mock(UserRepresentation.class);
            when(user.getId()).thenReturn("uid-1");
            when(env.keycloak.getUserByUsername("voultech-bot")).thenReturn(user);

            ActorPerSession s = newSession();
            RoutingMessage.NewOrderRequest req = newReq(order("O2", "ACC2", "voultech-bot"));
            s.onNewOrderRequest(req);

            // Comportamiento actual: una orden entrante reasigna la cuenta del usuario en Keycloak.
            verify(env.keycloak).updateUserAccount("uid-1", "ACC2");
            assertEquals("O2", env.idOrders.get("O2").getId());
            verify(account).tell(eq(req), isNull());
        }
    }

    // ── onReplaceRequest ────────────────────────────────────────────────────

    @Test
    void replace_conBaseIndexada_ruteaALaCuentaDeLaBase() throws Exception {
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);
            env.idOrders.put("O1", order("O1", "ACC1", "trader"));

            ActorPerSession s = newSession();
            RoutingMessage.OrderReplaceRequest rep =
                    RoutingMessage.OrderReplaceRequest.newBuilder().setId("O1").build();
            s.onReplaceRequest(rep);

            verify(account).tell(eq(rep), isNull());
        }
    }

    @Test
    void replace_sinBaseIndexada_seDescartaSinRutear() throws Exception {
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);

            ActorPerSession s = newSession();
            s.onReplaceRequest(RoutingMessage.OrderReplaceRequest.newBuilder().setId("DESCONOCIDA").build());

            verifyNoInteractions(account);
        }
    }

    @Test
    void replaceConAliasPersistidoUsaIdInternoEstable() throws Exception {
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);
            env.idOrders.put("CL-OLD", order("O1", "ACC1", "trader").toBuilder()
                    .setClOrdId("CL-OLD")
                    .build());

            ActorPerSession s = newSession();
            s.onReplaceRequest(RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId("CL-OLD")
                    .setPrice(100d)
                    .build());

            ArgumentCaptor<RoutingMessage.OrderReplaceRequest> sent =
                    ArgumentCaptor.forClass(RoutingMessage.OrderReplaceRequest.class);
            verify(account).tell(sent.capture(), isNull());
            assertEquals("O1", sent.getValue().getId());
        }
    }

    // ── onCancelRequest ─────────────────────────────────────────────────────

    @Test
    void cancel_conBaseIndexada_ruteaALaCuentaDeLaBase() throws Exception {
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);
            env.idOrders.put("O1", order("O1", "ACC1", "trader"));

            ActorPerSession s = newSession();
            RoutingMessage.OrderCancelRequest cancel =
                    RoutingMessage.OrderCancelRequest.newBuilder().setId("O1").build();
            s.onCancelRequest(cancel);

            verify(account).tell(eq(cancel), isNull());
        }
    }

    @Test
    void cancel_sinBaseIndexada_seDescartaSinRutear() throws Exception {
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);

            ActorPerSession s = newSession();
            s.onCancelRequest(RoutingMessage.OrderCancelRequest.newBuilder().setId("DESCONOCIDA").build());

            verifyNoInteractions(account);
        }
    }

    @Test
    void cancelConAliasPersistidoUsaIdInternoEstable() throws Exception {
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);
            env.idOrders.put("CL-OLD", order("O1", "ACC1", "trader").toBuilder()
                    .setClOrdId("CL-OLD")
                    .build());

            ActorPerSession s = newSession();
            s.onCancelRequest(RoutingMessage.OrderCancelRequest.newBuilder()
                    .setId("CL-OLD")
                    .build());

            ArgumentCaptor<RoutingMessage.OrderCancelRequest> sent =
                    ArgumentCaptor.forClass(RoutingMessage.OrderCancelRequest.class);
            verify(account).tell(sent.capture(), isNull());
            assertEquals("O1", sent.getValue().getId());
        }
    }

    @Test
    void cancel_noEstaRateLimited() throws Exception {
        // A diferencia de new/replace, cancel NO consulta recordIpOrderExceeded (comportamiento actual).
        try (Env env = new Env()) {
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);
            env.idOrders.put("O1", order("O1", "ACC1", "trader"));

            ActorPerSession s = newSession();
            s.onCancelRequest(RoutingMessage.OrderCancelRequest.newBuilder().setId("O1").build());

            env.mainApp.verify(() -> MainApp.recordIpOrderExceeded(anyString()), never());
            verify(account).tell(any(RoutingMessage.OrderCancelRequest.class), isNull());
        }
    }

    // ── Rate-limit por IP en el alta ────────────────────────────────────────

    @Test
    void nuevaOrden_ipExcedida_bloqueaIp_noIndexa_niRutea() throws Exception {
        try (Env env = new Env()) {
            env.mainApp.when(() -> MainApp.recordIpOrderExceeded(anyString())).thenReturn(true);
            ActorRef account = mock(ActorRef.class);
            env.accounts.put("ACC1", account);

            ActorPerSession s = newSession();
            s.onNewOrderRequest(newReq(order("O9", "ACC1", "trader")));

            assertNull(env.idOrders.get("O9"), "una orden rate-limited no debe indexarse");
            verifyNoInteractions(account);
            // Sesión null -> IP 'unknown'; se auto-bloquea esa IP.
            env.mainApp.verify(() -> MainApp.blockIp("unknown"));
        }
    }
}
