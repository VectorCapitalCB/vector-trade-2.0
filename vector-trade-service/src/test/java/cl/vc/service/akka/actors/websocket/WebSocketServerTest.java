package cl.vc.service.akka.actors.websocket;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.BuySideConnect;
import com.google.common.collect.BiMap;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Regresión del WebSocketServer de trading (endpoint Jetty WS).
 * Fija el comportamiento ACTUAL de onConnect/onMessage/onClose/onWebSocketError y del
 * extractor de IP (X-Forwarded-For > X-Real-IP > remoteAddress > "unknown").
 *
 * NO se abren sockets ni ActorSystem real: Session/RemoteEndpoint/UpgradeRequest son mocks,
 * el estado estático de MainApp se mockea con mockStatic y el mapa global de actores
 * (BuySideConnect.actorPerSessionMaps) se limpia entre tests.
 *
 * Se documenta también el comportamiento inseguro vigente: si la IP se bloquea a mitad de
 * sesión, onMessage cierra el socket pero NO purga el actor ya creado del mapa.
 */
class WebSocketServerTest {

    private final BiMap<String, ActorRef> map = BuySideConnect.getActorPerSessionMaps();

    private WebSocketServer server;
    private Session session;
    private RemoteEndpoint remote;
    private String key;

    @BeforeEach
    void setUp() {
        map.clear();
        server = new WebSocketServer();
        session = mock(Session.class);
        remote = mock(RemoteEndpoint.class);
        when(session.getRemote()).thenReturn(remote);
        // La clave del mapa es el toString() del RemoteEndpoint (estable para un mismo mock).
        key = remote.toString();
    }

    @AfterEach
    void tearDown() {
        map.clear();
    }

    // ---------------------------------------------------------------- extractIp

    @Test
    void extractIp_prefiereXForwardedFor_yTomaElPrimero() {
        UpgradeRequest req = mock(UpgradeRequest.class);
        when(session.getUpgradeRequest()).thenReturn(req);
        when(req.getHeader("X-Forwarded-For")).thenReturn("1.2.3.4, 5.6.7.8");

        assertEquals("1.2.3.4", WebSocketServer.extractIp(session));
    }

    @Test
    void extractIp_caeAXRealIp_cuandoForwardedForVacio() {
        UpgradeRequest req = mock(UpgradeRequest.class);
        when(session.getUpgradeRequest()).thenReturn(req);
        when(req.getHeader("X-Forwarded-For")).thenReturn("   ");
        when(req.getHeader("X-Real-IP")).thenReturn("9.9.9.9");

        assertEquals("9.9.9.9", WebSocketServer.extractIp(session));
    }

    @Test
    void extractIp_caeAlRemoteAddress_sinHeaders() {
        UpgradeRequest req = mock(UpgradeRequest.class);
        when(session.getUpgradeRequest()).thenReturn(req);
        when(session.getRemoteAddress()).thenReturn(new InetSocketAddress("10.0.0.1", 5555));

        assertEquals("10.0.0.1", WebSocketServer.extractIp(session));
    }

    @Test
    void extractIp_sessionNull_devuelveUnknown() {
        assertEquals("unknown", WebSocketServer.extractIp(null));
    }

    // ---------------------------------------------------------------- onConnect

    @Test
    void onConnect_ipBloqueada_cierra1008_yNoCreaActor() {
        try (MockedStatic<MainApp> mainApp = mockStatic(MainApp.class)) {
            mainApp.when(() -> MainApp.isIpBlocked(any())).thenReturn(true);

            server.onConnect(session);

            verify(session).close(eq(1008), anyString());
            mainApp.verify(MainApp::getSystem, never());
            assertFalse(map.containsKey(key), "IP bloqueada: no debe crear actor en el mapa");
        }
    }

    @Test
    void onConnect_ipPermitida_creaActor_yLoRegistraEnElMapa() {
        try (MockedStatic<MainApp> mainApp = mockStatic(MainApp.class)) {
            ActorSystem system = mock(ActorSystem.class);
            ActorRef actor = mock(ActorRef.class);
            mainApp.when(() -> MainApp.isIpBlocked(any())).thenReturn(false);
            mainApp.when(MainApp::getSystem).thenReturn(system);
            when(system.actorOf(any(Props.class))).thenReturn(actor);

            server.onConnect(session);

            assertSame(actor, map.get(key), "debe registrar el actor recién creado bajo la clave del remote");
            verify(session, never()).close(anyInt(), anyString());
        }
    }

    @Test
    void onConnect_sesionYaRegistrada_noCreaSegundoActor() {
        try (MockedStatic<MainApp> mainApp = mockStatic(MainApp.class)) {
            ActorSystem system = mock(ActorSystem.class);
            ActorRef existing = mock(ActorRef.class);
            map.put(key, existing);
            mainApp.when(() -> MainApp.isIpBlocked(any())).thenReturn(false);
            mainApp.when(MainApp::getSystem).thenReturn(system);

            server.onConnect(session);

            assertSame(existing, map.get(key));
            mainApp.verify(MainApp::getSystem, never());
        }
    }

    // ---------------------------------------------------------------- onMessage(byte[])

    @Test
    void onMessage_entregaByteBufferAlActor_cuandoIpOkYSesionRegistrada() throws Exception {
        ActorRef actor = mock(ActorRef.class);
        map.put(key, actor);
        byte[] buf = new byte[]{0, 1, 2, 3, 4, 5};

        try (MockedStatic<MainApp> mainApp = mockStatic(MainApp.class)) {
            mainApp.when(() -> MainApp.isIpBlocked(any())).thenReturn(false);
            mainApp.when(() -> MainApp.recordIpMessageExceeded(any())).thenReturn(false);

            server.onMessage(session, buf, 2, 3);

            ArgumentCaptor<ByteBuffer> cap = ArgumentCaptor.forClass(ByteBuffer.class);
            verify(actor).tell(cap.capture(), isNull());
            ByteBuffer bb = cap.getValue();
            assertEquals(2, bb.position());
            assertEquals(5, bb.limit(), "offset=2 length=3 -> [2,5)");
        }
    }

    @Test
    void onMessage_sesionNoRegistrada_noEntregaNada() throws Exception {
        try (MockedStatic<MainApp> mainApp = mockStatic(MainApp.class)) {
            mainApp.when(() -> MainApp.isIpBlocked(any())).thenReturn(false);
            mainApp.when(() -> MainApp.recordIpMessageExceeded(any())).thenReturn(false);

            // No lanza y no hay actor que reciba nada.
            assertDoesNotThrow(() -> server.onMessage(session, new byte[]{9}, 0, 1));
            assertTrue(map.isEmpty());
        }
    }

    @Test
    void onMessage_ipBloqueada_cierra1008_yNoEntrega() throws Exception {
        ActorRef actor = mock(ActorRef.class);
        map.put(key, actor);
        when(session.isOpen()).thenReturn(true);

        try (MockedStatic<MainApp> mainApp = mockStatic(MainApp.class)) {
            mainApp.when(() -> MainApp.isIpBlocked(any())).thenReturn(true);

            server.onMessage(session, new byte[]{1}, 0, 1);

            verify(session).close(eq(1008), anyString());
            verify(actor, never()).tell(any(), any());
            // Comportamiento vigente (inseguro): el actor sigue en el mapa aunque la IP esté bloqueada.
            assertTrue(map.containsKey(key));
        }
    }

    @Test
    void onMessage_rateLimitExcedido_autoBloqueaIp_yCierra1008() throws Exception {
        ActorRef actor = mock(ActorRef.class);
        map.put(key, actor);

        try (MockedStatic<MainApp> mainApp = mockStatic(MainApp.class)) {
            mainApp.when(() -> MainApp.isIpBlocked(any())).thenReturn(false);
            mainApp.when(() -> MainApp.recordIpMessageExceeded(any())).thenReturn(true);

            server.onMessage(session, new byte[]{1}, 0, 1);

            mainApp.verify(() -> MainApp.blockIp(any()));
            verify(session).close(eq(1008), anyString());
            verify(actor, never()).tell(any(), any());
        }
    }

    // ---------------------------------------------------------------- onMessage(String)

    @Test
    void onMessageString_soloLoguea_noRompe() {
        assertDoesNotThrow(() -> server.onMessage(session, "hola"));
    }

    // ---------------------------------------------------------------- onClose

    @Test
    void onClose_envenenaActor_yLoQuitaDelMapa() {
        ActorRef actor = mock(ActorRef.class);
        map.put(key, actor);

        server.onClose(session, 1000, "bye");

        ArgumentCaptor<Object> cap = ArgumentCaptor.forClass(Object.class);
        verify(actor).tell(cap.capture(), isNull());
        assertSame(PoisonPill.getInstance(), cap.getValue());
        assertFalse(map.containsKey(key), "el actor debe eliminarse del mapa al cerrar");
    }

    @Test
    void onClose_sesionNoRegistrada_noRompe() {
        assertDoesNotThrow(() -> server.onClose(session, 1000, "bye"));
        assertTrue(map.isEmpty());
    }

    // ---------------------------------------------------------------- onWebSocketError

    @Test
    void onError_conSesionAbierta_cierraLaSesion() {
        when(session.isOpen()).thenReturn(true);
        when(session.getRemoteAddress()).thenReturn(new InetSocketAddress("10.0.0.9", 1));

        server.onWebSocketError(session, new RuntimeException("boom"));

        verify(session).close();
    }

    @Test
    void onError_sesionNull_noLanzaNPE() {
        assertDoesNotThrow(() -> server.onWebSocketError(null, new RuntimeException("boom")));
    }
}