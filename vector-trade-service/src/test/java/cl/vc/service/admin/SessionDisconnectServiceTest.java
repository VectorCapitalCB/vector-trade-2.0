package cl.vc.service.admin;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.BuySideConnect;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SessionDisconnectServiceTest {

    private static final String USERNAME = "jperez";
    private static final String SESSION_KEY = "/192.168.1.100:52000";

    private Session mockSession;
    private RemoteEndpoint mockRemote;
    private ActorRef mockActorRef;

    @BeforeEach
    void setUp() {
        mockSession  = mock(Session.class);
        mockRemote   = mock(RemoteEndpoint.class);
        mockActorRef = mock(ActorRef.class);

        when(mockSession.getRemote()).thenReturn(mockRemote);
        when(mockRemote.toString()).thenReturn(SESSION_KEY);
        when(mockSession.getRemoteAddress()).thenReturn(new InetSocketAddress("192.168.1.100", 52000));
        when(mockSession.isOpen()).thenReturn(true);

        // Poblar mapas de MainApp
        MainApp.getUserActiveSessionsMap().put(USERNAME, mockSession);
        MainApp.getUserSessionConnectedAt().put(USERNAME, System.currentTimeMillis());

        // Poblar mapa de BuySideConnect (BiMap es mutable)
        BuySideConnect.getActorPerSessionMaps().put(SESSION_KEY, mockActorRef);
    }

    @AfterEach
    void tearDown() {
        MainApp.getUserActiveSessionsMap().remove(USERNAME);
        MainApp.getUserSessionConnectedAt().remove(USERNAME);
        BuySideConnect.getActorPerSessionMaps().remove(SESSION_KEY);
        MainApp.getBlockedUsers().remove(USERNAME);
        MainApp.getBlockedUsers().remove("nuevo_usuario");
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Tests de DESCONEXIÓN
    // ─────────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("disconnect()")
    class DisconnectTests {

        @Test
        @DisplayName("Envía PoisonPill al actor Akka")
        void disconnect_sendsPoisonPillToActor() {
            SessionDisconnectService.disconnect(USERNAME);

            ArgumentCaptor<Object> msgCaptor = ArgumentCaptor.forClass(Object.class);
            verify(mockActorRef).tell(msgCaptor.capture(), eq(ActorRef.noSender()));
            assertInstanceOf(PoisonPill.class, msgCaptor.getValue(),
                    "Debe enviar PoisonPill al actor para que dispare postStop");
        }

        @Test
        @DisplayName("Cierra la sesión WebSocket con código NORMAL")
        void disconnect_closesWebSocketSession() {
            SessionDisconnectService.disconnect(USERNAME);

            verify(mockSession).close(eq(1000), contains("administrador"));
        }

        @Test
        @DisplayName("Elimina al usuario del mapa de sesiones activas")
        void disconnect_removesFromUserActiveSessionsMap() {
            SessionDisconnectService.disconnect(USERNAME);

            assertFalse(MainApp.getUserActiveSessionsMap().containsKey(USERNAME));
        }

        @Test
        @DisplayName("Elimina al usuario del mapa de timestamps")
        void disconnect_removesFromConnectedAtMap() {
            SessionDisconnectService.disconnect(USERNAME);

            assertFalse(MainApp.getUserSessionConnectedAt().containsKey(USERNAME));
        }

        @Test
        @DisplayName("Elimina la sesión de BuySideConnect.actorPerSessionMaps")
        void disconnect_removesFromActorPerSessionMaps() {
            SessionDisconnectService.disconnect(USERNAME);

            assertFalse(BuySideConnect.getActorPerSessionMaps().containsKey(SESSION_KEY));
        }

        @Test
        @DisplayName("No cierra una sesión ya cerrada")
        void disconnect_doesNotCloseAlreadyClosedSession() {
            when(mockSession.isOpen()).thenReturn(false);

            SessionDisconnectService.disconnect(USERNAME);

            verify(mockSession, never()).close(anyInt(), anyString());
            verify(mockActorRef).tell(any(PoisonPill.class), eq(ActorRef.noSender()));
        }

        @Test
        @DisplayName("Lanza UserNotConnectedException si el usuario no está conectado")
        void disconnect_throwsIfUserNotFound() {
            assertThrows(SessionDisconnectService.UserNotConnectedException.class,
                    () -> SessionDisconnectService.disconnect("usuario_inexistente"));
        }

        @Test
        @DisplayName("Funciona aunque no haya actor en BuySideConnect (sesión huérfana)")
        void disconnect_worksWithOrphanSession() {
            BuySideConnect.getActorPerSessionMaps().remove(SESSION_KEY);

            assertDoesNotThrow(() -> SessionDisconnectService.disconnect(USERNAME));
            assertFalse(MainApp.getUserActiveSessionsMap().containsKey(USERNAME));
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Tests de BLOQUEO DE USUARIOS
    // ─────────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("blockUser()")
    class BlockUserTests {

        @Test
        @DisplayName("Añade el username al set de usuarios bloqueados")
        void blockUser_addsToBlockedSet() {
            SessionDisconnectService.blockUser(USERNAME);

            assertTrue(MainApp.isUserBlocked(USERNAME),
                    "El usuario debe estar en blockedUsers tras ser bloqueado");
        }

        @Test
        @DisplayName("Desconecta al usuario si estaba conectado")
        void blockUser_disconnectsConnectedUser() {
            SessionDisconnectService.blockUser(USERNAME);

            verify(mockSession).close(eq(1000), anyString());
            assertFalse(MainApp.getUserActiveSessionsMap().containsKey(USERNAME));
        }

        @Test
        @DisplayName("Envía PoisonPill al actor al bloquear un usuario conectado")
        void blockUser_killsAkkaActor() {
            SessionDisconnectService.blockUser(USERNAME);

            verify(mockActorRef).tell(any(PoisonPill.class), eq(ActorRef.noSender()));
            assertFalse(BuySideConnect.getActorPerSessionMaps().containsKey(SESSION_KEY));
        }

        @Test
        @DisplayName("Bloquea sin error si el usuario NO está conectado")
        void blockUser_toleratesUserNotConnected() {
            assertDoesNotThrow(() -> SessionDisconnectService.blockUser("nuevo_usuario"),
                    "Bloquear un usuario no conectado no debe lanzar excepción");
            assertTrue(MainApp.isUserBlocked("nuevo_usuario"));
        }

        @Test
        @DisplayName("Un usuario bloqueado aparece en getBlockedUsers()")
        void blockUser_reflectsInMainApp() {
            SessionDisconnectService.blockUser(USERNAME);

            assertTrue(MainApp.getBlockedUsers().contains(USERNAME));
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Tests de DESBLOQUEO DE USUARIOS
    // ─────────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("unblockUser()")
    class UnblockUserTests {

        @BeforeEach
        void blockFirst() {
            MainApp.blockUser(USERNAME);
        }

        @Test
        @DisplayName("Elimina el username del set de usuarios bloqueados")
        void unblockUser_removesFromBlockedSet() {
            SessionDisconnectService.unblockUser(USERNAME);

            assertFalse(MainApp.isUserBlocked(USERNAME),
                    "El usuario NO debe estar en blockedUsers tras ser desbloqueado");
        }

        @Test
        @DisplayName("No lanza excepción al desbloquear usuario que no estaba bloqueado")
        void unblockUser_toleratesNotBlocked() {
            assertDoesNotThrow(() -> SessionDisconnectService.unblockUser("usuario_no_bloqueado"));
        }
    }
}
