package cl.vc.service.admin;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.BuySideConnect;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;

/**
 * Servicio que centraliza la desconexión forzada y bloqueo de usuarios WebSocket desde el admin.
 *
 * Flujo disconnect:
 *  1. Obtener la Session y el ActorRef asociados al username
 *  2. Enviar PoisonPill al actor → dispara postStop → limpieza completa (event bus, cuentas, etc.)
 *  3. Cerrar la sesión WebSocket → envía close frame al cliente
 *  4. Eliminar de BuySideConnect.actorPerSessionMaps
 *  5. Eliminar de MainApp.userActiveSessionsMap / userSessionConnectedAt
 *
 * Flujo blockUser:
 *  1. Añadir username a MainApp.blockedUsers (persistido en Redis sin expiración)
 *  2. Si está conectado, ejecutar flujo disconnect
 */
@Slf4j
public class SessionDisconnectService {

    private SessionDisconnectService() {}

    /**
     * Desconecta forzadamente al usuario identificado por {@code username}.
     *
     * @return mensaje descriptivo del resultado
     * @throws UserNotConnectedException si el usuario no está en el mapa de sesiones activas
     */
    public static String disconnect(String username) {
        Session session = MainApp.getUserActiveSessionsMap().get(username);
        if (session == null) {
            throw new UserNotConnectedException("Usuario no conectado: " + username);
        }

        String sessionKey = session.getRemote() != null ? session.getRemote().toString() : null;

        // 1. Matar el actor Akka → dispara postStop con limpieza completa
        if (sessionKey != null) {
            ActorRef actorRef = BuySideConnect.getActorPerSessionMaps().get(sessionKey);
            if (actorRef != null) {
                actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
                BuySideConnect.getActorPerSessionMaps().remove(sessionKey);
                log.info("[SessionDisconnect] Actor eliminado para usuario '{}' sessionKey='{}'", username, sessionKey);
            }
        }

        // 2. Cerrar la sesión WebSocket (envía close frame al cliente)
        if (session.isOpen()) {
            try {
                session.close(StatusCode.NORMAL, "Desconectado por administrador");
            } catch (Exception e) {
                log.warn("[SessionDisconnect] Error al cerrar WebSocket de '{}': {}", username, e.getMessage());
            }
        }

        // 3. Limpiar mapas de seguimiento del admin
        MainApp.getUserActiveSessionsMap().remove(username);
        MainApp.getUserSessionConnectedAt().remove(username);

        log.info("[SessionDisconnect] Usuario '{}' desconectado correctamente por admin", username);
        return "Usuario '" + username + "' desconectado correctamente";
    }

    public static class UserNotConnectedException extends RuntimeException {
        public UserNotConnectedException(String msg) { super(msg); }
    }

    /**
     * Bloquea un usuario: lo desconecta si está en línea y persiste el bloqueo en Redis.
     * A partir de este momento el usuario no puede reconectarse.
     *
     * @return mensaje descriptivo del resultado
     */
    public static String blockUser(String username) {
        MainApp.blockUser(username);

        Session session = MainApp.getUserActiveSessionsMap().get(username);
        if (session == null) {
            log.info("[UserBlock] Usuario '{}' bloqueado (no estaba conectado)", username);
            return "Usuario '" + username + "' bloqueado";
        }

        // Desconectar sesión activa
        String sessionKey = session.getRemote() != null ? session.getRemote().toString() : null;
        if (sessionKey != null) {
            ActorRef actorRef = BuySideConnect.getActorPerSessionMaps().get(sessionKey);
            if (actorRef != null) {
                actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
                BuySideConnect.getActorPerSessionMaps().remove(sessionKey);
                log.info("[UserBlock] Actor eliminado para usuario '{}' sessionKey='{}'", username, sessionKey);
            }
        }

        if (session.isOpen()) {
            try {
                session.close(StatusCode.NORMAL, "Bloqueado por administrador");
            } catch (Exception e) {
                log.warn("[UserBlock] Error al cerrar WebSocket de '{}': {}", username, e.getMessage());
            }
        }

        MainApp.getUserActiveSessionsMap().remove(username);
        MainApp.getUserSessionConnectedAt().remove(username);

        log.info("[UserBlock] Usuario '{}' bloqueado y desconectado por admin", username);
        return "Usuario '" + username + "' bloqueado y desconectado";
    }

    /**
     * Desbloquea un usuario previamente bloqueado. Puede volver a conectarse.
     */
    public static void unblockUser(String username) {
        MainApp.unblockUser(username);
        log.info("[UserBlock] Usuario '{}' desbloqueado por admin", username);
    }
}
