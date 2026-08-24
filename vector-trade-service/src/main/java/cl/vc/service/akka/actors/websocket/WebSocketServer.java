package cl.vc.service.akka.actors.websocket;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.ActorPerSession;
import cl.vc.service.akka.actors.BuySideConnect;
import cl.vc.service.multibook.Multibook2Servlet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.common.extensions.compress.PerMessageDeflateExtension;
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter;

import javax.servlet.ServletException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Properties;

@WebSocket
@Slf4j
public class WebSocketServer extends Thread {

    static final int MAX_BINARY_MESSAGE_SIZE = 1024 * 1024;

    @Getter
    @Setter
    private Properties properties;

    @OnWebSocketConnect
    public void onConnect(Session session) {

        try {
            String ip = extractIp(session);

            // Rechazar IPs bloqueadas antes de crear el actor
            if (MainApp.isIpBlocked(ip)) {
                log.warn("[IpSecurity] Conexión rechazada — IP bloqueada: {}", ip);
                session.close(1008, "IP bloqueada por política de seguridad");
                return;
            }

            if (!BuySideConnect.getActorPerSessionMaps().containsKey(session.getRemote().toString())) {
                ActorRef client = MainApp.getSystem().actorOf(ActorPerSession.props(session).withDispatcher("ActorperSession"));
                BuySideConnect.getActorPerSessionMaps().put(session.getRemote().toString(), client);
            }

            log.info("la sesion se conecto {}", session.getRemote().toString());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    @OnWebSocketMessage
    public void onMessage(Session session, byte[] buf, int offset, int length) throws IOException {

        try {
            String ip = extractIp(session);

            // Rate-limit: si excede el umbral de mensajes, auto-bloquear la IP
            if (MainApp.isIpBlocked(ip)) {
                if (session != null && session.isOpen()) {
                    session.close(1008, "IP bloqueada por política de seguridad");
                }
                return;
            }

            if (MainApp.recordIpMessageExceeded(ip)) {
                MainApp.blockIp(ip);
                log.warn("[IpSecurity] IP auto-bloqueada por exceso de mensajes: {}", ip);
                session.close(1008, "IP bloqueada: rate limit excedido");
                return;
            }

            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, offset, length);

            if (BuySideConnect.getActorPerSessionMaps().containsKey(session.getRemote().toString())) {
                ActorRef actorRef = BuySideConnect.getActorPerSessionMaps().get(session.getRemote().toString());
                actorRef.tell(byteBuffer, ActorRef.noSender());
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    @OnWebSocketMessage
    public void onMessage(Session session, String message) throws IOException {
        try {
            log.info("receives message String {}", message);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        try {

            if (BuySideConnect.getActorPerSessionMaps().containsKey(session.getRemote().toString())) {
                ActorRef actorToDelete = BuySideConnect.getActorPerSessionMaps().get(session.getRemote().toString());
                actorToDelete.tell(PoisonPill.getInstance(), ActorRef.noSender());
                BuySideConnect.getActorPerSessionMaps().remove(session.getRemote().toString());
                log.error("Actor eliminado id {}", session.getRemote().toString());
            } else {
                log.error("Actor no fue encontrado para eliminar id {}", session.getRemote().toString());
            }

            log.info("la sesion se desconecto {} {} {} ", session.getRemote().toString(), statusCode, reason);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }

    @OnWebSocketError
    public void onWebSocketError(Session session, Throwable cause) {
        try {

            log.error("Error en WebSocket: Sesión {} - Causa: {}", session != null ? session.getRemoteAddress().getAddress() : "desconocida", cause.getMessage(), cause);

            if (session != null && session.isOpen()) {
                log.info("Cerrando sesión WebSocket debido a un error...");
                session.close();
            }

        } catch (Exception e) {
            log.error("Error al manejar el error de WebSocket: {}", e.getMessage(), e);
        }

    }


    @Override
    public void run() {

        try {

            Server server = new Server(Integer.parseInt(properties.getProperty("websocket.port")));

            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");
            server.setHandler(context);
            context.addServlet(ProtectedWebSocketServlet.class, "/");

            FilterHolder authFilter = new FilterHolder(new AuthenticationFilter());
            context.addFilter(authFilter, "/websocket/*", (EnumSet)null);

            FilterHolder authFilters = new FilterHolder(new AuthenticationFilter());
            context.addFilter(authFilters, "/*", (EnumSet)null);

            ServletHolder wsHolder = new ServletHolder("ws", ProtectedWebSocketServlet.class);
            context.addServlet(wsHolder, "/websocket/*");

            // Multibook 2.0: mismo puerto y mismo AuthenticationFilter que el upgrade del websocket.
            context.addServlet(new ServletHolder(new Multibook2Servlet()), "/api/multibook2/*");

            WebSocketUpgradeFilter wsFilter = WebSocketUpgradeFilter.configureContext(context);
            wsFilter.getFactory().getPolicy().setMaxBinaryMessageSize(MAX_BINARY_MESSAGE_SIZE);
            wsFilter.getFactory().getPolicy().setIdleTimeout(300000);
            if (MainApp.isWebSocketCompressionEnabled()) {
                wsFilter.getFactory().getExtensionFactory().register("permessage-deflate", PerMessageDeflateExtension.class);
                log.info("[WebSocket] permessage-deflate habilitado");
            } else {
                log.warn("[WebSocket] permessage-deflate deshabilitado por propiedad websocket.compression.enabled=false");
            }


            try {

                log.info("se inicia websocket en el puerto {}", properties.getProperty("websocket.port"));
                server.start();
                server.join();

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

        } catch (ServletException e) {
            log.error(e.getMessage(), e);
        }

    }

    /** Extrae la dirección IP del cliente de la sesión WebSocket. */
    static String extractIp(Session session) {
        try {
            if (session != null && session.getUpgradeRequest() != null) {
                String forwardedFor = session.getUpgradeRequest().getHeader("X-Forwarded-For");
                if (forwardedFor != null && !forwardedFor.isBlank()) {
                    String candidate = forwardedFor.split(",")[0].trim();
                    if (!candidate.isBlank()) {
                        return candidate;
                    }
                }

                String realIp = session.getUpgradeRequest().getHeader("X-Real-IP");
                if (realIp != null && !realIp.isBlank()) {
                    return realIp.trim();
                }
            }

            if (session != null && session.getRemoteAddress() != null) {
                return session.getRemoteAddress().getAddress().getHostAddress();
            }
        } catch (Exception ignored) {}
        return "unknown";
    }

}
