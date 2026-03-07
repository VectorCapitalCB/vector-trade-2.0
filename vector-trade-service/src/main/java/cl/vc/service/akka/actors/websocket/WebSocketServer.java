package cl.vc.service.akka.actors.websocket;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.ActorPerSession;
import cl.vc.service.akka.actors.BuySideConnect;
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
import org.json.JSONObject;

import javax.servlet.ServletException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

@WebSocket
@Slf4j
public class WebSocketServer extends Thread {

    @Getter
    @Setter
    private Properties properties;

    @OnWebSocketConnect
    public void onConnect(Session session) {

        try {

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
            JSONObject request = new JSONObject(message);
            String action = request.optString("action", "").trim().toLowerCase();

            if (action.isEmpty()) {
                sendError(session, "action es obligatoria");
                return;
            }

            if ("ping".equals(action)) {
                JSONObject response = new JSONObject();
                response.put("type", "pong");
                session.getRemote().sendString(response.toString());
                return;
            }

            sendError(session, "action no soportada: " + action);
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

    private void sendError(Session session, String message) throws IOException {
        JSONObject response = new JSONObject();
        response.put("type", "error");
        response.put("message", message);
        session.getRemote().sendString(response.toString());
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
            context.addFilter(authFilter, "/websocket/*", null);

            FilterHolder authFilters = new FilterHolder(new AuthenticationFilter());
            context.addFilter(authFilters, "/*", null);

            ServletHolder wsHolder = new ServletHolder("ws", ProtectedWebSocketServlet.class);
            context.addServlet(wsHolder, "/websocket/*");

            WebSocketUpgradeFilter wsFilter = WebSocketUpgradeFilter.configureContext(context);
            wsFilter.getFactory().getPolicy().setMaxBinaryMessageSize(100 * 1024 * 1024);
            wsFilter.getFactory().getPolicy().setIdleTimeout(300000);
            wsFilter.getFactory().getExtensionFactory().register("permessage-deflate", PerMessageDeflateExtension.class);


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


}
