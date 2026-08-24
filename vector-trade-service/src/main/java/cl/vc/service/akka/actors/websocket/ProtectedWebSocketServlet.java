package cl.vc.service.akka.actors.websocket;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

public class ProtectedWebSocketServlet extends WebSocketServlet {
    @Override
    public void configure(WebSocketServletFactory factory) {
        factory.getPolicy().setMaxBinaryMessageSize(WebSocketServer.MAX_BINARY_MESSAGE_SIZE);
        factory.register(WebSocketServer.class);
    }
}
