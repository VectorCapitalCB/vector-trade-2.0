package cl.vc.chat.websocket;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import java.util.Properties;

public class ChatWebSocketServlet extends WebSocketServlet {

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        Object obj = config.getServletContext().getAttribute("chat.properties");
        if (obj instanceof Properties) {
            ChatMongoRepository.init((Properties) obj);
        } else {
            ChatMongoRepository.init(new Properties());
        }
    }

    @Override
    public void configure(WebSocketServletFactory factory) {
        factory.register(ChatWebSocketEndpoint.class);
    }
}
