package cl.vc.chat;

import cl.vc.chat.websocket.ChatViewServlet;
import cl.vc.chat.websocket.ChatWebSocketServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ChatMain {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String configPath = args != null && args.length > 0
                ? args[0]
                : "src/main/resources/application.properties";

        try (InputStream in = new FileInputStream(configPath)) {
            properties.load(in);
        }

        int port = Integer.parseInt(properties.getProperty("chat.websocket.port", "8097"));

        Server server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setAttribute("chat.properties", properties);
        server.setHandler(context);

        context.addServlet(new ServletHolder("chat-ws", new ChatWebSocketServlet()), "/ws/*");
        context.addServlet(new ServletHolder("chat-view", new ChatViewServlet()), "/");

        server.start();
        server.join();
    }
}
