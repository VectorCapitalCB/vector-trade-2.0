package cl.vc.candle;

import cl.vc.candle.websocket.CandleMongoPublisher;
import cl.vc.candle.websocket.CandleWebSocketServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class CandleMain {
    private static final Logger log = LoggerFactory.getLogger(CandleMain.class);

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String configPath = args != null && args.length > 0
                ? args[0]
                : "src/main/resources/application.properties";
        log.info("Cargando configuracion desde {}", configPath);
        try (InputStream in = new FileInputStream(configPath)) {
            properties.load(in);
        }

        CandleMongoPublisher publisher = new CandleMongoPublisher(properties);
        publisher.start();

        int port = Integer.parseInt(properties.getProperty("candle.websocket.port", "8098"));
        Server server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder("candle-ws", new CandleWebSocketServlet()), "/ws/*");

        log.info("Iniciando websocket candle en puerto {} path /ws/*", port);
        server.start();
        log.info("Servicio candle iniciado");
        server.join();
    }
}
