package cl.vc.news;

import cl.vc.news.scraper.NewsMongoRepository;
import cl.vc.news.scraper.NewsScraperPublisher;
import cl.vc.news.websocket.NewsWebSocketServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class NewsMain {
    private static final Logger log = LoggerFactory.getLogger(NewsMain.class);

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String configPath = args != null && args.length > 0
                ? args[0]
                : "src/main/resources/application.properties";
        log.info("Cargando configuracion desde {}", configPath);
        try (InputStream in = new FileInputStream(configPath)) {
            properties.load(in);
        }

        NewsMongoRepository.init(properties);
        NewsScraperPublisher publisher = new NewsScraperPublisher(properties);
        publisher.start();

        int port = Integer.parseInt(properties.getProperty("news.websocket.port", "8099"));
        String wsPath = properties.getProperty("news.websocket.path", "/ws/");
        String mapping = normalizeWsMapping(wsPath);

        Server server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder("news-ws", new NewsWebSocketServlet()), mapping);

        log.info("Iniciando websocket news en puerto {} path {}", port, mapping);
        server.start();
        log.info("Servicio news iniciado");
        server.join();
    }

    private static String normalizeWsMapping(String wsPath) {
        String value = wsPath == null || wsPath.trim().isEmpty() ? "/ws/" : wsPath.trim();
        if (!value.startsWith("/")) {
            value = "/" + value;
        }
        if (!value.endsWith("/")) {
            value = value + "/";
        }
        return value + "*";
    }
}
