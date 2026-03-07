package cl.vc.news.websocket;

import cl.vc.news.scraper.NewsMongoRepository;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@WebSocket
public class NewsWebSocketEndpoint {
    private static final Logger log = LoggerFactory.getLogger(NewsWebSocketEndpoint.class);

    @OnWebSocketConnect
    public void onConnect(Session session) {
        NewsSessionRegistry.register(session);
        try {
            sendSnapshot(session, 300);
        } catch (Exception e) {
            log.warn("No se pudo enviar snapshot inicial news", e);
        }
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String raw) throws Exception {
        JSONObject request = new JSONObject(raw);
        String action = request.optString("action", request.optString("type", "")).trim().toLowerCase();

        if ("ping".equals(action)) {
            session.getRemote().sendString(new JSONObject().put("type", "pong").toString());
            return;
        }

        if ("snapshot".equals(action) || "snapshot_request".equals(action)) {
            int limit = request.optInt("limit", 300);
            sendSnapshot(session, limit);
            return;
        }

        if ("subscribe".equals(action)) {
            session.getRemote().sendString(new JSONObject()
                    .put("type", "ack")
                    .put("event", "subscribed")
                    .toString());
            return;
        }

        session.getRemote().sendString(new JSONObject()
                .put("type", "error")
                .put("message", "action no soportada")
                .toString());
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        log.info("Sesion websocket news cerrada session={} statusCode={} reason={}", session, statusCode, reason);
        NewsSessionRegistry.unregister(session);
    }

    private void sendSnapshot(Session session, int limit) throws Exception {
        List<NewsMongoRepository.NewsRow> messages = NewsMongoRepository.lastRows(limit);
        JSONArray rows = new JSONArray();
        for (NewsMongoRepository.NewsRow message : messages) {
            rows.put(new JSONObject()
                    .put("type", message.type == null || message.type.isBlank() ? "news" : message.type)
                    .put("message", message.message == null ? "" : message.message)
                    .put("url", message.url == null ? "" : message.url)
                    .put("title", message.title == null ? "" : message.title)
                    .put("source", message.source == null ? "" : message.source)
                    .put("impact", message.impact == null ? "NORMAL" : message.impact)
                    .put("publishedAt", message.publishedAt));
        }
        session.getRemote().sendString(new JSONObject()
                .put("type", "snapshot")
                .put("messages", rows)
                .toString());
    }
}
