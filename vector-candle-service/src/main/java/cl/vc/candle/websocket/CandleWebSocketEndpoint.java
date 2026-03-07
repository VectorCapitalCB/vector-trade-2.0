package cl.vc.candle.websocket;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebSocket
public class CandleWebSocketEndpoint {
    private static final Logger log = LoggerFactory.getLogger(CandleWebSocketEndpoint.class);

    @OnWebSocketConnect
    public void onConnect(Session session) {
        CandleSubscriptions.registerSession(session);
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String raw) throws Exception {
        JSONObject request = new JSONObject(raw);
        String action = request.optString("action", "").trim().toLowerCase();

        if ("subscribe".equals(action)) {
            String symbol = request.optString("symbol", "").trim();
            String timeframe = request.optString("timeframe", "").trim();
            if (symbol.isEmpty() || timeframe.isEmpty()) {
                sendError(session, "subscribe requiere symbol y timeframe");
                return;
            }
            CandleSubscriptionKey key = new CandleSubscriptionKey(symbol, timeframe);
            CandleSubscriptions.subscribe(session, key);
            log.info("Suscripcion agregada session={} symbol={} timeframe={}", session, symbol, timeframe);
            session.getRemote().sendString(new JSONObject().put("type", "ack").put("event", "subscribed").put("symbol", symbol).put("timeframe", timeframe).toString());
            return;
        }

        if ("unsubscribe".equals(action)) {
            String symbol = request.optString("symbol", "").trim();
            String timeframe = request.optString("timeframe", "").trim();
            if (symbol.isEmpty() || timeframe.isEmpty()) {
                sendError(session, "unsubscribe requiere symbol y timeframe");
                return;
            }
            CandleSubscriptionKey key = new CandleSubscriptionKey(symbol, timeframe);
            CandleSubscriptions.unsubscribe(session, key);
            log.info("Suscripcion removida session={} symbol={} timeframe={}", session, symbol, timeframe);
            session.getRemote().sendString(new JSONObject().put("type", "ack").put("event", "unsubscribed").put("symbol", symbol).put("timeframe", timeframe).toString());
            return;
        }

        if ("ping".equals(action)) {
            session.getRemote().sendString(new JSONObject().put("type", "pong").toString());
            return;
        }

        sendError(session, "action no soportada");
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        log.info("Sesion websocket cerrada session={} statusCode={} reason={}", session, statusCode, reason);
        CandleSubscriptions.removeSession(session);
    }

    private void sendError(Session session, String message) throws Exception {
        session.getRemote().sendString(new JSONObject().put("type", "error").put("message", message).toString());
    }
}
