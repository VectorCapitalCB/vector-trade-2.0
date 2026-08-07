package cl.vc.candle.websocket;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@WebSocket
public class CandleWebSocketEndpoint {
    private static final Logger log = LoggerFactory.getLogger(CandleWebSocketEndpoint.class);
    // Hay dias de replay con >16k documentos en instrument_daily; se acota salvo que el cliente pida otro limite.
    private static final int DEFAULT_DAILY_LIMIT = 500;

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

        if ("load_day_stats".equals(action)) {
            String date = request.optString("date", "").trim();
            if (date.isEmpty()) {
                sendError(session, "load_day_stats requiere date");
                return;
            }
            CandleProtoMarketPublisher publisher = CandleProtoMarketPublisher.getInstance();
            if (publisher == null) {
                sendError(session, "publisher de mercado no disponible");
                return;
            }
            LocalDate day;
            try {
                day = LocalDate.parse(date);
            } catch (Exception e) {
                sendError(session, "date invalida, formato esperado yyyy-MM-dd");
                return;
            }
            boolean sent = publisher.sendHistoricalStatsForDate(session, day);
            if (!sent) {
                sendError(session, "sin data historica para " + date);
            }
            return;
        }

        if ("load_intraday_series".equals(action)) {
            CandleProtoMarketPublisher publisher = CandleProtoMarketPublisher.getInstance();
            if (publisher == null) {
                sendError(session, "publisher de mercado no disponible");
                return;
            }
            LocalDate day = null;
            String date = request.optString("date", "").trim();
            if (!date.isEmpty()) {
                if (!isValidDay(date)) {
                    sendError(session, "date invalida, formato esperado yyyy-MM-dd");
                    return;
                }
                day = LocalDate.parse(date);
            }
            // Sin symbols devuelve todos los instrumentos con trades del dia; el front manda solo
            // los de su portafolio para no arrastrar ~250 series por respuesta.
            List<String> symbols = new ArrayList<>();
            JSONArray requested = request.optJSONArray("symbols");
            if (requested != null) {
                for (int i = 0; i < requested.length(); i++) {
                    String s = requested.optString(i, "").trim();
                    if (!s.isEmpty()) {
                        symbols.add(s);
                    }
                }
            }
            if (!publisher.sendIntradaySeries(session, day, symbols, request.optInt("bucketMinutes", 0))) {
                sendError(session, "no fue posible construir la serie intradia");
            }
            return;
        }

        if ("load_instrument_daily".equals(action)) {
            CandleMongoPublisher publisher = CandleMongoPublisher.getInstance();
            if (publisher == null) {
                sendError(session, "publisher de instrument_daily no disponible");
                return;
            }
            String market = request.optString("market", "BCS").trim();
            String symbol = request.optString("symbol", "").trim();
            boolean sent;
            if (symbol.isEmpty()) {
                String date = request.optString("date", "").trim();
                if (!isValidDay(date)) {
                    sendError(session, "load_instrument_daily requiere date (yyyy-MM-dd) o symbol + from/to");
                    return;
                }
                sent = publisher.sendInstrumentDailyByDay(session, market, date, request.optInt("limit", DEFAULT_DAILY_LIMIT));
            } else {
                String from = request.optString("from", "").trim();
                String to = request.optString("to", "").trim();
                if (!isValidDay(from) || !isValidDay(to)) {
                    sendError(session, "load_instrument_daily con symbol requiere from y to (yyyy-MM-dd)");
                    return;
                }
                sent = publisher.sendInstrumentDailyBySymbol(session, market, symbol, from, to);
            }
            if (!sent) {
                sendError(session, "no fue posible consultar instrument_daily");
            }
            return;
        }

        sendError(session, "action no soportada");
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        log.info("Sesion websocket cerrada session={} statusCode={} reason={}", session, statusCode, reason);
        CandleSubscriptions.removeSession(session);
    }

    private boolean isValidDay(String value) {
        try {
            LocalDate.parse(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void sendError(Session session, String message) throws Exception {
        session.getRemote().sendString(new JSONObject().put("type", "error").put("message", message).toString());
    }
}
