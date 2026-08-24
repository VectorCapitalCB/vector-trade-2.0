package cl.vc.blotter.ws;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.routing.RoundRobinPool;
import cl.vc.blotter.Repository;
import cl.vc.blotter.adaptor.ParseMessageActor;
import cl.vc.blotter.model.HistoricalCandle;
import cl.vc.blotter.model.TradeCandle;
import cl.vc.blotter.utils.Notifier;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.InterfaceTcp;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import cl.vc.module.protocolbuff.ws.vectortrade.MessageUtilVT;
import com.google.protobuf.Message;
import javafx.application.Platform;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.json.JSONArray;

import java.time.LocalDate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.java_websocket.exceptions.WebsocketNotConnectedException;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
public class SimpleWebSocketListener extends WebSocketAdapter implements InterfaceTcp {

    private static final int RECONNECT_DELAY_DEFAULT = 7;
    private static final int RECONNECT_DELAY_CHAT = 3;
    private static final int CHAT_HEARTBEAT_SECONDS = 3;
    private static final int NEWS_HEARTBEAT_SECONDS = 60;

    private static final int CONNECTION_TIMEOUT = 600000;  // 5 minutos
    private static final int CHAT_CONNECTION_TIMEOUT = 15000; // 15 segundos

    private static final int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;  // 100 MB

    private ActorRef parseMessageActor;

    private ActorRef clientActor;

    private NotificationMessage.Component component;

    private String username;

    private ScheduledExecutorService schedulersound = Executors.newSingleThreadScheduledExecutor();

    @Setter
    @Getter
    private boolean autoReconnect = true;

    @Setter
    @Getter
    private boolean closeFailure = false;

    private ScheduledExecutorService scheduler;
    private ScheduledExecutorService channelHeartbeatScheduler;

    private WebSocketClient client;

    private Runnable tasks;

    private String enviroment;

    private ClientUpgradeRequest request;
    private String channelName = "service";
    private volatile boolean channelConnected = false;


    public SimpleWebSocketListener(WebSocketClient client, ActorRef clientActor, ActorSystem actorSystem,
                                   NotificationMessage.Component component,
                                   String username, String enviroment, ClientUpgradeRequest request) {
        this(client, clientActor, actorSystem, component, username, enviroment, request, "service");
    }

    public SimpleWebSocketListener(WebSocketClient client, ActorRef clientActor, ActorSystem actorSystem,
                                   NotificationMessage.Component component,
                                   String username, String enviroment, ClientUpgradeRequest request,
                                   String channelName) {

        try {

            this.client = client;
            this.enviroment = enviroment;
            this.request = request;
            this.channelName = channelName == null ? "service" : channelName.toLowerCase();

            client.getPolicy().setMaxTextMessageSize(MAX_MESSAGE_SIZE);
            client.getPolicy().setMaxBinaryMessageSize(MAX_MESSAGE_SIZE);
            client.getPolicy().setIdleTimeout(getChannelTimeout());


            parseMessageActor = actorSystem.actorOf(new RoundRobinPool(20).props(ParseMessageActor.props(clientActor)));
            this.clientActor = clientActor;
            this.username = username;
            this.component = component;

            tasks = new Runnable() {
                public void run() {
                    Repository.setSound(true);
                    shutdownSoundScheduler();
                }
            };

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private boolean isServiceChannel() {
        return "service".equals(channelName);
    }

    private void updateChannelClientService() {
        if ("candle".equals(channelName)) {
            Repository.setCandleClientService(this);
        } else if ("chat".equals(channelName)) {
            Repository.setChatClientService(this);
        } else if ("news".equals(channelName)) {
            Repository.setNewsClientService(this);
        } else {
            Repository.setClientService(this);
        }
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        super.onWebSocketConnect(sess);
        channelConnected = true;
        Repository.setChannelConnected(channelName, true);
        try {
            sess.setIdleTimeout(getChannelTimeout());
        } catch (Exception ignored) {
        }

        Platform.runLater(() -> {
            if (isServiceChannel()) {
                if (schedulersound.isShutdown() || schedulersound.isTerminated()) {
                    schedulersound = Executors.newSingleThreadScheduledExecutor();
                }
                schedulersound.schedule(tasks, 7, TimeUnit.SECONDS);
            }

            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }

            if ("chat".equals(channelName)) {
                startChannelHeartbeat();
                sendChatConnectEvent();
                requestChatSnapshot();
            } else if ("news".equals(channelName)) {
                startChannelHeartbeat();
                requestNewsSnapshot();
            }
        });
    }

    @Override
    public void onWebSocketText(String message) {
        if ("chat".equals(channelName)) {
            Repository.appendChatMessage("SERVER: " + message);
            return;
        }
        if ("news".equals(channelName)) {
            handleNewsMessage(message);
            return;
        }
        if ("candle".equals(channelName)) {
            handleCandleMessage(message);
            return;
        }
        log.info("Received STRING [{}]: {}", channelName, message);
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {

        super.onWebSocketClose(statusCode, reason);
        channelConnected = false;
        Repository.setChannelConnected(channelName, false);
        stopChannelHeartbeat();

        log.error("######## DESCONEXION reason {} {}", reason, statusCode);


        /*
        if (this.username.equals("vnazar")) {

            Platform.runLater(() -> {

                try {
                    Repository.getPrincipalController().getMarketDataViewerController().getWorkingOrderController().getData().clear();
                    Repository.getPrincipalController().getMarketDataViewerController().getExecutionsOrderController().getData().clear();
                    Repository.getPrincipalController().getMarketDataViewerController().getWorkingOrderController().getTableExecutionReports().refresh();


                    Repository.getBookPortMaps().forEach((key, value) -> {
                        value.getTradesVO().clear();
                        value.getBidBook().clear();
                        value.getAskBook().clear();
                    });


                    Repository.getPrincipalController().getMarketDataViewerController().getBidViewTable().getItems().clear();
                    Repository.getPrincipalController().getMarketDataViewerController().getOfferViewTable().getItems().clear();

                    Repository.getPrincipalController().getMarketDataViewerController().getBidViewTable().refresh();
                    Repository.getPrincipalController().getMarketDataViewerController().getOfferViewTable().refresh();


                    Repository.getTradeGenerales().clear();
                    Repository.getPrincipalController().getMarketDataViewerController().getMarketDataTradeTableG().refresh();

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

            });

        }

         */


        if (statusCode == 1006 || statusCode == 1000) {

            try {

                if (isServiceChannel()) {
                    Repository.setSound(false);
                }
                try {
                    client.connect(this, new URI(enviroment), request).get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    initiateReconnection();
                    return;
                }

                if (this.isChannelConnected()) {

                    log.info("conectado de nuevo ");

                    updateChannelClientService();
                    if (isServiceChannel()) {
                        SessionsMessage.Connect connect = SessionsMessage.Connect.newBuilder().setUsername(Repository.getUsername()).build();
                        Repository.getClientService().sendMessage(connect);

                        Repository.createSuscripcion(
                                Repository.getDolarSymbol(),
                                MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL,
                                RoutingMessage.SettlType.T2,
                                RoutingMessage.SecurityType.CS);

                        try {
                            if (schedulersound.isShutdown() || schedulersound.isTerminated()) {
                                schedulersound = Executors.newSingleThreadScheduledExecutor();
                            }
                            schedulersound.schedule(tasks, 13, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            Thread.sleep(5000);
                            Repository.setSound(true);
                            shutdownSoundScheduler();
                        }
                    } else if ("chat".equals(channelName)) {
                        sendChatConnectEvent();
                        requestChatSnapshot();
                    } else if ("news".equals(channelName)) {
                        requestNewsSnapshot();
                    }

                } else {
                    initiateReconnection();
                }

            } catch (Exception e) {
                initiateReconnection();
                log.error(e.getMessage(), e);
            }

            return;

        }

        if (isServiceChannel()) {
            SessionsMessage.Disconnect disconnect = SessionsMessage.Disconnect.newBuilder().setTokenKeycloak(username)
                    .setText(reason)
                    .setComponent(NotificationMessage.Component.VECTOR_TRADE_SERVICES).build();
            clientActor.tell(disconnect, ActorRef.noSender());
        }


        if (closeFailure) {

            log.info("no se hace la reconexion");

            if (scheduler != null) {
                scheduler.shutdown();
            }
            autoReconnect = false;
            return;
        }

        if (isServiceChannel()) {
            handleDisconnection(reason);
        } else {
            log.warn("Canal {} desconectado: {}", channelName, reason);
        }

        if (autoReconnect) {
            autoReconnect = false;
            initiateReconnection();
        }


    }

    private void handleDisconnection(String reason) {
        try {

            NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                    .setComments(reason)
                    .setComponent(component)
                    .setTypeState(NotificationMessage.TypeState.DISCONNECTION)
                    .setLevel(NotificationMessage.Level.FATAL)
                    .setTime(TimeGenerator.getTimeProto())
                    .setTitle("Error Services").build();

            TransportingObjects transportingObjectss = new TransportingObjects(notification);
            clientActor.tell(transportingObjectss, ActorRef.noSender());


        } catch (Exception e) {
            log.error("Error handling disconnection", e);
        }
    }


    private void initiateReconnection() {

        try {
            if (scheduler != null && !scheduler.isShutdown() && !scheduler.isTerminated()) {
                return;
            }

            scheduler = Executors.newScheduledThreadPool(2);

            scheduler.scheduleWithFixedDelay(() -> {
                try {

                    log.info("Attempting to reconnect...");

                    Notifier.INSTANCE.notifyWarning("Desconexión", "Intentando reconectar " + channelName);

                    if (this.getSession() == null || !this.getSession().isOpen()) {
                        try {
                            client.connect(this, new URI(enviroment), request).get(5, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            client.start();
                        }
                    }

                    if (this.getSession() != null && this.getSession().isOpen()) {
                        autoReconnect = true;
                        log.info("Reconnection successful.");
                        updateChannelClientService();
                        if (isServiceChannel()) {
                            SessionsMessage.Connect connect = SessionsMessage.Connect.newBuilder().setUsername(Repository.getUsername()).build();
                            Repository.getClientService().sendMessage(connect);
                        } else if ("chat".equals(channelName)) {
                            sendChatConnectEvent();
                            requestChatSnapshot();
                        } else if ("news".equals(channelName)) {
                            requestNewsSnapshot();
                        }

                        /*
                        if (Repository.getPrincipalController() != null && Repository.getPrincipalController().getMarketDataViewerController() != null) {
                            Repository.getPrincipalController().getMarketDataViewerController().requestPortfolio();
                        }
                        */

                        scheduler.shutdown();

                    }
                } catch (Exception e) {
                    log.error("Reconnection attempt failed", e);
                }
            }, 0, getReconnectDelay(), TimeUnit.SECONDS);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(payload, offset, len);
        parseMessageActor.tell(byteBuffer, ActorRef.noSender());
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        Session session = getSession();
        boolean sessionOpen = session != null && session.isOpen();

        log.error("############### ERROR  WEBSOCKET [{}] ########## {}", channelName, cause.getMessage());
        log.error(cause.getMessage(), cause);

        // Avoid false "OFF" states when Jetty reports transient errors but channel is still open.
        if (sessionOpen) {
            return;
        }

        channelConnected = false;
        Repository.setChannelConnected(channelName, false);
        stopChannelHeartbeat();
        if (autoReconnect) {
            initiateReconnection();
        }
    }


    @Override
    public void sendMessage(String s) {
        try {
            if (getSession() != null && getSession().isOpen()) {
                getSession().getRemote().sendStringByFuture(s);
            } else {
                log.error("WebSocket {} is not connected. Cannot send string message.", channelName);
            }
        } catch (Exception e) {
            log.error("Failed to send string message on channel {}: {}", channelName, e.getMessage(), e);
        }
    }

    @Override
    public void sendMessage(Message message) {
        try {
            if (getSession() != null && getSession().isOpen()) {
                ByteBuffer message1 = MessageUtilVT.serializeMessageByteBuffer(message);
                getSession().getRemote().sendBytesByFuture(message1);
            } else {
                log.error("WebSocket is not connected. Cannot send message: {}", message);
            }

        } catch (WebsocketNotConnectedException e) {
            log.error("WebSocket not connected: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Failed to send message: {}", e.getMessage(), e);
        }

    }

    @Override
    public void stopService() {
        try {
            autoReconnect = false;
            stopChannelHeartbeat();
            shutdownSoundScheduler();
            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }
            if (getSession() != null && getSession().isOpen()) {
                getSession().close();
            }
            if (client != null && client.isStarted()) {
                client.stop();
            }
        } catch (Exception e) {
            log.error("Unexpected error while closing WebSocket session: {}", e.getMessage(), e);
        }

    }

    public void stopServiceForce() {
        try {

            log.info("Boton reconectar !!!!!!!!!!");

            if (getSession() != null && getSession().isOpen()) {
                log.info("Closing WebSocket session...");
                getSession().close();
                log.info("WebSocket session closed.");
            } else if (getSession() == null) {
                client.connect(this, new URI(enviroment), request).get(5, TimeUnit.SECONDS);

            } else {
                log.warn("WebSocket session is already closed or not initialized.");
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void startService() {

    }

    public boolean isChannelConnected() {
        Session s = getSession();
        return channelConnected && s != null && s.isOpen();
    }

    private long getChannelTimeout() {
        return "chat".equals(channelName) ? CHAT_CONNECTION_TIMEOUT : CONNECTION_TIMEOUT;
    }

    private int getReconnectDelay() {
        return "chat".equals(channelName) ? RECONNECT_DELAY_CHAT : RECONNECT_DELAY_DEFAULT;
    }

    private void requestChatSnapshot() {
        String me = Repository.getUsername();
        if (me == null || me.isBlank()) {
            return;
        }
        try {
            if (getSession() != null && getSession().isOpen()) {
                JSONObject json = new JSONObject();
                json.put("type", "snapshot_request");
                json.put("user", me.trim());
                sendMessage(json.toString());
            }
        } catch (Exception e) {
            log.warn("No se pudo solicitar snapshot de chat: {}", e.getMessage());
        }
    }

    private void sendChatConnectEvent() {
        if (!"chat".equals(channelName)) {
            return;
        }
        String me = Repository.getUsername();
        if (me == null || me.isBlank()) {
            return;
        }
        try {
            if (getSession() != null && getSession().isOpen()) {
                JSONObject json = new JSONObject();
                json.put("type", "chat_connect");
                json.put("user", me.trim());
                json.put("source", "vector-trade-front");
                sendMessage(json.toString());
            }
        } catch (Exception e) {
            log.warn("No se pudo enviar chat_connect: {}", e.getMessage());
        }
    }

    public void startAutoReconnect() {
        if (autoReconnect) {
            initiateReconnection();
        }
    }

    private void startChannelHeartbeat() {
        if (!"chat".equals(channelName) && !"news".equals(channelName)) {
            return;
        }
        stopChannelHeartbeat();
        int heartbeatSeconds = "chat".equals(channelName)
                ? CHAT_HEARTBEAT_SECONDS
                : NEWS_HEARTBEAT_SECONDS;
        channelHeartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        channelHeartbeatScheduler.scheduleWithFixedDelay(() -> {
            try {
                Session s = getSession();
                if (s != null && s.isOpen()) {
                    s.getRemote().sendPing(ByteBuffer.wrap(new byte[]{1}));
                }
            } catch (Exception e) {
                log.debug("Heartbeat {} ping failed: {}", channelName, e.getMessage());
            }
        }, heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
    }

    private void stopChannelHeartbeat() {
        if (channelHeartbeatScheduler != null) {
            channelHeartbeatScheduler.shutdownNow();
            channelHeartbeatScheduler = null;
        }
    }

    private void shutdownSoundScheduler() {
        if (schedulersound != null) {
            schedulersound.shutdownNow();
        }
    }

    private void requestNewsSnapshot() {
        if (!"news".equals(channelName)) {
            return;
        }
        try {
            if (getSession() != null && getSession().isOpen()) {
                JSONObject json = new JSONObject();
                json.put("type", "snapshot_request");
                json.put("limit", 300);
                sendMessage(json.toString());
            }
        } catch (Exception e) {
            log.warn("No se pudo solicitar snapshot de news: {}", e.getMessage());
        }
    }

    /**
     * Canal candle. Hoy solo interesa "intraday_series": la serie del dia por instrumento
     * que calcula el backend, para que el mini grafico de Datos del Mercado se vea al toque
     * en vez de esperar a acumular ticks en el cliente.
     *
     * Los null del JSON (antes del primer trade del papel) se guardan como Double.NaN y el
     * dibujo los salta; asi se distingue "sin dato" de "precio cero".
     */
    private void handleCandleMessage(String raw) {
        try {
            JSONObject json = new JSONObject(raw);
            if ("close_price_history".equals(json.optString("type"))) {
                handleClosePriceHistory(json);
                return;
            }
            if ("trade_candles".equals(json.optString("type"))) {
                handleTradeCandles(json);
                return;
            }
            if ("error".equals(json.optString("type"))) {
                String message = json.optString("message", "Error consultando Candle");
                log.warn("Respuesta de error del canal candle: {}", message);
                Platform.runLater(() -> Repository.setCandleRequestError(message));
                return;
            }
            if (!"intraday_series".equals(json.optString("type"))) {
                return;
            }
            // El candle-service resuelve el dia como "el ultimo con trades en Mongo"
            // (resolveLatestTradeDayByEventTime). Si la ingesta se detuvo, devuelve la serie de ese
            // dia viejo SIN avisar, y la columna Tendencia la dibuja como si fuera la de hoy: el
            // papel sale rojo aunque en vivo este subiendo. Se descarta y queda el buffer local,
            // que se llena con los ticks en vivo y por definicion es de hoy.
            String diaSerie = json.optString("date", "");
            String hoy = java.time.LocalDate.now(java.time.ZoneId.of("America/Santiago")).toString();
            if (!diaSerie.isEmpty() && !diaSerie.equals(hoy)) {
                log.warn("[Tendencia] serie intradia DESCARTADA: viene del {} y hoy es {}."
                        + " Revisar la ingesta (inyectorcandle) que alimenta la coleccion de trades.",
                        diaSerie, hoy);
                return;
            }

            var series = json.optJSONArray("series");
            if (series == null) return;

            Map<String, double[]> acumulado = new HashMap<>();
            java.util.Set<String> sinTrades = new java.util.HashSet<>();
            for (int i = 0; i < series.length(); i++) {
                JSONObject s = series.getJSONObject(i);
                String key = s.optString("key", "");
                String symbol = s.optString("symbol", "").trim();
                var puntos = s.optJSONArray("points");
                if (puntos == null || puntos.length() == 0) {
                    if ("no_trades".equals(s.optString("status")) && !symbol.isEmpty()) {
                        sinTrades.add(symbol);
                    }
                    continue;
                }
                if (key.isEmpty()) continue;

                double[] serie = new double[puntos.length()];
                for (int p = 0; p < puntos.length(); p++) {
                    serie[p] = puntos.isNull(p) ? Double.NaN : puntos.getDouble(p);
                }
                acumulado.put(key, serie);
            }
            if (!acumulado.isEmpty() || !sinTrades.isEmpty()) {
                Platform.runLater(() -> {
                    Repository.clearSeriesIntradia(sinTrades);
                    Repository.setSeriesIntradia(acumulado);
                    var principalController = Repository.getPrincipalController();
                    if (principalController != null) {
                        principalController.getMarketDataPortfolioViewControllers().values()
                                .forEach(controller -> controller.refreshTrendColumn());
                    }
                });
                log.info("[Tendencia] series recibidas={} sinTrades={}",
                        acumulado.size(), sinTrades.size());
            }
        } catch (Exception e) {
            log.error("No se pudo procesar el mensaje del canal candle", e);
        }
    }

    private void handleClosePriceHistory(JSONObject json) {
        String symbol = json.optString("symbol", "").trim().toUpperCase();
        JSONArray rows = json.optJSONArray("rows");
        if (symbol.isEmpty() || rows == null) {
            return;
        }

        List<HistoricalCandle> candles = new ArrayList<>();
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.optJSONObject(i);
            if (row == null) continue;
            try {
                double close = row.optDouble("close", 0d);
                if (close <= 0d) continue;
                candles.add(new HistoricalCandle(
                        symbol,
                        LocalDate.parse(row.getString("date")),
                        row.optDouble("open", close),
                        row.optDouble("high", close),
                        row.optDouble("low", close),
                        close,
                        Math.max(0d, row.optDouble("volume", 0d))));
            } catch (Exception e) {
                log.warn("Fila close_prices invalida para {}: {}", symbol, e.getMessage());
            }
        }
        candles.sort(java.util.Comparator.comparing(HistoricalCandle::date));
        Platform.runLater(() -> Repository.setClosePriceHistory(symbol, candles));
    }

    private void handleTradeCandles(JSONObject json) {
        String symbol = json.optString("symbol", "").trim().toUpperCase();
        int bucketMinutes = json.optInt("bucketMinutes", 0);
        JSONArray rows = json.optJSONArray("rows");
        if (symbol.isEmpty() || bucketMinutes <= 0 || rows == null) {
            return;
        }

        List<TradeCandle> candles = new ArrayList<>();
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.optJSONObject(i);
            if (row == null) continue;
            try {
                double last = row.optDouble("last", row.optDouble("close", 0d));
                if (last <= 0d) continue;
                candles.add(new TradeCandle(
                        symbol,
                        bucketMinutes,
                        Instant.ofEpochMilli(row.getLong("timestamp")),
                        row.optDouble("open", last),
                        row.optDouble("high", last),
                        row.optDouble("low", last),
                        last,
                        Math.max(0d, row.optDouble("volume", 0d))));
            } catch (Exception e) {
                log.warn("Fila trade_candles invalida para {}: {}", symbol, e.getMessage());
            }
        }
        candles.sort(java.util.Comparator.comparing(TradeCandle::start));
        Platform.runLater(() -> Repository.setTradeCandles(symbol, bucketMinutes, candles));
    }

    private void handleNewsMessage(String raw) {
        if (raw == null || raw.isBlank()) {
            return;
        }
        try {
            JSONObject obj = new JSONObject(raw);
            String type = obj.optString("type", "").trim().toLowerCase();
            if ("snapshot".equals(type)) {
                var arr = obj.optJSONArray("messages");
                if (arr != null) {
                    for (int i = 0; i < arr.length(); i++) {
                        Object entry = arr.opt(i);
                        if (entry instanceof JSONObject row) {
                            String m = row.optString("message", "");
                            String url = row.optString("url", "");
                            long publishedAt = row.optLong("publishedAt", System.currentTimeMillis());
                            String impact = row.optString("impact", "NORMAL");
                            if (m != null && !m.isBlank()) {
                                Repository.appendNewsMessage(m, url, publishedAt, impact);
                            }
                            continue;
                        }
                        String m = arr.optString(i, "");
                        if (m != null && !m.isBlank()) {
                            Repository.appendNewsMessage(m, "", System.currentTimeMillis(), "NORMAL");
                        }
                    }
                }
                return;
            }
            if ("news".equals(type) || "news_summary".equals(type)) {
                String message = obj.optString("message", "");
                String url = obj.optString("url", "");
                long publishedAt = obj.optLong("publishedAt", System.currentTimeMillis());
                String impact = obj.optString("impact", "NORMAL");
                if (!message.isBlank()) {
                    Repository.appendNewsMessage(message, url, publishedAt, impact);
                }
                return;
            }
        } catch (Exception ignored) {
            // Si no llega JSON valido, igual lo dejamos en el historial.
        }
        Repository.appendNewsMessage(raw, "", System.currentTimeMillis(), "NORMAL");
    }
}
