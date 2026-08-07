package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

/**
 * Cliente del Multibook 2.0 del OMS. Va contra el mismo host y puerto del websocket del servicio, y
 * reusa la misma credencial Basic del upgrade, asi que no hay endpoint ni autenticacion nuevos.
 * <p>
 * Todas las llamadas son bloqueantes: invocarlas fuera del hilo de JavaFX.
 */
@Slf4j
public final class MultibookApi {

    private static final int TIMEOUT_MS = 8_000;

    private MultibookApi() {
    }

    public static JSONObject load() {
        return call("", "GET", null);
    }

    public static JSONObject save(JSONObject document) {
        return call("", "PUT", document);
    }

    public static JSONObject rename(String from, String to) {
        return call("/rename?from=" + encode(from) + "&to=" + encode(to), "POST", null);
    }

    public static JSONObject setActive(String name) {
        return call("/active?name=" + encode(name), "POST", null);
    }

    public static JSONObject delete(String name) {
        return call("/" + encode(name), "DELETE", null);
    }

    /** Formato de libro del documento; el mismo que arma y lee el OMS. */
    public static JSONObject toJson(MarketDataMessage.Subscribe subscribe) {
        return new JSONObject()
                .put("symbol", subscribe.getSymbol())
                .put("exchange", subscribe.getSecurityExchange().name())
                .put("settlType", subscribe.getSettlType().name())
                .put("securityType", subscribe.getSecurityType().name())
                .put("depth", subscribe.getDepth().name())
                .put("trade", subscribe.getTrade())
                .put("statistic", subscribe.getStatistic())
                .put("book", subscribe.getBook())
                .put("tradeQty", subscribe.getTradeQty())
                .put("bookQty", subscribe.getBookQty());
    }

    public static MarketDataMessage.Subscribe toSubscribe(JSONObject book) {

        MarketDataMessage.Subscribe subscribe = MarketDataMessage.Subscribe.newBuilder()
                .setSymbol(book.optString("symbol").trim().toUpperCase(Locale.ROOT))
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.valueOf(
                        book.optString("exchange", "BCS").trim().toUpperCase(Locale.ROOT)))
                .setSettlType(RoutingMessage.SettlType.valueOf(
                        book.optString("settlType", "T2").trim().toUpperCase(Locale.ROOT)))
                .setSecurityType(RoutingMessage.SecurityType.valueOf(
                        book.optString("securityType", "CS").trim().toUpperCase(Locale.ROOT)))
                .setDepth(MarketDataMessage.Depth.valueOf(
                        book.optString("depth", "FULL_BOOK").trim().toUpperCase(Locale.ROOT)))
                .setTrade(book.optBoolean("trade", true))
                .setStatistic(book.optBoolean("statistic", true))
                .setBook(book.optBoolean("book", true))
                .setTradeQty(book.optInt("tradeQty", 0))
                .setBookQty(book.optInt("bookQty", 0))
                .build();

        return subscribe.toBuilder().setId(TopicGenerator.getTopicMKD(subscribe)).build();
    }

    private static JSONObject call(String path, String method, JSONObject body) {

        HttpURLConnection connection = null;

        try {
            connection = (HttpURLConnection) new URL(baseUrl() + path).openConnection();
            connection.setRequestMethod(method);
            connection.setConnectTimeout(TIMEOUT_MS);
            connection.setReadTimeout(TIMEOUT_MS);
            connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
                    .encodeToString(String.valueOf(Repository.getCredencial()).getBytes(StandardCharsets.UTF_8)));
            connection.setRequestProperty("Accept", "application/json");

            if (body != null) {
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
                try (OutputStream out = connection.getOutputStream()) {
                    out.write(body.toString().getBytes(StandardCharsets.UTF_8));
                }
            }

            int status = connection.getResponseCode();
            InputStream stream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
            String response = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);

            if (status >= 400) {
                log.error("[Multibook2] {} {} -> {} {}", method, path, status, response);
                return null;
            }

            return new JSONObject(response);

        } catch (Exception e) {
            log.error("[Multibook2] {} {} fallo: {}", method, path, e.getMessage(), e);
            return null;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /** ws://host:puerto/websocket/ -> http://host:puerto/api/multibook2 */
    private static String baseUrl() throws Exception {
        URL endpoint = new URL(String.valueOf(Repository.getServiceEndpoint())
                .replaceFirst("^wss://", "https://")
                .replaceFirst("^ws://", "http://"));
        String port = endpoint.getPort() == -1 ? "" : ":" + endpoint.getPort();
        return endpoint.getProtocol() + "://" + endpoint.getHost() + port + "/api/multibook2";
    }

    private static String encode(String value) {
        return URLEncoder.encode(String.valueOf(value), StandardCharsets.UTF_8);
    }
}
