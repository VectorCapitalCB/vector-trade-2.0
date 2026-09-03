package cl.vc.service.admin.servlet;

import cl.vc.service.MainApp;
import cl.vc.service.admin.AdminAuthFilter;
import cl.vc.service.admin.MonitorMetrics;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.servlet.DispatcherType;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contrato HTTP que consume MONITOR-VC. Levanta Jetty con el mismo filtro de auth y el servlet
 * reales, en un puerto efímero.
 *
 * <p>Interesa fijar tres cosas: que sin token no se entregue nada, que el JSON sea parseable con
 * las claves que el monitor lee, y que el borrado de custodia exija cuenta explícita (es la acción
 * destructiva del endpoint).
 *
 * NOTA 2.0: los dos tests de DELETE /api/monitor/custodia de produccion no se portaron porque
 * ese endpoint depende de la cache de custodia (PatrimonioBase, generaciones, suspension de
 * escrituras) que el 2.0 todavia no tiene. El resto es identico a produccion.
 */
class MonitorServletTest {

    private static final String TOKEN = "token-de-test";

    private static Server server;
    private static String base;
    private static HttpClient http;

    @BeforeAll
    static void boot() throws Exception {
        MainApp.getProperties().setProperty("admin.token", TOKEN);
        // Evita que el snapshot de Redis agende reintentos de reconexión durante el test.
        MainApp.getProperties().setProperty("redis.reconnect.runtime.retry.enabled", "false");

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(0); // efímero: no choca con nada en la máquina del dev
        server.addConnector(connector);

        ServletContextHandler ctx = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        ctx.setContextPath("/");
        ctx.addFilter(new FilterHolder(new AdminAuthFilter()), "/api/*", EnumSet.of(DispatcherType.REQUEST));
        ctx.addServlet(new ServletHolder(new MonitorServlet()), "/api/monitor/*");
        server.setHandler(ctx);
        server.start();

        base = "http://localhost:" + connector.getLocalPort();
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterAll
    static void shutdown() throws Exception {
        if (server != null) {
            server.stop();
        }
        MainApp.getProperties().remove("admin.token");
        MainApp.getProperties().remove("redis.reconnect.runtime.retry.enabled");
    }

    @Test
    void sinToken_noEntregaNada() throws Exception {
        HttpResponse<String> res = send("GET", "/api/monitor/health", null);
        assertEquals(401, res.statusCode(), "el endpoint debe quedar detrás de AdminAuthFilter");
    }

    @Test
    void tokenIncorrecto_noEntregaNada() throws Exception {
        HttpResponse<String> res = send("GET", "/api/monitor/health", "otro-token");
        assertEquals(401, res.statusCode());
    }

    @Test
    void health_traeLasClavesQueLeeElMonitor() throws Exception {
        HttpResponse<String> res = send("GET", "/api/monitor/health", TOKEN);
        assertEquals(200, res.statusCode());

        JSONObject body = new JSONObject(res.body());
        assertEquals("vector-trade-service", body.getString("service"));
        assertTrue(body.has("status"));
        assertTrue(body.has("redisConnected"));
        assertTrue(body.has("sqlConnected"));
        assertTrue(body.has("redisPersistenceEnabled"));
        assertTrue(body.has("accountActors"));
        assertTrue(body.getJSONObject("custody").has("cacheHitRatio"));
        assertTrue(body.getJSONObject("custody").has("maxSqlLoadMs"));
        assertTrue(body.getJSONObject("rejects").has("last5min"));
        assertTrue(body.has("accountLoadCycle"));
    }

    @Test
    void custodias_devuelveLaFilaDeLaCuentaMedida() throws Exception {
        MonitorMetrics.recordCustodyLoad("http-1/0", MonitorMetrics.SOURCE_SQL, "sin cache", 1_234L, 7, 999d);

        HttpResponse<String> res = send("GET", "/api/monitor/custodias?account=http-1/0", TOKEN);
        assertEquals(200, res.statusCode());

        JSONObject body = new JSONObject(res.body());
        assertEquals(1, body.getInt("count"));
        JSONObject row = body.getJSONArray("accounts").getJSONObject(0);
        assertEquals("http-1/0", row.getString("account"));
        assertEquals("SQL", row.getString("source"));
        assertEquals(1_234L, row.getLong("durationMs"));
        assertEquals(7, row.getInt("positions"));
    }

    @Test
    void rejectsRecent_devuelveMotivoNormalizado() throws Exception {
        MonitorMetrics.recordReject("http-2/0", "SQM-B", "ord-9", "Retornamos por orden sin custodia");

        HttpResponse<String> res = send("GET", "/api/monitor/rejects/recent?limit=5", TOKEN);
        assertEquals(200, res.statusCode());

        JSONObject body = new JSONObject(res.body());
        assertTrue(body.getInt("count") > 0);
        assertTrue(res.body().contains("SIN_CUSTODIA"));
    }



    @Test
    void rutaDesconocida_devuelve404ConLasRutasValidas() throws Exception {
        HttpResponse<String> res = send("GET", "/api/monitor/nope", TOKEN);
        assertEquals(404, res.statusCode());
        assertTrue(res.body().contains("/api/monitor/health"));
    }

    private HttpResponse<String> send(String method, String path, String token) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .method(method, HttpRequest.BodyPublishers.noBody());
        if (token != null) {
            b.header("Authorization", "Bearer " + token);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }
}
