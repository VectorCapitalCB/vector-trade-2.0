package cl.vc.service.admin.servlet;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.service.MainApp;
import cl.vc.service.util.ConnectionsVO;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Feature 3 — Reconexión en caliente de conexiones Netty (routing y market data).
 *
 * GET  /api/connections                         → estado de todas las conexiones
 * PUT  /api/connections/{mkd|routing}/{EXCHANGE} → reconecta con host/port nuevos
 * POST /api/connections/reload                   → recarga todos los JSONs de conexiones
 */
@Slf4j
public class ConnectionServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            Map<String, ConnectionsVO> routingCfg = loadConfig(
                    MainApp.getProperties().getProperty("connections"));
            Map<String, ConnectionsVO> mkdCfg = loadConfig(
                    MainApp.getProperties().getProperty("connectionsmkd"));

            JSONArray routing = buildArray("routing", MainApp.getConnections(), routingCfg);
            JSONArray mkd     = buildArray("mkd",     MainApp.getConnections_mkd(), mkdCfg);

            JSONObject result = new JSONObject();
            result.put("routing", routing);
            result.put("mkd",     mkd);
            res.getWriter().write(result.toString());

        } catch (Exception e) {
            log.error("[Admin/Connections] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        // pathInfo: /mkd/BCS  o  /routing/XSGO
        String pathInfo = req.getPathInfo();

        try {
            if (pathInfo == null) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"Usar /api/connections/{mkd|routing}/{EXCHANGE}\"}");
                return;
            }

            String[] parts = pathInfo.split("/");
            if (parts.length < 3) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"Formato: /api/connections/{tipo}/{EXCHANGE}\"}");
                return;
            }

            String type        = parts[1].toLowerCase();
            String exchangeStr = parts[2].toUpperCase();

            String body = req.getReader().lines().collect(Collectors.joining());

            String  host;
            int     port;
            boolean status;

            if (body == null || body.trim().isEmpty()) {
                // Reconectar usando los valores actuales del JSON de configuración
                String cfgPath = "mkd".equals(type)
                        ? MainApp.getProperties().getProperty("connectionsmkd")
                        : MainApp.getProperties().getProperty("connections");
                Map<String, ConnectionsVO> cfg = loadConfig(cfgPath);
                ConnectionsVO vo = cfg.get(exchangeStr);
                if (vo == null) {
                    res.setStatus(400);
                    res.getWriter().write("{\"error\":\"No hay configuración para: " + exchangeStr + "\"}");
                    return;
                }
                host   = vo.host;
                port   = vo.port;
                status = vo.status != null ? vo.status : true;
            } else {
                JSONObject json = new JSONObject(body);
                host   = json.getString("host");
                port   = json.getInt("port");
                status = json.optBoolean("status", true);
            }

            reconnect(type, exchangeStr, host, port, status);

            // ── Persistir cambio al JSON de configuración ──
            String cfgPath = "mkd".equals(type)
                    ? MainApp.getProperties().getProperty("connectionsmkd")
                    : MainApp.getProperties().getProperty("connections");
            saveConfig(cfgPath, exchangeStr, host, port, status);

            res.getWriter().write(new JSONObject()
                    .put("ok",       true)
                    .put("message",  "Conexión " + exchangeStr + " reconectada → " + host + ":" + port)
                    .put("exchange", exchangeStr)
                    .put("type",     type)
                    .put("host",     host)
                    .put("port",     port)
                    .toString());

        } catch (IllegalArgumentException e) {
            res.setStatus(400);
            res.getWriter().write("{\"error\":\"Exchange desconocido: " + e.getMessage() + "\"}");
        } catch (Exception e) {
            log.error("[Admin/Connections] PUT error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        // POST /api/connections/reload → recarga ambos JSONs
        try {
            String connectionsPath    = MainApp.getProperties().getProperty("connections");
            String connectionsMkdPath = MainApp.getProperties().getProperty("connectionsmkd");

            String jsonString = new String(Files.readAllBytes(Paths.get(connectionsPath)));
            String mkdString  = new String(Files.readAllBytes(Paths.get(connectionsMkdPath)));

            Map<String, ConnectionsVO> retMap = new Gson().fromJson(jsonString,
                    new TypeToken<Map<String, ConnectionsVO>>() {}.getType());
            Map<String, ConnectionsVO> mkdMap = new Gson().fromJson(mkdString,
                    new TypeToken<Map<String, ConnectionsVO>>() {}.getType());

            int reloaded = 0;

            for (Map.Entry<String, ConnectionsVO> entry : mkdMap.entrySet()) {
                try {
                    reconnect("mkd", entry.getKey(),
                            entry.getValue().host, entry.getValue().port, entry.getValue().status);
                    reloaded++;
                } catch (Exception e) {
                    log.warn("[Admin/Connections] No se pudo recargar MKD {}: {}", entry.getKey(), e.getMessage());
                }
            }

            for (Map.Entry<String, ConnectionsVO> entry : retMap.entrySet()) {
                try {
                    reconnect("routing", entry.getKey(),
                            entry.getValue().host, entry.getValue().port, entry.getValue().status);
                    reloaded++;
                } catch (Exception e) {
                    log.warn("[Admin/Connections] No se pudo recargar Routing {}: {}", entry.getKey(), e.getMessage());
                }
            }

            log.info("[Admin/Connections] Reload completo: {} conexiones recargadas", reloaded);
            res.getWriter().write(new JSONObject()
                    .put("ok",      true)
                    .put("message", reloaded + " conexiones recargadas desde JSON")
                    .put("reloaded", reloaded)
                    .toString());

        } catch (Exception e) {
            log.error("[Admin/Connections] reload error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /** Guarda host/port/status de un exchange en su JSON de configuración (atómico). */
    private void saveConfig(String path, String exchange, String host, int port, boolean status) {
        if (path == null) { log.warn("[Admin/Connections] saveConfig: path nulo, no se guarda"); return; }
        try {
            Map<String, ConnectionsVO> cfg = loadConfig(path);
            ConnectionsVO vo = cfg.computeIfAbsent(exchange, k -> new ConnectionsVO());
            vo.host   = host;
            vo.port   = port;
            vo.status = status;

            String json = new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(cfg);
            Path target = Paths.get(path);
            Path tmp    = Paths.get(path + ".tmp");
            Files.writeString(tmp, json);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            log.info("[Admin/Connections] Config guardada: {} → {}:{} status={}", exchange, host, port, status);
        } catch (Exception e) {
            log.error("[Admin/Connections] No se pudo guardar config {}: {}", path, e.getMessage(), e);
        }
    }

    /** Carga un JSON de configuración de conexiones desde disco. */
    private Map<String, ConnectionsVO> loadConfig(String path) {
        if (path == null || !new File(path).exists()) return new HashMap<>();
        try {
            String json = new String(Files.readAllBytes(Paths.get(path)));
            Map<String, ConnectionsVO> map = new Gson().fromJson(
                    json, new TypeToken<Map<String, ConnectionsVO>>() {}.getType());
            return map != null ? map : new HashMap<>();
        } catch (Exception e) {
            log.warn("[Admin/Connections] No se pudo leer config {}: {}", path, e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * Construye el JSONArray de conexiones cruzando el config JSON (host/port)
     * con el mapa de clientes vivos (isConnected).
     */
    private JSONArray buildArray(String type, Map<?, NettyProtobufClient> liveMap,
                                 Map<String, ConnectionsVO> config) {
        JSONArray arr = new JSONArray();

        // 1. Iteramos sobre el config: aparecen todas las exchanges (activas o no)
        for (Map.Entry<String, ConnectionsVO> entry : config.entrySet()) {
            String        exchName = entry.getKey();
            ConnectionsVO cfg      = entry.getValue();

            NettyProtobufClient client = liveMap.entrySet().stream()
                    .filter(e -> e.getKey().toString().equals(exchName))
                    .map(Map.Entry::getValue)
                    .findFirst().orElse(null);

            arr.put(new JSONObject()
                    .put("exchange",  exchName)
                    .put("type",      type)
                    .put("host",      cfg.host != null ? cfg.host : "—")
                    .put("port",      cfg.port)
                    .put("connected", client != null && client.isConnected()));
        }

        // 2. Clientes vivos que no estén en el config (por si acaso)
        for (Map.Entry<?, NettyProtobufClient> entry : liveMap.entrySet()) {
            String exchName = entry.getKey().toString();
            boolean found = false;
            for (int i = 0; i < arr.length(); i++) {
                if (arr.getJSONObject(i).getString("exchange").equals(exchName)) { found = true; break; }
            }
            if (!found) {
                arr.put(new JSONObject()
                        .put("exchange",  exchName)
                        .put("type",      type)
                        .put("host",      "—")
                        .put("port",      0)
                        .put("connected", entry.getValue() != null && entry.getValue().isConnected()));
            }
        }
        return arr;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lógica de reconexión compartida
    // ─────────────────────────────────────────────────────────────────────────

    private void reconnect(String type, String exchangeStr, String host, int port, boolean status) throws Exception {
        boolean isLog   = Boolean.parseBoolean(MainApp.getProperties().getProperty("islogs", "true"));
        String  pathLog = MainApp.getProperties().getProperty("path.logs");
        String  hostPort = host + ":" + port;

        if ("mkd".equals(type)) {
            MarketDataMessage.SecurityExchangeMarketData exch =
                    MarketDataMessage.SecurityExchangeMarketData.valueOf(exchangeStr);

            disconnectOld(MainApp.getConnections_mkd().get(exch));
            MainApp.getConnections_mkd().remove(exch);

            if (status) {
                String logs = pathLog + File.separator + exch.name();
                NettyProtobufClient client = new NettyProtobufClient(
                        hostPort, MainApp.getSellSideMKD(), logs, exch.name(),
                        NotificationMessage.Component.ORB, isLog);
                new Thread(client).start();
                MainApp.getConnections_mkd().put(exch, client);
                log.info("[Admin/Connections] MKD {} reconectado → {}:{}", exch.name(), host, port);
            } else {
                log.info("[Admin/Connections] MKD {} desactivado", exch.name());
            }

        } else if ("routing".equals(type)) {
            RoutingMessage.SecurityExchangeRouting exch =
                    RoutingMessage.SecurityExchangeRouting.valueOf(exchangeStr);

            disconnectOld(MainApp.getConnections().get(exch));
            MainApp.getConnections().remove(exch);

            if (status) {
                String logs = pathLog + File.separator + exch.name();
                NettyProtobufClient client = new NettyProtobufClient(
                        hostPort, MainApp.getSellSideRouting(), logs, exch.name(),
                        NotificationMessage.Component.XRO, true, MainApp.getIdConnection());
                new Thread(client).start();
                MainApp.getConnections().put(exch, client);
                log.info("[Admin/Connections] Routing {} reconectado → {}:{}", exch.name(), host, port);
            } else {
                log.info("[Admin/Connections] Routing {} desactivado", exch.name());
            }

        } else {
            throw new IllegalArgumentException("Tipo desconocido: " + type + ". Usar 'mkd' o 'routing'");
        }
    }

    /** Detiene limpiamente al cliente anterior. */
    private void disconnectOld(NettyProtobufClient old) {
        if (old == null) return;
        try {
            old.stopClient();
            log.info("[Admin/Connections] Cliente anterior detenido (stopClient)");
        } catch (Exception e) {
            log.debug("[Admin/Connections] stopClient() error (ignorado): {}", e.getMessage());
        }
    }
}
