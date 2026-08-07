package cl.vc.service.admin.servlet;

import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Feature extra — Edición de propiedades en caliente.
 *
 * GET  /api/properties           → lista todas las propiedades (valores sensibles enmascarados)
 * POST /api/properties           → actualiza una propiedad en memoria   { key, value }
 * POST /api/properties/save      → persiste todas las propiedades al archivo en disco
 * POST /api/properties/reload    → recarga desde disco (descarta cambios en memoria no guardados)
 */
@Slf4j
public class PropertiesServlet extends HttpServlet {

    private static final Set<String> SENSITIVE = Set.of(
            "redis.password", "password", "secret", "password.sql", "mongo.connection"
    );

    /** Propiedades que NO pueden cambiar en caliente (requieren reinicio). */
    private static final Set<String> REQUIRES_RESTART = Set.of(
            "admin.port", "websocket.port", "redis.host", "redis.port",
            "server.sql", "database.sql",
            "subscribeSecuritylist_BCS", "subscribeSecuritylist",
            "connections", "connectionsmkd", "zoneId",
            "username", "password", "url", "realm", "clientid", "secret",
            "redis.password", "redis.enable.persistencia",
            "IB.SECURITYLIST", "BCS.SECURITYLIST",
            "islogs", "path.logs", "bolsa.stats", "requiere.sql"
    );

    // ──────────────────────────────────────────────────────────────────────────

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            Properties props = MainApp.getProperties();
            JSONArray arr = new JSONArray();

            props.stringPropertyNames().stream()
                    .sorted()
                    .forEach(key -> {
                        String rawValue = props.getProperty(key);
                        boolean sensitive = SENSITIVE.contains(key);
                        arr.put(new JSONObject()
                                .put("key",            key)
                                .put("value",          sensitive ? "***" : rawValue)
                                .put("sensitive",      sensitive)
                                .put("requiresRestart", REQUIRES_RESTART.contains(key)));
                    });

            res.getWriter().write(arr.toString());

        } catch (Exception e) {
            log.error("[Admin/Properties] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();

        try {
            // POST /api/properties/save → escribe al archivo en disco
            if ("/save".equals(pathInfo)) {
                saveToDisk();
                res.getWriter().write(new JSONObject()
                        .put("ok",      true)
                        .put("message", "Propiedades guardadas en disco: " + MainApp.getPropertiesPath())
                        .toString());
                return;
            }

            // POST /api/properties/reload → recarga desde disco
            if ("/reload".equals(pathInfo)) {
                String path = MainApp.getPropertiesPath();
                if (path == null || !new File(path).exists()) {
                    res.setStatus(400);
                    res.getWriter().write("{\"error\":\"No se conoce la ruta del properties (¿se pasó como arg?)\"}");
                    return;
                }
                try (FileInputStream fis = new FileInputStream(path)) {
                    MainApp.getProperties().load(fis);
                }
                log.info("[Admin/Properties] Propiedades recargadas desde disco: {}", path);
                res.getWriter().write(new JSONObject()
                        .put("ok",      true)
                        .put("message", "Propiedades recargadas desde disco")
                        .toString());
                return;
            }

            // POST /api/properties → actualizar clave y guardar al disco
            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = new JSONObject(body);
            String key   = json.getString("key").trim();
            String value = json.getString("value");

            if (key.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "key no puede estar vacío").toString());
                return;
            }

            // Aplica en memoria (y actualiza campos cacheados si aplica)
            MainApp.applyProperty(key, value);
            log.info("[Admin/Properties] Propiedad actualizada: {}={}", key,
                    SENSITIVE.contains(key) ? "***" : value);

            // Auto-guarda al disco si hay ruta disponible
            String savedMsg = "";
            String path = MainApp.getPropertiesPath();
            if (path != null && new File(path).exists()) {
                try {
                    saveToDisk();
                    savedMsg = " Guardado en disco.";
                } catch (Exception ex) {
                    log.warn("[Admin/Properties] No se pudo auto-guardar en disco: {}", ex.getMessage());
                    savedMsg = " ⚠️ No se pudo guardar en disco: " + ex.getMessage();
                }
            } else {
                savedMsg = " ⚠️ Solo en memoria (no hay archivo de propiedades configurado).";
            }

            res.getWriter().write(new JSONObject()
                    .put("ok",             true)
                    .put("message",        "Propiedad '" + key + "' actualizada." + savedMsg)
                    .put("requiresRestart", REQUIRES_RESTART.contains(key))
                    .toString());

        } catch (Exception e) {
            log.error("[Admin/Properties] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    // ──────────────────────────────────────────────────────────────────────────

    private void saveToDisk() throws Exception {
        String path = MainApp.getPropertiesPath();
        if (path == null || !new File(path).exists()) {
            throw new IllegalStateException("No se conoce la ruta del properties (¿se pasó como arg?)");
        }

        // Escribe de forma segura: primero a un .tmp, luego reemplaza
        Path target = Paths.get(path);
        Path tmp    = target.resolveSibling(target.getFileName() + ".tmp");

        try (Writer w = new BufferedWriter(new FileWriter(tmp.toFile()))) {
            w.write("# Guardado automático por VT Admin — " + new Date() + "\n");
            MainApp.getProperties().stringPropertyNames().stream()
                    .sorted()
                    .forEach(key -> {
                        try { w.write(key + "=" + MainApp.getProperties().getProperty(key) + "\n"); }
                        catch (IOException ex) { throw new UncheckedIOException(ex); }
                    });
        }
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        log.info("[Admin/Properties] Propiedades guardadas en {}", path);
    }
}
