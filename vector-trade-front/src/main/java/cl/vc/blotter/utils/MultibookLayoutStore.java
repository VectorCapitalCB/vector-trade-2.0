package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Copia local del multibook: el documento completo (configuraciones, paginas y libros) mas, por
 * ventana, su geometria, su monitor, su pagina y la configuracion que estaba mirando.
 * <p>
 * La fuente compartida es el OMS ({@link MultibookApi}), que es lo que hace que la configuracion te
 * siga si cambias de PC. Esta copia es el respaldo: si el servicio no responde, el multibook sigue
 * guardando y al reabrir esta todo donde estaba.
 */
@Slf4j
public final class MultibookLayoutStore {

    private MultibookLayoutStore() {
    }

    public static synchronized JSONObject document() {
        return load().optJSONObject("document");
    }

    public static synchronized void saveDocument(JSONObject document) {
        write(load().put("document", document));
    }

    public static synchronized JSONObject window(int index) {
        JSONArray windows = windows();
        return (index >= 0 && index < windows.length()) ? windows.optJSONObject(index) : null;
    }

    public static synchronized void saveWindow(int index, JSONObject state) {
        JSONArray windows = windows();
        while (windows.length() <= index) {
            windows.put(new JSONObject());
        }
        windows.put(index, state);
        write(load().put("windows", windows));
    }

    private static JSONArray windows() {
        JSONArray windows = load().optJSONArray("windows");
        return windows == null ? new JSONArray() : windows;
    }

    private static JSONObject load() {
        try {
            Path path = storagePath();
            if (!Files.exists(path) || Files.size(path) == 0L) {
                return new JSONObject();
            }
            return new JSONObject(Files.readString(path, StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new JSONObject();
        }
    }

    private static void write(JSONObject layout) {
        try {
            Path path = storagePath();
            Files.createDirectories(path.getParent());
            Files.writeString(path, layout.toString(2), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static Path storagePath() {
        String home = System.getProperty("user.home");
        String company = Repository.getProperties().getProperty("company");
        if (company == null || company.isBlank()) {
            company = "vc";
        }
        String application = Repository.getProperties().getProperty("application");
        if (application == null || application.isBlank()) {
            application = "VectorTrade";
        }
        String username = Repository.getUsername();
        if (username == null || username.isBlank()) {
            username = "default";
        }
        String userSafe = username.trim().replaceAll("[\\\\/:*?\"<>|\\s]+", "_");
        return Paths.get(home, company, application, "multibook", userSafe + ".json");
    }
}
