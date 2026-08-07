package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import javafx.application.Platform;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ProgressBar;
import javafx.stage.Stage;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.Objects;

/**
 * Auto-updater basado en Velopack. Reemplaza a {@link ConfigGenerator}, que descargaba un zip
 * sin verificar hash, lo descomprimia sobre si mismo y recreaba el .lnk por PowerShell.
 *
 * Velopack no tiene SDK Java, asi que se integra "a mano":
 *   - {@link #handleStartupHooks(String[])} : PRIMERA linea de main(), intercepta los hooks
 *     de ciclo de vida (--veloapp-* / --squirrel-*) y sale sin abrir la UI.
 *   - {@link #checkForUpdate(Stage)} : check + download en Java (HTTP al feed), y aplica con
 *     el Update.exe de Velopack (swap atomico + relanzado, out-of-process).
 *
 * Layout Velopack (install per-user, %LOCALAPPDATA%\{packId}\):
 *     {root}\Update.exe          <- el updater (motor Rust)
 *     {root}\{packId}.exe        <- stub de arranque
 *     {root}\current\            <- binarios reales (el native-image corre desde aca)
 *     {root}\packages\           <- aca dejamos el .nupkg descargado
 *
 * El packId sale de la property {@code application}, para que QA (VectorTradeQA) y produccion
 * (VectorTrade2) no compartan directorio ni feed. DEBE coincidir con el --packId del pipeline.
 */
@Slf4j
public class VelopackUpdater {

    /** Canal del feed -> archivo releases.{channel}.json que genera vpk. */
    private static final String CHANNEL = "win";
    private static final String DEFAULT_PACK_ID = "VectorTrade2";

    // -------------------------------------------------------------------------
    //  1. Hooks de arranque: llamar como PRIMERA linea de main(), antes de launch()
    // -------------------------------------------------------------------------
    /**
     * Velopack lanza este exe con --veloapp-install|obsolete|updated|uninstall {version}
     * y ESPERA a que salga. Si no interceptamos, arranca toda la UI de JavaFX y Update.exe
     * la mata al vencer el timeout (la instalacion parece colgarse). Firstrun/restart llegan
     * por variable de entorno (VELOPACK_FIRSTRUN / VELOPACK_RESTART) y NO deben salir.
     */
    public static void handleStartupHooks(String[] args) {
        if (args == null || args.length < 1) return;
        String a = args[0];
        if (a.startsWith("--veloapp-")
                || a.equals("--squirrel-install")
                || a.equals("--squirrel-updated")
                || a.equals("--squirrel-obsolete")
                || a.equals("--squirrel-uninstall")) {
            // hook de ciclo de vida: no abrir UI, salir ya (muy por debajo del timeout)
            System.exit(0);
        }
    }

    /** true si la app esta instalada por Velopack (existe Update.exe). En dev es false y no
     *  se ofrece update, que es lo correcto: no queremos que un run desde el IDE se actualice. */
    public static boolean isInstalled() {
        if (!isWindows()) return false;
        Path root = findRoot();
        return root != null && Files.exists(root.resolve("Update.exe"));
    }

    // -------------------------------------------------------------------------
    //  2. Chequeo de actualizacion (llamar al arranque, tras mostrar la ventana)
    // -------------------------------------------------------------------------
    public static void checkForUpdate(Stage stage) {
        if (!isWindows()) return;
        new Thread(() -> {
            try {
                Path root = findRoot();
                Path updateExe = (root == null) ? null : root.resolve("Update.exe");
                if (updateExe == null || !Files.exists(updateExe)) {
                    log.info("Velopack no instalado (dev o sin Update.exe); se omite auto-update");
                    return;
                }

                String feedUrl = trimSlash(prop("url", null));
                String localVersion = prop("version", "0.0.0");
                if (feedUrl == null || feedUrl.isBlank()) {
                    log.warn("property 'url' vacia; no hay feed de updates configurado");
                    return;
                }

                JsonObject asset = pickNewerFull(feedUrl, localVersion);
                if (asset == null) {
                    log.info("{} al dia (instalada {})", packId(), localVersion);
                    return;
                }
                Repository.setVersion(str(asset, "Version"));
                Platform.runLater(() -> prompt(stage, feedUrl, root, updateExe, asset));
            } catch (Exception e) {
                log.error("checkForUpdate error", e);
            }
        }, "vt-velopack-check").start();
    }

    /** Descarga releases.{channel}.json y devuelve el asset Full con mayor version > local, o null. */
    private static JsonObject pickNewerFull(String feedUrl, String localVersion) {
        String json = httpGet(feedUrl + "/releases." + CHANNEL + ".json");
        if (json == null) return null;
        JsonArray assets = JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("Assets");
        if (assets == null) return null;

        JsonObject best = null;
        String bestVer = localVersion;
        for (JsonElement el : assets) {
            JsonObject a = el.getAsJsonObject();
            if (!"Full".equalsIgnoreCase(str(a, "Type"))) continue; // solo full por ahora
            String ver = str(a, "Version");
            if (compareVersions(ver, bestVer) > 0) {
                bestVer = ver;
                best = a;
            }
        }
        return best;
    }

    private static void prompt(Stage stage, String feedUrl, Path root, Path updateExe, JsonObject asset) {
        Alert a = styled(new Alert(Alert.AlertType.CONFIRMATION));
        a.setTitle("Actualizacion disponible");
        a.setHeaderText("Nueva version " + str(asset, "Version"));
        a.setContentText(opt(asset, "NotesMarkdown", "¿Deseas actualizar ahora?"));
        ButtonType yes = new ButtonType("Actualizar");
        ButtonType no = new ButtonType("Salir");
        a.getButtonTypes().setAll(yes, no);

        a.showAndWait().ifPresent(b -> {
            if (b != yes) { Platform.exit(); System.exit(0); return; }

            ProgressBar pb = new ProgressBar();
            pb.setProgress(ProgressBar.INDETERMINATE_PROGRESS);
            Alert prog = styled(new Alert(Alert.AlertType.INFORMATION));
            prog.setTitle("Actualizando");
            prog.setHeaderText("Descargando la actualizacion...");
            prog.getDialogPane().setContent(pb);
            prog.getButtonTypes().clear();
            prog.show();

            new Thread(() -> applyUpdate(feedUrl, root, updateExe, asset, prog), "vt-velopack-apply").start();
        });
    }

    private static void applyUpdate(String feedUrl, Path root, Path updateExe, JsonObject asset, Alert prog) {
        try {
            String fileName = str(asset, "FileName");
            Path packages = root.resolve("packages");
            Files.createDirectories(packages);
            Path nupkg = packages.resolve(fileName);

            // 1. descargar el .nupkg full a {root}\packages\
            downloadTo(feedUrl + "/" + fileName, nupkg);

            // 2. verificar integridad contra el hash del feed (SHA256 preferido, SHA1 fallback)
            String sha256 = str(asset, "SHA256");
            String sha1 = str(asset, "SHA1");
            if (!sha256.isBlank() && !hash(nupkg, "SHA-256").equalsIgnoreCase(sha256)) {
                closeAnd(prog, "El SHA-256 del paquete no coincide. Actualizacion abortada.");
                return;
            } else if (sha256.isBlank() && !sha1.isBlank() && !hash(nupkg, "SHA-1").equalsIgnoreCase(sha1)) {
                closeAnd(prog, "El SHA-1 del paquete no coincide. Actualizacion abortada.");
                return;
            }

            // 3. aplicar + reiniciar con el Update.exe de Velopack (swap atomico out-of-process);
            //    --waitPid hace que espere a que esta app salga antes de tocar los archivos.
            long pid = ProcessHandle.current().pid();
            new ProcessBuilder(updateExe.toString(), "apply",
                    "--waitPid", String.valueOf(pid),
                    "--package", nupkg.toString())
                    .inheritIO().start();

            Platform.runLater(() -> {
                prog.close();
                Platform.exit();
                System.exit(0);
            });
        } catch (Exception e) {
            log.error("applyUpdate error", e);
            closeAnd(prog, "Error actualizando: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------ helpers

    /** packId = property 'application'; debe coincidir con el --packId del pipeline. */
    private static String packId() {
        return prop("application", DEFAULT_PACK_ID);
    }

    private static String prop(String key, String def) {
        try {
            return Repository.getProperties().getProperty(key, def);
        } catch (Exception e) {
            return def;
        }
    }

    /** {root} = padre de la carpeta "current" desde donde corre el exe; fallback per-user. */
    private static Path findRoot() {
        try {
            String cmd = ProcessHandle.current().info().command().orElse(null);
            if (cmd != null) {
                Path current = Paths.get(cmd).getParent(); // {root}\current
                if (current != null && current.getFileName() != null
                        && current.getFileName().toString().equalsIgnoreCase("current")) {
                    return current.getParent();
                }
            }
        } catch (Exception ignored) {
        }
        String lad = System.getenv("LOCALAPPDATA");
        return (lad != null) ? Paths.get(lad, packId()) : null;
    }

    private static String httpGet(String url) {
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
            c.setConnectTimeout(5000);
            c.setReadTimeout(5000);
            c.setRequestMethod("GET");
            if (c.getResponseCode() != 200) {
                log.warn("feed {} -> HTTP {}", url, c.getResponseCode());
                return null;
            }
            try (InputStream in = c.getInputStream()) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.warn("no se pudo leer feed {}: {}", url, e.toString());
            return null;
        }
    }

    private static void downloadTo(String url, Path dest) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        c.setConnectTimeout(10000);
        c.setReadTimeout(120000);
        c.setRequestMethod("GET");
        if (c.getResponseCode() != 200) throw new Exception("HTTP " + c.getResponseCode() + " bajando " + url);
        try (InputStream in = c.getInputStream()) {
            Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
        }
        log.info("paquete descargado: {}", dest);
    }

    private static String hash(Path f, String algo) throws Exception {
        MessageDigest md = MessageDigest.getInstance(algo);
        try (InputStream in = Files.newInputStream(f)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) md.update(buf, 0, n);
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : md.digest()) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private static void closeAnd(Alert prog, String msg) {
        log.warn(msg);
        Platform.runLater(() -> {
            prog.close();
            Alert a = styled(new Alert(Alert.AlertType.ERROR));
            a.setTitle("Actualizacion");
            a.setHeaderText(null);
            a.setContentText(msg);
            a.showAndWait();
        });
    }

    private static Alert styled(Alert a) {
        try {
            String css = Repository.getSTYLE();
            if (css != null) {
                a.getDialogPane().getStylesheets()
                        .add(Objects.requireNonNull(VelopackUpdater.class.getResource(css)).toExternalForm());
            }
        } catch (Exception ignored) {
        }
        return a;
    }

    /** Compara versiones semver-ish (ignora sufijo -pre). */
    private static int compareVersions(String a, String b) {
        String[] pa = base(a).split("\\.");
        String[] pb = base(b).split("\\.");
        int n = Math.max(pa.length, pb.length);
        for (int i = 0; i < n; i++) {
            int va = (i < pa.length) ? parseIntSafe(pa[i]) : 0;
            int vb = (i < pb.length) ? parseIntSafe(pb[i]) : 0;
            if (va != vb) return Integer.compare(va, vb);
        }
        return 0;
    }

    private static String base(String v) {
        if (v == null) return "0";
        int dash = v.indexOf('-');
        return (dash >= 0 ? v.substring(0, dash) : v).trim();
    }

    private static int parseIntSafe(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return 0; }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    private static String trimSlash(String s) {
        return s == null ? null : s.replaceAll("/+$", "");
    }

    private static String str(JsonObject o, String k) {
        return (o.has(k) && !o.get(k).isJsonNull()) ? o.get(k).getAsString() : "";
    }

    private static String opt(JsonObject o, String k, String def) {
        String v = str(o, k);
        return v.isBlank() ? def : v;
    }
}
