package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ProgressBar;
import javafx.stage.Stage;
import javafx.util.Duration;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Slf4j
public class ConfigGenerator {

    public static final String CSS_STYLE_CSS = Repository.getSTYLE();
    public static final String CONFIG_XML = "config.xml";
    public static final String CONFIG_XML_MAC = "config.mac.xml";
    private static String URL;
    private static String shortcutName;
    private static String company;
    private static String application;
    private static String main_class;
    private static String CONFIG_URL;
    private static String LOCAL_CONFIG_FILE;
    private static String LOCAL_CONFIG_FILE_BKP;
    private static String DOWNLOAD_DIRECTORY;
    private static volatile String LAST_EXE_PATH = null;
    private static Thread thread;

    private static boolean isMac() {
        return System.getProperty("os.name")
                .toLowerCase(Locale.ROOT)
                .contains("mac");
    }

    public static void checkForUpdateAndGenerateConfig(Stage stage) {
        try {
            String[] possiblePaths = {
                    System.getProperty("user.home") + File.separator + "Desktop",
                    System.getProperty("user.home") + File.separator + "Escritorio",
                    System.getProperty("user.home") + File.separator + "OneDrive" + File.separator + "Desktop",
                    System.getProperty("user.home") + File.separator + "OneDrive" + File.separator + "Escritorio",
                    System.getProperty("user.home") + File.separator + "OneDrive - vector capital" + File.separator + "Documentos" + File.separator + "Escritorio"
            };
            checkForUpdateAndGenerateConfig2(stage, possiblePaths);
            checkForUpdateAndGenerateConfig2(stage, findDesktopPath());
        } catch (Exception e) {
            log.error("Error comprobando actualizaciones", e);
        }
    }

    public static void checkForUpdateAndGenerateConfig2(Stage stage, String[] possiblePaths) {
        try {
            URL = Repository.getProperties().getProperty("url");
            shortcutName = Repository.getProperties().getProperty("shortcutName");
            company = Repository.getProperties().getProperty("company");
            application = Repository.getProperties().getProperty("application");
            main_class = Repository.getProperties().getProperty("main_class");

            LOCAL_CONFIG_FILE = System.getProperty("user.home") + File.separator + company + File.separator + application + File.separator + CONFIG_XML;
            LOCAL_CONFIG_FILE_BKP = System.getProperty("user.home") + File.separator + company + File.separator + "vt" + File.separator + CONFIG_XML;
            DOWNLOAD_DIRECTORY = System.getProperty("user.home") + File.separator + company + File.separator + shortcutName;

            CONFIG_URL = isMac()
                    ? URL + "/" + CONFIG_XML_MAC
                    : URL + "/" + CONFIG_XML;

            String localVersion = getLocalVersion();
            String remoteVersion = getRemoteVersion();
            String remoteUpdate = getRemoteUpdate();

            log.info("Versión local: {}", localVersion);
            log.info("Versión remota: {}", remoteVersion);

            if (!localVersion.equals(remoteVersion)) {
                Platform.runLater(() -> showUpdateSummaryDialog(stage, remoteVersion, remoteUpdate, possiblePaths));
            } else {
                log.info("El archivo local está actualizado. No se necesita descargar.");
            }
        } catch (Exception e) {
            log.error("Error comprobando actualizaciones", e);
        }
    }

    public static String[] findDesktopPath() {
        String userHome = System.getProperty("user.home");
        if (isMac()) {
            return new String[]{ userHome + "/Desktop" };
        }
        return new String[]{
                System.getenv("USERPROFILE") + "\\Desktop",
                System.getenv("USERPROFILE") + "\\Escritorio",
                userHome + "\\OneDrive\\Desktop",
                userHome + "\\OneDrive\\Escritorio",
                userHome + "\\OneDrive - " + System.getenv("COMPUTERNAME") + "\\Desktop",
                userHome + "\\OneDrive - " + System.getenv("COMPUTERNAME") + "\\Escritorio",
                userHome + "\\Documents\\Escritorio"
        };
    }

    private static void showUpdateSummaryDialog(Stage stage, String remoteVersion, String remoteUpdate, String[] possiblePaths) {
        Alert summaryAlert = new Alert(Alert.AlertType.INFORMATION);
        summaryAlert.setTitle("Resumen de Actualización");
        summaryAlert.setHeaderText("Resumen detallado de la actualización a la versión " + remoteVersion);
        javafx.scene.control.TextArea textArea = new javafx.scene.control.TextArea(remoteUpdate);
        textArea.setEditable(false);
        textArea.setWrapText(true);
        summaryAlert.getDialogPane().setContent(textArea);
        summaryAlert.getDialogPane().getStylesheets().add(
                Objects.requireNonNull(ConfigGenerator.class.getResource(CSS_STYLE_CSS)).toExternalForm()
        );

        ButtonType buttonTypeUpdate = new ButtonType("Actualizar");
        ButtonType buttonTypeCancel = new ButtonType("Salir");
        summaryAlert.getButtonTypes().setAll(buttonTypeUpdate, buttonTypeCancel);

        summaryAlert.showAndWait().ifPresent(response -> {
            if (response == buttonTypeUpdate) {
                showUpdateConfirmationDialog(stage, remoteVersion, possiblePaths);
            } else {
                log.info("El usuario canceló la actualización.");
                Platform.exit();
                System.exit(0);
            }
        });
    }

    private static String getRemoteVersion() throws IOException {
        try (InputStream in = new URL(CONFIG_URL).openStream();
             Scanner scanner = new Scanner(in)) {
            scanner.useDelimiter("\\A");
            String remoteConfig = scanner.hasNext() ? scanner.next() : "";
            String remoteVersion = getVersionFromConfig(remoteConfig);
            Repository.setVersion(remoteVersion);
            return remoteVersion;
        }
    }

    private static String getRemoteUpdate() throws IOException {
        try (InputStream in = new URL(CONFIG_URL).openStream();
             Scanner scanner = new Scanner(in)) {
            scanner.useDelimiter("\\A");
            String remoteConfig = scanner.hasNext() ? scanner.next() : "";
            return getUpdateFromConfig(remoteConfig);
        }
    }

    private static String getLocalVersion() throws IOException {
        File localConfigFile = new File(LOCAL_CONFIG_FILE);
        if (localConfigFile.exists()) {
            return getVersionFromConfig(new String(Files.readAllBytes(localConfigFile.toPath())));
        }
        File localConfigFile_bkp = new File(LOCAL_CONFIG_FILE_BKP);
        if (localConfigFile_bkp.exists()) {
            return getVersionFromConfig(new String(Files.readAllBytes(localConfigFile_bkp.toPath())));
        }
        return "0.0.0";
    }

    private static String getVersionFromConfig(String config) {
        int start = config.indexOf("<version>") + "<version>".length();
        int end   = config.indexOf("</version>");
        return (start >= "<version>".length() && end > start) ? config.substring(start, end).trim() : "0.0.0";
    }

    private static String getUpdateFromConfig(String config) {
        int start = config.indexOf("<update>") + "<update>".length();
        int end   = config.indexOf("</update>");
        return (start >= "<update>".length() && end > start) ? config.substring(start, end).trim() : "";
    }

    private static void downloadFile(String remoteUri, String localPath, Task<Void> task) {

        try {
            log.info("Iniciando la descarga del archivo desde: {}", remoteUri);
            URL url = new URL(remoteUri);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new IOException("No se pudo descargar el archivo: " + connection.getResponseMessage());
            }

            File localFile = new File(localPath);
            File parentDir = localFile.getParentFile();
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                throw new IOException("No se pudo crear el directorio: " + parentDir);
            }

            try (InputStream in = connection.getInputStream();
                 FileOutputStream out = new FileOutputStream(localFile)) {

                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }

            log.info("Archivo descargado con éxito a: {}", localPath);

        } catch (IOException e) {
            log.error("Error durante la descarga del archivo: {}", e.getMessage());
        }
    }

    public static void unzip(String zipFilePath, String destDir) throws IOException {
        log.info("Iniciando la descompresión del archivo: {}", zipFilePath);
        File destDirFile = new File(destDir);
        if (!destDirFile.exists()) {
            if (!destDirFile.mkdirs()) {
                log.error("No se pudo crear el directorio de destino: {}",destDir);
                return;
            }
        }

        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                log.info("Descomprimiendo archivo: {}", entry.getName());
                File filePath = new File(destDir, entry.getName());
                if (entry.isDirectory()) {
                    if (!filePath.exists() && !filePath.mkdirs()) {
                        log.error("No se pudo crear el directorio:: {}",filePath);
                    }
                } else {
                    File parent = filePath.getParentFile();
                    if (parent != null && !parent.exists() && !parent.mkdirs()) {
                        throw new IOException("No se pudo crear el directorio: " + parent);
                    }
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
                        byte[] buffer = new byte[4096];
                        int read;
                        while ((read = zipIn.read(buffer)) != -1) {
                            bos.write(buffer, 0, read);
                        }
                    }
                }
                zipIn.closeEntry();
            }
        } catch (IOException e) {
            log.error("Error durante la descompresión del archivo: {}", e.getMessage());
            throw new IOException("Error al descomprimir el archivo: " + zipFilePath, e);
        }
        log.info("Descompresión completada con éxito.");
    }

    private static String generateConfigXml(String remoteUri, String version) {
        String execPath = isMac()
                ? DOWNLOAD_DIRECTORY + File.separator + shortcutName + ".app"
                : DOWNLOAD_DIRECTORY + File.separator + shortcutName + version + ".exe";
        return generateConfigXml(remoteUri, version, execPath);
    }

    private static String generateConfigXml(String remoteUri, String version, String execPath) {
        return String.format(
                "<configuration xmlns=\"https://update4j.org\">%n" +
                        "    <version>%s</version>%n" +
                        "    <launch>%n" +
                        "        <mainClass>%s</mainClass>%n" +
                        "    </launch>%n" +
                        "    <files>%n" +
                        "        <file path=\"%s\" uri=\"%s\"/>%n" +
                        "    </files>%n" +
                        "</configuration>",
                version, main_class, execPath, remoteUri
        );
    }

    private static void saveConfigXml(String configXml, String fileName) throws IOException {
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write(configXml);
            log.info("Archivo de configuración guardado en: {}", fileName);
        }
    }

    private static String findExtractedExe(String baseDir) {
        // Preferimos la versión conocida (la que setea getRemoteVersion -> Repository.setVersion)
        String preferred = Repository.getVersion();
        if (preferred == null || preferred.isBlank()) {
            // Valor altísimo para que no matchee exacto y elija la mayor si no hay preferred
            preferred = "9999.9999.9999";
        }
        String path = findExtractedExe(baseDir, preferred);
        if (path == null) {
            // Fallback: elige la mayor versión disponible
            path = findExtractedExe(baseDir, "9999.9999.9999");
        }
        return path;
    }

    private static String findExtractedExe(String baseDir, String preferredVersion) {
        File root = new File(baseDir);
        if (!root.isDirectory()) return null;

        // VectorTrade3.1.6.exe  -> captura 3.1.6
        Pattern pat = Pattern.compile("^" + Pattern.quote(shortcutName) + "(\\d+(?:\\.\\d+)*)\\.exe$", Pattern.CASE_INSENSITIVE);

        String bestVer = null;
        File bestFile = null;

        java.util.Deque<File> stack = new java.util.ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            File dir = stack.pop();
            File[] list = dir.listFiles();
            if (list == null) continue;

            for (File f : list) {
                if (f.isDirectory()) {
                    stack.push(f);
                    continue;
                }
                String name = f.getName();
                if (!name.toLowerCase(Locale.ROOT).endsWith(".exe")) continue;

                // 1) Coincidencia exacta con la versión preferida
                if (preferredVersion != null && !preferredVersion.isBlank()) {
                    String exact = shortcutName + preferredVersion + ".exe";
                    if (name.equalsIgnoreCase(exact)) {
                        return f.getAbsolutePath();
                    }
                }

                // 2) Si el nombre trae versión, nos quedamos con la mayor
                Matcher m = pat.matcher(name);
                if (m.matches()) {
                    String ver = m.group(1);
                    if (bestVer == null || compareVersions(ver, bestVer) > 0) {
                        bestVer = ver;
                        bestFile = f;
                    }
                } else if (bestFile == null && name.startsWith(shortcutName)) {
                    // 3) Fallback: exe que comienza con el nombre (por si no trae versión en el nombre)
                    bestFile = f;
                }
            }
        }
        return bestFile != null ? bestFile.getAbsolutePath() : null;
    }

    private static void showUpdateConfirmationDialog(Stage stage, String remoteVersion, String[] possiblePaths) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setTitle("Actualización Disponible");
            alert.setHeaderText("Una nueva versión de la aplicación está disponible.");
            alert.setContentText("¿Deseas actualizar ahora?");
            alert.getDialogPane().getStylesheets().add(
                    Objects.requireNonNull(ConfigGenerator.class.getResource(CSS_STYLE_CSS)).toExternalForm()
            );

            ButtonType buttonYes = new ButtonType("Sí");
            ButtonType buttonNo  = new ButtonType("Salir");
            alert.getButtonTypes().setAll(buttonYes, buttonNo);

            alert.showAndWait().ifPresent(response -> {

                if (response == buttonYes) {
                    ProgressBar progressBar = new ProgressBar();
                    progressBar.setProgress(ProgressBar.INDETERMINATE_PROGRESS);

                    Alert progressAlert = new Alert(Alert.AlertType.INFORMATION);
                    progressAlert.setTitle("Actualizando...");
                    progressAlert.setHeaderText("Descargando actualización...");
                    progressAlert.getDialogPane().setContent(progressBar);
                    progressAlert.getDialogPane().getStylesheets().add(
                            ConfigGenerator.class.getResource(CSS_STYLE_CSS).toExternalForm()
                    );
                    progressAlert.getButtonTypes().clear();
                    progressAlert.show();


                    Task<Void> downloadTask = new Task<>() {
                        @Override

                        protected Void call() throws IOException {
                            String downloadPath;
                            String remoteUri;


                                if (isMac()) {
                                    downloadPath = DOWNLOAD_DIRECTORY + File.separator + shortcutName + ".zip";
                                    remoteUri    = URL + "/" + shortcutName + remoteVersion + ".macos.zip";
                                } else {
                                    downloadPath = DOWNLOAD_DIRECTORY + File.separator + shortcutName + remoteVersion + ".zip";
                                    remoteUri    = URL + "/" + shortcutName + remoteVersion + "win.zip";
                                }

                                log.info("Iniciando la descarga del archivo... {}", remoteUri);
                                downloadFile(remoteUri, downloadPath, this);

                                unzip(downloadPath, DOWNLOAD_DIRECTORY);

                                // (Opcional) limpiar zip descargado
                                try { Files.deleteIfExists(Paths.get(downloadPath)); } catch (IOException ignore) {}

                                // Detecta el ejecutable realmente extraído
                                String exePath = isMac()
                                        ? Paths.get(DOWNLOAD_DIRECTORY, shortcutName + ".app").toString()
                                        : findExtractedExe(DOWNLOAD_DIRECTORY, remoteVersion);
                                ;

                                if (exePath == null) {
                                    log.error("No se encontró ningún .exe después de descomprimir.");
                                }

                                LAST_EXE_PATH = exePath;


                            try {

                                String cfg = generateConfigXml(remoteUri, remoteVersion, exePath);
                                saveConfigXml(cfg, LOCAL_CONFIG_FILE);

                                // Atajo: crea el acceso directo apuntando al ejecutable real
                                deleteOldShortcuts(possiblePaths);
                                cleanupOldExecutablesByConfig();
                                createShortcut(shortcutName, exePath, possiblePaths);

                            } catch (IOException e) {
                                log.error(e.getMessage(), e);
                                if (!isCancelled()) {
                                    cancel();
                                }
                            }

                            return null;
                        }
                    };


                    downloadTask.setOnSucceeded(e -> {
                        progressAlert.close();
                        showSuccessMessage();
                    });



                    downloadTask.setOnSucceeded(e -> {
                        progressAlert.close();
                        showSuccessMessage();
                    });

                    downloadTask.setOnFailed(e -> {

                        progressAlert.close();
                        downloadTask.cancel(true);
                        thread.interrupt();

                        Alert errorAlert = new Alert(Alert.AlertType.ERROR);
                        errorAlert.setTitle("Error");
                        errorAlert.setHeaderText("Ha ocurrido un error durante la actualización.");
                        errorAlert.setContentText(downloadTask.getException().getMessage());
                        errorAlert.getDialogPane().getStylesheets().add(Objects.requireNonNull(ConfigGenerator.class.getResource(CSS_STYLE_CSS)).toExternalForm());

                        Optional<ButtonType> result = errorAlert.showAndWait();

                        if (result.isPresent() && result.get() == ButtonType.OK) {
                            Platform.exit();
                            System.exit(0);
                        }

                    });

                    thread = new Thread(downloadTask);
                    thread.setDaemon(true);
                    thread.start();

                } else {
                    log.info("El usuario eligió salir.");
                    Platform.exit();
                    System.exit(0);
                }
            });
        });
    }

    private static void showSuccessMessage() {

        Alert successAlert = new Alert(Alert.AlertType.INFORMATION);
        successAlert.setTitle("Actualización Completada");
        successAlert.setHeaderText(null);
        successAlert.setContentText("La actualización se ha completado exitosamente. La aplicación se cerrará en 5 segundos.");
        successAlert.getDialogPane().getStylesheets().add(
                ConfigGenerator.class.getResource(CSS_STYLE_CSS).toExternalForm()
        );
        successAlert.show();
        new Timeline(new KeyFrame(
                Duration.seconds(5),
                event -> {
                    successAlert.close();
                    restartApplication();
                    Platform.exit();
                    System.exit(0);
                }
        )).play();
    }

    private static void restartApplication() {
        try {
            if (isMac()) {
                String appPath = Paths.get(DOWNLOAD_DIRECTORY, shortcutName + ".app").toString();
                new ProcessBuilder("open", appPath)
                        .inheritIO()
                        .start();
                return;
            }

            // Windows
            String exePath = (LAST_EXE_PATH != null) ? LAST_EXE_PATH : findExtractedExe(DOWNLOAD_DIRECTORY);
            if (exePath == null) {
                exePath = Paths.get(DOWNLOAD_DIRECTORY, shortcutName + Repository.getVersion() + ".exe").toString();
            }
            log.info("Reiniciando con exe: {}", exePath);

            File exeFile = new File(exePath);
            try {
                // Intento 1: lanzar directo
                new ProcessBuilder(exeFile.getAbsolutePath())
                        .directory(exeFile.getParentFile())
                        .inheritIO()
                        .start();
            } catch (IOException first) {
                log.warn("Fallo al iniciar directo, reintentando con 'cmd /c start': {}", first.toString());
                // Intento 2: usar el shell de Windows (abre en nueva ventana)
                new ProcessBuilder("cmd", "/c", "start", "", "\"" + exeFile.getAbsolutePath() + "\"")
                        .directory(exeFile.getParentFile())
                        .start();
            }
        } catch (Exception e) {
            log.error("Error al intentar reiniciar la aplicación", e);
        }
    }

    private static void deleteOldShortcuts(String[] possiblePaths) {
        if (isMac()) {
            // macOS: limpia .app en paths "posibles"
            for (String path : possiblePaths) {
                File desktopDir = new File(path);
                if (!desktopDir.isDirectory()) continue;
                File[] files = desktopDir.listFiles((d, n) -> n.equals(shortcutName + ".app"));
                if (files != null) for (File f : files) if (f.delete())
                    log.info("Acceso directo antiguo eliminado (macOS): {}", f.getAbsolutePath());
            }
            return;
        }

        // Windows: resuelve escritorios reales + posibles
        String[] realDesktops = resolveWindowsDesktops();
        java.util.LinkedHashSet<String> destinos = new java.util.LinkedHashSet<>();
        for (String d : realDesktops) if (d != null && !d.isBlank()) destinos.add(d);
        for (String p : possiblePaths)  if (p != null && !p.isBlank()) destinos.add(p);

        for (String path : destinos) {
            File desktopDir = new File(path);
            if (!desktopDir.isDirectory()) continue;
            File[] files = desktopDir.listFiles((d, n) -> n.equals(shortcutName + ".lnk"));
            if (files != null) {
                for (File f : files) {
                    if (f.delete()) {
                        log.info("Acceso directo antiguo eliminado: {}", f.getAbsolutePath());
                    }
                }
            }
        }
    }

    private static void createShortcut(String shortcutName, String targetPath, String[] possiblePaths) {
        if (isMac()) {
            log.info("Omitiendo creación de acceso directo en macOS.");
            return;
        }

        // 1) Prioriza escritorios reales
        String[] realDesktops = resolveWindowsDesktops();

        // 2) Construye lista final: reales + posibles
        java.util.LinkedHashSet<String> destinos = new java.util.LinkedHashSet<>();
        for (String d : realDesktops) if (d != null && !d.isBlank()) destinos.add(d);
        for (String p : possiblePaths)  if (p != null && !p.isBlank()) destinos.add(p);

        boolean shortcutCreated = false;

        for (String path : destinos) {
            try {
                File desktopDir = new File(path);
                if (!desktopDir.exists() || !desktopDir.isDirectory()) continue;

                String shortcutPath = path + File.separator + shortcutName + ".lnk";
                String tp = targetPath.replace("'", "''");
                String wd = new File(targetPath).getParent().replace("'", "''");
                String sp = shortcutPath.replace("'", "''");

                String ps = ""
                        + "$w=New-Object -ComObject WScript.Shell;"
                        + "$s=$w.CreateShortcut('" + sp + "');"
                        + "$s.TargetPath='" + tp + "';"
                        + "$s.WorkingDirectory='" + wd + "';"
                        + "$s.IconLocation='" + tp + ",0';"
                        + "$s.WindowStyle=1;"
                        + "$s.Save();";

                Process proc = new ProcessBuilder("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps)
                        .inheritIO()
                        .start();
                proc.waitFor();

                if (new File(shortcutPath).exists()) {
                    log.info("Acceso directo creado en: {}", shortcutPath);
                    shortcutCreated = true;
                    break;
                }
            } catch (Exception e) {
                log.error("Error al crear el acceso directo en {}", path, e);
            }
        }

        if (!shortcutCreated) {
            log.error("No se pudo crear el acceso directo en ningún escritorio (real ni listado).");
        }
    }

    private static String[] resolveWindowsDesktops() {
        try {
            String ps = "$u=[Environment]::GetFolderPath('Desktop');" +
                    "$p=[Environment]::GetFolderPath('CommonDesktopDirectory');" +
                    "Write-Output ($u); Write-Output ($p)";
            Process p = new ProcessBuilder("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps)
                    .redirectErrorStream(true)
                    .start();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line1 = br.readLine();
                String line2 = br.readLine();
                p.waitFor();
                java.util.List<String> outs = new java.util.ArrayList<>();
                if (line1 != null && !line1.isBlank()) outs.add(line1.trim());
                if (line2 != null && !line2.isBlank()) outs.add(line2.trim());
                return outs.toArray(new String[0]);
            }
        } catch (Exception e) {
            log.warn("No se pudieron resolver escritorios por PowerShell, se usarán posiblesPaths.", e);
            return new String[0];
        }
    }

    private static int compareVersions(String a, String b) {
        if (a == null) a = "0";
        if (b == null) b = "0";
        String[] pa = a.split("\\.");
        String[] pb = b.split("\\.");
        int n = Math.max(pa.length, pb.length);
        for (int i = 0; i < n; i++) {
            int va = (i < pa.length) ? parseIntSafe(pa[i]) : 0;
            int vb = (i < pb.length) ? parseIntSafe(pb[i]) : 0;
            if (va != vb) return Integer.compare(va, vb);
        }
        return 0;
    }

    private static int parseIntSafe(String s) {
        try { return Integer.parseInt(s); } catch (Exception e) { return 0; }
    }


    private static void cleanupOldExecutablesByConfig() {
        if (isMac()) {
            log.info("cleanupOldExecutablesByConfig: macOS -> sin limpieza de .exe");
            return;
        }
        try {
            String currentVersion = getLocalVersion(); // Ya tienes este método
            if (currentVersion == null || currentVersion.isBlank()) {
                log.warn("cleanupOldExecutablesByConfig: versión actual vacía; no se limpia.");
                return;
            }

            Path base = Paths.get(DOWNLOAD_DIRECTORY);
            if (!Files.isDirectory(base)) {
                log.warn("cleanupOldExecutablesByConfig: {} no es directorio", DOWNLOAD_DIRECTORY);
                return;
            }

            // Patrón para identificar archivos exe con nombre y versión (sin guion)
            Pattern pat = Pattern.compile("^" + Pattern.quote(shortcutName) + "(\\d+(?:\\.\\d+)*)\\.exe$", Pattern.CASE_INSENSITIVE);

            int deleted = 0, kept = 0;
            try (var stream = Files.walk(base)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    if (!Files.isRegularFile(p)) continue;
                    String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
                    if (!name.endsWith(".exe")) continue;

                    Matcher m = pat.matcher(name);
                    if (!m.matches()) { kept++; continue; } // No sigue el patrón, no lo tocamos

                    String version = m.group(1);
                    log.debug("Comprobando archivo: {} con versión: {}", name, version);

                    // Comparar la versión del archivo con la versión actual
                    int comparison = compareVersions(version, currentVersion);
                    log.debug("Comparando versiones: {} vs {} = {}", version, currentVersion, comparison);

                    if (comparison < 0) {
                        try {
                            Files.delete(p);
                            deleted++;
                            log.info("Eliminado exe antiguo: {}", p);
                        } catch (Exception ex) {
                            log.warn("No se pudo eliminar {}: {}", p, ex.toString());
                        }
                    } else {
                        log.info("No se eliminó: {} porque su versión es igual o mayor a la actual", p);  // Añadido para depuración
                        kept++;
                    }
                }
            }
            log.info("cleanupOldExecutablesByConfig: eliminados={}, preservados/omitidos={}", deleted, kept);
        } catch (Exception e) {
            log.error("cleanupOldExecutablesByConfig error", e);
        }
    }


}
