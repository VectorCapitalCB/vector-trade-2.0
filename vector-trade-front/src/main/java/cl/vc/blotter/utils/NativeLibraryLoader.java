package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class NativeLibraryLoader {

    private static final Lock lock = new ReentrantLock();
    public static String userDirPath = System.getProperty("user.home") + File.separator + "vc" + File.separator + "VectorTrade" + File.separator + "nativelib";

    public static void loadNativeLibraries() {

        String osName = System.getProperty("os.name").toLowerCase();
        List<String> libraryPaths = new ArrayList<>();

        String pathOriginal = System.getProperty("user.home") + File.separator + Repository.getProperties().getProperty("company")
                + File.separator + Repository.getProperties().getProperty("application");

        userDirPath = pathOriginal + File.separator + "nativelib";
        String credencialPath = pathOriginal + File.separator + "credentials.enc";

        log.info("os {} credencial {} ", osName, credencialPath);


        Repository.setCredencialPath(credencialPath);

        if (osName.contains("win")) {

            libraryPaths.add("/libs/win/fxplugins.dll");
            libraryPaths.add("/libs/win/glass.dll");
            libraryPaths.add("/libs/win/gstreamer-lite.dll");
            libraryPaths.add("/libs/win/glib-lite.dll");
            libraryPaths.add("/libs/win/javafx_font.dll");
            libraryPaths.add("/libs/win/jfxmedia.dll");
            libraryPaths.add("/libs/win/msvcp140_2.dll");
            libraryPaths.add("/libs/win/msvcp140_2.dll");
            //libraryPaths.add("/libs/win/jfxwebkit.dll");


        } else if (osName.contains("mac")) {

            String arch = System.getProperty("os.arch");
            log.info("Arquitectura JVM: " + arch);

            Repository.setCredencialPath(credencialPath);
            libraryPaths.add("/libs/ios/libfxplugins.dylib");
            libraryPaths.add("/libs/ios/libglass.dylib");
            libraryPaths.add("/libs/ios/libgstreamer-lite.dylib");
            libraryPaths.add("/libs/ios/libglib-lite.dylib");
            libraryPaths.add("/libs/ios/libjavafx_font.dylib");
            libraryPaths.add("/libs/ios/libjfxmedia.dylib");


        } else if (osName.contains("nix") || osName.contains("nux")) {

        } else {
            throw new UnsupportedOperationException("Unsupported operating system: " + osName);
        }

        File userDir = new File(userDirPath);

        if (!userDir.exists()) {
            userDir.mkdirs();
            log.info("creamos carpeta {}", userDirPath);
        }

        for (String libraryPath : libraryPaths) {
            try {
                copyLibraryFromResources(libraryPath, userDir);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }

        System.setProperty("java.library.path", userDirPath);

    }

    private static void copyLibraryFromResources(String path, File destDir) throws IOException {
        lock.lock();

        log.info("copiamos lib nativas");

        try (InputStream inputStream = NativeLibraryLoader.class.getResourceAsStream(path)) {
            if (inputStream == null) {
                throw new IOException("Library not found: " + path);
            }

            String fileName = getFileName(path);
            String fileExtension = getFileExtension(path);
            File libraryFile = new File(destDir, fileName + fileExtension);

            if (!libraryFile.exists()) {
                Files.copy(inputStream, libraryFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    private static String getFileExtension(String path) {
        int lastDot = path.lastIndexOf('.');
        return (lastDot == -1) ? "" : path.substring(lastDot);
    }

    private static String getFileName(String path) {
        int lastSlash = path.lastIndexOf('/');
        return (lastSlash == -1) ? path : path.substring(lastSlash + 1, path.lastIndexOf('.'));
    }
}