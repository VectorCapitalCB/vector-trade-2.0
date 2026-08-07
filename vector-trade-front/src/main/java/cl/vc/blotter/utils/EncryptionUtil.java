package cl.vc.blotter.utils;


import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.crypt.AESEncryption;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

@Slf4j
public class EncryptionUtil {

    public EncryptionUtil() throws Exception {
    }


    public String decrypt(String encryptedData) throws Exception {
        return AESEncryption.decrypt(encryptedData);
    }

    public boolean credentialsExist(String filePath) {
        return Files.exists(Paths.get(filePath));
    }

    public void guardarCredenciales(String usuario, String contrasena) {
        try (FileWriter writer = new FileWriter(Repository.getCredencialPath())) {
            String data = usuario + ":" + contrasena;
            String encryptedData = AESEncryption.encrypt(data);
            writer.write(encryptedData);
            log.info("Credenciales guardadas exitosamente.");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /** Aparta un credentials.enc que no se puede descifrar (de otra app o de una version
     *  anterior) para que el proximo arranque parta limpio en vez de fallar siempre. */
    public void apartarCredencialesIlegibles() {
        try {
            Path origen = Paths.get(Repository.getCredencialPath());
            Files.move(origen, origen.resolveSibling(origen.getFileName() + ".bak"),
                    StandardCopyOption.REPLACE_EXISTING);
            log.warn("credentials.enc ilegible; se aparto como .bak");
        } catch (Exception e) {
            log.error("no se pudo apartar credentials.enc", e);
        }
    }

    public void eliminarCredenciales() {
        try {
            File file = new File(Repository.getCredencialPath());
            if (file.exists()) {
                if (file.delete()) {
                    log.info("Credenciales eliminadas.");
                } else {
                    log.info("No se pudo eliminar el archivo de credenciales.");
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}