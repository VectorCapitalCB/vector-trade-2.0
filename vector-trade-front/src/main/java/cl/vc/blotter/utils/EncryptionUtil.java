package cl.vc.blotter.utils;


import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.crypt.AESEncryption;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

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