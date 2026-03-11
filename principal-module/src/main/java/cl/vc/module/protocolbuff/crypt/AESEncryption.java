package cl.vc.module.protocolbuff.crypt;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class AESEncryption {

    private static final String ALGORITHM = "AES";
    private static final byte[] KEY = {
            0x59, 0x6F, 0x75, 0x72, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x4B, 0x65, 0x79, 0x31, 0x32, 0x33, // Clave de 16 bytes (128 bits)
    };

    public static String encrypt(String value) {
        try {
            SecretKeySpec keySpec = new SecretKeySpec(KEY, ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
            byte[] encryptedValue = cipher.doFinal(value.getBytes());
            return Base64.getEncoder().encodeToString(encryptedValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String decrypt(String encryptedValue) {
        try {
            SecretKeySpec keySpec = new SecretKeySpec(KEY, ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec);
            byte[] decodedValue = Base64.getDecoder().decode(encryptedValue);
            byte[] decryptedValue = cipher.doFinal(decodedValue);
            return new String(decryptedValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        try {


            String sensitiveValue1 = "blotter-front";
            String encryptedValue1 = AESEncryption.encrypt(sensitiveValue1);
            String deencryptedValue2 = AESEncryption.decrypt(encryptedValue1);

            System.out.println("Valor descrip 1: " + deencryptedValue2);
            System.out.println("Valor encrip 2: " + encryptedValue1);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
