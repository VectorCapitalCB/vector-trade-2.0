package cl.vc.service.util;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.service.multibook.Multibook2Repository;
import org.json.JSONArray;
import org.json.JSONObject;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/** Migración operativa, con respaldo, desde MultiBook legado hacia MultiBook2.0. */
public final class MultibookRedisClone {

    private MultibookRedisClone() {
    }

    public static void main(String[] args) {
        if (args.length < 5) {
            throw new IllegalArgumentException(
                    "Uso: <sourceHost> <sourcePort> <targetHost> <targetPort> <username> [--apply]");
        }

        String sourcePassword = requiredEnvironment("SOURCE_REDIS_PASSWORD");
        String targetPassword = requiredEnvironment("TARGET_REDIS_PASSWORD");
        boolean apply = args.length > 5 && "--apply".equals(args[5]);

        RedissonClient source = client(args[0], Integer.parseInt(args[1]), sourcePassword);
        RedissonClient target = client(args[2], Integer.parseInt(args[3]), targetPassword);
        try {

            String username = args[4];
            RMap<String, List<BlotterMessage.SubMultibook>> legacy = source.getMap("MultiBook");
            List<BlotterMessage.SubMultibook> raw = legacy.get(username);
            if (raw == null || raw.isEmpty()) {
                throw new IllegalStateException("No existe MultiBook legado para " + username);
            }

            TreeMap<Integer, BlotterMessage.SubMultibook> effectiveByPosition = new TreeMap<>();
            raw.forEach(row -> effectiveByPosition.put(row.getPositions(), row));
            List<BlotterMessage.SubMultibook> effective = new ArrayList<>(effectiveByPosition.values());

            JSONArray pages = Multibook2Repository.toPages(effective, null);
            JSONObject document = new JSONObject()
                    .put("version", 3)
                    .put("active", "Producción .6")
                    .put("layouts", new JSONArray().put(new JSONObject()
                            .put("name", "Producción .6")
                            .put("updated", Instant.now().toString())
                            .put("pages", pages)));

            System.out.printf("usuario=%s raw=%d efectivos=%d paginas=%d aplicar=%s%n",
                    username, raw.size(), effective.size(), pages.length(), apply);
            for (int page = 0; page < pages.length(); page++) {
                JSONObject item = pages.getJSONObject(page);
                System.out.printf("pagina=%d libros=%d capacidad=%d profundidad=%d%n",
                        page + 1, item.getJSONArray("books").length(),
                        item.optInt("bookCount", 10), item.optInt("depth", 5));
            }

            if (!apply) {
                return;
            }

            RMap<String, String> targetMap = target.getMap("MultiBook2.0", StringCodec.INSTANCE);
            String existing = targetMap.get(username);
            String backupKey = null;
            if (existing != null && !existing.isBlank()) {
                backupKey = "MultiBook2.0:backup:" + username + ":"
                        + DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
                        .withZone(java.time.ZoneOffset.UTC).format(Instant.now());
                RBucket<String> backup = target.getBucket(backupKey, StringCodec.INSTANCE);
                backup.set(existing);
            }

            targetMap.put(username, document.toString());
            JSONObject verified = new JSONObject(targetMap.get(username));
            JSONArray verifiedPages = verified.getJSONArray("layouts").getJSONObject(0).getJSONArray("pages");
            int verifiedBooks = 0;
            for (int page = 0; page < verifiedPages.length(); page++) {
                verifiedBooks += verifiedPages.getJSONObject(page).getJSONArray("books").length();
            }
            if (verifiedBooks != effective.size()) {
                throw new IllegalStateException("La verificación del destino no coincide");
            }

            System.out.printf("clonado=OK backup=%s verificados=%d%n",
                    backupKey == null ? "sin-configuracion-previa" : backupKey, verifiedBooks);
        } finally {
            source.shutdown();
            target.shutdown();
        }
    }

    private static RedissonClient client(String host, int port, String password) {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + host + ":" + port)
                .setPassword(password)
                .setConnectTimeout(5_000)
                .setTimeout(8_000);
        return Redisson.create(config);
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Falta variable " + name);
        }
        return value;
    }
}
