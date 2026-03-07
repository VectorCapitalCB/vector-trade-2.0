package cl.vc.blotter.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import cl.vc.blotter.Repository;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Properties;

@Slf4j
public class ConfigManager {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private static String resolveFileName() {
        String user = Repository.getUsername();
        if (user == null || user.isBlank()) {
            user = System.getProperty("user.name");
            if (user == null || user.isBlank()) user = "default";
        }
        // sanitizar para nombre de archivo en Windows
        user = user.replaceAll("[\\\\/:*?\"<>|]", "_");
        return user + "columnConfig.json";
    }

    private static File resolveFile() {
        String userHome = System.getProperty("user.home");

        Properties props = Repository.getProperties(); // puede ser null al inicio
        String company = (props != null && props.getProperty("company") != null)
                ? props.getProperty("company")
                : "vc";
        String application = (props != null && props.getProperty("application") != null)
                ? props.getProperty("application")
                : "VectorTrade";

        File dir = new File(userHome + File.separator + company + File.separator + application);
        if (!dir.exists() && !dir.mkdirs()) {
            log.warn("No se pudo crear el directorio {}, usando user.home", dir);
            dir = new File(userHome);
        }
        return new File(dir, resolveFileName());
    }

    public static ColumnConfig loadConfig() {
        try {
            File file = resolveFile();
            if (file.exists()) {
                try (FileReader reader = new FileReader(file)) {
                    JsonObject jo = JsonParser.parseReader(reader).getAsJsonObject();
                    ColumnConfig cfg = new ColumnConfig();
                    cfg.setSymbol(getBool(jo, "symbol", true));
                    cfg.setSettlTypeCol(getBool(jo, "settlTypeCol", true));
                    cfg.setImbalanceGen(getBool(jo, "imbalanceGen", true));
                    cfg.setMarket(getBool(jo, "market", true));
                    cfg.setBidQtyGen(getBool(jo, "bidQtyGen", true));
                    cfg.setBidpriceGen(getBool(jo, "bidpriceGen", true));
                    cfg.setOfferpriceGen(getBool(jo, "offerpriceGen", true));
                    cfg.setOfferQtyGen(getBool(jo, "offerQtyGen", true));
                    cfg.setOpenpriceGen(getBool(jo, "openpriceGen", true));
                    cfg.setClosepriceGen(getBool(jo, "closepriceGen", true));
                    cfg.setHighpriceGen(getBool(jo, "highpriceGen", true));
                    cfg.setLowpriceGen(getBool(jo, "lowpriceGen", true));
                    cfg.setAmountGen(getBool(jo, "amountGen", true));
                    cfg.setVolumeGen(getBool(jo, "volumeGen", true));
                    cfg.setVwapGen(getBool(jo, "vwapGen", true));
                    cfg.setDesbalancetheoric(getBool(jo, "desbalancetheoric", true));
                    cfg.setPriceTheoric(getBool(jo, "priceTheoric", true));
                    cfg.setAmountTheoric(getBool(jo, "amountTheoric", true));
                    return cfg;
                }
            } else {
                log.info("columnConfig.json no existe aún; se usarán defaults y se creará al primer guardado.");
            }
        } catch (Exception e) {
            log.error("loadConfig error", e);
        }
        return new ColumnConfig(); // defaults en true
    }

    public static void saveConfig(ColumnConfig cfg) {
        try {
            File file = resolveFile();
            File parent = file.getParentFile();
            if (!parent.exists() && !parent.mkdirs()) {
                log.warn("No se pudo crear el directorio padre {}", parent);
            }
            JsonObject jo = new JsonObject();
            jo.addProperty("symbol", cfg.isSymbol());
            jo.addProperty("settlTypeCol", cfg.isSettlTypeCol());
            jo.addProperty("imbalanceGen", cfg.isImbalanceGen());
            jo.addProperty("market", cfg.isMarket());
            jo.addProperty("bidQtyGen", cfg.isBidQtyGen());
            jo.addProperty("bidpriceGen", cfg.isBidpriceGen());
            jo.addProperty("offerpriceGen", cfg.isOfferpriceGen());
            jo.addProperty("offerQtyGen", cfg.isOfferQtyGen());
            jo.addProperty("openpriceGen", cfg.isOpenpriceGen());
            jo.addProperty("closepriceGen", cfg.isClosepriceGen());
            jo.addProperty("highpriceGen", cfg.isHighpriceGen());
            jo.addProperty("lowpriceGen", cfg.isLowpriceGen());
            jo.addProperty("amountGen", cfg.isAmountGen());
            jo.addProperty("volumeGen", cfg.isVolumeGen());
            jo.addProperty("vwapGen", cfg.isVwapGen());
            jo.addProperty("desbalancetheoric", cfg.isDesbalancetheoric());
            jo.addProperty("priceTheoric", cfg.isPriceTheoric());
            jo.addProperty("amountTheoric", cfg.isAmountTheoric());

            try (FileWriter writer = new FileWriter(file)) {
                gson.toJson(jo, writer);
            }
        } catch (Exception e) {
            log.error("saveConfig error", e);
        }
    }

    private static boolean getBool(JsonObject jo, String key, boolean def) {
        try {
            if (jo != null && jo.has(key)) {
                JsonElement el = jo.get(key);
                if (el != null && el.isJsonPrimitive()) {
                    return el.getAsBoolean();
                }
            }
        } catch (Exception ignored) {}
        return def;
    }
}
