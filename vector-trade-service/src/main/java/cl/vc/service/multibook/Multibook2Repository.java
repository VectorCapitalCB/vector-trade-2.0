package cl.vc.service.multibook;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Multibook 2.0: por usuario, N configuraciones nombradas, cada una con sus paginas y sus libros.
 * Vive en el RMap de Redis {@code MultiBook2.0} como documento JSON:
 *
 * <pre>
 * { "version": 2,
 *   "active": "Bancos",
 *   "layouts": [ { "name": "Bancos", "updated": "...",
 *                  "pages": [ { "name": "IPSA",
 *                               "books": [ {"slot":0,"symbol":"CHILE","exchange":"BCS",
 *                                           "settlType":"T2","securityType":"CS"} ] } ] } ] }
 * </pre>
 *
 * Va como JSON y no como protobuf porque los mensajes viven en principal-module, que se resuelve por
 * jitpack desde tags de GitHub: agregar uno obliga a publicar tag y subir el OMS y el front a la vez.
 */
@Slf4j
public final class Multibook2Repository {

    public static final int BOOKS_PER_PAGE = 10;

    private static final String DEFAULT_LAYOUT = "Default";
    private static final int VERSION = 2;

    private Multibook2Repository() {
    }

    /** Documento del usuario; si no existe, se migra desde el mapa viejo {@code MultiBook}. */
    public static synchronized JSONObject load(String username) {

        String raw = MainApp.getMultibook2Maps() == null ? null : MainApp.getMultibook2Maps().get(username);

        if (raw != null && !raw.isBlank()) {
            try {
                return normalize(new JSONObject(raw));
            } catch (Exception e) {
                log.error("[Multibook2] documento ilegible de {}, se remigra: {}", username, e.getMessage());
            }
        }

        JSONObject migrated = migrateFromLegacy(username);
        save(username, migrated);
        return migrated;
    }

    public static synchronized void save(String username, JSONObject document) {
        if (MainApp.getMultibook2Maps() == null) {
            log.warn("[Multibook2] Redis no disponible, no se guarda el multibook de {}", username);
            return;
        }
        MainApp.getMultibook2Maps().put(username, normalize(document).toString());
    }

    public static synchronized boolean renameLayout(String username, String from, String to) {
        JSONObject document = load(username);
        JSONObject layout = findLayout(document, from);
        if (layout == null || to == null || to.isBlank() || findLayout(document, to) != null) {
            return false;
        }
        layout.put("name", to.trim()).put("updated", Instant.now().toString());
        if (from.equals(document.optString("active"))) {
            document.put("active", to.trim());
        }
        save(username, document);
        return true;
    }

    public static synchronized boolean deleteLayout(String username, String name) {
        JSONObject document = load(username);
        JSONArray layouts = document.getJSONArray("layouts");
        if (layouts.length() <= 1) {
            return false;
        }
        for (int i = 0; i < layouts.length(); i++) {
            if (layouts.getJSONObject(i).optString("name").equals(name)) {
                layouts.remove(i);
                if (name.equals(document.optString("active"))) {
                    document.put("active", layouts.getJSONObject(0).optString("name"));
                }
                save(username, document);
                return true;
            }
        }
        return false;
    }

    public static synchronized boolean setActive(String username, String name) {
        JSONObject document = load(username);
        if (findLayout(document, name) == null) {
            return false;
        }
        document.put("active", name);
        save(username, document);
        return true;
    }

    /** Filas del layout activo en el formato protobuf viejo, indexadas por posicion global. */
    public static List<BlotterMessage.SubMultibook> effectiveRows(String username) {
        return toRows(activeLayout(load(username)));
    }

    public static JSONObject activeLayout(JSONObject document) {
        JSONObject layout = findLayout(document, document.optString("active"));
        return layout != null ? layout : document.getJSONArray("layouts").getJSONObject(0);
    }

    public static JSONObject findLayout(JSONObject document, String name) {
        JSONArray layouts = document.optJSONArray("layouts");
        if (layouts == null || name == null) {
            return null;
        }
        for (int i = 0; i < layouts.length(); i++) {
            JSONObject layout = layouts.getJSONObject(i);
            if (name.equals(layout.optString("name"))) {
                return layout;
            }
        }
        return null;
    }

    /** Aplana un layout a las filas {posicion global, subscribe} que consume el resto del OMS. */
    public static List<BlotterMessage.SubMultibook> toRows(JSONObject layout) {

        List<BlotterMessage.SubMultibook> rows = new ArrayList<>();
        if (layout == null) {
            return rows;
        }

        JSONArray pages = layout.optJSONArray("pages");
        if (pages == null) {
            return rows;
        }

        for (int page = 0; page < pages.length(); page++) {
            JSONArray books = pages.getJSONObject(page).optJSONArray("books");
            if (books == null) {
                continue;
            }
            for (int i = 0; i < books.length(); i++) {
                JSONObject book = books.getJSONObject(i);
                if (book.optString("symbol").isBlank()) {
                    continue;
                }
                rows.add(BlotterMessage.SubMultibook.newBuilder()
                        .setPositions(page * BOOKS_PER_PAGE + book.optInt("slot"))
                        .setSubscribeBook(toSubscribe(book))
                        .build());
            }
        }

        return rows;
    }

    /**
     * Reemplaza las filas del layout activo por posicion global -- reemplaza, no acumula, que es lo
     * que hacia el mapa viejo y por eso crecia sin limite con posiciones repetidas.
     */
    public static synchronized List<BlotterMessage.SubMultibook> mergeRows(
            String username, List<BlotterMessage.SubMultibook> incoming) {

        JSONObject document = load(username);
        JSONObject layout = activeLayout(document);

        TreeMap<Integer, BlotterMessage.SubMultibook> byPosition = new TreeMap<>();
        toRows(layout).forEach(row -> byPosition.put(row.getPositions(), row));
        incoming.forEach(row -> byPosition.put(row.getPositions(), row));

        List<BlotterMessage.SubMultibook> rows = new ArrayList<>(byPosition.values());
        layout.put("pages", toPages(rows, layout.optJSONArray("pages")))
                .put("updated", Instant.now().toString());
        save(username, document);

        return rows;
    }

    /** Sobrescribe las filas de un layout (null = el activo) con las indicadas. */
    public static synchronized List<BlotterMessage.SubMultibook> replaceRows(
            String username, String layoutName, List<BlotterMessage.SubMultibook> rows) {

        JSONObject document = load(username);
        JSONObject layout = layoutName == null || layoutName.isBlank()
                ? activeLayout(document)
                : findLayout(document, layoutName);

        if (layout == null) {
            return List.of();
        }

        layout.put("pages", toPages(rows, layout.optJSONArray("pages")))
                .put("updated", Instant.now().toString());
        save(username, document);

        return toRows(layout);
    }

    /** Reparte filas por posicion global en paginas de {@value #BOOKS_PER_PAGE}. */
    public static JSONArray toPages(List<BlotterMessage.SubMultibook> rows, JSONArray existing) {

        TreeMap<Integer, BlotterMessage.SubMultibook> byPosition = new TreeMap<>();
        rows.forEach(row -> byPosition.put(row.getPositions(), row));

        int pageCount = Math.max(
                byPosition.isEmpty() ? 1 : (byPosition.lastKey() / BOOKS_PER_PAGE) + 1,
                existing == null ? 1 : existing.length());

        JSONArray pages = new JSONArray();
        for (int page = 0; page < pageCount; page++) {
            String name = (existing != null && page < existing.length())
                    ? existing.getJSONObject(page).optString("name", String.valueOf(page + 1))
                    : String.valueOf(page + 1);
            pages.put(new JSONObject().put("name", name).put("books", new JSONArray()));
        }

        for (Map.Entry<Integer, BlotterMessage.SubMultibook> entry : byPosition.entrySet()) {
            int position = entry.getKey();
            JSONObject book = toJson(entry.getValue().getSubscribeBook());
            book.put("slot", position % BOOKS_PER_PAGE);
            pages.getJSONObject(position / BOOKS_PER_PAGE).getJSONArray("books").put(book);
        }

        return pages;
    }

    public static JSONObject toJson(MarketDataMessage.Subscribe subscribe) {
        return new JSONObject()
                .put("symbol", subscribe.getSymbol())
                .put("exchange", subscribe.getSecurityExchange().name())
                .put("settlType", subscribe.getSettlType().name())
                .put("securityType", subscribe.getSecurityType().name())
                .put("depth", subscribe.getDepth().name())
                .put("trade", subscribe.getTrade())
                .put("statistic", subscribe.getStatistic())
                .put("book", subscribe.getBook())
                .put("tradeQty", subscribe.getTradeQty())
                .put("bookQty", subscribe.getBookQty());
    }

    public static MarketDataMessage.Subscribe toSubscribe(JSONObject book) {

        MarketDataMessage.Subscribe subscribe = MarketDataMessage.Subscribe.newBuilder()
                .setSymbol(book.optString("symbol").trim().toUpperCase(Locale.ROOT))
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.valueOf(
                        book.optString("exchange", "BCS").trim().toUpperCase(Locale.ROOT)))
                .setSettlType(RoutingMessage.SettlType.valueOf(
                        book.optString("settlType", "T2").trim().toUpperCase(Locale.ROOT)))
                .setSecurityType(RoutingMessage.SecurityType.valueOf(
                        book.optString("securityType", "CS").trim().toUpperCase(Locale.ROOT)))
                .setDepth(MarketDataMessage.Depth.valueOf(
                        book.optString("depth", "FULL_BOOK").trim().toUpperCase(Locale.ROOT)))
                .setTrade(book.optBoolean("trade", true))
                .setStatistic(book.optBoolean("statistic", true))
                .setBook(book.optBoolean("book", true))
                .setTradeQty(book.optInt("tradeQty", 0))
                .setBookQty(book.optInt("bookQty", 0))
                .build();

        return subscribe.toBuilder().setId(TopicGenerator.getTopicMKD(subscribe)).build();
    }

    /**
     * Arma el documento inicial desde el mapa viejo {@code MultiBook}, que acumulaba filas repetidas
     * por posicion: gana la ultima ocurrencia, igual que hace el panel admin al mostrarlo.
     */
    private static JSONObject migrateFromLegacy(String username) {

        List<BlotterMessage.SubMultibook> legacy = MainApp.getMultiBookMaps() == null
                ? null
                : MainApp.getMultiBookMaps().get(username);

        JSONArray pages = legacy == null || legacy.isEmpty()
                ? new JSONArray().put(new JSONObject().put("name", "1").put("books", new JSONArray()))
                : toPages(legacy, null);

        if (legacy != null && !legacy.isEmpty()) {
            log.info("[Multibook2] migradas {} filas de {} a la configuracion '{}'",
                    legacy.size(), username, DEFAULT_LAYOUT);
        }

        return new JSONObject()
                .put("version", VERSION)
                .put("active", DEFAULT_LAYOUT)
                .put("layouts", new JSONArray().put(new JSONObject()
                        .put("name", DEFAULT_LAYOUT)
                        .put("updated", Instant.now().toString())
                        .put("pages", pages)));
    }

    /** Garantiza version, al menos un layout y un activo valido. */
    private static JSONObject normalize(JSONObject document) {

        document.put("version", VERSION);

        JSONArray layouts = document.optJSONArray("layouts");
        if (layouts == null || layouts.isEmpty()) {
            layouts = new JSONArray().put(new JSONObject()
                    .put("name", DEFAULT_LAYOUT)
                    .put("updated", Instant.now().toString())
                    .put("pages", new JSONArray().put(
                            new JSONObject().put("name", "1").put("books", new JSONArray()))));
            document.put("layouts", layouts);
        }

        if (findLayout(document, document.optString("active")) == null) {
            document.put("active", layouts.getJSONObject(0).optString("name"));
        }

        return document;
    }
}
