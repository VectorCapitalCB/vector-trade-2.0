package cl.vc.service.admin.servlet;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.service.MainApp;
import cl.vc.service.multibook.Multibook2Repository;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Gestión manual del MultiBook por usuario, ya sobre Multibook 2.0.
 *
 * GET /api/multibook/users?q=fric
 *   → lista usuarios que tengan multibook persistido.
 *
 * GET /api/multibook/{username}[?layout=Bancos]
 *   → filas de la configuración indicada (por defecto la activa), más la lista de configuraciones.
 *
 * PUT /api/multibook/{username}[?layout=Bancos]
 *   body: {
 *     "rows": [ ... ],
 *     "removePositions": [38, 39]
 *   }
 *   → actualiza/agrega filas y elimina posiciones indicadas.
 */
@Slf4j
public class MultibookServlet extends HttpServlet {

    private static final int USER_SEARCH_LIMIT = 100;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();

        try {
            if ("/users".equals(pathInfo)) {
                String query = req.getParameter("q");
                JSONArray arr = new JSONArray();

                usernames().stream()
                        .sorted(String.CASE_INSENSITIVE_ORDER)
                        .filter(u -> query == null || query.isBlank()
                                || u.toLowerCase(Locale.ROOT).contains(query.trim().toLowerCase(Locale.ROOT)))
                        .limit(USER_SEARCH_LIMIT)
                        .forEach(arr::put);

                res.getWriter().write(arr.toString());
                return;
            }

            String username = resolveUsername(pathInfo);
            if (username == null || username.isBlank()) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"Debe indicar un username en la ruta\"}");
                return;
            }

            res.getWriter().write(buildState(username, req.getParameter("layout")).toString());

        } catch (Exception e) {
            log.error("[Admin/Multibook] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo();

        try {
            String username = resolveUsername(pathInfo);
            if (username == null || username.isBlank()) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"Debe indicar un username en la ruta\"}");
                return;
            }

            String layoutName = req.getParameter("layout");
            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = body == null || body.isBlank() ? new JSONObject() : new JSONObject(body);
            JSONArray rowsJson = json.optJSONArray("rows");
            JSONArray removeJson = json.optJSONArray("removePositions");

            // Multibook 2.0 ya no admite posiciones repetidas, así que basta un mapa por posición.
            TreeMap<Integer, BlotterMessage.SubMultibook> byPosition = new TreeMap<>();
            currentRows(username, layoutName).forEach(row -> byPosition.put(row.getPositions(), row));

            if (removeJson != null) {
                for (int i = 0; i < removeJson.length(); i++) {
                    byPosition.remove(removeJson.getInt(i));
                }
            }

            if (rowsJson != null) {
                Set<Integer> requested = new HashSet<>();
                for (int i = 0; i < rowsJson.length(); i++) {
                    if (!requested.add(rowsJson.getJSONObject(i).getInt("positions"))) {
                        res.setStatus(400);
                        res.getWriter().write("{\"error\":\"No se permiten posiciones duplicadas en el guardado\"}");
                        return;
                    }
                }

                for (int i = 0; i < rowsJson.length(); i++) {
                    JSONObject row = rowsJson.getJSONObject(i);
                    if (row.has("originalPositions")) {
                        byPosition.remove(row.getInt("originalPositions"));
                    }
                    int positions = row.getInt("positions");
                    byPosition.put(positions, BlotterMessage.SubMultibook.newBuilder()
                            .setPositions(positions)
                            .setSubscribeBook(Multibook2Repository.toSubscribe(row))
                            .build());
                }
            }

            Multibook2Repository.replaceRows(username, layoutName, new ArrayList<>(byPosition.values()));
            res.getWriter().write(buildState(username, layoutName).toString());

        } catch (Exception e) {
            log.error("[Admin/Multibook] PUT error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private Set<String> usernames() {
        Set<String> users = new TreeSet<>();
        if (MainApp.getMultibook2Maps() != null) {
            users.addAll(MainApp.getMultibook2Maps().keySet());
        }
        // Los que todavía no abren el blotter siguen solo en el mapa viejo.
        if (MainApp.getMultiBookMaps() != null) {
            MainApp.getMultiBookMaps().keySet().stream().filter(Objects::nonNull).forEach(users::add);
        }
        return users;
    }

    private List<BlotterMessage.SubMultibook> currentRows(String username, String layoutName) {
        JSONObject document = Multibook2Repository.load(username);
        JSONObject layout = layoutName == null || layoutName.isBlank()
                ? Multibook2Repository.activeLayout(document)
                : Multibook2Repository.findLayout(document, layoutName);
        return Multibook2Repository.toRows(layout);
    }

    private JSONObject buildState(String username, String layoutName) {

        JSONObject document = Multibook2Repository.load(username);
        JSONObject layout = layoutName == null || layoutName.isBlank()
                ? Multibook2Repository.activeLayout(document)
                : Multibook2Repository.findLayout(document, layoutName);

        List<BlotterMessage.SubMultibook> rows = Multibook2Repository.toRows(layout);

        JSONArray layouts = new JSONArray();
        JSONArray all = document.getJSONArray("layouts");
        for (int i = 0; i < all.length(); i++) {
            layouts.put(all.getJSONObject(i).optString("name"));
        }

        JSONArray rowsJson = new JSONArray();
        rows.forEach(row -> rowsJson.put(toJson(row)));

        return new JSONObject()
                .put("username", username)
                .put("exists", layout != null)
                .put("rawCount", rows.size())
                .put("effectiveCount", rows.size())
                .put("layouts", layouts)
                .put("layout", layout == null ? "" : layout.optString("name"))
                .put("activeLayout", document.optString("active"))
                .put("rows", rowsJson);
    }

    private JSONObject toJson(BlotterMessage.SubMultibook item) {

        MarketDataMessage.Subscribe subscribe = item.getSubscribeBook();

        return Multibook2Repository.toJson(subscribe)
                .put("originalPositions", item.getPositions())
                .put("positions", item.getPositions())
                .put("occurrences", 1)
                .put("lastIndex", item.getPositions())
                .put("id", subscribe.getId());
    }

    private String resolveUsername(String pathInfo) {
        if (pathInfo == null || pathInfo.isBlank() || "/".equals(pathInfo)) {
            return null;
        }
        String trimmed = pathInfo.startsWith("/") ? pathInfo.substring(1) : pathInfo;
        return URLDecoder.decode(trimmed, StandardCharsets.UTF_8);
    }
}
