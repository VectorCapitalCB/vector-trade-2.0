package cl.vc.service.admin.servlet;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.routing.ActorGroupPerAccount;
import org.json.JSONArray;
import org.json.JSONObject;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class PrestamosServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");

        try {
            String account = Optional.ofNullable(req.getParameter("account")).orElse("").trim();
            String source = Optional.ofNullable(req.getParameter("source")).orElse("effective").trim().toLowerCase(Locale.ROOT);

            if (account.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "account es requerido").toString());
                return;
            }

            List<BlotterMessage.Prestamos> rows = switch (source) {
                case "sql" -> loadSql(account);
                case "manual" -> new ArrayList<>(MainApp.getManualPrestamosOverride(account));
                default -> buildEffectiveRows(account);
            };

            JSONArray arr = new JSONArray();
            rows.forEach(row -> arr.put(toJson(row)));

            res.getWriter().write(new JSONObject()
                    .put("ok", true)
                    .put("account", account)
                    .put("source", source)
                    .put("count", rows.size())
                    .put("hasManualOverride", MainApp.hasManualPrestamosOverride(account))
                    .put("rows", arr)
                    .toString());
        } catch (Exception e) {
            log.error("[Admin/Prestamos] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");

        try {
            String pathInfo = Optional.ofNullable(req.getPathInfo()).orElse("");
            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = body == null || body.isBlank() ? new JSONObject() : new JSONObject(body);
            String account = json.optString("account", "").trim();

            if (account.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "account es requerido").toString());
                return;
            }

            switch (pathInfo) {
                case "/apply" -> applyManualRows(account, json, res);
                case "/clear" -> clearManualRows(account, res);
                default -> {
                    res.setStatus(400);
                    res.getWriter().write(new JSONObject().put("error", "Ruta no soportada: " + pathInfo).toString());
                }
            }
        } catch (Exception e) {
            log.error("[Admin/Prestamos] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private void applyManualRows(String account, JSONObject json, HttpServletResponse res) throws IOException {
        JSONArray rows = json.optJSONArray("rows");
        List<BlotterMessage.Prestamos> manualRows = new ArrayList<>();

        if (rows != null) {
            for (int i = 0; i < rows.length(); i++) {
                manualRows.add(fromJson(rows.getJSONObject(i)));
            }
        }

        MainApp.replaceManualPrestamosOverride(account, manualRows);
        refreshActor(account, true);

        res.getWriter().write(new JSONObject()
                .put("ok", true)
                .put("account", account)
                .put("applied", manualRows.size())
                .put("message", "Préstamos manuales aplicados para " + account)
                .toString());
    }

    private void clearManualRows(String account, HttpServletResponse res) throws IOException {
        MainApp.clearManualPrestamosOverride(account);
        refreshActor(account, true);

        res.getWriter().write(new JSONObject()
                .put("ok", true)
                .put("account", account)
                .put("message", "Override manual de préstamos limpiado para " + account)
                .toString());
    }

    private void refreshActor(String account, boolean recalculate) {
        ActorRef actor = MainApp.ensureAccountActor(account);
        if (actor != null) {
            actor.tell(new ActorGroupPerAccount.RefreshManualPrestamos(recalculate), ActorRef.noSender());
        }
    }

    private List<BlotterMessage.Prestamos> buildEffectiveRows(String account) {
        if (MainApp.hasManualPrestamosOverride(account)) {
            return MainApp.getManualPrestamosOverride(account).stream()
                    .filter(this::isPrestamoVigente)
                    .collect(Collectors.toCollection(ArrayList::new));
        }
        return loadSql(account).stream()
                .filter(this::isPrestamoVigente)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private List<BlotterMessage.Prestamos> loadSql(String account) {
        List<BlotterMessage.Prestamos.Builder> builders = cl.vc.service.util.SQLServerConnection.prestamos(account);
        if (builders == null) {
            return Collections.emptyList();
        }
        return builders.stream().map(BlotterMessage.Prestamos.Builder::build).collect(Collectors.toCollection(ArrayList::new));
    }

    private JSONObject toJson(BlotterMessage.Prestamos row) {
        return new JSONObject()
                .put("nemotecnico", row.getNemotecnico())
                .put("cantidadVigente", row.getCantidadVigente())
                .put("precioAyer", row.getPrecioAyer())
                .put("monto", row.getMonto())
                .put("fechaIngreso", row.getFechaIngreso())
                .put("plazoPimo", row.getPlazoPimo())
                .put("fechaVto", row.getFechaVto());
    }

    private BlotterMessage.Prestamos fromJson(JSONObject row) {
        BlotterMessage.Prestamos.Builder builder = BlotterMessage.Prestamos.newBuilder();
        builder.setNemotecnico(normalizeString(row.optString("nemotecnico", "")));
        builder.setCantidadVigente(row.optDouble("cantidadVigente", 0d));
        builder.setPrecioAyer(row.optDouble("precioAyer", 0d));
        builder.setMonto(row.optDouble("monto", 0d));
        builder.setFechaIngreso(normalizeString(row.optString("fechaIngreso", LocalDate.now(resolveZoneId()).toString())));
        builder.setPlazoPimo(normalizeString(row.optString("plazoPimo", "0")));
        builder.setFechaVto(normalizeString(row.optString("fechaVto", LocalDate.now(resolveZoneId()).toString())));
        return builder.build();
    }

    private String normalizeString(String value) {
        return value == null ? "" : value.trim();
    }

    private boolean isPrestamoVigente(BlotterMessage.Prestamos row) {
        if (row == null) {
            return false;
        }

        LocalDate today = LocalDate.now(resolveZoneId());
        LocalDate fechaIngreso = parseDate(row.getFechaIngreso());
        LocalDate fechaVto = parseDate(row.getFechaVto());

        if (fechaIngreso != null && fechaIngreso.isAfter(today)) {
            return false;
        }
        if (fechaVto != null) {
            return !fechaVto.isBefore(today);
        }
        return true;
    }

    private LocalDate parseDate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }

        List<DateTimeFormatter> formatters = List.of(
                DateTimeFormatter.ISO_LOCAL_DATE,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
                DateTimeFormatter.ofPattern("MM/dd/yyyy"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy")
        );

        String value = raw.trim();
        if (value.length() >= 10) {
            String prefix = value.substring(0, 10);
            try {
                return LocalDate.parse(prefix, DateTimeFormatter.ISO_LOCAL_DATE);
            } catch (DateTimeParseException ignored) {
            }
        }

        for (DateTimeFormatter formatter : formatters) {
            try {
                return LocalDate.parse(value, formatter);
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }

    private ZoneId resolveZoneId() {
        return MainApp.getZoneId() != null ? MainApp.getZoneId() : ZoneId.of("America/Santiago");
    }
}
