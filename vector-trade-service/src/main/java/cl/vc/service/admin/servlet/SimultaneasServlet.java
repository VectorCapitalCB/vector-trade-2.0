package cl.vc.service.admin.servlet;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.routing.ActorGroupPerAccount;
import cl.vc.service.util.ProtoDateProcessor;
import cl.vc.service.util.SQLServerConnection;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SimultaneasServlet extends HttpServlet {

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

            List<BlotterMessage.Simultaneas> rows = switch (source) {
                case "sql" -> SQLServerConnection.consultaSimultaneaByAccount(account);
                case "manual" -> new ArrayList<>(MainApp.getManualSimultaneasOverride(account));
                default -> buildEffectiveRows(account);
            };

            JSONArray arr = new JSONArray();
            rows.forEach(row -> arr.put(toJson(row)));

            res.getWriter().write(new JSONObject()
                    .put("ok", true)
                    .put("account", account)
                    .put("source", source)
                    .put("count", rows.size())
                    .put("hasManualOverride", MainApp.hasManualSimultaneasOverride(account))
                    .put("rows", arr)
                    .toString());
        } catch (Exception e) {
            log.error("[Admin/Simultaneas] GET error: {}", e.getMessage(), e);
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
            log.error("[Admin/Simultaneas] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private void applyManualRows(String account, JSONObject json, HttpServletResponse res) throws IOException {
        JSONArray rows = json.optJSONArray("rows");
        List<BlotterMessage.Simultaneas> manualRows = new ArrayList<>();

        if (rows != null) {
            for (int i = 0; i < rows.length(); i++) {
                JSONObject row = rows.getJSONObject(i);
                manualRows.add(fromJson(account, row, i));
            }
        }

        MainApp.replaceManualSimultaneasOverride(account, manualRows);
        refreshActor(account, true);

        res.getWriter().write(new JSONObject()
                .put("ok", true)
                .put("account", account)
                .put("applied", manualRows.size())
                .put("message", "Simultáneas manuales aplicadas para " + account)
                .toString());
    }

    private void clearManualRows(String account, HttpServletResponse res) throws IOException {
        MainApp.clearManualSimultaneasOverride(account);
        refreshActor(account, true);

        res.getWriter().write(new JSONObject()
                .put("ok", true)
                .put("account", account)
                .put("message", "Override manual limpiado para " + account)
                .toString());
    }

    private void refreshActor(String account, boolean recalculate) {
        ActorRef actor = MainApp.ensureAccountActor(account);
        if (actor != null) {
            actor.tell(new ActorGroupPerAccount.RefreshManualSimultaneas(recalculate), ActorRef.noSender());
        }
    }

    private List<BlotterMessage.Simultaneas> buildEffectiveRows(String account) {
        if (MainApp.hasManualSimultaneasOverride(account)) {
            return MainApp.getManualSimultaneasOverride(account).stream()
                    .filter(this::isSimultaneaVigente)
                    .collect(Collectors.toCollection(ArrayList::new));
        }

        LinkedHashMap<String, BlotterMessage.Simultaneas> rows = new LinkedHashMap<>();

        List<BlotterMessage.Simultaneas> sqlRows = MainApp.getAllSimultaneas();
        if (sqlRows != null) {
            sqlRows.forEach(row -> {
                if (account.equals(row.getNumCuenta()) && isSimultaneaVigente(row)) {
                    rows.put(row.getId(), row);
                }
            });
        }

        return new ArrayList<>(rows.values());
    }

    private JSONObject toJson(BlotterMessage.Simultaneas row) {
        return new JSONObject()
                .put("id", row.getId())
                .put("tipoSimul", row.getTipoSimul())
                .put("detalleSimultanea", row.getDetalleSimultanea())
                .put("identCliente", row.getIdentCliente())
                .put("numCuenta", row.getNumCuenta())
                .put("fechaOperacion", row.getFechaOperacion())
                .put("fechaVcto", row.getFechaVcto())
                .put("nombreCliente", row.getNombreCliente())
                .put("plazo", row.getPlazo())
                .put("plazoRem", row.getPlazoRem())
                .put("nemotecnico", row.getNemotecnico())
                .put("cantidad", row.getCantidad())
                .put("tasa", row.getTasa())
                .put("precioPH", row.getPrecioPH())
                .put("precioPlazo", row.getPrecioPlazo())
                .put("precioMercado", row.getPrecioMercado())
                .put("montoContado", row.getMontoContado())
                .put("costoDiario2", row.getCostoDiario2())
                .put("montoPresente", row.getMontoPresente())
                .put("montoPlazo", row.getMontoPlazo())
                .put("codInst", row.getCodInst())
                .put("cantidadOrig", row.getCantidadOrig())
                .put("corredorVenta", row.getCorredorVenta())
                .put("corredorCompra", row.getCorredorCompra())
                .put("folioFactPH", row.getFolioFactPH())
                .put("folioFactTP", row.getFolioFactTP());
    }

    private BlotterMessage.Simultaneas fromJson(String account, JSONObject row, int index) {
        BlotterMessage.Simultaneas.Builder builder = BlotterMessage.Simultaneas.newBuilder();
        ProtoDateProcessor.setDateProcesorIfMissing(builder);

        String id = normalizeString(row.optString("id", ""));
        if (id.isBlank()) {
            id = "MANUAL-" + account.replace('/', '_') + "-" + System.currentTimeMillis() + "-" + index;
        }

        builder.setId(id);
        builder.setTipoSimul(normalizeString(row.optString("tipoSimul", "Simultánea Manual")));
        builder.setDetalleSimultanea(normalizeString(row.optString("detalleSimultanea", String.valueOf(index + 1))));
        builder.setIdentCliente(normalizeString(row.optString("identCliente", account.split("/")[0])));
        builder.setNumCuenta(account);
        builder.setFechaOperacion(normalizeString(row.optString("fechaOperacion", LocalDate.now(resolveZoneId()).toString())));
        builder.setFechaVcto(normalizeString(row.optString("fechaVcto", LocalDate.now(resolveZoneId()).toString())));
        builder.setNombreCliente(normalizeString(row.optString("nombreCliente", "")));
        builder.setPlazo(normalizeString(row.optString("plazo", "0")));
        builder.setPlazoRem(normalizeString(row.optString("plazoRem", row.optString("plazo", "0"))));
        builder.setNemotecnico(normalizeString(row.optString("nemotecnico", "")));
        builder.setCantidad(normalizeString(row.optString("cantidad", "0")));
        builder.setTasa(normalizeString(row.optString("tasa", "0")));
        builder.setPrecioPH(normalizeString(row.optString("precioPH", "0")));
        builder.setPrecioPlazo(normalizeString(row.optString("precioPlazo", "0")));
        builder.setPrecioMercado(normalizeString(row.optString("precioMercado", "0")));
        builder.setMontoContado(normalizeString(row.optString("montoContado", "0")));
        builder.setCostoDiario2(normalizeString(row.optString("costoDiario2", "0")));
        builder.setMontoPresente(normalizeString(row.optString("montoPresente", "0")));
        builder.setMontoPlazo(normalizeString(row.optString("montoPlazo", "0")));
        builder.setCodInst(normalizeString(row.optString("codInst", "")));
        builder.setCantidadOrig(normalizeString(row.optString("cantidadOrig", row.optString("cantidad", "0"))));
        builder.setCorredorVenta(normalizeString(row.optString("corredorVenta", "")));
        builder.setCorredorCompra(normalizeString(row.optString("corredorCompra", "")));
        builder.setFolioFactPH(normalizeString(row.optString("folioFactPH", "")));
        builder.setFolioFactTP(normalizeString(row.optString("folioFactTP", "")));
        return builder.build();
    }

    private String normalizeString(String value) {
        return value == null ? "" : value.trim();
    }

    private boolean isSimultaneaVigente(BlotterMessage.Simultaneas row) {
        if (row == null) {
            return false;
        }

        LocalDate today = LocalDate.now(resolveZoneId());
        LocalDate fechaOperacion = parseDate(row.getFechaOperacion());
        LocalDate fechaVcto = parseDate(row.getFechaVcto());

        if (fechaOperacion != null && fechaOperacion.isAfter(today)) {
            return false;
        }
        if (fechaVcto != null) {
            return !fechaVcto.isBefore(today);
        }
        return fechaOperacion == null || !fechaOperacion.isAfter(today);
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
