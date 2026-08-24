package cl.vc.service.admin.servlet;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import cl.vc.service.admin.ManualSaldoOverride;
import cl.vc.service.akka.actors.routing.ActorGroupPerAccount;
import cl.vc.service.util.CalculoCreasys;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class SaldoServlet extends HttpServlet {

    private static final long APPLY_TIMEOUT_MS = 3000L;
    private static final long APPLY_POLL_MS = 100L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");

        try {
            String account = MainApp.resolveAccountKey(
                    Optional.ofNullable(req.getParameter("account")).orElse(""));
            String source = Optional.ofNullable(req.getParameter("source")).orElse("effective").trim().toLowerCase();

            if (account.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "account es requerido").toString());
                return;
            }

            // Validar formato de cuenta: bloquea metacaracteres LIKE (% _ [) que fugaban datos cross-account.
            if (!isValidAccount(account)) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "account inválido").toString());
                return;
            }

            JSONObject payload = switch (source) {
                case "manual" -> buildManualPayload(account);
                case "sql" -> buildSqlPayload(account);
                default -> buildEffectivePayload(account);
            };

            payload.put("ok", true);
            payload.put("account", account);
            payload.put("source", source);
            payload.put("hasManualOverride", MainApp.hasManualSaldoOverride(account));
            res.getWriter().write(payload.toString());
        } catch (Exception e) {
            log.error("[Admin/Saldo] GET error: {}", e.getMessage(), e);
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
            String account = MainApp.resolveAccountKey(json.optString("account", ""));

            if (account.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "account es requerido").toString());
                return;
            }
            if (!isValidAccount(account)) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "account invalido").toString());
                return;
            }

            switch (pathInfo) {
                case "/apply" -> applyManual(account, json, res);
                case "/clear" -> clearManual(account, res);
                default -> {
                    res.setStatus(400);
                    res.getWriter().write(new JSONObject().put("error", "Ruta no soportada: " + pathInfo).toString());
                }
            }
        } catch (Exception e) {
            log.error("[Admin/Saldo] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private void applyManual(String account, JSONObject json, HttpServletResponse res) throws IOException {
        ManualSaldoOverride override = new ManualSaldoOverride(
                json.optDouble("caja", 0d),
                json.optDouble("cuentaTransitorias", 0d),
                json.optDouble("garantiaEfectivo", 0d),
                json.optDouble("garantiasConstituidas", 0d),
                json.optDouble("garantiasExigidas", 0d),
                json.optDouble("garantiasReservadas", 0d),
                json.optDouble("limiteFinanciero", 0d),
                json.optDouble("garantiasDisponible", 0d),
                json.optDouble("ordenesActivasCompras", 0d),
                json.optDouble("ordenesActivasVentas", 0d),
                json.optDouble("ordenesCalzadasCompras", 0d),
                json.optDouble("ordenesCalzadasVentas", 0d),
                json.optDouble("ordenesCestaCompras", 0d),
                json.optDouble("ordenesCestaVentas", 0d),
                json.optDouble("rendimiento", 0d),
                json.optDouble("total", 0d)
        );

        MainApp.replaceManualSaldoOverride(account, override);
        refreshActor(account);

        JSONObject payload = awaitEffectivePayload(account, override);
        payload.put("ok", true);
        payload.put("account", account);
        payload.put("source", "effective");
        payload.put("hasManualOverride", true);
        payload.put("message", "Saldo manual aplicado para " + account);
        res.getWriter().write(payload.toString());
    }

    private void clearManual(String account, HttpServletResponse res) throws IOException {
        MainApp.clearManualSaldoOverride(account);
        refreshActor(account);

        JSONObject payload = awaitEffectivePayload(account, null);
        payload.put("ok", true);
        payload.put("account", account);
        payload.put("source", "effective");
        payload.put("hasManualOverride", false);
        payload.put("message", "Override manual de saldo limpiado para " + account);
        res.getWriter().write(payload.toString());
    }

    private void refreshActor(String account) {
        ActorRef actor = MainApp.ensureAccountActor(account);
        if (actor != null) {
            actor.tell(new ActorGroupPerAccount.RefreshManualSaldo(true), ActorRef.noSender());
        }
    }

    private JSONObject buildManualPayload(String account) {
        ManualSaldoOverride override = MainApp.getManualSaldoOverride(account);
        JSONObject effective = buildEffectivePayload(account);
        if (override == null) {
            return effective;
        }
        return mergeOverrideIntoPayload(effective, override);
    }

    private JSONObject buildSqlPayload(String account) {
        String normalized = account.replace("-", "/");
        HashMap<RoutingMessage.Currency, BlotterMessage.Patrimonio.Builder> maps = CalculoCreasys.saldoCaja(normalized);
        BlotterMessage.Patrimonio.Builder clpBuilder = maps.get(RoutingMessage.Currency.CLP);
        BlotterMessage.Patrimonio clp = clpBuilder != null ? clpBuilder.build() : null;

        JSONObject payload = buildEffectivePayload(account);
        if (clp != null) {
            payload.put("caja", clp.getCaja().getValues());
            payload.put("cuentaTransitorias", clp.getCuentaTransitoriasPorCobrarPagar().getValues());
            payload.put("garantiaEfectivo", clp.getGarantiaEfectivo().getValues());
        }
        return payload;
    }

    private JSONObject buildEffectivePayload(String account) {
        String dailyKey = account + "|" + LocalDate.now(resolveZoneId());
        BlotterMessage.Balance balance = MainApp.getBalanceRedis() != null ? MainApp.getBalanceRedis().get(dailyKey) : null;
        BlotterMessage.Patrimonio patrimonio = MainApp.getPatrimonioMapsRedis() != null ? MainApp.getPatrimonioMapsRedis().get(dailyKey) : null;

        if (balance == null && patrimonio == null) {
            return buildSqlPayloadFallback(account);
        }

        ManualSaldoOverride data = new ManualSaldoOverride();
        if (patrimonio != null) {
            data.setCaja(patrimonio.getCaja().getValues());
            data.setCuentaTransitorias(patrimonio.getCuentaTransitoriasPorCobrarPagar().getValues());
            data.setGarantiaEfectivo(patrimonio.getGarantiaEfectivo().getValues());
        }
        if (balance != null) {
            data.setGarantiasConstituidas(balance.getGarantiasConstituidas());
            data.setGarantiasExigidas(balance.getGarantiasExigidas());
            data.setGarantiasReservadas(balance.getGarantiasReservadas());
            data.setLimiteFinanciero(balance.getLimiteFinanciero());
            data.setGarantiasDisponible(balance.getGarantiasDisponible());
            data.setOrdenesActivasCompras(balance.getOrdenesActivasCompras());
            data.setOrdenesActivasVentas(balance.getOrdenesActivasVentas());
            data.setOrdenesCalzadasCompras(balance.getOrdenesCalzadasCompras());
            data.setOrdenesCalzadasVentas(balance.getOrdenesCalzadasVentas());
            data.setOrdenesCestaCompras(balance.getOrdenesCestaCompras());
            data.setOrdenesCestaVentas(balance.getOrdenesCestaVentas());
            data.setRendimiento(balance.getRendimiento());
            data.setTotal(balance.getTotal());
        }
        return toJson(data, balance, patrimonio);
    }

    private JSONObject buildSqlPayloadFallback(String account) {
        String normalized = account.replace("-", "/");
        HashMap<RoutingMessage.Currency, BlotterMessage.Patrimonio.Builder> maps = CalculoCreasys.saldoCaja(normalized);
        BlotterMessage.Patrimonio.Builder clpBuilder = maps.get(RoutingMessage.Currency.CLP);
        BlotterMessage.Patrimonio clp = clpBuilder != null ? clpBuilder.build() : null;

        ManualSaldoOverride data = new ManualSaldoOverride();
        if (clp != null) {
            data.setCaja(clp.getCaja().getValues());
            data.setCuentaTransitorias(clp.getCuentaTransitoriasPorCobrarPagar().getValues());
            data.setGarantiaEfectivo(clp.getGarantiaEfectivo().getValues());
        }
        return toJson(data, null, clp);
    }

    private JSONObject mergeOverrideIntoPayload(JSONObject payload, ManualSaldoOverride override) {
        if (override == null) {
            return payload;
        }
        payload.put("caja", override.getCaja());
        payload.put("cuentaTransitorias", override.getCuentaTransitorias());
        payload.put("garantiaEfectivo", override.getGarantiaEfectivo());
        payload.put("garantiasConstituidas", override.getGarantiasConstituidas());
        payload.put("garantiasExigidas", override.getGarantiasExigidas());
        payload.put("garantiasReservadas", override.getGarantiasReservadas());
        payload.put("limiteFinanciero", override.getLimiteFinanciero());
        payload.put("garantiasDisponible", override.getGarantiasDisponible());
        payload.put("ordenesActivasCompras", override.getOrdenesActivasCompras());
        payload.put("ordenesActivasVentas", override.getOrdenesActivasVentas());
        payload.put("ordenesCalzadasCompras", override.getOrdenesCalzadasCompras());
        payload.put("ordenesCalzadasVentas", override.getOrdenesCalzadasVentas());
        payload.put("ordenesCestaCompras", override.getOrdenesCestaCompras());
        payload.put("ordenesCestaVentas", override.getOrdenesCestaVentas());
        payload.put("rendimiento", override.getRendimiento());
        payload.put("total", override.getTotal());
        return payload;
    }

    private JSONObject awaitEffectivePayload(String account, ManualSaldoOverride expectedOverride) {
        long deadline = System.currentTimeMillis() + APPLY_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            JSONObject payload = buildEffectivePayload(account);
            if (matchesOverride(payload, expectedOverride)) {
                return payload;
            }
            try {
                Thread.sleep(APPLY_POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return buildEffectivePayload(account);
    }

    private boolean matchesOverride(JSONObject payload, ManualSaldoOverride override) {
        if (override == null) {
            return true;
        }
        return approx(payload.optDouble("caja", 0d), override.getCaja())
                && approx(payload.optDouble("cuentaTransitorias", 0d), override.getCuentaTransitorias())
                && approx(payload.optDouble("garantiaEfectivo", 0d), override.getGarantiaEfectivo())
                && approx(payload.optDouble("garantiasConstituidas", 0d), override.getGarantiasConstituidas())
                && approx(payload.optDouble("garantiasExigidas", 0d), override.getGarantiasExigidas())
                && approx(payload.optDouble("garantiasReservadas", 0d), override.getGarantiasReservadas())
                && approx(payload.optDouble("limiteFinanciero", 0d), override.getLimiteFinanciero())
                && approx(payload.optDouble("garantiasDisponible", 0d), override.getGarantiasDisponible())
                && approx(payload.optDouble("ordenesActivasCompras", 0d), override.getOrdenesActivasCompras())
                && approx(payload.optDouble("ordenesActivasVentas", 0d), override.getOrdenesActivasVentas())
                && approx(payload.optDouble("ordenesCalzadasCompras", 0d), override.getOrdenesCalzadasCompras())
                && approx(payload.optDouble("ordenesCalzadasVentas", 0d), override.getOrdenesCalzadasVentas())
                && approx(payload.optDouble("ordenesCestaCompras", 0d), override.getOrdenesCestaCompras())
                && approx(payload.optDouble("ordenesCestaVentas", 0d), override.getOrdenesCestaVentas())
                && approx(payload.optDouble("rendimiento", 0d), override.getRendimiento())
                && approx(payload.optDouble("total", 0d), override.getTotal());
    }

    private boolean approx(double left, double right) {
        return Math.abs(left - right) < 0.0001d;
    }

    private boolean isValidAccount(String account) {
        return account != null && account.matches("\\d{1,9}[-/]\\d{1,3}");
    }

    private JSONObject toJson(ManualSaldoOverride data, BlotterMessage.Balance balance, BlotterMessage.Patrimonio patrimonio) {
        JSONObject json = new JSONObject();
        json.put("caja", data.getCaja());
        json.put("cuentaTransitorias", data.getCuentaTransitorias());
        json.put("garantiaEfectivo", data.getGarantiaEfectivo());
        json.put("garantiasConstituidas", data.getGarantiasConstituidas());
        json.put("garantiasExigidas", data.getGarantiasExigidas());
        json.put("garantiasReservadas", data.getGarantiasReservadas());
        json.put("limiteFinanciero", data.getLimiteFinanciero());
        json.put("garantiasDisponible", data.getGarantiasDisponible());
        json.put("ordenesActivasCompras", data.getOrdenesActivasCompras());
        json.put("ordenesActivasVentas", data.getOrdenesActivasVentas());
        json.put("ordenesCalzadasCompras", data.getOrdenesCalzadasCompras());
        json.put("ordenesCalzadasVentas", data.getOrdenesCalzadasVentas());
        json.put("ordenesCestaCompras", data.getOrdenesCestaCompras());
        json.put("ordenesCestaVentas", data.getOrdenesCestaVentas());
        json.put("rendimiento", data.getRendimiento());
        json.put("total", data.getTotal());
        json.put("cartera", balance != null ? balance.getCartera() : 0d);
        json.put("cupo", balance != null ? balance.getCupo() : 0d);
        json.put("saldoDisponible", balance != null ? balance.getSaldoDisponible() : 0d);
        json.put("ordenesActivasComprasCalc", balance != null ? balance.getOrdenesActivasCompras() : 0d);
        json.put("ordenesActivasVentasCalc", balance != null ? balance.getOrdenesActivasVentas() : 0d);
        json.put("ordenesCalzadasComprasCalc", balance != null ? balance.getOrdenesCalzadasCompras() : 0d);
        json.put("ordenesCalzadasVentasCalc", balance != null ? balance.getOrdenesCalzadasVentas() : 0d);
        json.put("activos", patrimonio != null ? patrimonio.getActivos().getValues() : 0d);
        json.put("activosPct", patrimonio != null ? patrimonio.getActivos().getPorcentage() : 0d);
        json.put("liquidez", patrimonio != null ? patrimonio.getLiquidez().getValues() : 0d);
        json.put("liquidezPct", patrimonio != null ? patrimonio.getLiquidez().getPorcentage() : 0d);
        json.put("rentaVariable", patrimonio != null ? patrimonio.getRentaVariable().getValues() : 0d);
        json.put("rentaVariablePct", patrimonio != null ? patrimonio.getRentaVariable().getPorcentage() : 0d);
        json.put("rentaFija", patrimonio != null ? patrimonio.getRentaFija().getValues() : 0d);
        json.put("rentaFijaPct", patrimonio != null ? patrimonio.getRentaFija().getPorcentage() : 0d);
        json.put("simultaneasPatrimonio", patrimonio != null ? patrimonio.getSimultaneas().getValues() : 0d);
        json.put("simultaneasPct", patrimonio != null ? patrimonio.getSimultaneas().getPorcentage() : 0d);
        json.put("prestamosPatrimonio", patrimonio != null ? patrimonio.getPrestamos().getValues() : 0d);
        json.put("prestamosPct", patrimonio != null ? patrimonio.getPrestamos().getPorcentage() : 0d);
        json.put("accionesNacionales", patrimonio != null ? patrimonio.getAccionesNacionales().getValues() : 0d);
        json.put("accionesNacionalesPct", patrimonio != null ? patrimonio.getAccionesNacionales().getPorcentage() : 0d);
        json.put("accionesExtranjeras", patrimonio != null ? patrimonio.getAccionesextranjeras().getValues() : 0d);
        json.put("accionesExtranjerasPct", patrimonio != null ? patrimonio.getAccionesextranjeras().getPorcentage() : 0d);
        json.put("fondosMutuos", patrimonio != null ? patrimonio.getFondosMutos().getValues() : 0d);
        json.put("fondosMutuosPct", patrimonio != null ? patrimonio.getFondosMutos().getPorcentage() : 0d);
        json.put("rvNacional", patrimonio != null ? patrimonio.getRvNacional().getValues() : 0d);
        json.put("rvNacionalPct", patrimonio != null ? patrimonio.getRvNacional().getPorcentage() : 0d);
        json.put("rvExtranjeros", patrimonio != null ? patrimonio.getRvExtranjeros().getValues() : 0d);
        json.put("rvExtranjerosPct", patrimonio != null ? patrimonio.getRvExtranjeros().getPorcentage() : 0d);
        json.put("fondoInversionRentaVariable", patrimonio != null ? patrimonio.getFondoInversionRentaVariable().getValues() : 0d);
        json.put("fondoInversionRentaVariablePct", patrimonio != null ? patrimonio.getFondoInversionRentaVariable().getPorcentage() : 0d);
        json.put("activosInmobiliarios", patrimonio != null ? patrimonio.getActivosInmobiliarios().getValues() : 0d);
        json.put("activosInmobiliariosPct", patrimonio != null ? patrimonio.getActivosInmobiliarios().getPorcentage() : 0d);
        json.put("eftsRentaVariable", patrimonio != null ? patrimonio.getEftsRentaVariable().getValues() : 0d);
        json.put("eftsRentaVariablePct", patrimonio != null ? patrimonio.getEftsRentaVariable().getPorcentage() : 0d);
        json.put("inversionesAlternativas", patrimonio != null ? patrimonio.getInversionesAlternativas().getValues() : 0d);
        json.put("inversionesAlternativasPct", patrimonio != null ? patrimonio.getInversionesAlternativas().getPorcentage() : 0d);
        json.put("derivados", patrimonio != null ? patrimonio.getDerivados().getValues() : 0d);
        json.put("derivadosPct", patrimonio != null ? patrimonio.getDerivados().getPorcentage() : 0d);
        return json;
    }

    private ZoneId resolveZoneId() {
        return MainApp.getZoneId() != null ? MainApp.getZoneId() : ZoneId.of("America/Santiago");
    }
}
