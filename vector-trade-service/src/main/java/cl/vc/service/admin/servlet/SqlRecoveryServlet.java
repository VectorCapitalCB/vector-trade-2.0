package cl.vc.service.admin.servlet;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.routing.ActorGroupPerAccount;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class SqlRecoveryServlet extends HttpServlet {

    private static final Pattern ACCOUNT_PATTERN = Pattern.compile("\\d{1,9}/\\d{1,3}");
    private static final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "admin-sql-recovery");
        t.setDaemon(true);
        return t;
    });
    private static final AtomicLong runSequence = new AtomicLong(1);
    private static final Object runLock = new Object();

    private static volatile RecoveryRun activeRun;
    private static volatile RecoveryRun lastRun;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            res.getWriter().write(buildStatus().toString());
        } catch (Exception e) {
            log.error("[Admin/SqlRecovery] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = req.getPathInfo() == null ? "" : req.getPathInfo();
        log.info("[Admin/SqlRecovery] ⇩ POST {} recibido (desde {})", pathInfo, req.getRemoteAddr());
        try {
            if ("/start".equals(pathInfo)) {
                startRun(req, res);
                return;
            }
            if ("/stop".equals(pathInfo)) {
                stopRun(res);
                return;
            }

            res.setStatus(400);
            res.getWriter().write(new JSONObject()
                    .put("error", "Usa /api/sql-recovery/start o /api/sql-recovery/stop")
                    .toString());
        } catch (Exception e) {
            log.error("[Admin/SqlRecovery] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private void startRun(HttpServletRequest req, HttpServletResponse res) throws Exception {
        JSONObject body = readJson(req);
        int delayMs = clamp(body.optInt("delayMs", 400), 0, 10_000);
        int askTimeoutSec = clamp(body.optInt("askTimeoutSec", 45), 5, 300);
        String mode = body.optString("mode", "actors").trim().toLowerCase(Locale.ROOT);

        List<String> accounts;
        if ("accounts".equals(mode)) {
            accounts = normalizeAccounts(body.opt("accounts"));
        } else {
            accounts = new ArrayList<>(new TreeSet<>(snapshotActors().keySet()));
        }

        log.info("[Admin/SqlRecovery] /start: mode={} delayMs={} timeoutSec={} cuentasPedidas={}",
                mode, delayMs, askTimeoutSec, accounts.size());

        if (accounts.isEmpty()) {
            log.warn("[Admin/SqlRecovery] /start RECHAZADO: no hay cuentas para procesar (mode={})", mode);
            res.setStatus(400);
            res.getWriter().write(new JSONObject()
                    .put("error", "No hay cuentas para procesar")
                    .toString());
            return;
        }

        synchronized (runLock) {
            if (activeRun != null && activeRun.running) {
                log.warn("[Admin/SqlRecovery] /start RECHAZADO: ya hay una recarga corriendo (run={})",
                        activeRun.runId);
                res.setStatus(409);
                res.getWriter().write(new JSONObject()
                        .put("error", "Ya hay una recarga SQL corriendo")
                        .put("status", buildStatus())
                        .toString());
                return;
            }

            RecoveryRun run = new RecoveryRun();
            run.runId = runSequence.getAndIncrement();
            run.mode = mode;
            run.delayMs = delayMs;
            run.askTimeoutSec = askTimeoutSec;
            run.startedAt = Instant.now().toEpochMilli();
            run.requestedAccounts = new ArrayList<>(accounts);
            run.total = accounts.size();
            run.running = true;

            activeRun = run;
            lastRun = run;

            executor.submit(() -> executeRun(run));
            log.info("[Admin/SqlRecovery] ✔ /start ACEPTADO: run={} encolado ({} cuentas, mode={})",
                    run.runId, run.total, mode);
        }

        res.getWriter().write(new JSONObject()
                .put("ok", true)
                .put("message", "Recarga SQL iniciada")
                .put("status", buildStatus())
                .toString());
    }

    private void stopRun(HttpServletResponse res) throws IOException {
        RecoveryRun run = activeRun;
        if (run == null || !run.running) {
            log.info("[Admin/SqlRecovery] /stop: no hay una recarga activa");
            res.getWriter().write(new JSONObject()
                    .put("ok", true)
                    .put("message", "No hay una recarga SQL activa")
                    .put("status", buildStatus())
                    .toString());
            return;
        }

        log.info("[Admin/SqlRecovery] /stop: señal de stop enviada a run={}", run.runId);
        run.stopRequested = true;
        run.lastMessage = "stop solicitado";
        res.getWriter().write(new JSONObject()
                .put("ok", true)
                .put("message", "Señal de stop enviada")
                .put("status", buildStatus())
                .toString());
    }

    private void executeRun(RecoveryRun run) {
        String accountsStr = run.requestedAccounts.size() <= 25
                ? String.join(", ", run.requestedAccounts)
                : String.join(", ", run.requestedAccounts.subList(0, 25))
                        + " ... (+" + (run.requestedAccounts.size() - 25) + " más)";
        log.info("[Admin/SqlRecovery] ▶ run={} mode={} total={} delayMs={} timeoutSec={} cuentas=[{}]",
                run.runId, run.mode, run.total, run.delayMs, run.askTimeoutSec, accountsStr);

        try {
            for (String account : run.requestedAccounts) {
                if (run.stopRequested) {
                    run.lastMessage = "detenido por operador";
                    break;
                }

                run.currentAccount = account;
                AccountResult result = recalculateAccount(account, run.askTimeoutSec);
                appendResult(run, result);

                long elapsedSec = (Instant.now().toEpochMilli() - run.startedAt) / 1000;
                log.info("[Admin/SqlRecovery] progreso {}/{} (ok={} failed={} skip={}) transcurrido {}s",
                        run.completed, run.total, run.succeeded, run.failed, run.skipped, elapsedSec);

                if (!run.stopRequested && run.delayMs > 0 && run.completed < run.total) {
                    try {
                        Thread.sleep(run.delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        run.stopRequested = true;
                        run.lastMessage = "interrumpido";
                        break;
                    }
                }
            }

            run.ok = run.failed == 0;
            if (run.stopRequested) {
                run.lastMessage = run.lastMessage == null ? "detenido" : run.lastMessage;
            } else {
                run.lastMessage = String.format(Locale.US,
                        "completado: ok=%d failed=%d skipped=%d", run.succeeded, run.failed, run.skipped);
            }
        } catch (Exception e) {
            run.ok = false;
            run.lastMessage = e.getMessage();
            appendResult(run, AccountResult.error(run.currentAccount, 0L, e.getMessage()));
            log.error("[Admin/SqlRecovery] ✗ run={} error={}", run.runId, e.getMessage(), e);
        } finally {
            run.running = false;
            run.currentAccount = null;
            run.endedAt = Instant.now().toEpochMilli();
            synchronized (runLock) {
                if (activeRun == run) {
                    activeRun = null;
                }
                lastRun = run;
            }
            log.info("[Admin/SqlRecovery] ■ run={} finished ok={} completed={}/{} failed={} skipped={}",
                    run.runId, run.ok, run.completed, run.total, run.failed, run.skipped);
        }
    }

    private AccountResult recalculateAccount(String account, int askTimeoutSec) {
        Map<String, ActorRef> actors = snapshotActors();
        ActorRef actor = actors.get(account);
        if (actor == null) {
            log.info("[Admin/SqlRecovery] ⏭ cuenta {} omitida: sin actor vivo", account);
            return AccountResult.skipped(account, "sin actor vivo");
        }

        log.info("[Admin/SqlRecovery] ▶ recalculando cuenta {}", account);
        long startedAt = System.nanoTime();
        try {
            Future<Object> future = Patterns.ask(actor,
                    new ActorGroupPerAccount.CalculatePatriminio(),
                    askTimeoutSec * 1000L);

            Object response = Await.result(future, Duration.create(askTimeoutSec, TimeUnit.SECONDS));
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;

            if (response instanceof ActorGroupPerAccount.CalculatePatriminioResult) {
                ActorGroupPerAccount.CalculatePatriminioResult result =
                        (ActorGroupPerAccount.CalculatePatriminioResult) response;
                String status = result.isOk() ? "ok" : "error";
                if (result.isOk()) {
                    log.info("[Admin/SqlRecovery] ✅ cuenta {} {} en {} ms ({})",
                            account, status, durationMs, result.getMessage());
                } else {
                    log.warn("[Admin/SqlRecovery] ✗ cuenta {} {} en {} ms ({})",
                            account, status, durationMs, result.getMessage());
                }
                return new AccountResult(account, status, durationMs, result.getMessage());
            }

            log.info("[Admin/SqlRecovery] ✅ cuenta {} ok en {} ms (respuesta recibida)", account, durationMs);
            return new AccountResult(account, "ok", durationMs, "respuesta recibida");
        } catch (Exception e) {
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;
            log.warn("[Admin/SqlRecovery] ✗ cuenta {} error/timeout en {} ms: {}",
                    account, durationMs, e.getMessage());
            return AccountResult.error(account, durationMs, e.getMessage());
        }
    }

    private void appendResult(RecoveryRun run, AccountResult result) {
        synchronized (run.results) {
            run.results.add(result);
            if (run.results.size() > 250) {
                run.results.remove(0);
            }
        }

        run.completed++;
        switch (result.status) {
            case "ok":
                run.succeeded++;
                break;
            case "skipped":
                run.skipped++;
                break;
            default:
                run.failed++;
                break;
        }
    }

    private JSONObject buildStatus() {
        JSONObject root = new JSONObject();
        root.put("activeActors", snapshotActors().size());
        root.put("requiresSql", MainApp.getProperties() != null
                && Boolean.parseBoolean(MainApp.getProperties().getProperty("requiere.sql", "false")));
        root.put("running", activeRun != null && activeRun.running);
        root.put("activeRun", toJson(activeRun));
        root.put("lastRun", activeRun == lastRun ? JSONObject.NULL : toJson(lastRun));
        return root;
    }

    private Object toJson(RecoveryRun run) {
        if (run == null) {
            return JSONObject.NULL;
        }

        JSONObject json = new JSONObject();
        json.put("runId", run.runId);
        json.put("mode", run.mode);
        json.put("running", run.running);
        json.put("ok", run.ok);
        json.put("stopRequested", run.stopRequested);
        json.put("startedAt", run.startedAt);
        json.put("endedAt", run.endedAt);
        json.put("delayMs", run.delayMs);
        json.put("askTimeoutSec", run.askTimeoutSec);
        json.put("total", run.total);
        json.put("completed", run.completed);
        json.put("succeeded", run.succeeded);
        json.put("failed", run.failed);
        json.put("skipped", run.skipped);
        json.put("currentAccount", run.currentAccount != null ? run.currentAccount : JSONObject.NULL);
        json.put("lastMessage", run.lastMessage != null ? run.lastMessage : JSONObject.NULL);
        json.put("requestedAccounts", new JSONArray(run.requestedAccounts));

        JSONArray results = new JSONArray();
        synchronized (run.results) {
            for (AccountResult result : run.results) {
                results.put(new JSONObject()
                        .put("account", result.account)
                        .put("status", result.status)
                        .put("durationMs", result.durationMs)
                        .put("message", result.message != null ? result.message : JSONObject.NULL));
            }
        }
        json.put("results", results);
        return json;
    }

    private JSONObject readJson(HttpServletRequest req) throws IOException {
        String body = req.getReader().lines().collect(Collectors.joining());
        return body == null || body.isBlank() ? new JSONObject() : new JSONObject(body);
    }

    private List<String> normalizeAccounts(Object raw) {
        if (raw == null) {
            return Collections.emptyList();
        }

        Collection<String> tokens = new ArrayList<>();
        if (raw instanceof JSONArray) {
            JSONArray arr = (JSONArray) raw;
            for (int i = 0; i < arr.length(); i++) {
                tokens.add(String.valueOf(arr.get(i)));
            }
        } else {
            String text = String.valueOf(raw);
            for (String token : text.split("[,;\\n\\r\\t ]+")) {
                tokens.add(token);
            }
        }

        LinkedHashSet<String> accounts = new LinkedHashSet<>();
        for (String token : tokens) {
            String normalized = token == null ? "" : token.trim();
            if (ACCOUNT_PATTERN.matcher(normalized).matches()) {
                accounts.add(normalized);
            }
        }
        return new ArrayList<>(accounts);
    }

    private Map<String, ActorRef> snapshotActors() {
        return MainApp.getAccountGroupUser() == null
                ? Collections.emptyMap()
                : MainApp.getAccountGroupUser();
    }

    private int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private static final class RecoveryRun {
        private long runId;
        private String mode;
        private boolean running;
        private boolean ok;
        private boolean stopRequested;
        private long startedAt;
        private long endedAt;
        private int delayMs;
        private int askTimeoutSec;
        private int total;
        private int completed;
        private int succeeded;
        private int failed;
        private int skipped;
        private String currentAccount;
        private String lastMessage;
        private List<String> requestedAccounts = new ArrayList<>();
        private final List<AccountResult> results = Collections.synchronizedList(new ArrayList<>());
    }

    private static final class AccountResult {
        private final String account;
        private final String status;
        private final long durationMs;
        private final String message;

        private AccountResult(String account, String status, long durationMs, String message) {
            this.account = account;
            this.status = status;
            this.durationMs = durationMs;
            this.message = message;
        }

        private static AccountResult skipped(String account, String message) {
            return new AccountResult(account, "skipped", 0L, message);
        }

        private static AccountResult error(String account, long durationMs, String message) {
            return new AccountResult(account, "error", durationMs, message);
        }
    }
}
