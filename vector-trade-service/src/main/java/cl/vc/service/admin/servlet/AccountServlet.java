package cl.vc.service.admin.servlet;

import akka.actor.ActorRef;
import cl.vc.service.MainApp;
import cl.vc.service.admin.AccountLoadTracker;
import cl.vc.service.akka.actors.routing.ActorGroupPerAccount;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.GroupResource;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.UserRepresentation;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class AccountServlet extends HttpServlet {

    private static final ExecutorService priorityExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "admin-priority-calc");
        t.setPriority(Thread.MAX_PRIORITY);
        return t;
    });

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");

        try {
            String pathInfo = Optional.ofNullable(req.getPathInfo()).orElse("");
            if ("/progress".equals(pathInfo)) {
                writeProgress(res);
                return;
            }
            if ("/keycloak-config".equals(pathInfo)) {
                writeKeycloakConfig(req, res);
                return;
            }

            Map<String, ActorRef> accountActors = snapshotAccountActors();
            JSONArray arr = new JSONArray();
            accountActors.forEach((account, ref) -> {
                JSONObject obj = new JSONObject();
                obj.put("account", account);
                obj.put("actorPath", ref.path().toString());
                obj.put("hasOrders", false);
                arr.put(obj);
            });
            res.getWriter().write(arr.toString());
        } catch (Exception e) {
            log.error("[Admin/Accounts] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private void writeProgress(HttpServletResponse res) throws IOException {
        AccountLoadTracker tracker = MainApp.getAccountLoadTracker();
        Map<String, ActorRef> accountActors = snapshotAccountActors();

        JSONObject root = new JSONObject();
        root.put("processedUsersCount", MainApp.getProcessedUsersCount());
        root.put("priorityUsersConfiguredCount", MainApp.getPriorityUsersConfiguredCount());
        root.put("activeActors", accountActors.size());
        root.put("cachedMargins", MainApp.getAccountMarginCache().size());
        root.put("cachedLeverages", MainApp.getAccountLeverageCache().size());
        root.put("totalCyclesStarted", tracker.getTotalCyclesStarted());
        root.put("totalCyclesCompleted", tracker.getTotalCyclesCompleted());
        root.put("totalCyclesFailed", tracker.getTotalCyclesFailed());

        AccountLoadTracker.CycleSnapshot current = tracker.getActiveCycleSnapshot();
        root.put("currentCycle", current != null ? toJson(current) : JSONObject.NULL);

        JSONArray cycles = new JSONArray();
        tracker.getRecentCycles().forEach(cycle -> cycles.put(toJson(cycle)));
        root.put("recentCycles", cycles);

        JSONArray accounts = new JSONArray();
        Set<String> allAccounts = new TreeSet<>();
        allAccounts.addAll(accountActors.keySet());
        allAccounts.addAll(MainApp.getAccountMarginCache().keySet());
        allAccounts.addAll(MainApp.getAccountLeverageCache().keySet());
        tracker.getAllAccounts().forEach(account -> allAccounts.add(account.getAccount()));

        long hiddenVoultechAccountsCount = allAccounts.stream()
                .filter(account -> isVoultechAccount(account, tracker.getAccountStats(account)))
                .count();

        allAccounts.forEach(account -> {
            AccountLoadTracker.AccountTouchStats stats = tracker.getAccountStats(account);
            accounts.put(toJson(account, stats, accountActors.containsKey(account), isVoultechAccount(account, stats)));
        });

        root.put("accounts", accounts);
        root.put("accountsCount", allAccounts.size());
        root.put("visibleAccountsCount", allAccounts.size() - hiddenVoultechAccountsCount);
        root.put("hiddenVoultechAccountsCount", hiddenVoultechAccountsCount);

        res.getWriter().write(root.toString());
    }

    private JSONObject toJson(AccountLoadTracker.CycleSnapshot cycle) {
        return new JSONObject()
                .put("cycleId", cycle.getCycleId())
                .put("trigger", cycle.getTrigger())
                .put("running", cycle.isRunning())
                .put("success", cycle.isSuccess())
                .put("startedAt", cycle.getStartedAt())
                .put("endedAt", cycle.getEndedAt())
                .put("durationMs", cycle.getDurationMs())
                .put("totalUsers", cycle.getTotalUsers())
                .put("processedUsers", cycle.getProcessedUsers())
                .put("priorityUsersConfigured", cycle.getPriorityUsersConfigured())
                .put("prioritizedUsersProcessed", cycle.getPrioritizedUsersProcessed())
                .put("nonPrioritizedUsersProcessed", cycle.getNonPrioritizedUsersProcessed())
                .put("declaredAccounts", cycle.getDeclaredAccounts())
                .put("touchedAccounts", cycle.getTouchedAccounts())
                .put("actorCreations", cycle.getActorCreations())
                .put("marginUpdates", cycle.getMarginUpdates())
                .put("leverageUpdates", cycle.getLeverageUpdates())
                .put("initializedAccounts", cycle.getInitializedAccounts())
                .put("failedAccountInitializations", cycle.getFailedAccountInitializations())
                .put("userErrors", cycle.getUserErrors())
                .put("etaMs", cycle.getEtaMs())
                .put("scanCompleted", cycle.isScanCompleted())
                .put("pendingBackgroundRefreshes", cycle.getPendingBackgroundRefreshes())
                .put("completedBackgroundRefreshes", cycle.getCompletedBackgroundRefreshes())
                .put("phase", cycle.getPhase())
                .put("currentUsername", cycle.getCurrentUsername())
                .put("currentGroup", cycle.getCurrentGroup())
                .put("currentUserPriority", cycle.isCurrentUserPriority())
                .put("lastProcessedUsername", cycle.getLastProcessedUsername())
                .put("error", cycle.getError());
    }

    private JSONObject toJson(String account, AccountLoadTracker.AccountTouchStats stats, boolean hasActor, boolean isVoultech) {
        Set<String> owners = stats != null ? stats.getOwners() : Collections.emptySet();
        Set<String> accountOwners = stats != null ? stats.getAccountOwners() : Collections.emptySet();
        Set<String> marginOwners = stats != null ? stats.getMarginOwners() : Collections.emptySet();
        Set<String> leverageOwners = stats != null ? stats.getLeverageOwners() : Collections.emptySet();
        String configurationOwner = stats != null ? stats.getConfigurationOwner() : null;

        JSONObject obj = new JSONObject();
        obj.put("account", account);
        obj.put("touchCount", stats != null ? stats.getTouchCount() : 0L);
        obj.put("initializationCount", stats != null ? stats.getInitializationCount() : 0L);
        obj.put("lastUsername", stats != null ? stats.getLastUsername() : JSONObject.NULL);
        obj.put("lastTouchedAt", stats != null ? stats.getLastTouchedAt() : 0L);
        obj.put("lastInitializationAt", stats != null ? stats.getLastInitializationAt() : 0L);
        obj.put("lastInitializationDurationMs", stats != null ? stats.getLastInitializationDurationMs() : 0L);
        obj.put("lastInitializationSuccess", stats != null && stats.isLastInitializationSuccess());
        obj.put("hasActor", hasActor);

        // Validación de custodia (datos que vienen de SQL): si la cuenta no tiene posiciones ni saldo,
        // está vacía/null y el cliente no puede rutear -> hay que re-buscar en SQL (botón Recalcular).
        double[] custody = MainApp.getAccountCustodyStatus().get(account);
        if (custody != null) {
            int positions = (int) custody[0];
            double saldo = custody[1];
            obj.put("custodyKnown", true);
            obj.put("positionsCount", positions);
            obj.put("saldoDisponible", saldo);
            obj.put("custodyEmpty", positions <= 0 && saldo <= 0);
            obj.put("custodyUpdatedAt", (long) custody[2]);
        } else {
            obj.put("custodyKnown", false);
            obj.put("positionsCount", JSONObject.NULL);
            obj.put("saldoDisponible", JSONObject.NULL);
            obj.put("custodyEmpty", JSONObject.NULL);
            obj.put("custodyUpdatedAt", 0L);
        }

        obj.put("hasMarginCache", MainApp.getAccountMarginCache().containsKey(account));
        obj.put("hasLeverageCache", MainApp.getAccountLeverageCache().containsKey(account));
        obj.put("hasOrders", false);
        obj.put("isVoultech", isVoultech);
        obj.put("ownersCount", owners.size());
        obj.put("owners", new JSONArray(owners));
        obj.put("accountOwnersCount", accountOwners.size());
        obj.put("accountOwners", new JSONArray(accountOwners));
        obj.put("marginOwnersCount", marginOwners.size());
        obj.put("marginOwners", new JSONArray(marginOwners));
        obj.put("leverageOwnersCount", leverageOwners.size());
        obj.put("leverageOwners", new JSONArray(leverageOwners));
        obj.put("configurationOwner", configurationOwner != null ? configurationOwner : JSONObject.NULL);
        return obj;
    }

    private boolean isVoultechAccount(String account, AccountLoadTracker.AccountTouchStats stats) {
        if (MainApp.getVoultechAccountsSet().contains(account)) {
            return true;
        }
        if (stats == null) {
            return false;
        }
        return stats.getOwners().stream().anyMatch(owner -> "voultech".equalsIgnoreCase(owner))
                || "voultech".equalsIgnoreCase(stats.getLastUsername());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        String pathInfo = Optional.ofNullable(req.getPathInfo()).orElse("");

        try {
            if ("/keycloak-config/apply".equals(pathInfo)) {
                applyKeycloakConfig(req, res);
                return;
            }
            if ("/keycloak-config/activate".equals(pathInfo)) {
                activateAccountFromKeycloak(req, res);
                return;
            }

            if (pathInfo == null || pathInfo.length() < 2) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"Usar /api/accounts/{rut}/recalculate\"}");
                return;
            }

            String action = pathInfo.substring(pathInfo.lastIndexOf('/') + 1);
            if (!"recalculate".equals(action) && !"validate-keycloak".equals(action)) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"Acción desconocida: " + action + "\"}");
                return;
            }

            String input = pathInfo.substring(1, pathInfo.lastIndexOf("/" + action)).trim();
            if (input.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write("{\"error\":\"RUT o cuenta vacía\"}");
                return;
            }

            if ("validate-keycloak".equals(action)) {
                ValidationResult validation = validateAccountAgainstKeycloak(input);
                MainApp.getAccountLoadTracker().replaceAccountDeclarations(
                        input, validation.accountOwners, validation.marginOwners, validation.leverageOwners);

                String responseMsg = String.format(
                        "Revalidación Keycloak OK para %s → account=%d, margin=%d, palanca=%d",
                        input, validation.accountOwners.size(), validation.marginOwners.size(), validation.leverageOwners.size());
                log.info("[Admin/Accounts] 🔎 {}", responseMsg);
                res.getWriter().write(new JSONObject()
                        .put("ok", true)
                        .put("message", responseMsg)
                        .put("account", input)
                        .put("accountOwners", new JSONArray(validation.accountOwners))
                        .put("marginOwners", new JSONArray(validation.marginOwners))
                        .put("leverageOwners", new JSONArray(validation.leverageOwners))
                        .toString());
                return;
            }

            Map<String, ActorRef> map = MainApp.getAccountGroupUser();
            Map<String, ActorRef> snapshot = snapshotAccountActors();
            List<String> matches = new ArrayList<>();

            if (snapshot.containsKey(input)) {
                matches.add(input);
            } else {
                String prefix = input + "/";
                String prefixDash = input + "-";
                Stream<String> stream = snapshot.keySet().stream()
                        .filter(k -> k.startsWith(prefix) || k.startsWith(prefixDash));
                stream.forEach(matches::add);
            }

            String responseMsg;
            if (matches.isEmpty()) {
                log.info("[Admin/Accounts] Cuenta '{}' no existe en actores vivos — creando actor nuevo", input);
                ActorRef newActor = MainApp.getSystem()
                        .actorOf(ActorGroupPerAccount.props(input, 0.0, 1.0).withDispatcher("ActorperAccount"));
                map.put(input, newActor);

                priorityExecutor.submit(() -> {
                    log.info("[Admin/Accounts] 🆕 Inicializando actor nuevo para cuenta {}", input);
                    newActor.tell(ActorGroupPerAccount.Initialize.INSTANCE, ActorRef.noSender());
                });

                matches.add(input);
                responseMsg = "Actor creado e inicialización lanzada para: " + input;
            } else {
                for (String acct : matches) {
                    ActorRef ref = map.get(acct);
                    if (ref != null) {
                        priorityExecutor.submit(() -> {
                            log.info("[Admin/Accounts] ⚡ Recalculando cuadratura PRIORITARIA para cuenta {}", acct);
                            ref.tell(new ActorGroupPerAccount.CalculatePatriminio(), ActorRef.noSender());
                        });
                    }
                }
                responseMsg = matches.size() == 1
                        ? "Recálculo enviado para: " + matches.get(0)
                        : "Recálculo enviado para " + matches.size() + " cuentas: " + matches;
            }

            res.getWriter().write(new JSONObject()
                    .put("ok", true)
                    .put("message", responseMsg)
                    .put("matched", matches.size())
                    .put("accounts", new JSONArray(matches))
                    .toString());
        } catch (Exception e) {
            log.error("[Admin/Accounts] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private void writeKeycloakConfig(HttpServletRequest req, HttpServletResponse res) throws Exception {
        String account = Optional.ofNullable(req.getParameter("account")).orElse("").trim();
        if (account.isEmpty()) {
            res.setStatus(400);
            res.getWriter().write(new JSONObject().put("error", "account es requerido").toString());
            return;
        }
        if (!isValidAccount(account)) {
            res.setStatus(400);
            res.getWriter().write(new JSONObject().put("error", "Cuenta inválida. Formato esperado: 12345678/0").toString());
            return;
        }

        AccountConfigResult config = loadAccountConfig(account);
        MainApp.getAccountLoadTracker().replaceAccountDeclarations(
                account, config.accountOwners, config.marginOwners, config.leverageOwners);

        res.getWriter().write(toJson(config).toString());
    }

    private void applyKeycloakConfig(HttpServletRequest req, HttpServletResponse res) throws Exception {
        String body = req.getReader().lines().collect(Collectors.joining());
        JSONObject json = body == null || body.isBlank() ? new JSONObject() : new JSONObject(body);

        String account = json.optString("account", "").trim();
        if (account.isEmpty()) {
            res.setStatus(400);
            res.getWriter().write(new JSONObject().put("error", "account es requerido").toString());
            return;
        }
        if (!isValidAccount(account)) {
            res.setStatus(400);
            res.getWriter().write(new JSONObject().put("error", "Cuenta inválida. Formato esperado: 12345678/0").toString());
            return;
        }

        boolean hasMargin = json.has("margin") && !json.isNull("margin") && !String.valueOf(json.opt("margin")).isBlank();
        boolean hasLeverage = json.has("leverage") && !json.isNull("leverage") && !String.valueOf(json.opt("leverage")).isBlank();
        if (!hasMargin && !hasLeverage) {
            res.setStatus(400);
            res.getWriter().write(new JSONObject().put("error", "Debes informar margin o leverage").toString());
            return;
        }

        Double margin = hasMargin ? json.getDouble("margin") : null;
        Double leverage = hasLeverage ? json.getDouble("leverage") : null;
        String selectedUsername = json.optString("username", "").trim();

        AccountConfigResult config = loadAccountConfig(account);
        UserAccountConfig selectedOwner = resolveRequestedOwner(config, selectedUsername);
        if (selectedOwner == null) {
            res.setStatus(409);
            res.getWriter().write(new JSONObject()
                    .put("error", config.issue != null ? config.issue : "No se pudo determinar un owner en Keycloak para editar esta cuenta")
                    .put("details", toJson(config))
                    .toString());
            return;
        }

        updateKeycloakUserAccountConfig(selectedOwner.userRepresentation, account, margin, leverage);
        if (margin != null) {
            MainApp.getAccountMarginCache().put(account, margin);
        }
        if (leverage != null) {
            MainApp.getAccountLeverageCache().put(account, leverage);
        }
        MainApp.ensureUserProcessed(selectedOwner.userRepresentation.getUsername());
        propagateAccountConfigToCore(account, margin, leverage, selectedOwner.username);

        AccountConfigResult refreshed = loadAccountConfig(account);
        MainApp.getAccountLoadTracker().replaceAccountDeclarations(
                account, refreshed.accountOwners, refreshed.marginOwners, refreshed.leverageOwners);

        JSONObject payload = toJson(refreshed);
        payload.put("ok", true);
        payload.put("message", String.format(
                Locale.US,
                "Keycloak actualizado para %s (%s)%s%s",
                account,
                selectedOwner.username,
                margin != null ? " → margin=" + formatNullableNumber(margin) : "",
                leverage != null ? " → palanca=" + formatNullableNumber(leverage) : ""
        ));
        res.getWriter().write(payload.toString());
    }

    private void activateAccountFromKeycloak(HttpServletRequest req, HttpServletResponse res) throws Exception {
        String body = req.getReader().lines().collect(Collectors.joining());
        JSONObject json = body == null || body.isBlank() ? new JSONObject() : new JSONObject(body);

        String account = json.optString("account", "").trim();
        if (account.isEmpty()) {
            res.setStatus(400);
            res.getWriter().write(new JSONObject().put("error", "account es requerido").toString());
            return;
        }
        if (!isValidAccount(account)) {
            res.setStatus(400);
            res.getWriter().write(new JSONObject().put("error", "Cuenta inválida. Formato esperado: 12345678/0").toString());
            return;
        }

        String selectedUsername = json.optString("username", "").trim();

        AccountConfigResult config = loadAccountConfig(account);
        UserAccountConfig selectedOwner = resolveRequestedOwner(config, selectedUsername);
        if (selectedOwner == null) {
            res.setStatus(409);
            res.getWriter().write(new JSONObject()
                    .put("error", config.issue != null ? config.issue : "No se pudo resolver la cuenta en Keycloak para darla de alta")
                    .put("details", toJson(config))
                    .toString());
            return;
        }

        Double margin = selectedOwner.margin;
        Double leverage = selectedOwner.leverage;
        if (margin != null) {
            MainApp.getAccountMarginCache().put(account, margin);
        }
        if (leverage != null) {
            MainApp.getAccountLeverageCache().put(account, leverage);
        }
        MainApp.ensureUserProcessed(selectedOwner.userRepresentation.getUsername());
        propagateAccountConfigToCore(account, margin, leverage, selectedOwner.username);

        AccountConfigResult refreshed = loadAccountConfig(account);
        MainApp.getAccountLoadTracker().replaceAccountDeclarations(
                account, refreshed.accountOwners, refreshed.marginOwners, refreshed.leverageOwners);

        JSONObject payload = toJson(refreshed);
        payload.put("ok", true);
        payload.put("message", String.format(
                Locale.US,
                "Cuenta %s dada de alta desde Keycloak (%s) con margin=%s y palanca=%s",
                account,
                selectedOwner.username,
                formatNullableNumber(margin),
                formatNullableNumber(leverage != null ? leverage : 3d)
        ));
        res.getWriter().write(payload.toString());
    }

    private Map<String, ActorRef> snapshotAccountActors() {
        return new LinkedHashMap<>(MainApp.getAccountGroupUser());
    }

    private JSONObject toJson(AccountConfigResult config) {
        JSONObject root = new JSONObject();
        root.put("ok", true);
        root.put("account", config.account);
        root.put("accountOwners", new JSONArray(config.accountOwners));
        root.put("marginOwners", new JSONArray(config.marginOwners));
        root.put("leverageOwners", new JSONArray(config.leverageOwners));
        root.put("configurationOwner", config.configurationOwner != null ? config.configurationOwner : JSONObject.NULL);
        root.put("editableOwner", config.editableOwner != null ? config.editableOwner : JSONObject.NULL);
        root.put("issue", config.issue != null ? config.issue : JSONObject.NULL);
        root.put("keycloakMargin", config.keycloakMargin != null ? config.keycloakMargin : JSONObject.NULL);
        root.put("keycloakLeverage", config.keycloakLeverage != null ? config.keycloakLeverage : JSONObject.NULL);
        root.put("cachedMargin", MainApp.getAccountMarginCache().getOrDefault(config.account, 0d));
        root.put("cachedLeverage", MainApp.getAccountLeverageCache().getOrDefault(config.account, 3d));
        ActorRef actorRef = MainApp.getAccountGroupUser().get(config.account);
        root.put("hasActor", actorRef != null);
        root.put("actorPath", actorRef != null ? actorRef.path().toString() : JSONObject.NULL);
        root.put("canEdit", !config.rows.isEmpty());

        JSONArray matches = new JSONArray();
        for (UserAccountConfig row : config.rows) {
            matches.put(new JSONObject()
                    .put("username", row.username)
                    .put("userId", row.userId != null ? row.userId : JSONObject.NULL)
                    .put("accountDeclared", row.accountDeclared)
                    .put("margin", row.margin != null ? row.margin : JSONObject.NULL)
                    .put("leverage", row.leverage != null ? row.leverage : JSONObject.NULL)
                    .put("marginAttributeKey", row.marginAttributeKey != null ? row.marginAttributeKey : JSONObject.NULL)
                    .put("leverageAttributeKey", row.leverageAttributeKey != null ? row.leverageAttributeKey : JSONObject.NULL));
        }
        root.put("matches", matches);
        return root;
    }

    private AccountConfigResult loadAccountConfig(String account) throws Exception {
        AccountConfigResult result = new AccountConfigResult(account);
        forEachKeycloakUser(user -> collectAccountConfig(account, user, result));
        resolveEditableOwner(result);
        return result;
    }

    private void collectAccountConfig(String account, UserRepresentation user, AccountConfigResult result) {
        if (user == null) {
            return;
        }
        String username = Optional.ofNullable(user.getUsername()).orElse("").trim();
        if (username.isEmpty()) {
            return;
        }

        boolean accountDeclared = extractDeclaredAccounts(user).contains(account);
        MetricValue marginMetric = readMetric(user, account, "marginaccount");
        MetricValue leverageMetric = readMetric(user, account, "palanca");

        if (!accountDeclared && marginMetric == null && leverageMetric == null) {
            return;
        }

        String rowKey = buildAccountConfigRowKey(account, user);
        if (!result.seenRowKeys.add(rowKey)) {
            return;
        }

        UserAccountConfig row = new UserAccountConfig();
        row.username = username;
        row.userId = user.getId();
        row.accountDeclared = accountDeclared;
        row.margin = marginMetric != null ? marginMetric.value : null;
        row.leverage = leverageMetric != null ? leverageMetric.value : null;
        row.marginAttributeKey = marginMetric != null ? marginMetric.attributeKey : null;
        row.leverageAttributeKey = leverageMetric != null ? leverageMetric.attributeKey : null;
        row.userRepresentation = user;
        result.rows.add(row);

        if (accountDeclared) {
            result.accountOwners.add(username);
        }
        if (marginMetric != null) {
            result.marginOwners.add(username);
        }
        if (leverageMetric != null) {
            result.leverageOwners.add(username);
        }
    }

    private String buildAccountConfigRowKey(String account, UserRepresentation user) {
        String userId = Optional.ofNullable(user.getId()).orElse("").trim();
        String username = Optional.ofNullable(user.getUsername()).orElse("").trim().toLowerCase(Locale.ROOT);
        String principal = !userId.isEmpty() ? userId : username;
        return account + "|" + principal;
    }

    private void resolveEditableOwner(AccountConfigResult result) {
        Set<String> configOwners = new TreeSet<>();
        configOwners.addAll(result.marginOwners);
        configOwners.addAll(result.leverageOwners);
        result.configurationOwner = configOwners.size() == 1 ? configOwners.iterator().next() : null;

        String editableOwner = null;
        if (result.configurationOwner != null) {
            editableOwner = result.configurationOwner;
        } else {
            Set<String> allOwners = new TreeSet<>();
            allOwners.addAll(result.accountOwners);
            allOwners.addAll(result.marginOwners);
            allOwners.addAll(result.leverageOwners);
            if (allOwners.size() == 1) {
                editableOwner = allOwners.iterator().next();
            }
        }

        if (editableOwner == null) {
            if (result.rows.isEmpty()) {
                result.issue = "La cuenta no existe en atributos de Keycloak";
            } else if (result.leverageOwners.size() > 1) {
                result.issue = "La cuenta tiene conflicto de owners en palanca. Selecciona el usuario correcto para editar.";
            } else if (result.marginOwners.size() > 1) {
                result.issue = "La cuenta tiene varios owners en marginaccount. Selecciona el usuario correcto si vas a editar margin.";
            } else {
                result.issue = "La cuenta está en varios usuarios. Selecciona el usuario correcto para editarla.";
            }
        } else {
            result.editableOwner = editableOwner;
        }
        for (UserAccountConfig row : result.rows) {
            if (editableOwner != null && !editableOwner.equalsIgnoreCase(row.username)) {
                continue;
            }
            result.ownerUser = row.userRepresentation;
            result.keycloakMargin = row.margin;
            result.keycloakLeverage = row.leverage;
            return;
        }
    }

    private UserAccountConfig resolveRequestedOwner(AccountConfigResult config, String requestedUsername) {
        if (config == null || config.rows.isEmpty()) {
            return null;
        }
        if (requestedUsername != null && !requestedUsername.isBlank()) {
            for (UserAccountConfig row : config.rows) {
                if (requestedUsername.equalsIgnoreCase(row.username)) {
                    return row;
                }
            }
            return null;
        }
        if (config.editableOwner != null) {
            for (UserAccountConfig row : config.rows) {
                if (config.editableOwner.equalsIgnoreCase(row.username)) {
                    return row;
                }
            }
        }
        return config.rows.get(0);
    }

    private MetricValue readMetric(UserRepresentation user, String account, String keyFragment) {
        if (user == null || user.getAttributes() == null || user.getAttributes().isEmpty()) {
            return null;
        }
        for (Map.Entry<String, List<String>> entry : user.getAttributes().entrySet()) {
            String key = Optional.ofNullable(entry.getKey()).orElse("").toLowerCase(Locale.ROOT);
            if (!key.contains(keyFragment)) {
                continue;
            }
            for (String raw : entry.getValue()) {
                if (raw == null || !raw.contains("|")) {
                    continue;
                }
                String[] value = raw.split("\\|", 2);
                String parsedAccount = value[0].trim().replace(" ", "");
                if (!account.equals(parsedAccount)) {
                    continue;
                }
                try {
                    MetricValue metric = new MetricValue();
                    metric.attributeKey = entry.getKey();
                    metric.value = Double.valueOf(value[1].trim().replace(" ", ""));
                    return metric;
                } catch (Exception e) {
                    log.warn("[Admin/Accounts] No se pudo parsear {}='{}' para {}", entry.getKey(), raw, account);
                }
            }
        }
        return null;
    }

    private void updateKeycloakUserAccountConfig(UserRepresentation user, String account, Double margin, Double leverage) throws Exception {
        Keycloak keycloak = null;
        try {
            keycloak = buildKeycloakClient();
            RealmResource realmResource = keycloak.realm(MainApp.getKeycloakService().getRealm());
            UserResource userResource = realmResource.users().get(user.getId());
            UserRepresentation current = userResource.toRepresentation();

            Map<String, List<String>> attrs = current.getAttributes() != null
                    ? new LinkedHashMap<>(current.getAttributes())
                    : new LinkedHashMap<>();

            if (margin != null) {
                upsertMetricAttribute(attrs, "marginaccount", account, margin);
            }
            if (leverage != null) {
                upsertMetricAttribute(attrs, "palanca", account, leverage);
            }
            current.setAttributes(attrs);
            userResource.update(current);
        } finally {
            if (keycloak != null) {
                keycloak.close();
            }
        }
    }

    private void propagateAccountConfigToCore(String account, Double margin, Double leverage, String username) {
        ActorRef existing = MainApp.getAccountGroupUser().get(account);
        if (existing != null) {
            existing.tell(new ActorGroupPerAccount.UpdateRiskConfig(margin, leverage, username), ActorRef.noSender());
            return;
        }

        ActorRef created = MainApp.ensureAccountActor(account);
        if (created != null) {
            created.tell(ActorGroupPerAccount.Initialize.INSTANCE, ActorRef.noSender());
        }
    }

    private void upsertMetricAttribute(Map<String, List<String>> attrs, String fragment, String account, double value) {
        String attributeKey = findMetricAttributeKey(attrs, fragment, account);
        if (attributeKey == null) {
            attributeKey = findFirstMetricAttributeKey(attrs, fragment);
        }
        if (attributeKey == null) {
            attributeKey = fragment;
        }

        List<String> values = new ArrayList<>(attrs.getOrDefault(attributeKey, Collections.emptyList()));
        String serialized = account + "|" + formatNumber(value);
        boolean replaced = false;
        for (int i = 0; i < values.size(); i++) {
            if (account.equals(parseAccountValue(values.get(i)))) {
                values.set(i, serialized);
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            values.add(serialized);
        }
        attrs.put(attributeKey, values);
    }

    private String findMetricAttributeKey(Map<String, List<String>> attrs, String fragment, String account) {
        if (attrs == null || attrs.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, List<String>> entry : attrs.entrySet()) {
            String key = Optional.ofNullable(entry.getKey()).orElse("").toLowerCase(Locale.ROOT);
            if (!key.contains(fragment)) {
                continue;
            }
            for (String raw : entry.getValue()) {
                if (account.equals(parseAccountValue(raw))) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    private String findFirstMetricAttributeKey(Map<String, List<String>> attrs, String fragment) {
        if (attrs == null || attrs.isEmpty()) {
            return null;
        }
        for (String key : attrs.keySet()) {
            if (key != null && key.toLowerCase(Locale.ROOT).contains(fragment)) {
                return key;
            }
        }
        return null;
    }

    private String formatNumber(double value) {
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }

    private String formatNullableNumber(Double value) {
        return value == null ? "default" : formatNumber(value);
    }

    private Keycloak buildKeycloakClient() {
        if (MainApp.getKeycloakService() == null) {
            throw new IllegalStateException("KeycloakService aún no está inicializado en el core. Espera a que termine el arranque e inténtalo de nuevo.");
        }
        return KeycloakBuilder.builder()
                .serverUrl(MainApp.getKeycloakService().getKeycloakUrl())
                .realm(MainApp.getKeycloakService().getRealm())
                .clientId(MainApp.getKeycloakService().getClientId())
                .username(MainApp.getKeycloakService().getAdminUsername())
                .password(MainApp.getKeycloakService().getAdminPassword())
                .clientSecret(MainApp.getKeycloakService().getClientSecret())
                .build();
    }

    private void forEachKeycloakUser(ThrowingUserConsumer consumer) throws Exception {
        Keycloak keycloak = null;
        try {
            keycloak = buildKeycloakClient();
            RealmResource realmResource = keycloak.realm(MainApp.getKeycloakService().getRealm());
            for (GroupRepresentation rootGroup : MainApp.getKeycloakService().getAllGroups()) {
                for (GroupRepresentation subGroup : rootGroup.getSubGroups()) {
                    GroupResource groupResource = realmResource.groups().group(subGroup.getId());
                    int first = 0;
                    int maxResults = 50;
                    while (true) {
                        List<UserRepresentation> users = groupResource.members(first, maxResults);
                        for (UserRepresentation user : users) {
                            consumer.accept(user);
                        }
                        first += maxResults;
                        if (users.isEmpty()) {
                            break;
                        }
                    }
                }
            }
        } finally {
            if (keycloak != null) {
                keycloak.close();
            }
        }
    }

    private ValidationResult validateAccountAgainstKeycloak(String account) throws Exception {
        ValidationResult result = new ValidationResult();
        forEachKeycloakUser(user -> collectValidationForUser(account, user, result));
        return result;
    }

    private void collectValidationForUser(String account, UserRepresentation user, ValidationResult result) {
        if (user == null || user.getAttributes() == null || user.getAttributes().isEmpty()) {
            return;
        }

        String username = Optional.ofNullable(user.getUsername()).orElse("").trim();
        if (username.isEmpty()) {
            return;
        }

        if (extractDeclaredAccounts(user).contains(account)) {
            result.accountOwners.add(username);
        }

        for (Map.Entry<String, List<String>> entry : user.getAttributes().entrySet()) {
            String key = Optional.ofNullable(entry.getKey()).orElse("").toLowerCase(Locale.ROOT);
            if (key.contains("marginaccount")) {
                for (String raw : entry.getValue()) {
                    String parsedAccount = parseAccountValue(raw);
                    if (account.equals(parsedAccount)) {
                        result.marginOwners.add(username);
                    }
                }
            }

            if (key.contains("palanca")) {
                for (String raw : entry.getValue()) {
                    String parsedAccount = parseAccountValue(raw);
                    if (account.equals(parsedAccount)) {
                        result.leverageOwners.add(username);
                    }
                }
            }
        }
    }

    private Set<String> extractDeclaredAccounts(UserRepresentation user) {
        if (user == null || user.getAttributes() == null) {
            return Collections.emptySet();
        }

        Map<String, List<String>> attrs = user.getAttributes();
        List<String> rawValues = null;
        for (String key : attrs.keySet()) {
            if ("account".equalsIgnoreCase(key)) {
                rawValues = attrs.get(key);
                break;
            }
        }

        if (rawValues == null || rawValues.isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> out = new TreeSet<>();
        for (String value : rawValues) {
            if (value == null) {
                continue;
            }
            for (String token : value.split("[,;|\\s]+")) {
                String candidate = token.trim();
                if (isValidAccount(candidate)) {
                    out.add(candidate);
                }
            }
        }
        return out;
    }

    private String parseAccountValue(String raw) {
        if (raw == null || !raw.contains("|")) {
            return "";
        }
        String account = raw.split("\\|")[0].trim().replace(" ", "");
        return isValidAccount(account) ? account : "";
    }

    private boolean isValidAccount(String account) {
        return account != null && account.matches("\\d{1,9}/\\d{1,3}");
    }

    private static final class ValidationResult {
        private final Set<String> accountOwners = new TreeSet<>();
        private final Set<String> marginOwners = new TreeSet<>();
        private final Set<String> leverageOwners = new TreeSet<>();
    }

    @FunctionalInterface
    private interface ThrowingUserConsumer {
        void accept(UserRepresentation user) throws Exception;
    }

    private static final class MetricValue {
        private String attributeKey;
        private Double value;
    }

    private static final class UserAccountConfig {
        private String username;
        private String userId;
        private boolean accountDeclared;
        private Double margin;
        private Double leverage;
        private String marginAttributeKey;
        private String leverageAttributeKey;
        private UserRepresentation userRepresentation;
    }

    private static final class AccountConfigResult {
        private final String account;
        private final Set<String> accountOwners = new TreeSet<>();
        private final Set<String> marginOwners = new TreeSet<>();
        private final Set<String> leverageOwners = new TreeSet<>();
        private final List<UserAccountConfig> rows = new ArrayList<>();
        private final Set<String> seenRowKeys = new HashSet<>();
        private String configurationOwner;
        private String editableOwner;
        private Double keycloakMargin;
        private Double keycloakLeverage;
        private UserRepresentation ownerUser;
        private String issue;

        private AccountConfigResult(String account) {
            this.account = account;
        }
    }
}
