package cl.vc.service.admin;

import lombok.Getter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitorea los ciclos de carga de usuarios/cuentas desde Keycloak y la
 * inicialización de actores de cuenta. Se usa para observabilidad en el admin.
 */
public class AccountLoadTracker {

    private static final int MAX_RECENT_CYCLES = 20;

    private final AtomicLong cycleSequence = new AtomicLong(0);
    private final Deque<CycleSnapshot> recentCycles = new ArrayDeque<>();
    private final Map<String, AccountTouchStats> accountStats = new ConcurrentHashMap<>();
    private final Map<String, UserProcessStats> userStats = new ConcurrentHashMap<>();

    private ActiveCycle activeCycle;

    @Getter
    private long totalCyclesStarted;

    @Getter
    private long totalCyclesCompleted;

    @Getter
    private long totalCyclesFailed;

    public synchronized long startCycle(String trigger, int totalUsers, int priorityUsersConfigured) {
        long cycleId = cycleSequence.incrementAndGet();
        totalCyclesStarted++;
        activeCycle = new ActiveCycle(cycleId, trigger, totalUsers, priorityUsersConfigured);
        return cycleId;
    }

    public synchronized void onUserStart(long cycleId, String username, String group, boolean priority, int declaredAccounts) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle == null) {
            return;
        }
        cycle.currentUsername = username;
        cycle.currentGroup = group;
        cycle.currentUserPriority = priority;
        cycle.declaredAccounts += Math.max(0, declaredAccounts);
    }

    public synchronized void onAccountSeen(long cycleId, String account, String username) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle != null && account != null && !account.isBlank()) {
            cycle.touchedAccounts.add(account);
        }
        updateAccountStats(account, username, false, 0L, false);
    }

    public synchronized void onAccountDeclared(long cycleId, String account, String username) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle != null && account != null && !account.isBlank()) {
            cycle.touchedAccounts.add(account);
        }
        AccountTouchStats stats = updateAccountStats(account, username, false, 0L, false);
        if (stats != null && username != null && !username.isBlank()) {
            stats.accountOwners.add(username);
        }
    }

    public synchronized void onActorCreated(long cycleId, String account, String username) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle != null) {
            cycle.actorCreations++;
        }
        onAccountSeen(cycleId, account, username);
    }

    public synchronized void onMarginUpdated(long cycleId, String account, String username) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle != null) {
            cycle.marginUpdates++;
        }
        onAccountSeen(cycleId, account, username);
    }

    public synchronized void onMarginDeclared(String account, String username) {
        AccountTouchStats stats = updateAccountStats(account, username, false, 0L, false);
        if (stats != null && username != null && !username.isBlank()) {
            stats.marginOwners.add(username);
        }
    }

    public synchronized void onLeverageUpdated(long cycleId, String account, String username) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle != null) {
            cycle.leverageUpdates++;
        }
        onAccountSeen(cycleId, account, username);
    }

    public synchronized void onLeverageDeclared(String account, String username) {
        AccountTouchStats stats = updateAccountStats(account, username, false, 0L, false);
        if (stats != null && username != null && !username.isBlank()) {
            stats.leverageOwners.add(username);
        }
    }

    public synchronized void onBackgroundRefreshStart(long cycleId, String account) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle == null) {
            return;
        }
        if (account != null && !account.isBlank()) {
            cycle.touchedAccounts.add(account);
        }
        cycle.pendingBackgroundRefreshes++;
    }

    public synchronized void onBackgroundRefreshFinished(long cycleId, boolean success, String errorMessage) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle == null) {
            return;
        }
        cycle.pendingBackgroundRefreshes = Math.max(0, cycle.pendingBackgroundRefreshes - 1);
        cycle.completedBackgroundRefreshes++;
        if (!success) {
            cycle.finalSuccess = false;
            if (errorMessage != null && !errorMessage.isBlank()) {
                cycle.lastError = errorMessage;
            }
        }
        if (cycle.scanCompleted && cycle.pendingBackgroundRefreshes == 0) {
            finalizeCycle(cycle, cycle.finalSuccess, cycle.lastError);
        }
    }

    public synchronized void replaceAccountDeclarations(String account, Collection<String> accountOwners,
                                                        Collection<String> marginOwners, Collection<String> leverageOwners) {
        if (account == null || account.isBlank()) {
            return;
        }
        AccountTouchStats stats = accountStats.computeIfAbsent(account, AccountTouchStats::new);
        stats.accountOwners.clear();
        stats.marginOwners.clear();
        stats.leverageOwners.clear();
        if (accountOwners != null) {
            stats.accountOwners.addAll(accountOwners);
        }
        if (marginOwners != null) {
            stats.marginOwners.addAll(marginOwners);
        }
        if (leverageOwners != null) {
            stats.leverageOwners.addAll(leverageOwners);
        }
        stats.owners.clear();
        stats.owners.addAll(stats.accountOwners);
        stats.owners.addAll(stats.marginOwners);
        stats.owners.addAll(stats.leverageOwners);
        stats.lastTouchedAt = System.currentTimeMillis();
    }

    public synchronized void onAccountInitialized(long cycleId, String account, String username, long durationMs, boolean success) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle != null) {
            if (success) {
                cycle.initializedAccounts++;
            } else {
                cycle.failedAccountInitializations++;
            }
        }
        updateAccountStats(account, username, true, durationMs, success);
    }

    public synchronized void onUserError(long cycleId, String username, String errorMessage) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle != null) {
            cycle.userErrors++;
            cycle.lastError = errorMessage;
        }

        if (username != null && !username.isBlank()) {
            UserProcessStats stats = userStats.computeIfAbsent(username, UserProcessStats::new);
            stats.errorCount++;
            stats.lastError = errorMessage;
            stats.lastProcessedAt = System.currentTimeMillis();
        }
    }

    public synchronized void onUserFinished(long cycleId, String username, boolean priority, long durationMs, int processedUsers, int totalUsers, long etaMs) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle != null) {
            cycle.processedUsers = processedUsers;
            cycle.lastProcessedUsername = username;
            cycle.currentUsername = null;
            cycle.currentGroup = null;
            cycle.currentUserPriority = false;
            cycle.etaMs = Math.max(0L, etaMs);
            if (priority) {
                cycle.prioritizedUsersProcessed++;
            } else {
                cycle.nonPrioritizedUsersProcessed++;
            }
            cycle.totalUsers = totalUsers;
        }

        if (username != null && !username.isBlank()) {
            UserProcessStats stats = userStats.computeIfAbsent(username, UserProcessStats::new);
            stats.processCount++;
            stats.lastProcessedAt = System.currentTimeMillis();
            stats.lastDurationMs = durationMs;
            stats.priority = priority;
        }
    }

    public synchronized void finishCycle(long cycleId, boolean success, String errorMessage) {
        ActiveCycle cycle = requireCycle(cycleId);
        if (cycle == null) {
            return;
        }
        cycle.scanCompleted = true;
        cycle.finalSuccess = success;
        cycle.etaMs = 0L;
        cycle.currentUsername = null;
        cycle.currentGroup = null;
        cycle.currentUserPriority = false;
        if (errorMessage != null && !errorMessage.isBlank()) {
            cycle.lastError = errorMessage;
        }
        if (cycle.pendingBackgroundRefreshes == 0) {
            finalizeCycle(cycle, cycle.finalSuccess, cycle.lastError);
        }
    }

    public synchronized CycleSnapshot getActiveCycleSnapshot() {
        return activeCycle != null ? activeCycle.toSnapshot(System.currentTimeMillis(), true, true, null) : null;
    }

    public synchronized List<CycleSnapshot> getRecentCycles() {
        return new ArrayList<>(recentCycles);
    }

    public synchronized List<AccountTouchStats> getTopAccounts(int limit) {
        List<AccountTouchStats> out = new ArrayList<>(accountStats.values());
        out.sort(Comparator
                .comparingLong(AccountTouchStats::getInitializationCount).reversed()
                .thenComparing(Comparator.comparingLong(AccountTouchStats::getTouchCount).reversed())
                .thenComparing(AccountTouchStats::getAccount));
        return out.subList(0, Math.min(limit, out.size()));
    }

    public synchronized List<AccountTouchStats> getAllAccounts() {
        List<AccountTouchStats> out = new ArrayList<>(accountStats.values());
        out.sort(Comparator.comparing(AccountTouchStats::getAccount));
        return out;
    }

    public synchronized AccountTouchStats getAccountStats(String account) {
        return accountStats.get(account);
    }

    public synchronized List<UserProcessStats> getTopUsers(int limit) {
        List<UserProcessStats> out = new ArrayList<>(userStats.values());
        out.sort(Comparator
                .comparingLong(UserProcessStats::getProcessCount).reversed()
                .thenComparing(Comparator.comparingLong(UserProcessStats::getErrorCount).reversed())
                .thenComparing(UserProcessStats::getUsername));
        return out.subList(0, Math.min(limit, out.size()));
    }

    private ActiveCycle requireCycle(long cycleId) {
        if (activeCycle == null || activeCycle.cycleId != cycleId) {
            return null;
        }
        return activeCycle;
    }

    private void finalizeCycle(ActiveCycle cycle, boolean success, String errorMessage) {
        long finishedAt = System.currentTimeMillis();
        CycleSnapshot snapshot = cycle.toSnapshot(finishedAt, false, success, errorMessage);
        totalCyclesCompleted++;
        if (!success) {
            totalCyclesFailed++;
        }

        recentCycles.addFirst(snapshot);
        while (recentCycles.size() > MAX_RECENT_CYCLES) {
            recentCycles.removeLast();
        }

        activeCycle = null;
    }

    private AccountTouchStats updateAccountStats(String account, String username, boolean initialized, long durationMs, boolean success) {
        if (account == null || account.isBlank()) {
            return null;
        }

        AccountTouchStats stats = accountStats.computeIfAbsent(account, AccountTouchStats::new);
        if (username != null && !username.isBlank()) {
            stats.owners.add(username);
        }
        if (!initialized) {
            stats.touchCount++;
            stats.lastUsername = username;
            stats.lastTouchedAt = System.currentTimeMillis();
        }

        if (initialized) {
            stats.initializationCount++;
            stats.lastUsername = username;
            stats.lastInitializationAt = System.currentTimeMillis();
            stats.lastInitializationDurationMs = durationMs;
            stats.lastInitializationSuccess = success;
        }
        return stats;
    }

    private static final class ActiveCycle {
        private final long cycleId;
        private final String trigger;
        private final long startedAt;
        private final Set<String> touchedAccounts = new HashSet<>();
        private int totalUsers;
        private final int priorityUsersConfigured;
        private int processedUsers;
        private int prioritizedUsersProcessed;
        private int nonPrioritizedUsersProcessed;
        private int declaredAccounts;
        private int actorCreations;
        private int marginUpdates;
        private int leverageUpdates;
        private int initializedAccounts;
        private int failedAccountInitializations;
        private int userErrors;
        private long etaMs;
        private String currentUsername;
        private String currentGroup;
        private boolean currentUserPriority;
        private String lastProcessedUsername;
        private String lastError;
        private boolean scanCompleted;
        private boolean finalSuccess = true;
        private int pendingBackgroundRefreshes;
        private int completedBackgroundRefreshes;

        private ActiveCycle(long cycleId, String trigger, int totalUsers, int priorityUsersConfigured) {
            this.cycleId = cycleId;
            this.trigger = trigger;
            this.totalUsers = totalUsers;
            this.priorityUsersConfigured = priorityUsersConfigured;
            this.startedAt = System.currentTimeMillis();
        }

        private CycleSnapshot toSnapshot(long nowMs, boolean running, boolean success, String errorMessage) {
            String effectiveError = errorMessage != null ? errorMessage : lastError;
            return new CycleSnapshot(
                    cycleId,
                    trigger,
                    running,
                    success,
                    startedAt,
                    running ? 0L : nowMs,
                    Math.max(0L, nowMs - startedAt),
                    totalUsers,
                    processedUsers,
                    priorityUsersConfigured,
                    prioritizedUsersProcessed,
                    nonPrioritizedUsersProcessed,
                    declaredAccounts,
                    touchedAccounts.size(),
                    actorCreations,
                    marginUpdates,
                    leverageUpdates,
                    initializedAccounts,
                    failedAccountInitializations,
                    userErrors,
                    etaMs,
                    scanCompleted,
                    pendingBackgroundRefreshes,
                    completedBackgroundRefreshes,
                    scanCompleted ? "post_process" : "scan_users",
                    currentUsername,
                    currentGroup,
                    currentUserPriority,
                    lastProcessedUsername,
                    effectiveError
            );
        }
    }

    @Getter
    public static final class CycleSnapshot {
        private final long cycleId;
        private final String trigger;
        private final boolean running;
        private final boolean success;
        private final long startedAt;
        private final long endedAt;
        private final long durationMs;
        private final int totalUsers;
        private final int processedUsers;
        private final int priorityUsersConfigured;
        private final int prioritizedUsersProcessed;
        private final int nonPrioritizedUsersProcessed;
        private final int declaredAccounts;
        private final int touchedAccounts;
        private final int actorCreations;
        private final int marginUpdates;
        private final int leverageUpdates;
        private final int initializedAccounts;
        private final int failedAccountInitializations;
        private final int userErrors;
        private final long etaMs;
        private final boolean scanCompleted;
        private final int pendingBackgroundRefreshes;
        private final int completedBackgroundRefreshes;
        private final String phase;
        private final String currentUsername;
        private final String currentGroup;
        private final boolean currentUserPriority;
        private final String lastProcessedUsername;
        private final String error;

        private CycleSnapshot(long cycleId, String trigger, boolean running, boolean success, long startedAt, long endedAt,
                              long durationMs, int totalUsers, int processedUsers, int priorityUsersConfigured,
                              int prioritizedUsersProcessed, int nonPrioritizedUsersProcessed, int declaredAccounts,
                              int touchedAccounts, int actorCreations, int marginUpdates, int leverageUpdates,
                              int initializedAccounts, int failedAccountInitializations, int userErrors, long etaMs,
                              boolean scanCompleted, int pendingBackgroundRefreshes, int completedBackgroundRefreshes, String phase,
                              String currentUsername, String currentGroup, boolean currentUserPriority,
                              String lastProcessedUsername, String error) {
            this.cycleId = cycleId;
            this.trigger = trigger;
            this.running = running;
            this.success = success;
            this.startedAt = startedAt;
            this.endedAt = endedAt;
            this.durationMs = durationMs;
            this.totalUsers = totalUsers;
            this.processedUsers = processedUsers;
            this.priorityUsersConfigured = priorityUsersConfigured;
            this.prioritizedUsersProcessed = prioritizedUsersProcessed;
            this.nonPrioritizedUsersProcessed = nonPrioritizedUsersProcessed;
            this.declaredAccounts = declaredAccounts;
            this.touchedAccounts = touchedAccounts;
            this.actorCreations = actorCreations;
            this.marginUpdates = marginUpdates;
            this.leverageUpdates = leverageUpdates;
            this.initializedAccounts = initializedAccounts;
            this.failedAccountInitializations = failedAccountInitializations;
            this.userErrors = userErrors;
            this.etaMs = etaMs;
            this.scanCompleted = scanCompleted;
            this.pendingBackgroundRefreshes = pendingBackgroundRefreshes;
            this.completedBackgroundRefreshes = completedBackgroundRefreshes;
            this.phase = phase;
            this.currentUsername = currentUsername;
            this.currentGroup = currentGroup;
            this.currentUserPriority = currentUserPriority;
            this.lastProcessedUsername = lastProcessedUsername;
            this.error = error;
        }
    }

    @Getter
    public static final class AccountTouchStats {
        private final String account;
        private final Set<String> owners = new TreeSet<>();
        private final Set<String> accountOwners = new TreeSet<>();
        private final Set<String> marginOwners = new TreeSet<>();
        private final Set<String> leverageOwners = new TreeSet<>();
        private long touchCount;
        private long initializationCount;
        private String lastUsername;
        private long lastTouchedAt;
        private long lastInitializationAt;
        private long lastInitializationDurationMs;
        private boolean lastInitializationSuccess;

        private AccountTouchStats(String account) {
            this.account = account;
        }

        public int getOwnersCount() {
            return owners.size();
        }

        public String getConfigurationOwner() {
            Set<String> configOwners = new TreeSet<>();
            configOwners.addAll(marginOwners);
            configOwners.addAll(leverageOwners);
            return configOwners.size() == 1 ? configOwners.iterator().next() : null;
        }
    }

    @Getter
    public static final class UserProcessStats {
        private final String username;
        private long processCount;
        private long errorCount;
        private long lastProcessedAt;
        private long lastDurationMs;
        private boolean priority;
        private String lastError;

        private UserProcessStats(String username) {
            this.username = username;
        }
    }
}
