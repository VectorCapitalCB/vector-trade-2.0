package cl.vc.service.admin;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AccountLoadTrackerTest {

    @Test
    @DisplayName("Registra un ciclo completo con usuarios, cuentas e inicializaciones")
    void tracksCycleProgress() {
        AccountLoadTracker tracker = new AccountLoadTracker();

        long cycleId = tracker.startCycle("startup", 2, 1);

        tracker.onUserStart(cycleId, "daedo", "mesa", true, 2);
        tracker.onAccountSeen(cycleId, "12345678/0", "daedo");
        tracker.onActorCreated(cycleId, "12345678/0", "daedo");
        tracker.onAccountInitialized(cycleId, "12345678/0", "daedo", 1200L, true);
        tracker.onUserFinished(cycleId, "daedo", true, 2500L, 1, 2, 3000L);

        tracker.onUserStart(cycleId, "jperez", "mesa", false, 1);
        tracker.onAccountSeen(cycleId, "87654321/0", "jperez");
        tracker.onUserError(cycleId, "jperez", "timeout sql");
        tracker.onAccountInitialized(cycleId, "87654321/0", "jperez", 5000L, false);
        tracker.onUserFinished(cycleId, "jperez", false, 5200L, 2, 2, 0L);

        AccountLoadTracker.CycleSnapshot active = tracker.getActiveCycleSnapshot();
        assertNotNull(active);
        assertEquals(2, active.getProcessedUsers());
        assertEquals(2, active.getTouchedAccounts());
        assertEquals(1, active.getInitializedAccounts());
        assertEquals(1, active.getFailedAccountInitializations());

        tracker.finishCycle(cycleId, true, null);

        assertNull(tracker.getActiveCycleSnapshot());
        assertEquals(1, tracker.getTotalCyclesCompleted());
        assertEquals(0, tracker.getTotalCyclesFailed());
        assertEquals(1, tracker.getRecentCycles().size());

        AccountLoadTracker.AccountTouchStats firstAccount = tracker.getTopAccounts(10).stream()
                .filter(s -> "12345678/0".equals(s.getAccount()))
                .findFirst()
                .orElseThrow();
        assertEquals(2L, firstAccount.getTouchCount());
        assertEquals(1L, firstAccount.getInitializationCount());
        assertEquals(1, firstAccount.getOwnersCount());
        assertTrue(firstAccount.getOwners().contains("daedo"));

        AccountLoadTracker.UserProcessStats firstUser = tracker.getTopUsers(10).stream()
                .filter(s -> "daedo".equals(s.getUsername()))
                .findFirst()
                .orElseThrow();
        assertEquals(1L, firstUser.getProcessCount());
        assertTrue(firstUser.isPriority());
    }

    @Test
    @DisplayName("Marca fallas de ciclo cuando termina con error")
    void tracksFailedCycle() {
        AccountLoadTracker tracker = new AccountLoadTracker();
        long cycleId = tracker.startCycle("scheduled", 1, 0);

        tracker.onUserError(cycleId, "jperez", "boom");
        tracker.finishCycle(cycleId, false, "boom");

        assertEquals(1, tracker.getTotalCyclesCompleted());
        assertEquals(1, tracker.getTotalCyclesFailed());
        assertFalse(tracker.getRecentCycles().get(0).isSuccess());
        assertEquals("boom", tracker.getRecentCycles().get(0).getError());
    }

    @Test
    @DisplayName("Registra todos los usuarios dueños de una misma cuenta")
    void tracksMultipleOwnersPerAccount() {
        AccountLoadTracker tracker = new AccountLoadTracker();
        long cycleId = tracker.startCycle("startup", 2, 0);

        tracker.onAccountDeclared(cycleId, "11111111/0", "alice");
        tracker.onAccountDeclared(cycleId, "11111111/0", "bob");
        tracker.onMarginDeclared("11111111/0", "alice");
        tracker.onLeverageDeclared("11111111/0", "alice");
        tracker.onAccountInitialized(cycleId, "11111111/0", "bob", 900L, true);

        AccountLoadTracker.AccountTouchStats account = tracker.getAccountStats("11111111/0");
        assertNotNull(account);
        assertEquals(2, account.getOwnersCount());
        assertTrue(account.getOwners().contains("alice"));
        assertTrue(account.getOwners().contains("bob"));
        assertTrue(account.getAccountOwners().contains("alice"));
        assertTrue(account.getAccountOwners().contains("bob"));
        assertTrue(account.getMarginOwners().contains("alice"));
        assertTrue(account.getLeverageOwners().contains("alice"));
        assertEquals("alice", account.getConfigurationOwner());
    }

    @Test
    @DisplayName("Reemplaza dueños declarados al revalidar una cuenta desde Keycloak")
    void replacesDeclarationsOnKeycloakRevalidation() {
        AccountLoadTracker tracker = new AccountLoadTracker();
        long cycleId = tracker.startCycle("startup", 1, 0);

        tracker.onAccountDeclared(cycleId, "18415523/0", "daedo");
        tracker.onAccountDeclared(cycleId, "18415523/0", "vnazar");
        tracker.onMarginDeclared("18415523/0", "daedo");
        tracker.onLeverageDeclared("18415523/0", "vnazar");

        tracker.replaceAccountDeclarations(
                "18415523/0",
                java.util.Set.of("daedo", "vnazar"),
                java.util.Set.of("daedo"),
                java.util.Set.of("daedo")
        );

        AccountLoadTracker.AccountTouchStats account = tracker.getAccountStats("18415523/0");
        assertNotNull(account);
        assertEquals(java.util.Set.of("daedo", "vnazar"), account.getAccountOwners());
        assertEquals(java.util.Set.of("daedo"), account.getMarginOwners());
        assertEquals(java.util.Set.of("daedo"), account.getLeverageOwners());
        assertEquals("daedo", account.getConfigurationOwner());
    }

    @Test
    @DisplayName("Mantiene el ciclo activo mientras queden recálculos de fondo pendientes")
    void keepsCycleRunningUntilBackgroundRefreshesFinish() {
        AccountLoadTracker tracker = new AccountLoadTracker();
        long cycleId = tracker.startCycle("startup", 1, 0);

        tracker.onUserStart(cycleId, "daedo", "mesa", true, 1);
        tracker.onBackgroundRefreshStart(cycleId, "18415523/0");
        tracker.onUserFinished(cycleId, "daedo", true, 100L, 1, 1, 0L);
        tracker.finishCycle(cycleId, true, null);

        AccountLoadTracker.CycleSnapshot active = tracker.getActiveCycleSnapshot();
        assertNotNull(active);
        assertTrue(active.isRunning());
        assertTrue(active.isScanCompleted());
        assertEquals("post_process", active.getPhase());
        assertEquals(1, active.getPendingBackgroundRefreshes());

        tracker.onBackgroundRefreshFinished(cycleId, true, null);

        assertNull(tracker.getActiveCycleSnapshot());
        assertEquals(1, tracker.getTotalCyclesCompleted());
        assertEquals(0, tracker.getTotalCyclesFailed());
        assertEquals("post_process", tracker.getRecentCycles().get(0).getPhase());
    }
}
