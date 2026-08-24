package cl.vc.service.util;

import org.junit.jupiter.api.Test;

import static cl.vc.service.util.StrategyRecoveryState.RejectAction.*;
import static org.junit.jupiter.api.Assertions.*;

class StrategyRecoveryStateTest {

    @Test
    void latestRateLimitExtendsPauseAndOnlyLatestTimerResumes() {
        StrategyRecoveryState state = new StrategyRecoveryState();
        long first = state.pauseForRateLimit();
        long second = state.pauseForRateLimit();

        assertFalse(state.resumeAfterRateLimit(first));
        assertTrue(state.isRateLimitPaused());
        assertTrue(state.resumeAfterRateLimit(second));
        assertFalse(state.isRateLimitPaused());
    }

    @Test
    void successfulUpdateResetsRejectCounter() {
        StrategyRecoveryState state = new StrategyRecoveryState();
        assertEquals(RETRY, state.registerReject(5));
        assertEquals(RETRY, state.registerReject(5));

        state.successfulNonPendingUpdate();

        assertEquals(0, state.getConsecutiveRejects());
        assertFalse(state.isCancelPending());
    }

    @Test
    void cancelsExactlyAtConfiguredThreshold() {
        StrategyRecoveryState state = new StrategyRecoveryState();
        for (int i = 1; i < 5; i++) {
            assertEquals(RETRY, state.registerReject(5));
        }
        assertEquals(CANCEL_LIVE_ORDER, state.registerReject(5));
        assertTrue(state.isCancelPending());
    }

    @Test
    void cancelRejectClearsPendingAndResumes() {
        StrategyRecoveryState state = new StrategyRecoveryState();
        for (int i = 0; i < 3; i++) {
            state.registerReject(3);
        }

        assertEquals(CANCEL_REJECTED_RESUME, state.registerReject(3));
        assertFalse(state.isCancelPending());
        assertEquals(0, state.getConsecutiveRejects());
    }
}
