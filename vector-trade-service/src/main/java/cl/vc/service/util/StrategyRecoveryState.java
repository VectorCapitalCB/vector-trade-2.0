package cl.vc.service.util;

public final class StrategyRecoveryState {

    public enum RejectAction {
        RETRY,
        CANCEL_LIVE_ORDER,
        CANCEL_REJECTED_RESUME
    }

    private int consecutiveRejects;
    private boolean cancelPending;
    private boolean rateLimitPaused;
    private long pauseGeneration;

    public long pauseForRateLimit() {
        rateLimitPaused = true;
        consecutiveRejects = 0;
        return ++pauseGeneration;
    }

    public boolean resumeAfterRateLimit(long generation) {
        if (generation != pauseGeneration) {
            return false;
        }
        rateLimitPaused = false;
        return true;
    }

    public RejectAction registerReject(int threshold) {
        if (cancelPending) {
            cancelPending = false;
            consecutiveRejects = 0;
            return RejectAction.CANCEL_REJECTED_RESUME;
        }
        consecutiveRejects++;
        if (consecutiveRejects >= threshold) {
            cancelPending = true;
            return RejectAction.CANCEL_LIVE_ORDER;
        }
        return RejectAction.RETRY;
    }

    public void successfulNonPendingUpdate() {
        consecutiveRejects = 0;
        cancelPending = false;
    }

    public void releaseStaleBlock() {
        consecutiveRejects = 0;
        cancelPending = false;
    }

    public boolean isRateLimitPaused() {
        return rateLimitPaused;
    }

    public boolean isCancelPending() {
        return cancelPending;
    }

    public int getConsecutiveRejects() {
        return consecutiveRejects;
    }
}
