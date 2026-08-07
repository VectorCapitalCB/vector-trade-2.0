package cl.vc.service.security;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rate limiter per IP with sliding 1-second windows.
 *
 * Tracks two independent counters per IP:
 *   - total messages (WebSocket frames)
 *   - order requests (NewOrder + Replace)
 *
 * Auto-block logic:
 *   When any counter exceeds its threshold the caller receives {@code true}
 *   (meaning: block this IP). The event is also registered toward the
 *   protection-mode counter. When the number of auto-block events in a
 *   60-second window reaches {@code autoBlockThreshold}, protection mode
 *   is activated: effective thresholds are halved so borderline-abusive IPs
 *   are cut off sooner while legitimate clients already connected are not
 *   disturbed.
 */
@Slf4j
public class IpRateLimiter {

    /** Default max WebSocket messages per second per IP. */
    public static final int DEFAULT_MAX_MESSAGES_PER_SECOND = 100;
    /** Default max order requests (new + replace) per second per IP. */
    public static final int DEFAULT_MAX_ORDERS_PER_SECOND   = 10;
    /** Default number of auto-block events in 60 s that trigger protection mode. */
    public static final int DEFAULT_AUTO_BLOCK_THRESHOLD    = 5;

    @Getter private final int maxMessagesPerSecond;
    @Getter private final int maxOrdersPerSecond;
    @Getter private final int autoBlockThreshold;

    // Per-IP sliding windows: long[0] = window-start epoch ms, long[1] = count
    private final ConcurrentHashMap<String, long[]> messageWindows = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, long[]> orderWindows   = new ConcurrentHashMap<>();

    // Protection-mode tracking
    private final AtomicInteger autoBlockEvents      = new AtomicInteger(0);
    private final AtomicLong    autoBlockWindowStart = new AtomicLong(System.currentTimeMillis());
    private volatile boolean    protectionMode       = false;

    public IpRateLimiter() {
        this(DEFAULT_MAX_MESSAGES_PER_SECOND, DEFAULT_MAX_ORDERS_PER_SECOND, DEFAULT_AUTO_BLOCK_THRESHOLD);
    }

    public IpRateLimiter(int maxMessagesPerSecond, int maxOrdersPerSecond, int autoBlockThreshold) {
        this.maxMessagesPerSecond = maxMessagesPerSecond;
        this.maxOrdersPerSecond   = maxOrdersPerSecond;
        this.autoBlockThreshold   = autoBlockThreshold;
    }

    // ── Public recording methods ──────────────────────────────────────────────

    /**
     * Records a generic WebSocket message from {@code ip}.
     *
     * @return {@code true} if the message rate was exceeded and the IP should be blocked.
     */
    public boolean recordMessage(String ip) {
        int effectiveMax = protectionMode ? Math.max(1, maxMessagesPerSecond / 2) : maxMessagesPerSecond;
        return recordActivity(ip, messageWindows, effectiveMax);
    }

    /**
     * Records an order request (new order or replace) from {@code ip}.
     *
     * @return {@code true} if the order rate was exceeded and the IP should be blocked.
     */
    public boolean recordOrder(String ip) {
        int effectiveMax = protectionMode ? Math.max(1, maxOrdersPerSecond / 2) : maxOrdersPerSecond;
        return recordActivity(ip, orderWindows, effectiveMax);
    }

    // ── Protection-mode API ───────────────────────────────────────────────────

    public boolean isProtectionModeActive() {
        return protectionMode;
    }

    public void resetProtectionMode() {
        protectionMode = false;
        autoBlockEvents.set(0);
        autoBlockWindowStart.set(System.currentTimeMillis());
        log.info("[IpSecurity] ✅ Protection mode reset by admin");
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    public int getAutoBlockEventCount() {
        return autoBlockEvents.get();
    }

    public int getTrackedIpCount() {
        return messageWindows.size();
    }

    /** Returns a snapshot of per-IP message counts for admin display. */
    public Map<String, long[]> getMessageWindowSnapshot() {
        return Map.copyOf(messageWindows);
    }

    /** Clears counters for a specific IP (called after manual unblock). */
    public void clearIpStats(String ip) {
        messageWindows.remove(ip);
        orderWindows.remove(ip);
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private boolean recordActivity(String ip, ConcurrentHashMap<String, long[]> windows, int max) {
        long now = System.currentTimeMillis();
        long[] window = windows.computeIfAbsent(ip, k -> new long[]{now, 0});

        synchronized (window) {
            if (now - window[0] >= 1000) {
                // Start a new 1-second window
                window[0] = now;
                window[1] = 1;
                return false;
            }
            window[1]++;
            if (window[1] > max) {
                log.warn("[IpSecurity] 🚨 Rate limit exceeded — IP: {} ({} req in {}ms, limit={})",
                        ip, window[1], now - window[0], max);
                onAutoBlock();
                return true;
            }
            return false;
        }
    }

    private void onAutoBlock() {
        long now = System.currentTimeMillis();

        // Reset the 60-second event window if expired
        if (now - autoBlockWindowStart.get() >= 60_000) {
            synchronized (autoBlockWindowStart) {
                if (now - autoBlockWindowStart.get() >= 60_000) {
                    autoBlockWindowStart.set(now);
                    autoBlockEvents.set(0);
                }
            }
        }

        int events = autoBlockEvents.incrementAndGet();
        if (!protectionMode && events >= autoBlockThreshold) {
            protectionMode = true;
            log.warn("[IpSecurity] ⚠️ PROTECTION MODE ACTIVATED — {} auto-block events in last 60 s", events);
        }
    }
}
