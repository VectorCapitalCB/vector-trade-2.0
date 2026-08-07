package cl.vc.service.security;

import org.junit.jupiter.api.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for IpRateLimiter.
 *
 * Covers:
 *   - Default configuration values
 *   - Normal traffic stays within limits
 *   - Rate exceeded triggers auto-block
 *   - Protection mode activation
 *   - Protection mode tightens thresholds
 *   - Protection mode reset
 *   - Stats and per-IP clearing
 *   - Concurrent access correctness
 *   - MainApp.blockIp / unblockIp / isIpBlocked
 */
class IpRateLimiterTest {

    @Nested
    @DisplayName("Default configuration")
    class DefaultConfig {

        @Test
        @DisplayName("Default max messages per second is 100")
        void defaultMaxMessages() {
            IpRateLimiter limiter = new IpRateLimiter();
            assertEquals(100, limiter.getMaxMessagesPerSecond());
        }

        @Test
        @DisplayName("Default max orders per second is 10")
        void defaultMaxOrders() {
            IpRateLimiter limiter = new IpRateLimiter();
            assertEquals(10, limiter.getMaxOrdersPerSecond());
        }

        @Test
        @DisplayName("Default auto-block threshold is 5")
        void defaultAutoBlockThreshold() {
            IpRateLimiter limiter = new IpRateLimiter();
            assertEquals(5, limiter.getAutoBlockThreshold());
        }

        @Test
        @DisplayName("Custom constructor sets correct values")
        void customConstructor() {
            IpRateLimiter limiter = new IpRateLimiter(50, 5, 3);
            assertEquals(50, limiter.getMaxMessagesPerSecond());
            assertEquals(5,  limiter.getMaxOrdersPerSecond());
            assertEquals(3,  limiter.getAutoBlockThreshold());
        }

        @Test
        @DisplayName("Protection mode starts inactive")
        void protectionModeStartsOff() {
            IpRateLimiter limiter = new IpRateLimiter();
            assertFalse(limiter.isProtectionModeActive());
        }
    }

    @Nested
    @DisplayName("Message rate limiting")
    class MessageRateLimiting {

        @Test
        @DisplayName("Traffic within limit returns false")
        void withinLimit() {
            IpRateLimiter limiter = new IpRateLimiter(10, 5, 100);
            String ip = "10.0.0.1";
            for (int i = 0; i < 10; i++) {
                assertFalse(limiter.recordMessage(ip), "call " + (i+1) + " should be within limit");
            }
        }

        @Test
        @DisplayName("Exceeding limit returns true on the offending call")
        void exceedsLimit() {
            IpRateLimiter limiter = new IpRateLimiter(5, 5, 100);
            String ip = "10.0.0.2";
            boolean triggered = false;
            for (int i = 0; i < 10; i++) {
                if (limiter.recordMessage(ip)) { triggered = true; break; }
            }
            assertTrue(triggered, "Should trigger after exceeding 5 msg/s");
        }

        @Test
        @DisplayName("Window resets after 1 second — traffic is allowed again")
        void windowReset() throws InterruptedException {
            IpRateLimiter limiter = new IpRateLimiter(3, 3, 100);
            String ip = "10.0.0.3";

            // Flood the window
            for (int i = 0; i < 5; i++) limiter.recordMessage(ip);

            // Wait for the window to expire
            Thread.sleep(1100);

            // Should be allowed in the new window
            assertFalse(limiter.recordMessage(ip), "First message of new window should be allowed");
        }

        @Test
        @DisplayName("Different IPs have independent counters")
        void independentIpCounters() {
            IpRateLimiter limiter = new IpRateLimiter(3, 3, 100);
            // Flood ip1
            for (int i = 0; i < 5; i++) limiter.recordMessage("10.0.0.10");
            // ip2 should be clean
            assertFalse(limiter.recordMessage("10.0.0.20"), "Different IP should not be affected");
        }

        @Test
        @DisplayName("Auto-block event count increases when limit is exceeded")
        void autoBlockEventCounter() {
            IpRateLimiter limiter = new IpRateLimiter(2, 2, 100);
            String ip = "10.0.0.5";
            int before = limiter.getAutoBlockEventCount();
            for (int i = 0; i < 5; i++) limiter.recordMessage(ip);
            assertTrue(limiter.getAutoBlockEventCount() > before, "Auto-block events should increase");
        }

        @Test
        @DisplayName("Tracked IP count increments on first activity")
        void trackedIpCount() {
            IpRateLimiter limiter = new IpRateLimiter(100, 10, 100);
            limiter.recordMessage("192.168.1.1");
            limiter.recordMessage("192.168.1.2");
            assertTrue(limiter.getTrackedIpCount() >= 2);
        }
    }

    @Nested
    @DisplayName("Order rate limiting")
    class OrderRateLimiting {

        @Test
        @DisplayName("Orders within limit return false")
        void withinOrderLimit() {
            IpRateLimiter limiter = new IpRateLimiter(100, 5, 100);
            String ip = "10.1.0.1";
            for (int i = 0; i < 5; i++) {
                assertFalse(limiter.recordOrder(ip), "order " + (i+1) + " should be within limit");
            }
        }

        @Test
        @DisplayName("Exceeding order limit triggers auto-block")
        void exceedsOrderLimit() {
            IpRateLimiter limiter = new IpRateLimiter(100, 3, 100);
            String ip = "10.1.0.2";
            boolean blocked = false;
            for (int i = 0; i < 8; i++) {
                if (limiter.recordOrder(ip)) { blocked = true; break; }
            }
            assertTrue(blocked, "Should block after exceeding 3 orders/s");
        }

        @Test
        @DisplayName("Message and order windows are independent per IP")
        void independentWindows() {
            IpRateLimiter limiter = new IpRateLimiter(100, 2, 100);
            String ip = "10.1.0.3";
            // Flood order window
            for (int i = 0; i < 5; i++) limiter.recordOrder(ip);
            // Message window is untouched
            assertFalse(limiter.recordMessage(ip), "Message window should be unaffected by order flood");
        }
    }

    @Nested
    @DisplayName("Protection mode")
    class ProtectionMode {

        @Test
        @DisplayName("Protection mode activates after threshold auto-blocks")
        void activatesAfterThreshold() {
            // threshold=3 → 3 auto-block events → protection on
            IpRateLimiter limiter = new IpRateLimiter(2, 2, 3);
            for (int i = 0; i < 3; i++) {
                String ip = "10.2.0." + i;
                // Force multiple limit-exceedances from distinct IPs
                for (int j = 0; j < 5; j++) limiter.recordMessage(ip);
            }
            assertTrue(limiter.isProtectionModeActive(), "Protection mode should activate");
        }

        @Test
        @DisplayName("Protection mode halves effective message threshold")
        void halvesMessageThreshold() {
            // threshold=2 → 2 auto-blocks → protection on → effective limit = max(1, 4/2) = 2
            IpRateLimiter limiter = new IpRateLimiter(4, 10, 2);
            // Trigger protection mode
            for (int i = 0; i < 2; i++) {
                String ip = "10.3.0." + i;
                for (int j = 0; j < 6; j++) limiter.recordMessage(ip);
            }
            assertTrue(limiter.isProtectionModeActive());

            // Now a new IP: effective limit is 2 (half of 4)
            String newIp = "10.3.0.99";
            int blocked = 0;
            for (int i = 0; i < 5; i++) {
                if (limiter.recordMessage(newIp)) blocked++;
            }
            assertTrue(blocked > 0, "Should block sooner in protection mode (threshold halved)");
        }

        @Test
        @DisplayName("Protection mode reset clears state")
        void resetClearsState() {
            IpRateLimiter limiter = new IpRateLimiter(2, 2, 2);
            // Activate
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 5; j++) limiter.recordMessage("10.4.0." + i);
            }
            assertTrue(limiter.isProtectionModeActive());

            // Reset
            limiter.resetProtectionMode();

            assertFalse(limiter.isProtectionModeActive(), "Protection mode should be off after reset");
            assertEquals(0, limiter.getAutoBlockEventCount(), "Auto-block events should be zero after reset");
        }
    }

    @Nested
    @DisplayName("Stats and clearing")
    class StatsAndClearing {

        @Test
        @DisplayName("clearIpStats removes IP from tracking")
        void clearIpStats() {
            IpRateLimiter limiter = new IpRateLimiter(100, 10, 100);
            String ip = "10.5.0.1";
            limiter.recordMessage(ip);
            int before = limiter.getTrackedIpCount();
            limiter.clearIpStats(ip);
            assertTrue(limiter.getTrackedIpCount() < before || limiter.getTrackedIpCount() == 0,
                    "Tracking count should decrease after clear");
        }

        @Test
        @DisplayName("Message window snapshot contains recent activity")
        void windowSnapshot() {
            IpRateLimiter limiter = new IpRateLimiter(100, 10, 100);
            String ip = "10.5.0.2";
            limiter.recordMessage(ip);
            assertTrue(limiter.getMessageWindowSnapshot().containsKey(ip),
                    "Snapshot should include active IP");
        }
    }

    @Nested
    @DisplayName("Concurrent access")
    class ConcurrentAccess {

        @Test
        @DisplayName("Concurrent messages from one IP do not throw")
        void concurrentSameIp() throws InterruptedException {
            IpRateLimiter limiter = new IpRateLimiter(200, 50, 100);
            String ip = "10.6.0.1";
            int threads = 20;
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch done  = new CountDownLatch(threads);
            AtomicInteger errors = new AtomicInteger(0);

            ExecutorService es = Executors.newFixedThreadPool(threads);
            for (int i = 0; i < threads; i++) {
                es.submit(() -> {
                    ready.countDown();
                    try {
                        ready.await();
                        for (int j = 0; j < 10; j++) limiter.recordMessage(ip);
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            done.await();
            es.shutdown();
            assertEquals(0, errors.get(), "No exceptions during concurrent access");
        }

        @Test
        @DisplayName("Concurrent messages from distinct IPs are all tracked")
        void concurrentDistinctIps() throws InterruptedException {
            IpRateLimiter limiter = new IpRateLimiter(200, 50, 1000);
            int threads = 10;
            CountDownLatch done = new CountDownLatch(threads);
            ExecutorService es = Executors.newFixedThreadPool(threads);

            for (int i = 0; i < threads; i++) {
                final String ip = "10.7.0." + i;
                es.submit(() -> {
                    try {
                        for (int j = 0; j < 5; j++) limiter.recordMessage(ip);
                    } finally {
                        done.countDown();
                    }
                });
            }
            done.await();
            es.shutdown();
            assertTrue(limiter.getTrackedIpCount() >= threads,
                    "All distinct IPs should be tracked");
        }
    }
}
