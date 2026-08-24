package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StrategyRecoverySupportTest {

    @Test
    void detectsOrderRateLimitCaseInsensitively() {
        assertTrue(StrategyRecoverySupport.isOrderRateLimit(
                "[-850004] User has breached order rate limit"));
        assertTrue(StrategyRecoverySupport.isOrderRateLimit("ORDER RATE LIMIT"));
        assertFalse(StrategyRecoverySupport.isOrderRateLimit("OrigClOrdID not found"));
        assertFalse(StrategyRecoverySupport.isOrderRateLimit(null));
    }

    @Test
    void clampsRejectThresholdBetweenThreeAndFive() {
        assertEquals(3, StrategyRecoverySupport.rejectThreshold("1"));
        assertEquals(3, StrategyRecoverySupport.rejectThreshold("3"));
        assertEquals(4, StrategyRecoverySupport.rejectThreshold("4"));
        assertEquals(5, StrategyRecoverySupport.rejectThreshold("5"));
        assertEquals(5, StrategyRecoverySupport.rejectThreshold("9"));
        assertEquals(5, StrategyRecoverySupport.rejectThreshold("invalid"));
    }

    @Test
    void pendingNewIsNeverEligibleForWatchdogReplay() {
        assertFalse(StrategyRecoverySupport.isExchangeRecognizedActive(
                RoutingMessage.OrderStatus.PENDING_NEW));
        assertTrue(StrategyRecoverySupport.isExchangeRecognizedActive(
                RoutingMessage.OrderStatus.NEW));
        assertTrue(StrategyRecoverySupport.isExchangeRecognizedActive(
                RoutingMessage.OrderStatus.REPLACED));
        assertTrue(StrategyRecoverySupport.isExchangeRecognizedActive(
                RoutingMessage.OrderStatus.PARTIALLY_FILLED));
        assertFalse(StrategyRecoverySupport.isExchangeRecognizedActive(
                RoutingMessage.OrderStatus.FILLED));
    }

    @Test
    void reportsLimitAndActiveAgainTransitions() {
        assertEquals(StrategyRecoverySupport.LIMIT_REACHED_REASON,
                StrategyRecoverySupport.limitStatusReason(false, true, false));
        assertEquals(StrategyRecoverySupport.ACTIVE_AGAIN_REASON,
                StrategyRecoverySupport.limitStatusReason(true, false, false));
        assertEquals(StrategyRecoverySupport.LIMIT_REACHED_REASON,
                StrategyRecoverySupport.limitStatusReason(true, true, true));
        assertNull(StrategyRecoverySupport.limitStatusReason(false, false, false));
    }
}
