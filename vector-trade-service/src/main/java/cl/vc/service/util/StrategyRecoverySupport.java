package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;

import java.util.Locale;

public final class StrategyRecoverySupport {

    public static final String RATE_LIMIT_REASON = "order rate limit";
    public static final String LIMIT_REACHED_REASON = "Orden llegó a su límite";
    public static final String ACTIVE_AGAIN_REASON = "Orden activa nuevamente";

    private StrategyRecoverySupport() {
    }

    public static boolean isOrderRateLimit(String text) {
        return text != null && text.toLowerCase(Locale.ROOT).contains(RATE_LIMIT_REASON);
    }

    public static int rejectThreshold(String configuredValue) {
        int threshold = 5;
        try {
            threshold = Integer.parseInt(configuredValue == null ? "5" : configuredValue.trim());
        } catch (NumberFormatException ignore) {
        }
        return Math.max(3, Math.min(5, threshold));
    }

    public static boolean isExchangeRecognizedActive(RoutingMessage.OrderStatus status) {
        return status == RoutingMessage.OrderStatus.NEW
                || status == RoutingMessage.OrderStatus.REPLACED
                || status == RoutingMessage.OrderStatus.PARTIALLY_FILLED;
    }

    public static boolean isPendingExecution(RoutingMessage.ExecutionType execType) {
        return execType == RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE
                || execType == RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL;
    }

    public static String limitStatusReason(boolean previousAtLimit, boolean atLimit,
                                           boolean refreshAtLimit) {
        if (previousAtLimit == atLimit && !(refreshAtLimit && atLimit)) {
            return null;
        }
        return atLimit ? LIMIT_REACHED_REASON : ACTIVE_AGAIN_REASON;
    }
}
