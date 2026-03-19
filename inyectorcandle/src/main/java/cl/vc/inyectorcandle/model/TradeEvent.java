package cl.vc.inyectorcandle.model;

import java.math.BigDecimal;
import java.time.Instant;

public record TradeEvent(
        InstrumentKey key,
        Instant eventTime,
        BigDecimal price,
        BigDecimal quantity,
        BigDecimal amount,
        String aggressorSide,
        String mdEntryId,
        char mdUpdateAction,
        String mdReqId,
        String sourceMsgType
) {
}
