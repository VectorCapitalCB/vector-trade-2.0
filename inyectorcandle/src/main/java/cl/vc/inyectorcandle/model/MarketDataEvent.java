package cl.vc.inyectorcandle.model;

import java.math.BigDecimal;
import java.time.Instant;

public record MarketDataEvent(
        InstrumentKey key,
        Instant eventTime,
        char mdEntryType,
        BigDecimal price,
        BigDecimal size,
        String mdEntryId,
        String mdReqId,
        String sourceMsgType
) {
}
