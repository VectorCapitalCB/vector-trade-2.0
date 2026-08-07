package cl.vc.inyectorcandle.model;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;

/**
 * Estadistica oficial de un instrumento para un dia de mercado, tal como la publica la bolsa
 * (no agregada por nosotros). {@code market} y {@code currency} vienen de configuracion: el mismo
 * modelo sirve para BCS, NUAM o cualquier otra plaza.
 */
public record InstrumentDailyStats(
        String market,
        String symbol,
        String securityId,
        String currency,
        LocalDate tradingDay,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal last,
        BigDecimal close,
        BigDecimal previousClose,
        BigDecimal volume,
        BigDecimal turnover,
        BigDecimal auctionPrice,
        BigDecimal lowLimit,
        BigDecimal highLimit,
        BigDecimal referencePrice,
        /** Cuantos 35=W se vieron; solo lo llena el feed FIX. */
        long updates,
        /** Operaciones del dia; solo lo llena el feed ITCH, el 35=W no publica conteo. */
        long tradeCount,
        Instant lastUpdate
) {
    private static final BigDecimal HUNDRED = new BigDecimal("100");

    public String id() {
        return market + "|" + symbol + "|" + tradingDay;
    }

    /** Variacion del ultimo precio contra el cierre previo. Null si falta alguno de los dos. */
    public BigDecimal variationPct() {
        BigDecimal reference = previousClose != null ? previousClose : open;
        if (last == null || reference == null || reference.signum() == 0) {
            return null;
        }
        return last.subtract(reference, MathContext.DECIMAL64)
                .multiply(HUNDRED, MathContext.DECIMAL64)
                .divide(reference, 6, RoundingMode.HALF_UP);
    }

    /** Precio promedio ponderado del dia segun monto y volumen oficiales. */
    public BigDecimal vwap() {
        if (turnover == null || volume == null || volume.signum() == 0) {
            return null;
        }
        return turnover.divide(volume, 6, RoundingMode.HALF_UP);
    }
}
