package cl.vc.inyectorcandle.model;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Actividad de una corredora en un dia. Solo el feed ITCH publica {@code owner}/{@code counterparty}
 * en las ejecuciones; por FIX esta informacion no llega en ningun tag.
 */
public record BrokerDailyStats(
        String market,
        String broker,
        LocalDate tradingDay,
        long trades,
        BigDecimal volume,
        BigDecimal turnover,
        BigDecimal buyVolume,
        BigDecimal sellVolume,
        BigDecimal buyTurnover,
        BigDecimal sellTurnover
) {
    public String id() {
        return market + "|" + broker + "|" + tradingDay;
    }

    /** Positivo si la corredora compro mas de lo que vendio en el dia. */
    public BigDecimal netTurnover() {
        return buyTurnover.subtract(sellTurnover);
    }
}
