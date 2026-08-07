package cl.vc.inyectorcandle.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;

/**
 * Agregado de mercado para un dia, construido desde las {@link InstrumentDailyStats} oficiales.
 * <p>
 * Las formulas replican las de {@code CandleProtoMarketPublisher} y {@code ActorTradeGeneralStats}
 * para que los numeros sean comparables con lo que ya emite el resto del sistema, aunque algunas
 * no signifiquen lo que su nombre sugiere (ver {@link #precioMaximoAcumulado}).
 * <p>
 * Solo entran los instrumentos que efectivamente transaron: promediar sobre los que no operaron
 * diluiria todo con ceros.
 */
public record MarketDaySummary(
        String market,
        LocalDate tradingDay,
        double totalVolumen,
        double montoTotal,
        double capitalizacionTotal,
        double capitalizacionPromedio,
        double volatilidadPromedio,
        double rangoPromedio,
        double indicePromedio,
        double indiceMaximo,
        double indiceMinimo,
        double liquidezMedia,
        double sentimientoPositivo,
        double sentimientoNegativo,
        double precioPromedioAcumulado,
        /** Suma de maximos, no promedio ni maximo: el nombre viene del proto y se conserva. */
        double precioMaximoAcumulado,
        double tendenciaPromedio,
        String tendenciaGeneral,
        long numeroTotalTrades,
        java.time.Instant sessionStart,
        java.time.Instant sessionEnd,
        List<InstrumentDailyStats> masTranzado,
        List<InstrumentDailyStats> masVolatil,
        List<InstrumentDailyStats> mejores,
        List<InstrumentDailyStats> peores
) {

    public String snapshotKey() {
        return "1d:" + tradingDay;
    }

    /**
     * Devuelve null si ningun instrumento transo ese dia: un doc en cero mentiria.
     *
     * @param session horario de rueda de la plaza; define {@code hora_inicio}/{@code hora_fin},
     *                que el front usa para fechar el snapshot historico.
     */
    public static MarketDaySummary of(String market, LocalDate day, List<InstrumentDailyStats> all, int topN,
                                      MarketSession session) {
        List<InstrumentDailyStats> rows = all.stream()
                .filter(r -> positive(r.turnover()))
                .toList();
        if (rows.isEmpty()) {
            return null;
        }

        int n = rows.size();
        double totalVolumen = sum(rows, InstrumentDailyStats::volume);
        double montoTotal = sum(rows, InstrumentDailyStats::turnover);
        double capTotal = rows.stream().mapToDouble(r -> d(r.last()) * d(r.volume())).sum();

        double positivos = rows.stream().filter(r -> d(r.variationPct()) > 0).count() * 100.0 / n;
        double negativos = rows.stream().filter(r -> d(r.variationPct()) < 0).count() * 100.0 / n;

        double pondLast = totalVolumen > 0 ? rows.stream().mapToDouble(r -> d(r.last()) * d(r.volume())).sum() / totalVolumen : 0.0;
        double pondHigh = totalVolumen > 0 ? rows.stream().mapToDouble(r -> d(r.high()) * d(r.volume())).sum() / totalVolumen : 0.0;
        double pondLow = totalVolumen > 0 ? rows.stream().mapToDouble(r -> d(r.low()) * d(r.volume())).sum() / totalVolumen : 0.0;

        return new MarketDaySummary(
                market,
                day,
                totalVolumen,
                montoTotal,
                capTotal,
                capTotal / n,
                rows.stream().mapToDouble(r -> Math.abs(d(r.variationPct()))).average().orElse(0.0) / 100.0,
                rows.stream().mapToDouble(r -> d(r.high()) - d(r.low())).average().orElse(0.0),
                pondLast,
                pondHigh,
                pondLow,
                montoTotal > 0 ? totalVolumen / montoTotal : 0.0,
                positivos,
                negativos,
                rows.stream().mapToDouble(r -> d(r.last())).average().orElse(0.0),
                rows.stream().mapToDouble(r -> d(r.high())).sum(),
                rows.stream().mapToDouble(r -> d(r.variationPct())).average().orElse(0.0),
                positivos > 50 ? "alcista" : negativos > 50 ? "bajista" : "neutral",
                rows.stream().mapToLong(InstrumentDailyStats::tradeCount).sum(),
                session.start(day),
                session.end(day),
                top(rows, Comparator.comparingDouble(r -> -d(r.turnover())), topN),
                top(rows, Comparator.comparingDouble(r -> -Math.abs(d(r.variationPct()))), topN),
                top(rows, Comparator.comparingDouble(r -> -d(r.variationPct())), topN),
                top(rows, Comparator.comparingDouble(r -> d(r.variationPct())), topN)
        );
    }

    private static List<InstrumentDailyStats> top(List<InstrumentDailyStats> rows,
                                                  Comparator<InstrumentDailyStats> order, int topN) {
        return rows.stream().sorted(order).limit(topN).toList();
    }

    private static double sum(List<InstrumentDailyStats> rows,
                              java.util.function.Function<InstrumentDailyStats, BigDecimal> field) {
        return rows.stream().mapToDouble(r -> d(field.apply(r))).sum();
    }

    private static boolean positive(BigDecimal value) {
        return value != null && value.signum() > 0;
    }

    private static double d(BigDecimal value) {
        return value == null ? 0.0 : value.doubleValue();
    }
}
