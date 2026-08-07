package cl.vc.candle.websocket;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fija los invariantes de la parte pura de {@code buildBolsaStatsForDay}. Todo lo que la pestana
 * Estadisticas muestra en el header y en los 6 rankings sale de aqui.
 */
class BolsaStatsBuildTest {

    private static final Instant T0 = Instant.parse("2026-08-07T13:30:00Z");
    private static final Instant T1 = Instant.parse("2026-08-07T20:00:00Z");

    private static MarketDataMessage.RankinSymbol row(String symbol, double last, double high, double low,
                                                      double varPct, double volumen, double monto) {
        return MarketDataMessage.RankinSymbol.newBuilder()
                .setId(symbol)
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSymbol(symbol)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .setVariacionPct(varPct)
                .setPrecioUltimo(last)
                .setPrecioMaximo(high)
                .setPrecioMinimo(low)
                .setVolumen(volumen)
                .setMonto(monto)
                .build();
    }

    private static MarketDataMessage.BolsaStats build(List<MarketDataMessage.RankinSymbol> rows, long trades, int topN) {
        return CandleProtoMarketPublisher.buildBolsaStatsFromRows(rows, trades, T0, T1, topN);
    }

    /** Muestra realista: 3 al alza, 2 a la baja, 1 plano. */
    private static List<MarketDataMessage.RankinSymbol> muestra() {
        return List.of(
                row("CHILE", 189.45, 189.89, 188.70, -1.1737, 12_996_331, 2_456_546_342.75),
                row("SQM-B", 65350.0, 65521.0, 65277.0, -1.4329, 21_875, 1_429_880_851.0),
                row("BCI", 67808.0, 67950.0, 67720.0, 2.7394, 7_286, 494_825_760.0),
                row("SCHWAGER", 5.77, 5.789, 5.77, 8.2552, 3_077_892, 17_789_647.467),
                row("EZU1", 70.9, 70.9, 70.9, 5.0682, 70, 4_963.0),
                row("COLO COLO", 150.0, 150.0, 150.0, 0.0, 46, 6_900.0));
    }

    private static void assertFinite(MarketDataMessage.BolsaStats s) {
        double[] kpis = {
                s.getTotalVolumen(), s.getMontoTotal(), s.getCapitalizacionTotal(), s.getCapitalizacionPromedio(),
                s.getVolatilidadPromedio(), s.getRangoPromedio(), s.getIndicePromedio(), s.getIndiceMaximo(),
                s.getIndiceMinimo(), s.getLiquidezMedia(), s.getSentimientoPositivo(), s.getSentimientoNegativo(),
                s.getPrecioPromedioAcumulado(), s.getPrecioMaximoAcumulado(), s.getTendenciaPromedio()};
        for (double v : kpis) {
            assertFalse(Double.isNaN(v), "KPI NaN en " + s);
            assertFalse(Double.isInfinite(v), "KPI infinito en " + s);
        }
    }

    // ---------- invariantes del header ----------

    @Test
    void sentimientosNoSuperanCien() {
        MarketDataMessage.BolsaStats s = build(muestra(), 100, 20);
        assertTrue(s.getSentimientoPositivo() + s.getSentimientoNegativo() <= 100.0 + 1e-9,
                "pos+neg=" + (s.getSentimientoPositivo() + s.getSentimientoNegativo()));
        // 3 suben, 2 bajan, 1 plano sobre 6 instrumentos
        assertEquals(50.0, s.getSentimientoPositivo(), 1e-9);
        assertEquals(100.0 / 3.0, s.getSentimientoNegativo(), 1e-9);
    }

    @Test
    void indiceMinimoMenorQuePromedioMenorQueMaximo() {
        MarketDataMessage.BolsaStats s = build(muestra(), 100, 20);
        assertTrue(s.getIndiceMinimo() <= s.getIndicePromedio(),
                s.getIndiceMinimo() + " > " + s.getIndicePromedio());
        assertTrue(s.getIndicePromedio() <= s.getIndiceMaximo(),
                s.getIndicePromedio() + " > " + s.getIndiceMaximo());
    }

    @Test
    void rangoPromedioNoNegativo() {
        assertTrue(build(muestra(), 100, 20).getRangoPromedio() >= 0.0);
    }

    @Test
    void diaVacioNoProduceNaNNiInfinito() {
        MarketDataMessage.BolsaStats s = build(List.of(), 0, 20);
        assertFinite(s);
        assertEquals(0.0, s.getTotalVolumen(), 0.0);
        assertEquals(0L, s.getNumeroTotalTrades());
        assertEquals("neutral", s.getTendenciaGeneral());
        assertEquals(0, s.getMasTranzadoCount());
    }

    @Test
    void volumenCeroNoProduceNaNNiInfinito() {
        List<MarketDataMessage.RankinSymbol> rows = List.of(
                row("AAA", 100.0, 110.0, 90.0, 1.0, 0.0, 0.0),
                row("BBB", 200.0, 210.0, 190.0, -1.0, 0.0, 0.0));
        MarketDataMessage.BolsaStats s = build(rows, 0, 20);
        assertFinite(s);
        assertEquals(0.0, s.getIndicePromedio(), 0.0);
        assertEquals(0.0, s.getIndiceMaximo(), 0.0);
        assertEquals(0.0, s.getIndiceMinimo(), 0.0);
        assertEquals(0.0, s.getLiquidezMedia(), 0.0);
    }

    @Test
    void montoCeroConVolumenNoProduceNaNNiInfinito() {
        // Trades sin campo amount: volumen > 0 pero monto = 0 en todas las filas.
        List<MarketDataMessage.RankinSymbol> rows = List.of(
                row("AAA", 100.0, 110.0, 90.0, 1.0, 5_000.0, 0.0),
                row("BBB", 200.0, 210.0, 190.0, -1.0, 3_000.0, 0.0));
        MarketDataMessage.BolsaStats s = build(rows, 7, 20);
        assertFinite(s);
        assertEquals(0.0, s.getMontoTotal(), 0.0);
        assertEquals(0.0, s.getLiquidezMedia(), 0.0, "sin monto no hay ratio: debe caer a 0, no a Infinity");
    }

    @Test
    void numeroTotalTradesEsElQueSeLePasa() {
        assertEquals(6475L, build(muestra(), 6475L, 20).getNumeroTotalTrades());
    }

    @Test
    void tendenciaGeneralSigueAlSentimientoDominante() {
        List<MarketDataMessage.RankinSymbol> alza = List.of(
                row("A", 10, 11, 9, 5.0, 100, 1000),
                row("B", 10, 11, 9, 3.0, 100, 1000),
                row("C", 10, 11, 9, -1.0, 100, 1000));
        assertEquals("alcista", build(alza, 3, 20).getTendenciaGeneral());

        List<MarketDataMessage.RankinSymbol> baja = List.of(
                row("A", 10, 11, 9, -5.0, 100, 1000),
                row("B", 10, 11, 9, -3.0, 100, 1000),
                row("C", 10, 11, 9, 1.0, 100, 1000));
        assertEquals("bajista", build(baja, 3, 20).getTendenciaGeneral());

        // Empate 1/1 mas un plano: ninguno supera el 50%.
        List<MarketDataMessage.RankinSymbol> plano = List.of(
                row("A", 10, 11, 9, 5.0, 100, 1000),
                row("B", 10, 11, 9, -5.0, 100, 1000),
                row("C", 10, 11, 9, 0.0, 100, 1000));
        assertEquals("neutral", build(plano, 3, 20).getTendenciaGeneral());
    }

    @Test
    void precioMaximoAcumuladoEsSumaDeMaximos() {
        // Semantica actual: SUMA, no promedio ni maximo. Si algun dia cambia, este test debe cambiar.
        double esperado = muestra().stream().mapToDouble(MarketDataMessage.RankinSymbol::getPrecioMaximo).sum();
        assertEquals(esperado, build(muestra(), 100, 20).getPrecioMaximoAcumulado(), 1e-6);
    }

    @Test
    void volatilidadPromedioSeEmiteComoFraccion() {
        // El front la re-multiplica por 100: aqui tiene que salir avg(|var%|)/100.
        double esperado = muestra().stream()
                .mapToDouble(r -> Math.abs(r.getVariacionPct())).average().orElse(0.0) / 100.0;
        assertEquals(esperado, build(muestra(), 100, 20).getVolatilidadPromedio(), 1e-12);
    }

    // ---------- invariantes de los rankings ----------

    @Test
    void rankingsSinDuplicadosYRespetanTopN() {
        List<MarketDataMessage.RankinSymbol> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(row("SYM" + i, 100 + i, 110 + i, 90 + i, (i % 7) - 3, 1_000 + i, 10_000 + i));
        }
        MarketDataMessage.BolsaStats s = build(rows, 500, 20);
        for (List<MarketDataMessage.RankinSymbol> rank : List.of(
                s.getMasTranzadoList(), s.getMasVolatilList(), s.getBestRankinList(),
                s.getWorseRankinList(), s.getMasCayoList(), s.getMenosCayoList())) {
            assertEquals(20, rank.size(), "el ranking debe respetar topN");
            Set<String> vistos = new HashSet<>();
            for (MarketDataMessage.RankinSymbol r : rank) {
                assertTrue(vistos.add(r.getSymbol()), "simbolo duplicado en el ranking: " + r.getSymbol());
            }
        }
    }

    @Test
    void topNMayorQueLasFilasNoRellenaNiRepite() {
        MarketDataMessage.BolsaStats s = build(muestra(), 100, 200);
        assertEquals(muestra().size(), s.getMasTranzadoCount());
        assertEquals(muestra().size(), s.getBestRankinCount());
    }

    @Test
    void rankingsEstanEfectivamenteOrdenados() {
        MarketDataMessage.BolsaStats s = build(muestra(), 100, 20);
        assertDesc(s.getMasTranzadoList(), MarketDataMessage.RankinSymbol::getMonto);
        assertDesc(s.getMasVolatilList(), r -> Math.abs(r.getVariacionPct()));
        assertDesc(s.getBestRankinList(), MarketDataMessage.RankinSymbol::getVariacionPct);
        assertDesc(s.getMenosCayoList(), MarketDataMessage.RankinSymbol::getVariacionPct);
        assertAsc(s.getWorseRankinList(), MarketDataMessage.RankinSymbol::getVariacionPct);
        assertAsc(s.getMasCayoList(), MarketDataMessage.RankinSymbol::getVariacionPct);
    }

    /**
     * Documenta la duplicacion actual: el ComboBox ofrece 6 filtros pero "Mas cayo"/"Menos cayo"
     * devuelven exactamente lo mismo que "Peores"/"Mejores". Verificado tambien contra el documento
     * real 1d:2026-08-07 de bolsa_stats_history. Cambiar la semantica requiere decision de producto.
     */
    @Test
    void masCayoYMenosCayoDuplicanWorseYBest() {
        MarketDataMessage.BolsaStats s = build(muestra(), 100, 20);
        assertEquals(s.getWorseRankinList(), s.getMasCayoList());
        assertEquals(s.getBestRankinList(), s.getMenosCayoList());
    }

    private static void assertDesc(List<MarketDataMessage.RankinSymbol> rank,
                                   java.util.function.ToDoubleFunction<MarketDataMessage.RankinSymbol> key) {
        for (int i = 1; i < rank.size(); i++) {
            assertTrue(key.applyAsDouble(rank.get(i - 1)) >= key.applyAsDouble(rank.get(i)),
                    "no esta ordenado desc en la posicion " + i);
        }
    }

    private static void assertAsc(List<MarketDataMessage.RankinSymbol> rank,
                                  java.util.function.ToDoubleFunction<MarketDataMessage.RankinSymbol> key) {
        for (int i = 1; i < rank.size(); i++) {
            assertTrue(key.applyAsDouble(rank.get(i - 1)) <= key.applyAsDouble(rank.get(i)),
                    "no esta ordenado asc en la posicion " + i);
        }
    }
}
