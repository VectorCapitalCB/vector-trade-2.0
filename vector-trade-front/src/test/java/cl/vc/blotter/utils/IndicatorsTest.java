package cl.vc.blotter.utils;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Indicadores tecnicos: valores calculados a mano.
 *
 * Un indicador mal calculado no revienta ni se ve raro en el grafico: dibuja una linea plausible
 * y equivocada, y alguien opera con ella. Por eso se verifican numeros concretos y no solo que
 * "corra sin excepcion".
 */
public class IndicatorsTest {

    private static final double E = 1e-9;
    private static final ZoneId CL = ZoneId.of("America/Santiago");

    private static void assertNaN(double v, String msg) {
        assertTrue(Double.isNaN(v), msg + " (era " + v + ")");
    }

    // ------------------------------------------------------------------ SMA

    @Test
    public void smaCalculaYDejaElWarmupEnNaN() {
        double[] out = Indicators.sma(new double[]{1, 2, 3, 4, 5}, 3);
        assertNaN(out[0], "sin 3 muestras aun");
        assertNaN(out[1], "sin 3 muestras aun");
        assertEquals(2d, out[2], E);   // (1+2+3)/3
        assertEquals(3d, out[3], E);   // (2+3+4)/3
        assertEquals(4d, out[4], E);   // (3+4+5)/3
    }

    @Test
    public void smaConMenosDatosQueElPeriodoEsTodoNaN() {
        for (double v : Indicators.sma(new double[]{1, 2}, 5)) assertNaN(v, "serie corta");
    }

    // ------------------------------------------------------------------ EMA

    @Test
    public void emaSeSiembraConLaSmaYLuegoEsRecursiva() {
        // periodo 3 -> k = 2/4 = 0.5; siembra en [2] = (1+2+3)/3 = 2
        double[] out = Indicators.ema(new double[]{1, 2, 3, 10}, 3);
        assertNaN(out[1], "warm-up");
        assertEquals(2d, out[2], E);
        assertEquals(6d, out[3], E);   // (10-2)*0.5 + 2
    }

    @Test
    public void emaReaccionaMasQueSmaAlUltimoDato() {
        double[] v = {1, 2, 3, 10};
        assertEquals(5d, Indicators.sma(v, 3)[3], E);
        assertEquals(6d, Indicators.ema(v, 3)[3], E);
    }

    // ------------------------------------------------------------------ RSI

    @Test
    public void rsiDeSerieSiempreAlAlzaEsCien() {
        double[] out = Indicators.rsi(new double[]{1, 2, 3, 4, 5, 6, 7, 8}, 3);
        assertEquals(100d, Indicators.last(out), E);
    }

    @Test
    public void rsiDeSeriePlanaEsCincuenta() {
        // Sin ganancias ni perdidas no hay RS definido; 50 es la convencion (neutral).
        double[] out = Indicators.rsi(new double[]{5, 5, 5, 5, 5, 5}, 3);
        assertEquals(50d, Indicators.last(out), E);
    }

    @Test
    public void rsiSiempreEntreCeroYCien() {
        double[] closes = new double[60];
        for (int i = 0; i < closes.length; i++) closes[i] = 100 + Math.sin(i) * 12 + (i % 7);
        for (double v : Indicators.rsi(closes, 14)) {
            if (Double.isNaN(v)) continue;
            assertTrue(v >= 0d && v <= 100d, "RSI fuera de rango: " + v);
        }
    }

    @Test
    public void rsiEmpiezaExactamenteEnElIndiceDelPeriodo() {
        double[] out = Indicators.rsi(new double[]{1, 2, 3, 4, 5, 6}, 3);
        assertNaN(out[2], "aun no hay 3 deltas");
        assertTrue(!Double.isNaN(out[3]), "el primer RSI va en el indice = periodo");
    }

    // ----------------------------------------------------------------- MACD

    @Test
    public void macdHistogramaEsLineaMenosSenal() {
        double[] closes = new double[80];
        for (int i = 0; i < closes.length; i++) closes[i] = 100 + i * 0.5 + Math.sin(i) * 3;
        Indicators.Macd m = Indicators.macd(closes, 12, 26, 9);
        int comprobados = 0;
        for (int i = 0; i < closes.length; i++) {
            if (Double.isNaN(m.signal[i])) continue;
            assertEquals(m.line[i] - m.signal[i], m.histogram[i], E);
            comprobados++;
        }
        assertTrue(comprobados > 0, "no hubo ningun punto con senal valida");
    }

    @Test
    public void macdConSerieCortaNoRevienta() {
        Indicators.Macd m = Indicators.macd(new double[]{1, 2, 3}, 12, 26, 9);
        assertNull(Indicators.last(m.line));
        assertNull(Indicators.last(m.signal));
    }

    // ------------------------------------------------------------ Bollinger

    @Test
    public void bollingerConSerieConstanteColapsaSobreLaMedia() {
        Indicators.Bollinger b = Indicators.bollinger(new double[]{7, 7, 7, 7, 7}, 3, 2d);
        assertEquals(7d, b.middle[4], E);
        assertEquals(7d, b.upper[4], E);   // desviacion 0
        assertEquals(7d, b.lower[4], E);
    }

    @Test
    public void bollingerUsaDesviacionPoblacional() {
        // [2,4,6] -> media 4; sd poblacional = sqrt(((-2)^2+0+2^2)/3) = sqrt(8/3)
        Indicators.Bollinger b = Indicators.bollinger(new double[]{2, 4, 6}, 3, 1d);
        double sd = Math.sqrt(8d / 3d);
        assertEquals(4d, b.middle[2], E);
        assertEquals(4d + sd, b.upper[2], E);
        assertEquals(4d - sd, b.lower[2], E);
    }

    @Test
    public void bollingerBandasSiempreEncierranALaMedia() {
        double[] closes = new double[40];
        for (int i = 0; i < closes.length; i++) closes[i] = 50 + Math.cos(i) * 4;
        Indicators.Bollinger b = Indicators.bollinger(closes, 20, 2d);
        for (int i = 0; i < closes.length; i++) {
            if (Double.isNaN(b.middle[i])) continue;
            assertTrue(b.upper[i] >= b.middle[i] && b.middle[i] >= b.lower[i], "bandas invertidas en " + i);
        }
    }

    // ------------------------------------------------------------------ ATR

    @Test
    public void trueRangeTomaElMayorDeLosTresRangos() {
        double[] h = {10, 12, 11};
        double[] l = {8, 9, 10};
        double[] c = {9, 11, 10};
        double[] tr = Indicators.trueRange(h, l, c);
        assertNaN(tr[0], "la primera barra no tiene cierre anterior");
        assertEquals(3d, tr[1], E);   // max(12-9=3, |12-9|=3, |9-9|=0)
        assertEquals(1d, tr[2], E);   // max(11-10=1, |11-11|=0, |10-11|=1)
    }

    @Test
    public void atrSuavizaConWilder() {
        double[] h = {10, 12, 11};
        double[] l = {8, 9, 10};
        double[] c = {9, 11, 10};
        double[] atr = Indicators.atr(h, l, c, 1);
        assertEquals(3d, atr[1], E);
        assertEquals(1d, atr[2], E);  // con periodo 1 Wilder degenera al TR actual
    }

    @Test
    public void atrNuncaEsNegativo() {
        int n = 50;
        double[] h = new double[n], l = new double[n], c = new double[n];
        for (int i = 0; i < n; i++) {
            c[i] = 100 + Math.sin(i) * 5;
            h[i] = c[i] + 1.5;
            l[i] = c[i] - 1.5;
        }
        for (double v : Indicators.atr(h, l, c, 14)) {
            if (Double.isNaN(v)) continue;
            assertTrue(v >= 0d, "ATR negativo: " + v);
        }
    }

    // ----------------------------------------------------------------- VWAP

    @Test
    public void vwapPonderaPorVolumen() {
        double[] p = {10, 20};
        long[] ts = {dia(1, 10), dia(1, 11)};
        double[] out = Indicators.vwapSession(p, p, p, new double[]{100, 100}, ts, CL);
        assertEquals(10d, out[0], E);
        assertEquals(15d, out[1], E);   // (10*100 + 20*100) / 200
    }

    @Test
    public void vwapSeReiniciaCadaDiaDeMercado() {
        // Es el punto critico: un VWAP que arrastra dias no sirve como referencia de ejecucion.
        double[] p = {10, 10, 20};
        long[] ts = {dia(1, 10), dia(1, 11), dia(2, 10)};
        double[] out = Indicators.vwapSession(p, p, p, new double[]{100, 100, 100}, ts, CL);
        assertEquals(10d, out[1], E);
        assertEquals(20d, out[2], E, "el dia 2 arranca de cero, no arrastra el dia 1");
    }

    @Test
    public void vwapIgnoraBarrasSinVolumen() {
        double[] p = {10, 999};
        long[] ts = {dia(1, 10), dia(1, 11)};
        double[] out = Indicators.vwapSession(p, p, p, new double[]{100, 0}, ts, CL);
        assertEquals(10d, out[1], E, "una barra sin volumen no debe mover el VWAP");
    }

    @Test
    public void vwapUsaElPrecioTipico() {
        double[] h = {12}, l = {6}, c = {9};   // tipico = (12+6+9)/3 = 9
        long[] ts = {dia(1, 10)};
        assertEquals(9d, Indicators.vwapSession(h, l, c, new double[]{50}, ts, CL)[0], E);
    }

    private static long dia(int day, int hour) {
        return java.time.ZonedDateTime.of(2026, 8, day, hour, 0, 0, 0, CL).toInstant().toEpochMilli();
    }

    // ---------------------------------------------------------- Estocastico

    @Test
    public void estocasticoEnElMaximoEsCienYEnElMinimoEsCero() {
        double[] h = {10, 10, 10};
        double[] l = {0, 0, 0};
        Indicators.Stochastic arriba = Indicators.stochastic(h, l, new double[]{5, 5, 10}, 3, 1);
        Indicators.Stochastic abajo = Indicators.stochastic(h, l, new double[]{5, 5, 0}, 3, 1);
        assertEquals(100d, arriba.k[2], E);
        assertEquals(0d, abajo.k[2], E);
    }

    @Test
    public void estocasticoConRangoCeroDevuelveCincuenta() {
        // Papel sin movimiento en la ventana: no se puede dividir por cero.
        double[] v = {5, 5, 5};
        assertEquals(50d, Indicators.stochastic(v, v, v, 3, 1).k[2], E);
    }

    // ------------------------------------------------------------------ OBV

    @Test
    public void obvSumaEnAlzaRestaEnBajaYNoMueveEnPlano() {
        double[] c = {10, 11, 10, 10};
        double[] vol = {0, 5, 3, 7};
        double[] out = Indicators.obv(c, vol);
        assertEquals(0d, out[0], E);
        assertEquals(5d, out[1], E);    // sube: +5
        assertEquals(2d, out[2], E);    // baja: -3
        assertEquals(2d, out[3], E);    // plano: no cambia
    }

    // ------------------------------------------------------------- Ichimoku

    /** Serie sintetica: high = c+1, low = c-1, con c creciente 1..n. */
    private static Indicators.Ichimoku ichi(int n, int tenkan, int kijun, int senkouB, int shift) {
        double[] h = new double[n], l = new double[n], c = new double[n];
        for (int i = 0; i < n; i++) {
            c[i] = i + 1;
            h[i] = c[i] + 1;
            l[i] = c[i] - 1;
        }
        return Indicators.ichimoku(h, l, c, tenkan, kijun, senkouB, shift);
    }

    @Test
    public void tenkanEsElPuntoMedioDelCanalDeSuVentana() {
        // Ventana 3 terminando en i=2: high max = 4 (c=3 +1), low min = 0 (c=1 -1) -> medio 2
        Indicators.Ichimoku k = ichi(10, 3, 5, 7, 2);
        assertNaN(k.tenkan[1], "warm-up de Tenkan");
        assertEquals(2d, k.tenkan[2], E);
        assertEquals(3d, k.tenkan[3], E);
    }

    @Test
    public void senkouSeProyectaHaciaAdelanteYElArregloEsMasLargo() {
        // Es el error clasico: si no se proyecta, la nube queda pegada al precio y no anticipa.
        int n = 20, shift = 26;
        Indicators.Ichimoku k = ichi(n, 9, 26, 52, shift);
        assertEquals(n + shift, k.senkouA.length, "Senkou A debe medir n + shift");
        assertEquals(n + shift, k.senkouB.length, "Senkou B debe medir n + shift");
        assertEquals(n, k.tenkan.length, "Tenkan va alineada 1:1");
        assertEquals(shift, k.shift);
    }

    @Test
    public void senkouAEsElPromedioDeTenkanYKijunDesplazado() {
        int shift = 2;
        Indicators.Ichimoku k = ichi(12, 3, 5, 7, shift);
        for (int i = 0; i < 12; i++) {
            if (Double.isNaN(k.tenkan[i]) || Double.isNaN(k.kijun[i])) continue;
            double esperado = (k.tenkan[i] + k.kijun[i]) / 2d;
            assertEquals(esperado, k.senkouA[i + shift], E, "Senkou A mal desplazada en " + i);
        }
    }

    @Test
    public void chikouEsElCierreDesplazadoHaciaAtras() {
        int n = 10, shift = 3;
        Indicators.Ichimoku k = ichi(n, 3, 5, 7, shift);
        // close[i] = i+1, y chikou[i-shift] = close[i]
        assertEquals(4d, k.chikou[0], E);   // close[3]
        assertEquals(10d, k.chikou[6], E);  // close[9]
        for (int i = n - shift; i < n; i++) {
            assertNaN(k.chikou[i], "las ultimas " + shift + " barras no tienen Chikou");
        }
    }

    @Test
    public void ichimokuConSerieCortaNoRevienta() {
        Indicators.Ichimoku k = ichi(3, 9, 26, 52, 26);
        assertNull(Indicators.last(k.tenkan));
        assertNull(Indicators.last(k.senkouA));
        assertEquals(3 + 26, k.senkouA.length);
    }

    // -------------------------------------------------------------- helpers

    @Test
    public void lastDevuelveElUltimoNoNaN() {
        assertEquals(3d, Indicators.last(new double[]{1, 2, 3}), E);
        assertEquals(2d, Indicators.last(new double[]{1, 2, Double.NaN}), E);
        assertNull(Indicators.last(new double[]{Double.NaN, Double.NaN}));
        assertNull(Indicators.last(null));
    }

    @Test
    public void entradasNulasNoRevientan() {
        assertEquals(0, Indicators.sma(null, 5).length);
        assertEquals(0, Indicators.ema(null, 5).length);
        assertEquals(0, Indicators.rsi(null, 14).length);
        assertEquals(0, Indicators.atr(null, null, null, 14).length);
        assertEquals(0, Indicators.obv(null, null).length);
        assertNull(Indicators.last(Indicators.macd(null, 12, 26, 9).line));
    }
}
