package cl.vc.blotter.utils;

import java.time.LocalDate;
import java.time.ZoneId;

/**
 * Indicadores tecnicos. Clase pura: sin JavaFX, sin JFreeChart, sin estado.
 *
 * POR QUE EXISTE: la matematica vivia dentro de CandleController y devolvia UN solo valor (el
 * ultimo), asi que RSI y MACD solo se podian mostrar como texto en un label; graficarlos era
 * imposible. Aca todo devuelve la SERIE completa, alineada 1:1 con el arreglo de entrada.
 *
 * CONVENCION: los tramos sin valor suficiente (warm-up) van con {@link Double#NaN}, no con null.
 * JFreeChart interpreta NaN como hueco y no dibuja, que es justo lo que se quiere; y evita el
 * boxing en series de miles de puntos.
 *
 * SUAVIZADO: RSI y ATR usan suavizado de Wilder (el estandar de las plataformas profesionales),
 * no un promedio simple. Cambiarlo altera los valores respecto de cualquier otra plataforma.
 */
public final class Indicators {

    private Indicators() {
    }

    // ---------------------------------------------------------------- medias

    /** Media movil simple. out[i] = promedio de v[i-period+1..i]. */
    public static double[] sma(double[] v, int period) {
        double[] out = nan(v == null ? 0 : v.length);
        if (v == null || period <= 0 || v.length < period) return out;
        double sum = 0d;
        for (int i = 0; i < v.length; i++) {
            sum += v[i];
            if (i >= period) sum -= v[i - period];
            if (i >= period - 1) out[i] = sum / period;
        }
        return out;
    }

    /**
     * Media movil exponencial. Se siembra con la SMA de las primeras `period` muestras en el
     * indice period-1 y de ahi es recursiva, que es la convencion de las plataformas de mercado.
     */
    public static double[] ema(double[] v, int period) {
        double[] out = nan(v == null ? 0 : v.length);
        if (v == null || period <= 0 || v.length < period) return out;
        double k = 2d / (period + 1d);
        double seed = 0d;
        for (int i = 0; i < period; i++) seed += v[i];
        out[period - 1] = seed / period;
        for (int i = period; i < v.length; i++) {
            out[i] = (v[i] - out[i - 1]) * k + out[i - 1];
        }
        return out;
    }

    // ------------------------------------------------------------------ RSI

    /** RSI con suavizado de Wilder. Rango 0..100. */
    public static double[] rsi(double[] close, int period) {
        double[] out = nan(close == null ? 0 : close.length);
        if (close == null || period <= 0 || close.length <= period) return out;

        double gain = 0d;
        double loss = 0d;
        for (int i = 1; i <= period; i++) {
            double d = close[i] - close[i - 1];
            if (d >= 0) gain += d; else loss -= d;
        }
        double avgGain = gain / period;
        double avgLoss = loss / period;
        out[period] = rsiFrom(avgGain, avgLoss);

        for (int i = period + 1; i < close.length; i++) {
            double d = close[i] - close[i - 1];
            double g = d > 0 ? d : 0d;
            double l = d < 0 ? -d : 0d;
            // Wilder: avg = (avg*(p-1) + actual) / p
            avgGain = (avgGain * (period - 1) + g) / period;
            avgLoss = (avgLoss * (period - 1) + l) / period;
            out[i] = rsiFrom(avgGain, avgLoss);
        }
        return out;
    }

    private static double rsiFrom(double avgGain, double avgLoss) {
        if (avgLoss == 0d) return avgGain == 0d ? 50d : 100d;  // sin perdidas: saturado arriba
        double rs = avgGain / avgLoss;
        return 100d - (100d / (1d + rs));
    }

    // ----------------------------------------------------------------- MACD

    public static final class Macd {
        public final double[] line;
        public final double[] signal;
        public final double[] histogram;

        Macd(double[] line, double[] signal, double[] histogram) {
            this.line = line;
            this.signal = signal;
            this.histogram = histogram;
        }
    }

    /** MACD clasico: linea = EMA(fast) - EMA(slow), senal = EMA(signal) de la linea. */
    public static Macd macd(double[] close, int fast, int slow, int signalPeriod) {
        int n = close == null ? 0 : close.length;
        double[] line = nan(n);
        double[] signal = nan(n);
        double[] hist = nan(n);
        if (close == null || fast <= 0 || slow <= 0 || signalPeriod <= 0 || n == 0) {
            return new Macd(line, signal, hist);
        }
        double[] emaFast = ema(close, fast);
        double[] emaSlow = ema(close, slow);
        int firstValid = -1;
        for (int i = 0; i < n; i++) {
            if (!Double.isNaN(emaFast[i]) && !Double.isNaN(emaSlow[i])) {
                line[i] = emaFast[i] - emaSlow[i];
                if (firstValid < 0) firstValid = i;
            }
        }
        if (firstValid < 0) return new Macd(line, signal, hist);

        // La EMA de la senal se calcula SOLO sobre el tramo valido de la linea y se remapea:
        // si se corriera sobre el arreglo completo, los NaN del warm-up contaminarian la siembra.
        int len = n - firstValid;
        double[] compact = new double[len];
        System.arraycopy(line, firstValid, compact, 0, len);
        double[] compactSignal = ema(compact, signalPeriod);
        for (int i = 0; i < len; i++) {
            if (!Double.isNaN(compactSignal[i])) {
                signal[firstValid + i] = compactSignal[i];
                hist[firstValid + i] = compact[i] - compactSignal[i];
            }
        }
        return new Macd(line, signal, hist);
    }

    // ------------------------------------------------------------ Bollinger

    public static final class Bollinger {
        public final double[] middle;
        public final double[] upper;
        public final double[] lower;

        Bollinger(double[] middle, double[] upper, double[] lower) {
            this.middle = middle;
            this.upper = upper;
            this.lower = lower;
        }
    }

    /**
     * Bandas de Bollinger. Usa desviacion estandar POBLACIONAL sobre la ventana (divide por n),
     * que es lo que hacen las plataformas de mercado; la muestral (n-1) da bandas mas anchas.
     */
    public static Bollinger bollinger(double[] close, int period, double k) {
        int n = close == null ? 0 : close.length;
        double[] mid = sma(close, period);
        double[] up = nan(n);
        double[] low = nan(n);
        if (close == null || period <= 0 || n < period) return new Bollinger(mid, up, low);

        for (int i = period - 1; i < n; i++) {
            double m = mid[i];
            double acc = 0d;
            for (int j = i - period + 1; j <= i; j++) {
                double d = close[j] - m;
                acc += d * d;
            }
            double sd = Math.sqrt(acc / period);
            up[i] = m + k * sd;
            low[i] = m - k * sd;
        }
        return new Bollinger(mid, up, low);
    }

    // ------------------------------------------------------------------ ATR

    /** True Range de cada barra. La primera queda NaN: necesita el cierre anterior. */
    public static double[] trueRange(double[] high, double[] low, double[] close) {
        int n = len(high, low, close);
        double[] out = nan(n);
        for (int i = 1; i < n; i++) {
            double hl = high[i] - low[i];
            double hc = Math.abs(high[i] - close[i - 1]);
            double lc = Math.abs(low[i] - close[i - 1]);
            out[i] = Math.max(hl, Math.max(hc, lc));
        }
        return out;
    }

    /** ATR con suavizado de Wilder. Sirve para dimensionar stops, no para direccion. */
    public static double[] atr(double[] high, double[] low, double[] close, int period) {
        int n = len(high, low, close);
        double[] out = nan(n);
        if (period <= 0 || n <= period) return out;
        double[] tr = trueRange(high, low, close);

        double sum = 0d;
        for (int i = 1; i <= period; i++) sum += tr[i];
        out[period] = sum / period;
        for (int i = period + 1; i < n; i++) {
            out[i] = (out[i - 1] * (period - 1) + tr[i]) / period;
        }
        return out;
    }

    // ----------------------------------------------------------------- VWAP

    /**
     * VWAP acumulado por sesion: Σ(precioTipico × volumen) / Σ(volumen), reiniciado en cada dia
     * de mercado.
     *
     * El reinicio diario NO es un detalle: un VWAP que arrastra dias es inutil como referencia de
     * ejecucion, que es justo para lo que la mesa lo mira (el core tiene una estrategia VWAP).
     *
     * @param epochMillis timestamp de cada barra, para detectar el cambio de dia
     * @param zone        zona del mercado (America/Santiago), no la del sistema
     */
    public static double[] vwapSession(double[] high, double[] low, double[] close,
                                       double[] volume, long[] epochMillis, ZoneId zone) {
        int n = len(high, low, close);
        double[] out = nan(n);
        if (volume == null || epochMillis == null || volume.length < n || epochMillis.length < n) {
            return out;
        }
        ZoneId z = zone == null ? ZoneId.systemDefault() : zone;
        LocalDate currentDay = null;
        double pv = 0d;
        double vol = 0d;
        for (int i = 0; i < n; i++) {
            LocalDate day = java.time.Instant.ofEpochMilli(epochMillis[i]).atZone(z).toLocalDate();
            if (currentDay == null || !currentDay.equals(day)) {
                currentDay = day;
                pv = 0d;
                vol = 0d;
            }
            double typical = (high[i] + low[i] + close[i]) / 3d;
            double v = volume[i];
            if (v > 0d) {
                pv += typical * v;
                vol += v;
            }
            out[i] = vol > 0d ? pv / vol : Double.NaN;
        }
        return out;
    }

    // ---------------------------------------------------------- Estocastico

    public static final class Stochastic {
        public final double[] k;
        public final double[] d;

        Stochastic(double[] k, double[] d) {
            this.k = k;
            this.d = d;
        }
    }

    /** Estocastico %K/%D. %K sobre la ventana, %D = SMA(%K). */
    public static Stochastic stochastic(double[] high, double[] low, double[] close,
                                        int kPeriod, int dPeriod) {
        int n = len(high, low, close);
        double[] k = nan(n);
        if (kPeriod > 0 && n >= kPeriod) {
            for (int i = kPeriod - 1; i < n; i++) {
                double hh = Double.NEGATIVE_INFINITY;
                double ll = Double.POSITIVE_INFINITY;
                for (int j = i - kPeriod + 1; j <= i; j++) {
                    if (high[j] > hh) hh = high[j];
                    if (low[j] < ll) ll = low[j];
                }
                double rango = hh - ll;
                // Rango cero (papel sin movimiento en la ventana): 50 es la convencion, no 0/0.
                k[i] = rango == 0d ? 50d : 100d * (close[i] - ll) / rango;
            }
        }
        return new Stochastic(k, sma(k, dPeriod));
    }

    // ------------------------------------------------------------------ OBV

    /** On-Balance Volume acumulado. Mide si el volumen acompana al precio. */
    public static double[] obv(double[] close, double[] volume) {
        int n = close == null ? 0 : close.length;
        double[] out = nan(n);
        if (close == null || volume == null || volume.length < n || n == 0) return out;
        double acc = 0d;
        out[0] = 0d;
        for (int i = 1; i < n; i++) {
            if (close[i] > close[i - 1]) acc += volume[i];
            else if (close[i] < close[i - 1]) acc -= volume[i];
            out[i] = acc;
        }
        return out;
    }

    // ------------------------------------------------------------- Ichimoku

    public static final class Ichimoku {
        /** Tenkan-sen: alineada 1:1 con la entrada. */
        public final double[] tenkan;
        /** Kijun-sen: alineada 1:1 con la entrada. */
        public final double[] kijun;
        /** Chikou span: cierre desplazado HACIA ATRAS `shift` barras. Largo n. */
        public final double[] chikou;
        /** Senkou A: proyectada HACIA ADELANTE `shift` barras. Largo n + shift. */
        public final double[] senkouA;
        /** Senkou B: proyectada HACIA ADELANTE `shift` barras. Largo n + shift. */
        public final double[] senkouB;
        /** Barras de desplazamiento (26 por convencion). */
        public final int shift;

        Ichimoku(double[] tenkan, double[] kijun, double[] chikou,
                 double[] senkouA, double[] senkouB, int shift) {
            this.tenkan = tenkan;
            this.kijun = kijun;
            this.chikou = chikou;
            this.senkouA = senkouA;
            this.senkouB = senkouB;
            this.shift = shift;
        }
    }

    /**
     * Ichimoku Kinko Hyo.
     *
     * OJO CON LOS DESPLAZAMIENTOS, que es donde casi todas las implementaciones se equivocan:
     *  - Senkou A y B se dibujan `shift` barras EN EL FUTURO (mas alla de la ultima vela). Por eso
     *    sus arreglos miden n + shift: los ultimos `shift` puntos no tienen barra asociada todavia
     *    y quien grafique debe extrapolar los timestamps.
     *  - Chikou es el cierre dibujado `shift` barras EN EL PASADO, asi que su arreglo mide n y los
     *    ultimos `shift` valores quedan NaN.
     * Sin esos corrimientos la "nube" queda pegada al precio y no anticipa nada, que es justo su uso.
     *
     * @param tenkanP  9 por convencion
     * @param kijunP   26
     * @param senkouBP 52
     * @param shift    26
     */
    public static Ichimoku ichimoku(double[] high, double[] low, double[] close,
                                    int tenkanP, int kijunP, int senkouBP, int shift) {
        int n = len(high, low, close);
        int s = Math.max(0, shift);
        double[] tenkan = midChannel(high, low, tenkanP, n);
        double[] kijun = midChannel(high, low, kijunP, n);
        double[] baseB = midChannel(high, low, senkouBP, n);

        double[] senkouA = nan(n + s);
        double[] senkouB = nan(n + s);
        for (int i = 0; i < n; i++) {
            if (!Double.isNaN(tenkan[i]) && !Double.isNaN(kijun[i])) {
                senkouA[i + s] = (tenkan[i] + kijun[i]) / 2d;
            }
            if (!Double.isNaN(baseB[i])) {
                senkouB[i + s] = baseB[i];
            }
        }

        double[] chikou = nan(n);
        for (int i = s; i < n; i++) {
            chikou[i - s] = close[i];
        }

        return new Ichimoku(tenkan, kijun, chikou, senkouA, senkouB, s);
    }

    /** (maximo mas alto + minimo mas bajo) / 2 sobre la ventana. Base de todas las lineas Ichimoku. */
    private static double[] midChannel(double[] high, double[] low, int period, int n) {
        double[] out = nan(n);
        if (period <= 0 || n < period) return out;
        for (int i = period - 1; i < n; i++) {
            double hh = Double.NEGATIVE_INFINITY;
            double ll = Double.POSITIVE_INFINITY;
            for (int j = i - period + 1; j <= i; j++) {
                if (high[j] > hh) hh = high[j];
                if (low[j] < ll) ll = low[j];
            }
            out[i] = (hh + ll) / 2d;
        }
        return out;
    }

    // -------------------------------------------------------------- helpers

    private static double[] nan(int n) {
        double[] a = new double[Math.max(0, n)];
        java.util.Arrays.fill(a, Double.NaN);
        return a;
    }

    /** Largo comun de los tres arreglos; 0 si alguno es null o no calzan. */
    private static int len(double[] high, double[] low, double[] close) {
        if (high == null || low == null || close == null) return 0;
        int n = Math.min(high.length, Math.min(low.length, close.length));
        return Math.max(0, n);
    }

    /** Ultimo valor no-NaN de una serie, o null si no hay ninguno. Para los labels numericos. */
    public static Double last(double[] serie) {
        if (serie == null) return null;
        for (int i = serie.length - 1; i >= 0; i--) {
            if (!Double.isNaN(serie[i])) return serie[i];
        }
        return null;
    }
}
