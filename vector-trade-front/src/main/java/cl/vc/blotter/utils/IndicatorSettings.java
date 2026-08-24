package cl.vc.blotter.utils;

import java.util.prefs.Preferences;

/**
 * Parametros de los indicadores del grafico de velas, editables por el operador y persistidos.
 *
 * Todo se normaliza en {@link #normalizar()}: los periodos van a un rango sano y las relaciones que
 * invalidan un indicador se corrigen en vez de dejar que dibuje basura plausible. En particular
 * MACD con fast >= slow da un histograma invertido que parece un dato y no lo es.
 *
 * Se guarda en {@link Preferences} igual que {@link ChartIndicator} y el resto del blotter.
 */
public final class IndicatorSettings {

    /** Un periodo mas alla de esto no aporta y hace lenta la ventana movil de Bollinger/Ichimoku. */
    public static final int MIN_PERIODO = 1;
    public static final int MAX_PERIODO = 500;
    public static final double MIN_K = 0.1d;
    public static final double MAX_K = 10d;

    public int smaPeriod = 20;
    public int emaPeriod = 20;

    public int bollingerPeriod = 20;
    public double bollingerK = 2.0d;

    public int rsiPeriod = 14;

    public int macdFast = 12;
    public int macdSlow = 26;
    public int macdSignal = 9;

    public int atrPeriod = 14;

    public int ichimokuTenkan = 9;
    public int ichimokuKijun = 26;
    public int ichimokuSenkouB = 52;
    public int ichimokuShift = 26;

    private static final Preferences PREFS =
            Preferences.userNodeForPackage(IndicatorSettings.class).node("candle-indicator-params");

    public IndicatorSettings() {
    }

    /** Copia, para que el dialogo edite un borrador y sólo se aplique al aceptar. */
    public IndicatorSettings copy() {
        IndicatorSettings s = new IndicatorSettings();
        s.smaPeriod = smaPeriod;
        s.emaPeriod = emaPeriod;
        s.bollingerPeriod = bollingerPeriod;
        s.bollingerK = bollingerK;
        s.rsiPeriod = rsiPeriod;
        s.macdFast = macdFast;
        s.macdSlow = macdSlow;
        s.macdSignal = macdSignal;
        s.atrPeriod = atrPeriod;
        s.ichimokuTenkan = ichimokuTenkan;
        s.ichimokuKijun = ichimokuKijun;
        s.ichimokuSenkouB = ichimokuSenkouB;
        s.ichimokuShift = ichimokuShift;
        return s;
    }

    /**
     * Deja los valores en un estado usable. Corrige en vez de rechazar: el operador esta moviendo
     * spinners, no llenando un formulario, y un indicador que desaparece sin explicacion es peor
     * que uno ajustado al limite.
     */
    public IndicatorSettings normalizar() {
        smaPeriod = clampP(smaPeriod, 20);
        emaPeriod = clampP(emaPeriod, 20);
        bollingerPeriod = clampP(bollingerPeriod, 20);
        rsiPeriod = clampP(rsiPeriod, 14);
        atrPeriod = clampP(atrPeriod, 14);
        macdFast = clampP(macdFast, 12);
        macdSlow = clampP(macdSlow, 26);
        macdSignal = clampP(macdSignal, 9);
        ichimokuTenkan = clampP(ichimokuTenkan, 9);
        ichimokuKijun = clampP(ichimokuKijun, 26);
        ichimokuSenkouB = clampP(ichimokuSenkouB, 52);
        ichimokuShift = clampP(ichimokuShift, 26);

        if (Double.isNaN(bollingerK) || Double.isInfinite(bollingerK)) bollingerK = 2.0d;
        bollingerK = Math.max(MIN_K, Math.min(MAX_K, bollingerK));

        // MACD: la linea es EMA(fast) - EMA(slow). Con fast >= slow el signo se invierte y el
        // histograma miente. Se baja fast por debajo de slow.
        if (macdFast >= macdSlow) {
            macdFast = Math.max(MIN_PERIODO, macdSlow - 1);
        }
        return this;
    }

    private static int clampP(int v, int fallback) {
        if (v < MIN_PERIODO || v > MAX_PERIODO) {
            return Math.max(MIN_PERIODO, Math.min(MAX_PERIODO, v == 0 ? fallback : v));
        }
        return v;
    }

    // ------------------------------------------------------------ etiquetas

    /** Texto del check en el menu, con los parametros vigentes. */
    public String label(ChartIndicator ind) {
        switch (ind) {
            case SMA20:     return "SMA " + smaPeriod;
            case EMA20:     return "EMA " + emaPeriod;
            case BOLLINGER: return "Bollinger (" + bollingerPeriod + ", " + trim(bollingerK) + ")";
            case VWAP:      return "VWAP sesión";
            case ICHIMOKU:  return "Ichimoku (" + ichimokuTenkan + "/" + ichimokuKijun
                    + "/" + ichimokuSenkouB + ")";
            case RSI:       return "RSI " + rsiPeriod;
            case MACD:      return "MACD (" + macdFast + ", " + macdSlow + ", " + macdSignal + ")";
            case ATR:       return "ATR " + atrPeriod;
            default:        return ind.name();
        }
    }

    /** 2.0 -> "2", 2.5 -> "2.5": evita "Bollinger (20, 2.0)". */
    static String trim(double v) {
        return v == Math.rint(v) ? String.valueOf((long) v) : String.valueOf(v);
    }

    // --------------------------------------------------------- persistencia

    public static IndicatorSettings cargar() {
        IndicatorSettings s = new IndicatorSettings();
        s.smaPeriod = PREFS.getInt("smaPeriod", s.smaPeriod);
        s.emaPeriod = PREFS.getInt("emaPeriod", s.emaPeriod);
        s.bollingerPeriod = PREFS.getInt("bollingerPeriod", s.bollingerPeriod);
        s.bollingerK = PREFS.getDouble("bollingerK", s.bollingerK);
        s.rsiPeriod = PREFS.getInt("rsiPeriod", s.rsiPeriod);
        s.macdFast = PREFS.getInt("macdFast", s.macdFast);
        s.macdSlow = PREFS.getInt("macdSlow", s.macdSlow);
        s.macdSignal = PREFS.getInt("macdSignal", s.macdSignal);
        s.atrPeriod = PREFS.getInt("atrPeriod", s.atrPeriod);
        s.ichimokuTenkan = PREFS.getInt("ichimokuTenkan", s.ichimokuTenkan);
        s.ichimokuKijun = PREFS.getInt("ichimokuKijun", s.ichimokuKijun);
        s.ichimokuSenkouB = PREFS.getInt("ichimokuSenkouB", s.ichimokuSenkouB);
        s.ichimokuShift = PREFS.getInt("ichimokuShift", s.ichimokuShift);
        // Normalizar SIEMPRE al cargar: unas preferencias viejas o editadas a mano no deben
        // poder dejar el grafico en un estado invalido.
        return s.normalizar();
    }

    public void guardar() {
        normalizar();
        PREFS.putInt("smaPeriod", smaPeriod);
        PREFS.putInt("emaPeriod", emaPeriod);
        PREFS.putInt("bollingerPeriod", bollingerPeriod);
        PREFS.putDouble("bollingerK", bollingerK);
        PREFS.putInt("rsiPeriod", rsiPeriod);
        PREFS.putInt("macdFast", macdFast);
        PREFS.putInt("macdSlow", macdSlow);
        PREFS.putInt("macdSignal", macdSignal);
        PREFS.putInt("atrPeriod", atrPeriod);
        PREFS.putInt("ichimokuTenkan", ichimokuTenkan);
        PREFS.putInt("ichimokuKijun", ichimokuKijun);
        PREFS.putInt("ichimokuSenkouB", ichimokuSenkouB);
        PREFS.putInt("ichimokuShift", ichimokuShift);
    }
}
