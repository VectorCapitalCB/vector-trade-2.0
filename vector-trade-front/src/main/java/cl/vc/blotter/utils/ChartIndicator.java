package cl.vc.blotter.utils;

import java.util.EnumSet;
import java.util.Set;
import java.util.prefs.Preferences;

/**
 * Indicadores que se pueden prender/apagar en el grafico de velas.
 *
 * Por que existe: con todos encendidos a la vez el panel de precio queda ilegible (cuatro medias
 * mas las bandas mas la nube sobre las velas). El default deliberadamente NO los prende todos.
 *
 * La seleccion se guarda en {@link Preferences} igual que el resto de las preferencias del blotter
 * ({@code Repository.prefs}, {@code WindowGeometryStore}), asi sobrevive a cerrar la ventana.
 */
public enum ChartIndicator {

    // ---- sobre el precio ----
    SMA20(Grupo.PRECIO, true),
    EMA20(Grupo.PRECIO, true),
    BOLLINGER(Grupo.PRECIO, false),
    VWAP(Grupo.PRECIO, true),
    ICHIMOKU(Grupo.PRECIO, false),

    // ---- paneles propios debajo del precio ----
    RSI(Grupo.PANEL, true),
    MACD(Grupo.PANEL, true),
    ATR(Grupo.PANEL, false);

    public enum Grupo {
        PRECIO("Sobre el precio"),
        PANEL("Paneles");

        public final String titulo;

        Grupo(String titulo) {
            this.titulo = titulo;
        }
    }

    public final Grupo grupo;
    public final boolean porDefecto;

    ChartIndicator(Grupo grupo, boolean porDefecto) {
        this.grupo = grupo;
        this.porDefecto = porDefecto;
    }

    /**
     * Texto visible, que depende de los parametros vigentes ("SMA 50", no "SMA 20").
     * Vive en {@link IndicatorSettings#label} para no duplicar el formateo.
     */
    public String etiqueta(IndicatorSettings settings) {
        return settings == null ? name() : settings.label(this);
    }

    private static final Preferences PREFS =
            Preferences.userNodeForPackage(ChartIndicator.class).node("candle-indicators");

    /** Los que vienen encendidos de fabrica: un grafico limpio, no todos a la vez. */
    public static Set<ChartIndicator> porDefectoSet() {
        Set<ChartIndicator> out = EnumSet.noneOf(ChartIndicator.class);
        for (ChartIndicator i : values()) {
            if (i.porDefecto) out.add(i);
        }
        return out;
    }

    /**
     * Lee la seleccion guardada. Si un indicador nunca se guardo (por ejemplo porque se agrego
     * despues), cae a su valor por defecto en vez de quedar apagado en silencio.
     */
    public static Set<ChartIndicator> cargar() {
        Set<ChartIndicator> out = EnumSet.noneOf(ChartIndicator.class);
        for (ChartIndicator i : values()) {
            if (PREFS.getBoolean(i.name(), i.porDefecto)) out.add(i);
        }
        return out;
    }

    public static void guardar(Set<ChartIndicator> seleccion) {
        if (seleccion == null) return;
        for (ChartIndicator i : values()) {
            PREFS.putBoolean(i.name(), seleccion.contains(i));
        }
    }
}
