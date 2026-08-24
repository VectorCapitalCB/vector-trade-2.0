package cl.vc.blotter.utils;

import java.util.prefs.Preferences;

/**
 * Preferencias del banner (la huincha superior).
 *
 * Hoy la huincha muestra fijo los 12 papeles mas transados del dia, del ranking `mas_tranzado` del
 * BolsaStats. Esto permite elegir entre ese ranking y los papeles del portafolio del operador.
 *
 * POR QUE PORTAFOLIO Y NO UNA LISTA LIBRE: los simbolos de los portafolios YA estan suscritos a
 * market data, asi que el costo incremental es cero y ademas se ven en vivo en vez de cada 30 s.
 * Una lista libre exigiria suscripciones propias, y hoy {@code Repository.unSuscripcion()} es un
 * metodo vacio con un //todo: cada papel agregado quedaria suscrito con FULL_BOOK hasta cerrar la
 * app. Eso hay que arreglarlo antes (con refcount, porque el libro principal, el multibook y los
 * portafolios comparten los mismos topics).
 */
public final class BannerPrefs {

    public enum Fuente {
        /** Ranking mas_tranzado del BolsaStats: el comportamiento historico. */
        RANKING,
        /** Papeles del portafolio del operador: ya suscritos, precio en vivo. */
        PORTAFOLIO
    }

    public static final int MIN_PAPELES = 4;
    public static final int MAX_PAPELES = 30;
    public static final int DEFAULT_PAPELES = 12;

    private static final Preferences PREFS =
            Preferences.userNodeForPackage(BannerPrefs.class).node("banner");

    private BannerPrefs() {
    }

    public static Fuente fuente() {
        try {
            return Fuente.valueOf(PREFS.get("fuente", Fuente.RANKING.name()));
        } catch (Exception e) {
            return Fuente.RANKING;   // preferencia corrupta: no dejar el banner sin fuente
        }
    }

    public static void setFuente(Fuente f) {
        if (f != null) PREFS.put("fuente", f.name());
    }

    /** Cuantos papeles mostrar. Acotado: bajo 4 no se ve un banner y sobre 30 no alcanza a leerse. */
    public static int papeles() {
        return clamp(PREFS.getInt("papeles", DEFAULT_PAPELES));
    }

    public static void setPapeles(int n) {
        PREFS.putInt("papeles", clamp(n));
    }

    private static int clamp(int n) {
        return Math.max(MIN_PAPELES, Math.min(MAX_PAPELES, n));
    }
}
