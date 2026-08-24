package cl.vc.blotter.utils;

import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.paint.Color;

/**
 * Mini grafico de linea de una serie intradia. Lo comparten la columna "Tendencia" de Datos
 * del Mercado y la huincha de papeles, asi el mismo papel se ve igual en los dos lados.
 *
 * La serie del backend trae NaN antes del primer trade del papel: esos puntos no se dibujan
 * y tampoco entran en el rango vertical.
 *
 * REFERENCIA: EL CIERRE DE AYER, NO EL PRIMER PRECIO DEL DIA.
 * La tendencia de un papel se mide contra el cierre anterior; es la convencion de mercado y es
 * la misma base con que se calcula la variacion %. Antes esta clase comparaba el ultimo precio
 * contra el PRIMERO de la serie: un papel que abria con gap a la baja y despues subia toda la
 * rueda salia verde, cuando para el mercado estaba rojo. Ademas se dibuja la linea del cierre
 * anterior, sin la cual no se puede ver de un vistazo si el papel esta arriba o abajo.
 */
public final class Sparkline {

    private static final double ESCALA_MINIMA = 0.02d;
    private static final double MARGEN_ESCALA = 1.15d;
    private static final Color SUBE = Color.web("#23a126");
    private static final Color BAJA = Color.web("#de292c");
    private static final Color PLANO = Color.web("#7d8487");
    private static final Color SIN_DATOS = Color.web("#5c6365");
    private static final Color REFERENCIA = Color.web("#7d8487", 0.55);

    private Sparkline() {
    }

    /** Sin cierre anterior conocido: cae a comparar contra el primer precio de la serie. */
    public static void pintar(Canvas lienzo, double[] serie) {
        pintar(lienzo, serie, Double.NaN);
    }

    /**
     * @param previousClose cierre del dia habil anterior; NaN o <= 0 si no se conoce todavia
     *                      (por ejemplo mientras la cache de Mongo del core aun no lo tiene).
     */
    public static void pintar(Canvas lienzo, double[] serie, double previousClose) {
        if (lienzo == null) return;
        double[] serieDibujo = serieConReferencia(serie, previousClose);
        if (contarValidos(serieDibujo) < 2) {
            pintarSinDatos(lienzo);
            return;
        }

        GraphicsContext g = lienzo.getGraphicsContext2D();
        double w = lienzo.getWidth(), h = lienzo.getHeight();
        g.clearRect(0, 0, w, h);

        double[] rango = rangoVisual(serieDibujo, previousClose);
        double min = rango[0], max = rango[1];
        double amplitud = max - min;

        // Papel plano y pegado a su referencia: una linea al medio en vez de dividir por cero.
        if (amplitud <= 0) {
            g.setStroke(PLANO);
            g.setLineWidth(1);
            g.strokeLine(1, h / 2, w - 1, h / 2);
            return;
        }

        // Linea del cierre anterior: es la que permite leer el sparkline. Va primero, de fondo.
        if (referenciaValida(previousClose)) {
            double yRef = h - 2 - ((previousClose - min) / amplitud) * (h - 4);
            g.setStroke(REFERENCIA);
            g.setLineWidth(1);
            g.setLineDashes(2, 2);
            g.strokeLine(1, yRef, w - 1, yRef);
            g.setLineDashes(null);   // no dejar el patron activo para el trazo siguiente
        }

        g.setStroke(color(serieDibujo, previousClose));
        g.setLineWidth(1.5);

        double paso = (w - 2) / (serieDibujo.length - 1);
        double xPrev = Double.NaN, yPrev = Double.NaN;
        for (int i = 0; i < serieDibujo.length; i++) {
            if (Double.isNaN(serieDibujo[i])) continue;
            double x = 1 + i * paso;
            double y = h - 2 - ((serieDibujo[i] - min) / amplitud) * (h - 4);
            if (!Double.isNaN(xPrev)) {
                g.strokeLine(xPrev, yPrev, x, y);
            }
            xPrev = x;
            yPrev = y;
        }
    }

    // ------------------------------------------------------------------------
    //  Logica pura: separada del dibujo para poder testearla sin toolkit JavaFX.
    // ------------------------------------------------------------------------

    static boolean referenciaValida(double previousClose) {
        return !Double.isNaN(previousClose) && !Double.isInfinite(previousClose) && previousClose > 0d;
    }

    static double[] serieConReferencia(double[] serie, double previousClose) {
        if (!referenciaValida(previousClose)) {
            return serie == null ? new double[0] : serie;
        }
        int validos = contarValidos(serie);
        if (validos == 0) {
            return new double[]{previousClose};
        }
        double primero = Double.NaN;
        if (serie != null) {
            for (double v : serie) {
                if (!Double.isNaN(v)) {
                    primero = v;
                    break;
                }
            }
        }
        if (!Double.isNaN(primero) && Math.abs(primero - previousClose) < 0.0000001d) {
            return serie;
        }
        double[] out = new double[(serie == null ? 0 : serie.length) + 1];
        out[0] = previousClose;
        if (serie != null) {
            System.arraycopy(serie, 0, out, 1, serie.length);
        }
        return out;
    }

    /**
     * Color del trazo: verde si el ultimo precio esta sobre el cierre anterior, rojo si esta
     * debajo, gris si esta igual. Sin cierre anterior se compara contra el primer precio valido
     * de la serie, que era el comportamiento anterior.
     */
    static Color color(double[] serie, double previousClose) {
        double primero = Double.NaN, ultimo = Double.NaN;
        if (serie != null) {
            for (double v : serie) {
                if (Double.isNaN(v)) continue;
                if (Double.isNaN(primero)) primero = v;
                ultimo = v;
            }
        }
        if (Double.isNaN(ultimo)) return PLANO;

        double referencia = referenciaValida(previousClose) ? previousClose : primero;
        if (Double.isNaN(referencia)) return PLANO;
        int cmp = Double.compare(ultimo, referencia);
        return cmp > 0 ? SUBE : cmp < 0 ? BAJA : PLANO;
    }

    /**
     * Rango vertical {min, max}. INCLUYE el cierre anterior: si quedara fuera, su linea de
     * referencia se dibujaria pegada al borde o directamente fuera del lienzo, que es justo
     * cuando mas importa verla (papel que se movio entero por encima o por debajo de ayer).
     */
    static double[] rangoVertical(double[] serie, double previousClose) {
        double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        if (serie != null) {
            for (double v : serie) {
                if (Double.isNaN(v)) continue;
                if (v < min) min = v;
                if (v > max) max = v;
            }
        }
        if (min == Double.MAX_VALUE) return new double[]{0d, 0d};
        if (referenciaValida(previousClose)) {
            min = Math.min(min, previousClose);
            max = Math.max(max, previousClose);
        }
        return new double[]{min, max};
    }

    /**
     * Escala visual simetrica respecto del cierre anterior. Una variacion pequena usa como
     * minimo +/-2%, para que movimientos como 0,8% no llenen toda la altura del grafico.
     */
    static double[] rangoVisual(double[] serie, double previousClose) {
        if (!referenciaValida(previousClose)) {
            return rangoVertical(serie, previousClose);
        }
        double maxDesviacion = 0d;
        if (serie != null) {
            for (double value : serie) {
                if (!Double.isFinite(value) || value <= 0d) continue;
                maxDesviacion = Math.max(maxDesviacion,
                        Math.abs((value - previousClose) / previousClose));
            }
        }
        double escala = Math.max(ESCALA_MINIMA, maxDesviacion * MARGEN_ESCALA);
        return new double[]{
                previousClose * (1d - escala),
                previousClose * (1d + escala)
        };
    }

    static int contarValidos(double[] serie) {
        int n = 0;
        if (serie == null) return 0;
        for (double v : serie) {
            if (!Double.isNaN(v)) n++;
        }
        return n;
    }

    /**
     * Aun sin serie suficiente: guion tenue, para distinguir "todavia no hay 2 puntos" de
     * "la celda no se esta pintando". Sin esto ambos casos se ven identicos.
     */
    private static void pintarSinDatos(Canvas lienzo) {
        GraphicsContext g = lienzo.getGraphicsContext2D();
        double w = lienzo.getWidth(), h = lienzo.getHeight();
        g.clearRect(0, 0, w, h);
        g.setStroke(SIN_DATOS);
        g.setLineWidth(1);
        g.strokeLine(w / 2 - 6, h / 2, w / 2 + 6, h / 2);
    }
}
