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
 */
public final class Sparkline {

    private static final Color SUBE = Color.web("#23a126");
    private static final Color BAJA = Color.web("#de292c");
    private static final Color PLANO = Color.web("#7d8487");
    private static final Color SIN_DATOS = Color.web("#5c6365");

    private Sparkline() {
    }

    public static void pintar(Canvas lienzo, double[] serie) {
        if (serie == null || serie.length < 2) {
            pintarSinDatos(lienzo);
            return;
        }

        GraphicsContext g = lienzo.getGraphicsContext2D();
        double w = lienzo.getWidth(), h = lienzo.getHeight();
        g.clearRect(0, 0, w, h);

        double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        int validos = 0;
        for (double v : serie) {
            if (Double.isNaN(v)) continue;
            if (v < min) min = v;
            if (v > max) max = v;
            validos++;
        }
        if (validos < 2) {
            pintarSinDatos(lienzo);
            return;
        }

        double rango = max - min;
        // Papel plano: una linea al medio en vez de dividir por cero.
        if (rango <= 0) {
            g.setStroke(PLANO);
            g.setLineWidth(1);
            g.strokeLine(1, h / 2, w - 1, h / 2);
            return;
        }

        // Verde si cerro sobre el primer precio del dia, rojo si bajo: la convencion de
        // mercado, no una eleccion estetica.
        double primero = Double.NaN, ultimo = Double.NaN;
        for (double v : serie) {
            if (Double.isNaN(v)) continue;
            if (Double.isNaN(primero)) primero = v;
            ultimo = v;
        }
        g.setStroke(ultimo >= primero ? SUBE : BAJA);
        g.setLineWidth(1.5);

        double paso = (w - 2) / (serie.length - 1);
        double xPrev = Double.NaN, yPrev = Double.NaN;
        for (int i = 0; i < serie.length; i++) {
            if (Double.isNaN(serie[i])) continue;
            double x = 1 + i * paso;
            double y = h - 2 - ((serie[i] - min) / rango) * (h - 4);
            if (!Double.isNaN(xPrev)) {
                g.strokeLine(xPrev, yPrev, x, y);
            }
            xPrev = x;
            yPrev = y;
        }
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
