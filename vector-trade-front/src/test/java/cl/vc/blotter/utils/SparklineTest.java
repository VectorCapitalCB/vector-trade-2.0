package cl.vc.blotter.utils;

import javafx.scene.paint.Color;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Color y rango del sparkline de la columna Tendencia.
 *
 * El caso que motivó esto: un papel que ABRE CON GAP A LA BAJA y después sube toda la rueda.
 * Comparando contra el primer precio del día sale verde; contra el cierre de ayer sale rojo, que
 * es lo que dice el mercado y lo que muestra la columna de variación % al lado. Las dos cosas
 * tienen que coincidir o la fila se contradice a sí misma.
 *
 * Sólo se prueban los helpers puros: pintar() necesita un Canvas y por tanto el toolkit JavaFX.
 */
public class SparklineTest {

    private static final Color VERDE = Color.web("#23a126");
    private static final Color ROJO = Color.web("#de292c");
    private static final Color GRIS = Color.web("#7d8487");

    // ------------------------------------------------------------------ color

    @Test
    public void gapABajaQueLuegoSubeEsROJOContraElCierreDeAyer() {
        // Ayer cerró en 100. Abre en 90 y sube hasta 95: para el mercado sigue -5%.
        double[] serie = {90, 92, 95};
        assertEquals(ROJO, Sparkline.color(serie, 100d),
                "subió durante el día pero sigue bajo el cierre de ayer: es rojo");
    }

    @Test
    public void esaMismaSerieSaldriaVerdeSiSeCompararaContraElPrimerPrecio() {
        // Documenta el bug anterior: sin cierre de ayer, la misma serie se pinta verde.
        double[] serie = {90, 92, 95};
        assertEquals(VERDE, Sparkline.color(serie, Double.NaN));
    }

    @Test
    public void gapAlAlzaQueLuegoBajaEsVERDEContraElCierreDeAyer() {
        double[] serie = {110, 108, 105};
        assertEquals(VERDE, Sparkline.color(serie, 100d));
        assertEquals(ROJO, Sparkline.color(serie, Double.NaN), "contra el primer precio saldría rojo");
    }

    @Test
    public void mismoPrecioQueAyerEsGris() {
        assertEquals(GRIS, Sparkline.color(new double[]{98, 99, 100}, 100d));
    }

    @Test
    public void sinCierreDeAyerCaeAlPrimerPrecioValido() {
        assertEquals(VERDE, Sparkline.color(new double[]{Double.NaN, 10, 12}, Double.NaN));
        assertEquals(ROJO, Sparkline.color(new double[]{Double.NaN, 12, 10}, 0d), "cierre 0 = desconocido");
    }

    @Test
    public void serieVaciaOTodaNaNNoRevienta() {
        assertEquals(GRIS, Sparkline.color(new double[]{}, 100d));
        assertEquals(GRIS, Sparkline.color(new double[]{Double.NaN, Double.NaN}, 100d));
        assertEquals(GRIS, Sparkline.color(null, 100d));
    }

    // ----------------------------------------------------------------- rango

    @Test
    public void elRangoIncluyeElCierreDeAyerAunqueQuedeFueraDeLaSerie() {
        // Si no se incluyera, la línea de referencia se dibujaría fuera del lienzo justo cuando
        // más importa: un papel que se movió entero por debajo de ayer.
        double[] r = Sparkline.rangoVertical(new double[]{90, 92, 95}, 100d);
        assertEquals(90d, r[0], 1e-9);
        assertEquals(100d, r[1], 1e-9, "el cierre de ayer tiene que entrar en el rango");
    }

    @Test
    public void elRangoIgnoraUnCierreInvalido() {
        double[] r = Sparkline.rangoVertical(new double[]{90, 95}, Double.NaN);
        assertEquals(90d, r[0], 1e-9);
        assertEquals(95d, r[1], 1e-9);
    }

    @Test
    public void elRangoIgnoraLosNaNDeLaSerie() {
        double[] r = Sparkline.rangoVertical(new double[]{Double.NaN, 50, Double.NaN, 70}, Double.NaN);
        assertEquals(50d, r[0], 1e-9);
        assertEquals(70d, r[1], 1e-9);
    }

    @Test
    public void serieSinValoresDaRangoCero() {
        double[] r = Sparkline.rangoVertical(new double[]{Double.NaN}, Double.NaN);
        assertEquals(0d, r[0], 1e-9);
        assertEquals(0d, r[1], 1e-9);
    }

    @Test
    public void movimientoMenorAUnPorcientoUsaEscalaMinimaDeDosPorciento() {
        double cierre = 67_990d;
        double[] r = Sparkline.rangoVisual(new double[]{cierre, 68_532d}, cierre);
        assertEquals(cierre * 0.98d, r[0], 1e-9);
        assertEquals(cierre * 1.02d, r[1], 1e-9);
    }

    // ------------------------------------------------------------ referencia

    @Test
    public void soloUnCierrePositivoYFinitoEsReferenciaValida() {
        assertTrue(Sparkline.referenciaValida(100d));
        assertTrue(!Sparkline.referenciaValida(0d), "0 = el core aún no lo tiene en cache");
        assertTrue(!Sparkline.referenciaValida(-5d));
        assertTrue(!Sparkline.referenciaValida(Double.NaN));
        assertTrue(!Sparkline.referenciaValida(Double.POSITIVE_INFINITY));
    }

    @Test
    public void contarValidosIgnoraNaN() {
        assertEquals(2, Sparkline.contarValidos(new double[]{Double.NaN, 1, Double.NaN, 2}));
        assertEquals(0, Sparkline.contarValidos(null));
    }

    @Test
    public void serieDibujoParteEnCierreAnterior() {
        assertArrayEquals(new double[]{100d, 98d, 99d},
                Sparkline.serieConReferencia(new double[]{98d, 99d}, 100d),
                1e-9);
    }

    @Test
    public void noDuplicaElCierreAnteriorSiYaVieneComoPrimerPunto() {
        assertArrayEquals(new double[]{100d, 101d},
                Sparkline.serieConReferencia(new double[]{100d, 101d}, 100d),
                1e-9);
    }
}
