package cl.vc.blotter.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parametros de indicadores: normalizacion, etiquetas y persistencia.
 *
 * Importa porque el operador los edita a mano. Un periodo 0 o un MACD con la rapida por sobre la
 * lenta no revientan: dibujan una linea plausible y equivocada, que es el peor resultado posible.
 */
public class IndicatorSettingsTest {

    // ---------------------------------------------------------- validacion

    @Test
    public void macdCorrigeLaRapidaSiQuedaSobreLaLenta() {
        // line = EMA(fast) - EMA(slow): con fast >= slow el signo se invierte y el histograma miente.
        IndicatorSettings s = new IndicatorSettings();
        s.macdFast = 30;
        s.macdSlow = 26;
        s.normalizar();
        assertTrue(s.macdFast < s.macdSlow, "la rapida quedo sobre la lenta: " + s.macdFast + "/" + s.macdSlow);
        assertEquals(25, s.macdFast);
    }

    @Test
    public void macdConRapidaIgualALentaTambienSeCorrige() {
        IndicatorSettings s = new IndicatorSettings();
        s.macdFast = 26;
        s.macdSlow = 26;
        s.normalizar();
        assertTrue(s.macdFast < s.macdSlow);
    }

    @Test
    public void periodosFueraDeRangoSeAcotan() {
        IndicatorSettings s = new IndicatorSettings();
        s.smaPeriod = -5;
        s.rsiPeriod = 99999;
        s.atrPeriod = 0;
        s.normalizar();
        assertTrue(s.smaPeriod >= IndicatorSettings.MIN_PERIODO, "SMA quedo en " + s.smaPeriod);
        assertTrue(s.rsiPeriod <= IndicatorSettings.MAX_PERIODO, "RSI quedo en " + s.rsiPeriod);
        assertTrue(s.atrPeriod >= IndicatorSettings.MIN_PERIODO, "ATR quedo en " + s.atrPeriod);
    }

    @Test
    public void kDeBollingerSeAcotaYToleraNaN() {
        IndicatorSettings s = new IndicatorSettings();
        s.bollingerK = 0d;
        s.normalizar();
        assertTrue(s.bollingerK >= IndicatorSettings.MIN_K, "k=0 daria bandas pegadas a la media");

        s.bollingerK = Double.NaN;
        s.normalizar();
        assertEquals(2.0d, s.bollingerK, 1e-9, "NaN debe caer al default, no propagarse al grafico");

        s.bollingerK = 999d;
        s.normalizar();
        assertTrue(s.bollingerK <= IndicatorSettings.MAX_K);
    }

    @Test
    public void normalizarEsIdempotente() {
        IndicatorSettings s = new IndicatorSettings();
        s.macdFast = 40;
        s.bollingerK = -3d;
        s.normalizar();
        IndicatorSettings esperado = s.copy();
        s.normalizar();
        assertEquals(esperado.macdFast, s.macdFast);
        assertEquals(esperado.bollingerK, s.bollingerK, 1e-9);
    }

    // ------------------------------------------------------------ etiquetas

    @Test
    public void lasEtiquetasReflejanLosParametros() {
        IndicatorSettings s = new IndicatorSettings();
        s.smaPeriod = 50;
        s.rsiPeriod = 9;
        s.bollingerPeriod = 30;
        s.bollingerK = 2.5d;
        assertEquals("SMA 50", s.label(ChartIndicator.SMA20));
        assertEquals("RSI 9", s.label(ChartIndicator.RSI));
        assertEquals("Bollinger (30, 2.5)", s.label(ChartIndicator.BOLLINGER));
    }

    @Test
    public void kEnteraNoMuestraDecimalSobrante() {
        IndicatorSettings s = new IndicatorSettings();
        s.bollingerK = 2.0d;
        assertEquals("Bollinger (20, 2)", s.label(ChartIndicator.BOLLINGER),
                "no debe decir 'Bollinger (20, 2.0)'");
    }

    @Test
    public void macdEIchimokuMuestranSusTresValores() {
        IndicatorSettings s = new IndicatorSettings();
        assertEquals("MACD (12, 26, 9)", s.label(ChartIndicator.MACD));
        assertEquals("Ichimoku (9/26/52)", s.label(ChartIndicator.ICHIMOKU));
    }

    // ----------------------------------------------------------------- copy

    @Test
    public void copyEsIndependienteDelOriginal() {
        // El dialogo edita un borrador: cancelar no debe dejar el grafico a medio cambiar.
        IndicatorSettings original = new IndicatorSettings();
        IndicatorSettings borrador = original.copy();
        borrador.smaPeriod = 200;
        assertNotSame(original, borrador);
        assertEquals(20, original.smaPeriod, "el original se contamino con la edicion del borrador");
    }

    // --------------------------------------------------------- persistencia

    @Test
    public void guardarYCargarConservaLosValores() {
        IndicatorSettings original = IndicatorSettings.cargar();
        try {
            IndicatorSettings s = new IndicatorSettings();
            s.smaPeriod = 55;
            s.bollingerK = 1.5d;
            s.macdFast = 8;
            s.macdSlow = 21;
            s.guardar();

            IndicatorSettings leido = IndicatorSettings.cargar();
            assertEquals(55, leido.smaPeriod);
            assertEquals(1.5d, leido.bollingerK, 1e-9);
            assertEquals(8, leido.macdFast);
            assertEquals(21, leido.macdSlow);
        } finally {
            original.guardar();
        }
    }

    @Test
    public void cargarNormalizaLoQueVengaGuardado() {
        // Preferencias viejas o editadas a mano no deben poder dejar el grafico invalido.
        IndicatorSettings original = IndicatorSettings.cargar();
        try {
            IndicatorSettings malo = new IndicatorSettings();
            malo.macdSlow = 10;
            malo.macdFast = 10;   // normalizar() lo corrige al guardar
            malo.guardar();
            assertTrue(IndicatorSettings.cargar().macdFast < IndicatorSettings.cargar().macdSlow);
        } finally {
            original.guardar();
        }
    }
}
