package cl.vc.blotter.utils;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Seleccion de indicadores del grafico de velas.
 *
 * El requisito concreto de la mesa fue "no me los muestres todos de golpe": el grafico tiene que
 * abrir limpio y el operador ir sumando. Eso es lo que se verifica aca, no el dibujo.
 */
public class ChartIndicatorTest {

    @Test
    public void elDefaultNoPrendeTodosLosIndicadores() {
        Set<ChartIndicator> def = ChartIndicator.porDefectoSet();
        assertTrue(def.size() < ChartIndicator.values().length,
                "el grafico debe abrir limpio, no con los " + ChartIndicator.values().length + " indicadores");
    }

    @Test
    public void elDefaultDejaApagadosLosQueSaturanElPanelDePrecio() {
        // Ichimoku son 3 lineas mas la nube y Bollinger 2 bandas: encendidos por defecto tapan las velas.
        Set<ChartIndicator> def = ChartIndicator.porDefectoSet();
        assertFalse(def.contains(ChartIndicator.ICHIMOKU), "Ichimoku no puede venir encendido");
        assertFalse(def.contains(ChartIndicator.BOLLINGER), "Bollinger no puede venir encendido");
    }

    @Test
    public void elDefaultTraeLoMinimoUtil() {
        Set<ChartIndicator> def = ChartIndicator.porDefectoSet();
        assertTrue(def.contains(ChartIndicator.VWAP), "VWAP es la referencia de ejecucion de la mesa");
        assertTrue(def.contains(ChartIndicator.RSI));
        assertTrue(def.contains(ChartIndicator.MACD));
    }

    @Test
    public void todosLosIndicadoresTienenEtiquetaYGrupo() {
        for (ChartIndicator i : ChartIndicator.values()) {
            String etiqueta = i.etiqueta(new IndicatorSettings());
            assertTrue(etiqueta != null && !etiqueta.isBlank(), i.name() + " sin etiqueta visible");
            assertTrue(i.grupo != null, i.name() + " sin grupo");
        }
    }

    @Test
    public void losIndicadoresEstanOrdenadosPorGrupo() {
        // El menu agrupa recorriendo values() en orden: si los grupos se intercalan, saldrian
        // encabezados repetidos.
        ChartIndicator.Grupo previo = null;
        EnumSet<ChartIndicator.Grupo> vistos = EnumSet.noneOf(ChartIndicator.Grupo.class);
        for (ChartIndicator i : ChartIndicator.values()) {
            if (i.grupo != previo) {
                assertFalse(vistos.contains(i.grupo),
                        "el grupo " + i.grupo + " aparece en dos tramos de values()");
                vistos.add(i.grupo);
                previo = i.grupo;
            }
        }
    }

    @Test
    public void guardarYCargarConservaLaSeleccion() {
        Set<ChartIndicator> original = ChartIndicator.cargar();
        try {
            Set<ChartIndicator> elegido = EnumSet.of(ChartIndicator.ATR, ChartIndicator.ICHIMOKU);
            ChartIndicator.guardar(elegido);
            assertEquals(elegido, ChartIndicator.cargar());

            // Y la seleccion vacia ("Solo velas") tambien tiene que persistir como vacia,
            // no volver al default.
            ChartIndicator.guardar(EnumSet.noneOf(ChartIndicator.class));
            assertTrue(ChartIndicator.cargar().isEmpty(), "'Solo velas' debe sobrevivir al reinicio");
        } finally {
            ChartIndicator.guardar(original);
        }
    }
}
