package cl.vc.blotter.utils;

import javafx.application.Platform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * El contador de velas dibujadas de NativeCandleChart tiene que distinguir "pinte velas" de
 * "mostre el cartel de sin datos".
 *
 * POR QUE EXISTE: el smoke nativo de CI se apoya en {@code getLastDrawnCandles() > 0} para decidir
 * si el grafico funciona. Si ese contador diera un numero positivo con la ventana vacia, el gate
 * volveria a ser decorativo — que es exactamente lo que paso dos veces: primero el smoke no
 * llamaba al verificador del grafico, y despues lo llamaba pero solo comprobaba que el objeto del
 * grafico no fuera null, cosa que se cumple igual mostrando "Sin datos para mostrar".
 *
 * Estos tests son la prueba de que la asercion del smoke es falsificable: sin serie da 0, con
 * serie da mas de 0.
 */
class NativeCandleChartDrawTest {

    private static void arrancarToolkit() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        try {
            Platform.startup(started::countDown);
        } catch (IllegalStateException yaArranco) {
            started.countDown();
        }
        assertTrue(started.await(20, TimeUnit.SECONDS), "el toolkit de JavaFX no arranco");
    }

    /**
     * Dimensiona el grafico y devuelve las velas que pinto en el layout.
     *
     * <p>A proposito NO monta un Stage ni una Scene. La primera version de este helper mostraba
     * una ventana por test y eso rompia FooterFxmlTest y terminaba tumbando la JVM de surefire
     * (exit 134): varios tests comparten un unico toolkit de JavaFX y abrir ventanas ensucia ese
     * estado global. Con {@code resize()} + {@code layout()} se ejerce igual el camino que
     * importa —{@code layoutChildren()} dimensiona el Canvas y llama {@code redraw()}— sin tocar
     * ventanas.
     */
    private static int velasDibujadas(List<NativeCandleChart.CandlePoint> serie) throws Exception {
        arrancarToolkit();
        AtomicInteger dibujadas = new AtomicInteger(Integer.MIN_VALUE);
        AtomicReference<Throwable> fallo = new AtomicReference<>();
        CountDownLatch listo = new CountDownLatch(1);

        Platform.runLater(() -> {
            try {
                NativeCandleChart chart = new NativeCandleChart();
                if (!serie.isEmpty()) {
                    chart.setData("Velas (1D) - TEST", serie, 1440, null, null);
                }
                chart.resize(900, 600);
                // requestLayout explicito: Parent.layout() no hace nada si el nodo esta CLEAN.
                chart.requestLayout();
                chart.layout();
                dibujadas.set(chart.getLastDrawnCandles());
            } catch (Throwable t) {
                fallo.set(t);
            } finally {
                listo.countDown();
            }
        });

        assertTrue(listo.await(60, TimeUnit.SECONDS), "timeout dimensionando el grafico");
        if (fallo.get() != null) {
            throw new AssertionError("el grafico nativo revento al dibujar", fallo.get());
        }
        return dibujadas.get();
    }

    private static List<NativeCandleChart.CandlePoint> serieSintetica(int barras) {
        List<NativeCandleChart.CandlePoint> serie = new ArrayList<>(barras);
        Instant t = Instant.parse("2026-01-05T13:00:00Z");
        double base = 1000d;
        for (int i = 0; i < barras; i++) {
            double open = base + Math.sin(i / 6d) * 25d;
            double close = base + Math.sin((i + 1) / 6d) * 25d;
            serie.add(new NativeCandleChart.CandlePoint(
                    t.plus(i, ChronoUnit.DAYS),
                    open,
                    Math.max(open, close) + 6d,
                    Math.min(open, close) - 6d,
                    close));
            base += 0.4d;
        }
        return serie;
    }

    @Test
    @DisplayName("Sin serie no dibuja ninguna vela: es el estado que el gate viejo dejaba pasar")
    void sinSerieNoDibujaNada() throws Exception {
        assertEquals(0, velasDibujadas(List.of()),
                "Sin datos el grafico muestra 'Sin datos para mostrar' y no debe reportar velas"
                        + " dibujadas. Si aca da > 0, la asercion del smoke nativo es decorativa y"
                        + " vuelve a poder publicar un instalador con el grafico muerto.");
    }

    @Test
    @DisplayName("Con serie dibuja velas de verdad")
    void conSerieDibujaVelas() throws Exception {
        int dibujadas = velasDibujadas(serieSintetica(120));
        assertTrue(dibujadas > 0,
                "Con 120 velas sembradas el Canvas tiene que pintar al menos una. Reporto: "
                        + dibujadas);
    }

    @Test
    @DisplayName("El grafico nativo se construye sin tocar AWT")
    void seConstruyeSinAwt() throws Exception {
        // Si NativeCandleChart arrastrara AWT, esto reventaria aca en el ejecutable nativo.
        // En la JVM no puede fallar por AWT (la JVM si lo tiene), asi que el valor real de este
        // test es de regresion estructural; la prueba de fuego es el smoke sobre el .exe.
        assertTrue(velasDibujadas(serieSintetica(30)) > 0, "no dibujo con 30 barras");
    }
}
