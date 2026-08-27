package cl.vc.blotter.controller;

import cl.vc.blotter.utils.NativeCandleChart;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CandleController no puede tener estado estatico de AWT, y NativeCandleChart no puede tener
 * NADA de AWT.
 *
 * POR QUE EXISTE: los releases nativos 2.0.7 y 2.0.8 murieron con
 * "UnsatisfiedLinkError: Can't load library: awt" al abrir el grafico de velas. La causa no era
 * el ChartViewer del FXML, como se creyo al principio: eran dos campos
 * {@code private static final java.awt.Color UP_COLOR/DOWN_COLOR} en CandleController. Al ser
 * estaticos vivian en el {@code <clinit>} de la clase, que se dispara en el
 * {@code loader.load()} de CandleWindow — o sea ANTES del {@code if (nativeChart != null)} que
 * elige la superficie JavaFX. Y {@code java.awt.Color.<clinit>} llama
 * {@code Toolkit.loadLibraries()}, que hace {@code System.loadLibrary("awt")}. En el ejecutable
 * de Gluon no hay awt: el doble click en Tendencia moria antes de poder tomar la rama nativa.
 * La rama nativa llegaba tarde por construccion.
 *
 * Hoy esos colores viven en el holder {@code CandleController.JfreeColors}, que recien se
 * inicializa al leerlo dentro de la rama JFreeChart.
 *
 * ALCANCE, para no darse una falsa sensacion de seguridad: esto cubre el error que efectivamente
 * ocurrio (campos estaticos de tipo AWT) y no cubre el caso general de un bloque estatico que
 * llame a AWT sin declarar un campo (por ejemplo {@code Toolkit.getDefaultToolkit()}). El gate
 * de verdad para eso es el smoke nativo de CI, que ahora exige velas pintadas; ver
 * {@code CandleWindow.verifyNativeOpen}. Este test es la alarma temprana y barata.
 */
class CandleControllerNoAwtStaticsTest {

    private static final List<String> PAQUETES_PROHIBIDOS =
            List.of("java.awt", "javax.swing", "javax.imageio", "sun.awt", "sun.java2d");

    @Test
    @DisplayName("CandleController no declara campos estaticos de AWT/Swing")
    void candleControllerSinEstaticosAwt() {
        List<String> ofensores = estaticosProhibidos(CandleController.class);
        assertTrue(ofensores.isEmpty(),
                "CandleController volvio a tener estado estatico de AWT, asi que su <clinit> va a"
                        + " intentar cargar la libreria awt y el grafico de velas nativo vuelve a"
                        + " morir en Windows. Muevelos a un holder como JfreeColors, que solo se"
                        + " inicializa dentro de la rama JFreeChart. Ofensores: " + ofensores);
    }

    @Test
    @DisplayName("El holder JfreeColors si carga AWT, y por eso tiene que estar aparte")
    void elHolderEsElQueAislaAwt() throws Exception {
        Class<?> holder = Class.forName("cl.vc.blotter.controller.CandleController$JfreeColors");
        List<String> awt = estaticosDeAwt(holder);
        assertTrue(awt.size() == 2,
                "Se esperaba que JfreeColors concentre los dos java.awt.Color del renderer"
                        + " JFreeChart. Si quedo vacio, alguien movio los colores de vuelta al"
                        + " controller o los borro; revisa donde viven ahora. Encontrado: " + awt);
    }

    @Test
    @DisplayName("NativeCandleChart no toca AWT en absoluto: es la superficie del ejecutable nativo")
    void graficoNativoSinAwtNiEnCamposNiEnFirmas() {
        List<String> ofensores = new ArrayList<>(estaticosProhibidos(NativeCandleChart.class));
        for (Field field : NativeCandleChart.class.getDeclaredFields()) {
            if (esProhibido(field.getType().getName())) {
                ofensores.add("campo " + field.getName() + " : " + field.getType().getName());
            }
        }
        for (var method : NativeCandleChart.class.getDeclaredMethods()) {
            if (esProhibido(method.getReturnType().getName())) {
                ofensores.add("retorno de " + method.getName() + " : " + method.getReturnType().getName());
            }
            for (Class<?> param : method.getParameterTypes()) {
                if (esProhibido(param.getName())) {
                    ofensores.add("parametro de " + method.getName() + " : " + param.getName());
                }
            }
        }
        assertTrue(ofensores.isEmpty(),
                "NativeCandleChart es justamente la superficie que existe para NO depender de AWT"
                        + " en el ejecutable nativo. Si aparece AWT aca, el rediseño pierde sentido."
                        + " Ofensores: " + ofensores);
    }

    private static List<String> estaticosProhibidos(Class<?> tipo) {
        List<String> ofensores = new ArrayList<>();
        for (Field field : tipo.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            String nombreTipo = field.getType().getName();
            if (esProhibido(nombreTipo)) {
                ofensores.add(field.getName() + " : " + nombreTipo);
            }
        }
        return ofensores;
    }

    private static List<String> estaticosDeAwt(Class<?> tipo) {
        List<String> encontrados = new ArrayList<>();
        for (Field field : tipo.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())
                    && field.getType().getName().startsWith("java.awt")) {
                encontrados.add(field.getName() + " : " + field.getType().getName());
            }
        }
        return encontrados;
    }

    private static boolean esProhibido(String nombreTipo) {
        String base = nombreTipo.replace("[]", "").replaceAll("^\\[+L?", "");
        return PAQUETES_PROHIBIDOS.stream().anyMatch(base::startsWith);
    }
}
