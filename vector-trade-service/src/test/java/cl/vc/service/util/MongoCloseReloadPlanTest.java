package cl.vc.service.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Plan de lectura y ritmo de la recarga de cierres.
 *
 * Estas dos piezas son las que definen que se lee, en que orden y a que velocidad. Un error aca no
 * revienta: simplemente deja papeles sin recargar o satura Mongo, y eso no se nota hasta produccion.
 *
 * buildReloadPlan y throttleMillis son package-private estaticos: se llaman por reflexion para no
 * ampliar su visibilidad solo por el test.
 */
public class MongoCloseReloadPlanTest {

    @SuppressWarnings("unchecked")
    private static List<Object> plan(List<String> universe, Map<String, List<String>> byType,
                                     List<String> typeOrder, List<String> priority) throws Exception {
        Method m = MongoCloseRepository.class.getDeclaredMethod(
                "buildReloadPlan", java.util.Collection.class, Map.class, List.class, List.class);
        m.setAccessible(true);
        return (List<Object>) m.invoke(null, universe, byType, typeOrder, priority);
    }

    private static String groupName(Object group) throws Exception {
        Field f = group.getClass().getDeclaredField("name");
        f.setAccessible(true);
        return (String) f.get(group);
    }

    @SuppressWarnings("unchecked")
    private static List<String> groupSymbols(Object group) throws Exception {
        Field f = group.getClass().getDeclaredField("symbols");
        f.setAccessible(true);
        return (List<String>) f.get(group);
    }

    private static long throttle(int symbols, double rate, long elapsedMs) throws Exception {
        Method m = MongoCloseRepository.class.getDeclaredMethod(
                "throttleMillis", int.class, double.class, long.class);
        m.setAccessible(true);
        try {
            return (long) m.invoke(null, symbols, rate, elapsedMs);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    // ---------------------------------------------------------------- plan

    @Test
    public void elSimboloDePrioridadVaPrimeroAunqueNoEsteEnMongoNiEnLaSecurityList() throws Exception {
        // La mesa lo pidio "si o si": si no esta en el universo, igual se intenta leer.
        List<Object> p = plan(List.of("SQM-B", "FALABELLA"),
                Map.of("CS", List.of("SQM-B", "FALABELLA")),
                List.of("CS"),
                List.of("CFMITNIPSA"));

        assertEquals("PRIORIDAD", groupName(p.get(0)));
        assertEquals(List.of("CFMITNIPSA"), groupSymbols(p.get(0)));
    }

    @Test
    public void respetaElOrdenDeTiposPedido() throws Exception {
        List<Object> p = plan(List.of("A", "B", "C"),
                Map.of("CS", List.of("A"), "CFI", List.of("B"), "ETF", List.of("C")),
                List.of("CS", "CFI", "ETF"),
                List.of());

        assertEquals(List.of("CS", "CFI", "ETF"),
                List.of(groupName(p.get(0)), groupName(p.get(1)), groupName(p.get(2))));
    }

    @Test
    public void losSimbolosSinTipoConocidoVanAlFinalEnOtros() throws Exception {
        // Este es el hueco que cubre OTROS: con 16.000 simbolos en Mongo y una SecurityList
        // incompleta, sin este grupo miles quedarian sin recargar y en silencio.
        List<Object> p = plan(List.of("SQM-B", "RARO1", "RARO2"),
                Map.of("CS", List.of("SQM-B")),
                List.of("CS"),
                List.of());

        assertEquals("CS", groupName(p.get(0)));
        assertEquals("OTROS", groupName(p.get(1)));
        assertEquals(List.of("RARO1", "RARO2"), groupSymbols(p.get(1)));
    }

    @Test
    public void cadaSimboloApareceUnaSolaVez() throws Exception {
        // CFMITNIPSA esta tambien clasificado como CFI y presente en el universo: no debe leerse dos veces.
        List<Object> p = plan(List.of("CFMITNIPSA", "SQM-B"),
                Map.of("CS", List.of("SQM-B"), "CFI", List.of("CFMITNIPSA")),
                List.of("CS", "CFI"),
                List.of("CFMITNIPSA"));

        int apariciones = 0;
        for (Object g : p) {
            for (String s : groupSymbols(g)) if ("CFMITNIPSA".equals(s)) apariciones++;
        }
        assertEquals(1, apariciones, "CFMITNIPSA se leeria dos veces");
    }

    @Test
    public void ignoraTiposDeLaSecurityListQueNoEstanEnMongo() throws Exception {
        // Si la SecurityList trae papeles que no tienen cierre, no se gastan lotes en ellos.
        List<Object> p = plan(List.of("SQM-B"),
                Map.of("CS", List.of("SQM-B", "NO_ESTA_EN_MONGO")),
                List.of("CS"),
                List.of());

        assertEquals(List.of("SQM-B"), groupSymbols(p.get(0)));
    }

    @Test
    public void universoVacioYSinPrioridadDaPlanVacio() throws Exception {
        assertTrue(plan(List.of(), Map.of(), List.of("CS"), List.of()).isEmpty());
    }

    @Test
    public void toleraNulosSinReventar() throws Exception {
        List<Object> p = plan(null, null, null, null);
        assertTrue(p.isEmpty());
    }

    // ------------------------------------------------------------ throttle

    @Test
    public void elRitmoDescuentaLoQueTardoLaQuery() throws Exception {
        // 100 simbolos a 10/s = 10 s de presupuesto; si la query tardo 2 s, duerme 8 s.
        assertEquals(8000L, throttle(100, 10d, 2000L));
    }

    @Test
    public void siElLoteTardaMasQueSuPresupuestoNoDuerme() throws Exception {
        // No acumula deuda: el ritmo cae solo en vez de intentar recuperarlo.
        assertEquals(0L, throttle(100, 10d, 15000L));
    }

    @Test
    public void diezPorSegundoSobreDieciseisMilDaVeintisieteMinutos() throws Exception {
        // El numero que pidio la mesa, comprobado: 16.000 / 10 = 1.600 s = 26 min 40 s.
        long totalMs = 0;
        int batch = 100;
        for (int i = 0; i < 16000 / batch; i++) totalMs += throttle(batch, 10d, 0L);
        assertEquals(1_600_000L, totalMs);
        assertEquals(26, totalMs / 60000);
    }

    @Test
    public void ritmoInvalidoNoDuerme() throws Exception {
        assertEquals(0L, throttle(100, 0d, 0L));
        assertEquals(0L, throttle(100, -5d, 0L));
        assertEquals(0L, throttle(0, 10d, 0L));
    }
}
