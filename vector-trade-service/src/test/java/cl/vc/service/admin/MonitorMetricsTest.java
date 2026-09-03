package cl.vc.service.admin;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Métricas que consume MONITOR-VC. Importa que los motivos queden agrupados (si cada texto de
 * rechazo fuera su propia categoría, la cardinalidad haría inservible el panel) y que el ratio de
 * aciertos de cache refleje de dónde salió la custodia.
 */
class MonitorMetricsTest {

    @Test
    void clasificaLosMotivosPropiosDeRiesgo() {
        assertEquals("SIN_CUSTODIA",
                MonitorMetrics.classifyReject("Retornamos por orden sin custodia"));
        assertEquals("SIMBOLO_BLOQUEADO",
                MonitorMetrics.classifyReject("Orden rechazada por riesgo — símbolo bloqueado: SQM-B"));
        assertEquals("PROTECCION_OD",
                MonitorMetrics.classifyReject("Orden rechazada OD: cuenta 123/0 ya tiene punta BUY activa en SQM-B"));
        assertEquals("ORIG_CL_ORD_ID",
                MonitorMetrics.classifyReject("OrigClOrdId missing from sequence"));
    }

    @Test
    void motivoDesconocidoNoInventaCategoria() {
        assertEquals("EXTERNO", MonitorMetrics.classifyReject("Order rejected by exchange 99"));
        assertEquals("SIN_TEXTO", MonitorMetrics.classifyReject(null));
        assertEquals("SIN_TEXTO", MonitorMetrics.classifyReject("   "));
    }

    @Test
    void elRatioDeCacheDistingueRedisDeSql() {
        MonitorMetrics.recordCustodyLoad("ratio-1/0", MonitorMetrics.SOURCE_REDIS, "cache", 5L, 10, 0d);
        MonitorMetrics.recordCustodyLoad("ratio-2/0", MonitorMetrics.SOURCE_REDIS, "cache", 7L, 10, 0d);
        MonitorMetrics.recordCustodyLoad("ratio-3/0", MonitorMetrics.SOURCE_SQL, "sin cache", 4_000L, 10, 0d);

        Map<String, Object> snap = MonitorMetrics.custodySnapshot();

        assertTrue(((Number) snap.get("loadsFromRedis")).longValue() >= 2);
        assertTrue(((Number) snap.get("loadsFromSql")).longValue() >= 1);
        // La latencia de SQL se lleva aparte: mezclada con los hits de Redis el promedio no sirve.
        assertTrue(((Number) snap.get("maxSqlLoadMs")).longValue() >= 4_000L);
        assertNotNull(snap.get("cacheHitRatio"));
    }

    @Test
    void guardaLaUltimaCargaPorCuentaYSePuedeOlvidar() {
        MonitorMetrics.recordCustodyLoad("olvido/0", MonitorMetrics.SOURCE_SQL, "sin cache", 120L, 3, 500d);
        MonitorMetrics.CustodyLoad load = MonitorMetrics.custodyLoad("olvido/0");

        assertNotNull(load);
        assertEquals(MonitorMetrics.SOURCE_SQL, load.source());
        assertEquals(3, load.positions());

        // Al invalidar la custodia de una cuenta no debe quedar la medición vieja.
        MonitorMetrics.forgetCustodyLoad("olvido/0");
        assertNull(MonitorMetrics.custodyLoad("olvido/0"));
    }

    @Test
    void cuentaRechazosEnLaVentanaReciente() {
        long antes = MonitorMetrics.rejectsLastMinutes(5);

        MonitorMetrics.recordReject("rej/0", "SQM-B", "id-1", "Orden rechazada por riesgo");
        MonitorMetrics.recordReject("rej/0", "SQM-B", "id-2", "Retornamos por orden sin custodia");

        assertEquals(antes + 2, MonitorMetrics.rejectsLastMinutes(5));
        assertFalse(MonitorMetrics.recentRejects(10).isEmpty());

        @SuppressWarnings("unchecked")
        Map<String, Long> byReason = (Map<String, Long>) MonitorMetrics.rejectsSnapshot().get("byReason");
        assertTrue(byReason.containsKey("SIN_CUSTODIA"));
        assertTrue(byReason.containsKey("RIESGO"));
    }
}
