package cl.vc.service.admin.servlet;

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifica el fix de la reinyección de órdenes (POST /api/orders/resend-log):
 * antes la hora se seteaba como un Timestamp vacío (epoch 0 = 1970) y el front mostraba ~21 h.
 * Ahora se parsea el 'time' real del mensaje (UTC ISO) o, como respaldo, el timestamp del log.
 */
class OrderServletTest {

    private Timestamp isoToProto(String iso) throws Exception {
        Method m = OrderServlet.class.getDeclaredMethod("isoToProto", String.class);
        m.setAccessible(true);
        return (Timestamp) m.invoke(null, iso);
    }

    private Timestamp orderTime(String isoTime, String logTs) throws Exception {
        Method m = OrderServlet.class.getDeclaredMethod("orderTime", String.class, String.class);
        m.setAccessible(true);
        return (Timestamp) m.invoke(null, isoTime, logTs);
    }

    @Test
    void timeDelMensaje_seParseaUTC_yNoEsEpoch0() throws Exception {
        String iso = "2026-06-04T14:40:39.304Z";
        Instant expected = Instant.parse(iso);

        Timestamp ts = orderTime(iso, "2026-06-04 10:40:27,711");

        assertNotNull(ts);
        assertNotEquals(0L, ts.getSeconds(), "antes quedaba en epoch 0 (1970) -> front mostraba 21h");
        assertEquals(expected.getEpochSecond(), ts.getSeconds());
        assertEquals(expected.getNano(), ts.getNanos());
    }

    @Test
    void isoInvalido_devuelveNull_noEpoch0() throws Exception {
        assertNull(isoToProto(null));
        assertNull(isoToProto(""));
        assertNull(isoToProto("no-es-fecha"));
    }

    @Test
    void expireTimeIso_seParsea() throws Exception {
        Timestamp ts = isoToProto("2026-06-04T19:44:00Z");
        assertNotNull(ts);
        assertEquals(Instant.parse("2026-06-04T19:44:00Z").getEpochSecond(), ts.getSeconds());
    }
}
