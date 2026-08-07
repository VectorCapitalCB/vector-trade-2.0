package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.Timestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cubre el camino de escritura del historico SIN Mongo: lo que corre en el hilo del actor es
 * solo "arma el Document y encola", y eso es lo que se verifica aca.
 */
class MongoHistoryRepositoryTest {

    private static final String ACCOUNT = "12345678/0";

    @BeforeEach
    void reset() throws Exception {
        setStatic("queue", new ArrayBlockingQueue<>(10));
        setStatic("connected", true);
        ((AtomicLong) getStatic("dropped")).set(0);
    }

    private RoutingMessage.Order.Builder fill(String execId) {
        return RoutingMessage.Order.newBuilder()
                .setId("ORD-1")
                .setAccount(ACCOUNT)
                .setSymbol("SQM-B")
                .setSide(RoutingMessage.Side.BUY)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setExecId(execId)
                .setOrdStatus(RoutingMessage.OrderStatus.FILLED)
                .setPrice(45000d)
                .setLastPx(44980d)
                .setLastQty(100d)
                .setCumQty(100d)
                .setAvgPrice(44980d);
    }

    @Test
    @DisplayName("una ejecucion se encola con los campos que necesita la vista")
    void recordExecution_encolaConCamposDePrecio() throws Exception {
        Instant when = Instant.parse("2026-03-17T14:30:00Z");
        MongoHistoryRepository.recordExecution(fill("EXEC-1")
                .setTime(Timestamp.newBuilder().setSeconds(when.getEpochSecond()).build())
                .build());

        Document d = singleQueuedDocument();
        assertEquals(ACCOUNT, d.getString("account"));
        assertEquals("SQM-B", d.getString("symbol"));
        assertEquals("BUY", d.getString("side"));
        assertEquals("EXEC-1", d.getString("execId"));
        assertEquals(44980d, d.getDouble("lastPx"), 1e-9);
        assertEquals(100d, d.getDouble("lastQty"), 1e-9);
        assertEquals(Date.from(when), d.get("transactTime"));
    }

    @Test
    @DisplayName("sin 'time' del sellside se estampa la hora de recepcion, no queda en 1970")
    void recordExecution_sinTime_usaAhora() throws Exception {
        Instant before = Instant.now().minusSeconds(1);
        MongoHistoryRepository.recordExecution(fill("EXEC-2").build());

        Date stamped = (Date) singleQueuedDocument().get("transactTime");
        assertTrue(stamped.toInstant().isAfter(before),
                "transactTime deberia ser la hora de recepcion; si no, el documento cae fuera de todo rango del calendario");
    }

    @Test
    @DisplayName("con el historico apagado no encola ni revienta")
    void recordExecution_desconectado_esNoOp() throws Exception {
        setStatic("connected", false);
        MongoHistoryRepository.recordExecution(fill("EXEC-3").build());
        assertTrue(queue().isEmpty());
    }

    @Test
    @DisplayName("null no revienta el hilo del actor")
    void recordExecution_null_esNoOp() throws Exception {
        MongoHistoryRepository.recordExecution(null);
        MongoHistoryRepository.recordOrderTerminal(null);
        assertTrue(queue().isEmpty());
    }

    @Test
    @DisplayName("con la cola llena se descarta y se cuenta: el ruteo nunca se frena")
    void colaLlena_descartaYCuenta() throws Exception {
        setStatic("queue", new ArrayBlockingQueue<>(2));
        for (int i = 0; i < 5; i++) {
            MongoHistoryRepository.recordExecution(fill("EXEC-" + i).build());
        }
        assertEquals(2, queue().size());
        assertEquals(3L, ((AtomicLong) getStatic("dropped")).get());
        assertEquals(3L, MongoHistoryRepository.status().get("dropped"));
    }

    @Test
    @DisplayName("la orden terminal guarda cantidades, no precio de ejecucion")
    void recordOrderTerminal_guardaCantidades() throws Exception {
        MongoHistoryRepository.recordOrderTerminal(fill("EXEC-4")
                .setOrderQty(100d)
                .setClOrdId("CL-1")
                .build());

        Document d = singleQueuedDocument();
        assertEquals("ORD-1", d.getString("orderId"));
        assertEquals("CL-1", d.getString("clOrdId"));
        assertEquals(100d, d.getDouble("orderQty"), 1e-9);
        assertEquals("FILLED", d.getString("ordStatus"));
        assertNull(d.get("execId"), "el documento de orden no lleva execId; ese es el de ejecuciones");
    }

    // ── helpers de reflexion sobre los estaticos del repositorio ──

    @SuppressWarnings("unchecked")
    private BlockingQueue<Object> queue() throws Exception {
        return (BlockingQueue<Object>) getStatic("queue");
    }

    private Document singleQueuedDocument() throws Exception {
        BlockingQueue<Object> q = queue();
        assertEquals(1, q.size(), "se esperaba exactamente un documento encolado");
        Object pending = q.poll();
        Method docAccessor = pending.getClass().getDeclaredMethod("doc");
        docAccessor.setAccessible(true);
        return (Document) docAccessor.invoke(pending);
    }

    private static Object getStatic(String name) throws Exception {
        Field f = MongoHistoryRepository.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(null);
    }

    private static void setStatic(String name, Object value) throws Exception {
        Field f = MongoHistoryRepository.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(null, value);
    }
}
