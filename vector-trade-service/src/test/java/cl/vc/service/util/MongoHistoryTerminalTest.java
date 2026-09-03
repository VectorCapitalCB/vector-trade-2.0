package cl.vc.service.util;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Seguimiento del id de la orden en el historico multi-dia.
 *
 * Regresion que fija: en el 2.0 {@code recordOrderTerminal} delegaba en
 * {@code recordOrderSummary}, que corta en {@link MongoHistoryRepository#isPersistableFill} —
 * exige execType EXEC_TRADE y estado FILLED/PARTIALLY_FILLED. Una orden CANCELED o REJECTED que
 * nunca ejecuto no llegaba nunca al historico y se perdia el rastro de su id: el operador no
 * podia reconstruir que paso con la orden. Produccion la registra igual
 * (PROD CORE MongoHistoryRepository.recordOrderTerminal), y ese es el comportamiento que se porta.
 *
 * El filtro de fills se conserva intacto para el camino de ejecuciones: lo que cambia es solo
 * la puerta del estado TERMINAL.
 */
class MongoHistoryTerminalTest {

    private RoutingMessage.Order order(RoutingMessage.ExecutionType execType,
                                       RoutingMessage.OrderStatus status,
                                       double cumQty) {
        return RoutingMessage.Order.newBuilder()
                .setId("ord-terminal-1")
                .setAccount("12345-6")
                .setSymbol("SQM-B")
                .setSide(RoutingMessage.Side.BUY)
                .setExecType(execType)
                .setOrdStatus(status)
                .setCumQty(cumQty)
                .setOrderQty(1_000d)
                .build();
    }

    @Test
    void unFillSiEsPersistibleComoEjecucion() {
        assertTrue(MongoHistoryRepository.isPersistableFill(
                order(RoutingMessage.ExecutionType.EXEC_TRADE,
                        RoutingMessage.OrderStatus.PARTIALLY_FILLED, 100d)));
        assertTrue(MongoHistoryRepository.isPersistableFill(
                order(RoutingMessage.ExecutionType.EXEC_TRADE,
                        RoutingMessage.OrderStatus.FILLED, 1_000d)));
    }

    @Test
    void unaCanceladaSinEjecucionNoPasaElFiltroDeFills() {
        // Este es el motivo por el que recordOrderTerminal NO puede delegar en recordOrderSummary:
        // el filtro de fills la descarta, y con ella se pierde el rastro del id.
        assertFalse(MongoHistoryRepository.isPersistableFill(
                order(RoutingMessage.ExecutionType.EXEC_CANCELED,
                        RoutingMessage.OrderStatus.CANCELED, 0d)));
        assertFalse(MongoHistoryRepository.isPersistableFill(
                order(RoutingMessage.ExecutionType.EXEC_REJECTED,
                        RoutingMessage.OrderStatus.REJECTED, 0d)));
    }

    @Test
    void recordOrderTerminalNoDependeDelFiltroDeFills() throws Exception {
        // recordOrderTerminal ya no llama a isPersistableFill: su unica guarda es 'connected'.
        // Se verifica sobre el bytecode para no depender de una conexion Mongo viva en el test.
        String src = java.nio.file.Files.readString(java.nio.file.Path.of(
                "src/main/java/cl/vc/service/util/MongoHistoryRepository.java"));
        int i = src.indexOf("public static void recordOrderTerminal");
        assertTrue(i > 0, "recordOrderTerminal debe existir");
        String body = src.substring(i, src.indexOf("\n    }", i));
        assertFalse(body.contains("isPersistableFill"),
                "recordOrderTerminal no debe filtrar por fill: una CANCELED/REJECTED tambien se registra");
        assertFalse(body.contains("recordOrderSummary"),
                "recordOrderTerminal no debe delegar en recordOrderSummary");
        assertTrue(body.contains("WriteKind.SUMMARY"),
                "debe encolar el documento resumen de la orden");
    }
}
