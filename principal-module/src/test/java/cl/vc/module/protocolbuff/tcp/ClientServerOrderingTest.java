package cl.vc.module.protocolbuff.tcp;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import io.netty.channel.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Extremo a extremo sobre la conexion real: el servidor empuja una rafaga y el cliente la recibe
 * EN EL MISMO ORDEN.
 *
 * <p>Es la direccion que importa: la bolsa/multiplexor empuja execution reports y el servicio los
 * consume como cliente. Se valida el camino completo — canal netty, decodificacion protobuf y el
 * {@code tell} al receiverActor de {@link NettyProtobufClient} — no una simulacion.
 *
 * <p>Complementa a {@link LoggerActorOrderingTest}, que cubre el orden del archivo de log.
 */
class ClientServerOrderingTest {

    private static final int RAFAGA = 3000;
    private static final long TIMEOUT_MS = 60_000L;

    private ActorSystem system;
    private NettyProtobufServer server;
    private NettyProtobufClient client;
    private Path dirLogs;

    @AfterEach
    void tearDown() throws Exception {
        if (system != null) {
            system.terminate();
        }
        if (dirLogs != null) {
            try (var s = Files.walk(dirLogs)) {
                s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }

    /** Actor que anota el execId de cada Order que le llega, en orden de llegada. */
    static class Grabador extends AbstractActor {
        private final List<String> recibidos;

        Grabador(List<String> recibidos) {
            this.recibidos = recibidos;
        }

        static Props props(List<String> recibidos) {
            return Props.create(Grabador.class, () -> new Grabador(recibidos));
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(TransportingObjects.class, t -> {
                        if (t.getMessage() instanceof RoutingMessage.Order o) {
                            recibidos.add(o.getExecId());
                        }
                    })
                    .matchAny(m -> { /* Connect/Disconnect/Ping: no interesan para el orden */ })
                    .build();
        }
    }

    @Test
    void elClienteRecibeLaRafagaEnOrden() throws Exception {
        system = ActorSystem.create("client-server-ordering-test");
        dirLogs = Files.createTempDirectory("proto-e2e-logs");

        int puerto = puertoLibre();
        List<String> recibidosServidor = new CopyOnWriteArrayList<>();
        List<String> recibidosCliente = new CopyOnWriteArrayList<>();

        server = new NettyProtobufServer("0.0.0.0:" + puerto,
                system.actorOf(Grabador.props(recibidosServidor)),
                dirLogs.resolve("SRV").toString(), "TEST", false);
        server.start();

        client = new NettyProtobufClient("127.0.0.1:" + puerto,
                system.actorOf(Grabador.props(recibidosCliente)),
                dirLogs.resolve("CLI").toString(), "TEST",
                NotificationMessage.Component.XRO, false);
        new Thread(client).start();

        Channel canal = esperarCanalConectado();
        assertNotNull(canal, "el cliente no se conecto al servidor dentro del timeout");

        // El servidor empuja la rafaga por el canal real, en orden estricto.
        List<RoutingMessage.Order> rafaga = new ArrayList<>(RAFAGA);
        for (int i = 0; i < RAFAGA; i++) {
            rafaga.add(fill(i));
        }
        for (RoutingMessage.Order o : rafaga) {
            canal.writeAndFlush(o);
        }

        esperar(recibidosCliente, RAFAGA);

        assertEquals(RAFAGA, recibidosCliente.size(),
                "el cliente no recibio toda la rafaga (llegaron " + recibidosCliente.size() + ")");
        for (int i = 0; i < RAFAGA; i++) {
            assertEquals(String.valueOf(i), recibidosCliente.get(i),
                    "el cliente recibio fuera de orden en la posicion " + i
                            + ": si sale primero, tiene que llegar primero");
        }
    }

    /** Toma el canal del cliente desde la lista interna del servidor (solo lectura, para el test). */
    @SuppressWarnings("unchecked")
    private Channel esperarCanalConectado() throws Exception {
        Field f = NettyProtobufServer.class.getDeclaredField("channels");
        f.setAccessible(true);
        long limite = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < limite) {
            List<Channel> canales = (List<Channel>) f.get(server);
            synchronized (canales) {
                if (!canales.isEmpty() && canales.get(0).isActive()) {
                    return canales.get(0);
                }
            }
            Thread.sleep(50);
        }
        return null;
    }

    private void esperar(List<String> recibidos, int esperados) throws InterruptedException {
        long limite = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < limite && recibidos.size() < esperados) {
            Thread.sleep(20);
        }
    }

    private int puertoLibre() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private RoutingMessage.Order fill(int i) {
        return RoutingMessage.Order.newBuilder()
                .setId("orden-e2e")
                .setAccount("12345-6")
                .setSymbol("CHILE")
                .setSide(RoutingMessage.Side.BUY)
                .setExecId(String.valueOf(i))
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                .setLastQty(106d)
                .setLastPx(194.58)
                .setCumQty(106d * (i + 1))
                .setOrderQty(106d * RAFAGA)
                .build();
    }
}
