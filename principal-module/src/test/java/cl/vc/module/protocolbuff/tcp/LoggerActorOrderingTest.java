package cl.vc.module.protocolbuff.tcp;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.FileAppender;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * El log del protocolo debe ser FIFO estricto: si un mensaje entra primero, sale primero.
 *
 * <p>Origen: en el incidente de la orden 92b29dc1 (CHILE, 2026-09-01) el archivo XSGO_*.log mostraba
 * los execution reports en un orden y el servicio los procesaba en otro. La causa es que el logger
 * se creaba como {@code new RoundRobinPool(10).props(LoggerActor.props(...))}: diez actores
 * escribiendo el mismo archivo en paralelo, de modo que el archivo dejaba de ser testigo del orden
 * real del cable. Sin orden confiable en el log, cualquier forense de secuencia es inservible.
 *
 * <p>El pool ademas no aportaba throughput: {@code FileAppender} serializa internamente con un lock
 * (OutputStreamAppender), asi que los diez actores solo se bloqueaban entre si.
 *
 * <p>Estos tests corren sin red ni disco compartido: escriben a un archivo temporal y esperan por
 * condicion con timeout acotado, nunca con sleeps fijos.
 */
class LoggerActorOrderingTest {

    /** Suficiente para que un pool desordene de forma reproducible, y rapido de escribir. */
    private static final int MENSAJES = 2000;
    private static final long TIMEOUT_MS = 120_000L;

    private static final Pattern ID = Pattern.compile("\"id\":\"(\\d+)\"");

    private ActorSystem system;
    private Path archivo;
    private LoggerContext context;
    private Logger fileLog;

    @BeforeEach
    void setUp() throws IOException {
        system = ActorSystem.create("logger-ordering-test");
        archivo = Files.createTempFile("proto-log-order", ".log");

        context = (LoggerContext) LoggerFactory.getILoggerFactory();

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("[%date{ISO8601}] %msg%n");
        encoder.start();

        FileAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender = new FileAppender<>();
        appender.setContext(context);
        appender.setFile(archivo.toString());
        appender.setAppend(true);
        appender.setEncoder(encoder);
        appender.start();

        fileLog = context.getLogger("test.proto.order." + System.nanoTime());
        fileLog.setAdditive(false);
        fileLog.setLevel(Level.ALL);
        fileLog.addAppender(appender);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (system != null) {
            system.terminate();
        }
        if (fileLog != null) {
            fileLog.detachAndStopAllAppenders();
        }
        if (archivo != null) {
            Files.deleteIfExists(archivo);
        }
    }

    /** Un mensaje por id, en secuencia: el archivo debe reproducir exactamente esa secuencia. */
    @Test
    void elLogRespetaElOrdenDeLlegada() throws Exception {
        ActorRef logger = LoggerActor.create(system, fileLog, true);   // wiring real de produccion

        for (int i = 0; i < MENSAJES; i++) {
            logger.tell(orden(i), ActorRef.noSender());
        }

        List<Integer> escritos = esperarLineas(MENSAJES);

        assertEquals(MENSAJES, escritos.size(), "se perdieron lineas de log");
        for (int i = 0; i < MENSAJES; i++) {
            assertEquals(i, escritos.get(i),
                    "el log salio desordenado en la posicion " + i
                            + ": si entra primero, tiene que salir primero");
        }
    }

    /**
     * HFT: el productor (hilo de netty) no puede quedar bloqueado por el disco.
     * {@code tell} es asincrono, asi que encolar N mensajes debe costar microsegundos por mensaje
     * aunque el appender todavia este escribiendo.
     */
    @Test
    void encolarNoBloqueaAlProductor() throws Exception {
        ActorRef logger = LoggerActor.create(system, fileLog, true);   // wiring real de produccion
        RoutingMessage.Order[] mensajes = new RoutingMessage.Order[MENSAJES];
        for (int i = 0; i < MENSAJES; i++) {
            mensajes[i] = orden(i);   // se construyen antes: se mide solo el encolado
        }

        long t0 = System.nanoTime();
        for (RoutingMessage.Order m : mensajes) {
            logger.tell(m, ActorRef.noSender());
        }
        long encoladoMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        // Umbral deliberadamente holgado: detecta que el productor se bloquee en I/O,
        // sin volverse flaky en una maquina cargada.
        assertTrue(encoladoMs < 2_000L,
                "encolar " + MENSAJES + " mensajes tomo " + encoladoMs
                        + " ms: el productor se esta bloqueando en el logger");

        // Y aun asi no se pierde ni se desordena nada.
        List<Integer> escritos = esperarLineas(MENSAJES);
        assertEquals(MENSAJES, escritos.size());
        assertEquals(0, escritos.get(0));
        assertEquals(MENSAJES - 1, escritos.get(MENSAJES - 1));
    }

    /**
     * Estres: volumen alto desde un productor (como el event loop de netty en produccion).
     * Valida que no se pierda ni se desordene una sola linea, y mide el throughput.
     */
    @Test
    void estres_altoVolumen_sinPerdidaNiDesorden() throws Exception {
        final int VOLUMEN = 50_000;
        ActorRef logger = LoggerActor.create(system, fileLog, true);

        RoutingMessage.Order[] mensajes = new RoutingMessage.Order[VOLUMEN];
        for (int i = 0; i < VOLUMEN; i++) {
            mensajes[i] = orden(i);
        }

        long t0 = System.nanoTime();
        for (RoutingMessage.Order m : mensajes) {
            logger.tell(m, ActorRef.noSender());
        }
        long encoladoMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        List<Integer> escritos = esperarLineas(VOLUMEN);
        long totalMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        System.out.printf("[STRESS-LOG] %d lineas | encolado=%d ms | total=%d ms | %,d lineas/s%n",
                VOLUMEN, encoladoMs, totalMs, totalMs > 0 ? (VOLUMEN * 1000L / totalMs) : VOLUMEN);

        assertEquals(VOLUMEN, escritos.size(), "se perdieron lineas bajo estres");
        for (int i = 0; i < VOLUMEN; i++) {
            assertEquals(i, escritos.get(i), "desorden bajo estres en la posicion " + i);
        }
    }

    /**
     * Concurrencia: varios hilos escribiendo a la vez. En produccion hay un solo productor, asi que
     * aqui no se exige orden global entre hilos: se exige que NO se pierda ni se corrompa nada.
     */
    @Test
    void concurrencia_variosProductores_noPierdenNiCorrompen() throws Exception {
        final int HILOS = 8;
        final int POR_HILO = 5_000;
        final int TOTAL = HILOS * POR_HILO;

        ActorRef logger = LoggerActor.create(system, fileLog, true);
        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(HILOS);
        java.util.concurrent.CountDownLatch partida = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch listos = new java.util.concurrent.CountDownLatch(HILOS);

        try {
            for (int h = 0; h < HILOS; h++) {
                final int base = h * POR_HILO;
                pool.submit(() -> {
                    try {
                        partida.await();
                        for (int i = 0; i < POR_HILO; i++) {
                            logger.tell(orden(base + i), ActorRef.noSender());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        listos.countDown();
                    }
                });
            }

            long t0 = System.nanoTime();
            partida.countDown();
            assertTrue(listos.await(60, TimeUnit.SECONDS), "los productores no terminaron");

            List<Integer> escritos = esperarLineas(TOTAL);
            long totalMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

            System.out.printf("[STRESS-LOG] %d hilos x %d = %d lineas | total=%d ms | %,d lineas/s%n",
                    HILOS, POR_HILO, TOTAL, totalMs, totalMs > 0 ? (TOTAL * 1000L / totalMs) : TOTAL);

            assertEquals(TOTAL, escritos.size(), "se perdieron lineas bajo concurrencia");
            // Ninguna linea corrupta ni duplicada: el conjunto tiene que ser exactamente 0..TOTAL-1.
            assertEquals(TOTAL, new java.util.HashSet<>(escritos).size(),
                    "hay lineas duplicadas o corruptas");
        } finally {
            pool.shutdownNow();
        }
    }

    /** Espera por condicion (no sleep fijo) a que el archivo tenga las N lineas. */
    private List<Integer> esperarLineas(int esperadas) throws Exception {
        long limite = System.currentTimeMillis() + TIMEOUT_MS;
        List<Integer> ids = new ArrayList<>();
        while (System.currentTimeMillis() < limite) {
            ids = idsDelArchivo();
            if (ids.size() >= esperadas) {
                return ids;
            }
            Thread.sleep(20);
        }
        return ids;
    }

    private List<Integer> idsDelArchivo() throws IOException {
        List<Integer> ids = new ArrayList<>();
        for (String linea : Files.readAllLines(archivo)) {
            Matcher m = ID.matcher(linea);
            if (m.find()) {
                ids.add(Integer.parseInt(m.group(1)));
            }
        }
        return ids;
    }

    private RoutingMessage.Order orden(int i) {
        return RoutingMessage.Order.newBuilder()
                .setId(String.valueOf(i))
                .setAccount("12345-6")
                .setSymbol("CHILE")
                .setSide(RoutingMessage.Side.BUY)
                .setExecId(String.valueOf(i))
                .setOrderQty(100d)
                .setPrice(197.31)
                .build();
    }
}
