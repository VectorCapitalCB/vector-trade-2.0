package cl.vc.service;

import cl.vc.module.protocolbuff.notification.NotificationMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrencia de las colecciones de notificaciones compartidas.
 *
 * Incidente real del 2026-09-03 en RICCI, al arrancar el core:
 * <pre>
 * 13:54:28.354 [SellsideConnect] - connected, FH_IBKR ORB
 * 13:54:28.354 [SellsideConnect] - connected, XSGO XRO
 * 13:54:28.355 [SellsideConnect] - connected, IB_SMART XRO
 * 13:54:28.354 [SellsideConnect] - connected, BCS ORB
 * 13:54:28.363 [SellsideConnect] - Index 1 out of bounds for length 0
 *   java.lang.ArrayIndexOutOfBoundsException
 *     at java.util.ArrayList.add(...)
 *     at cl.vc.service.akka.actors.SellsideConnect.onConnect(SellsideConnect.java:178)
 * </pre>
 *
 * Cuatro conexiones levantaron en el MISMO milisegundo, cada una en su actor, y las cuatro
 * hicieron {@code MainApp.getNotificationMap().add(...)} sobre un ArrayList plano. ArrayList.add
 * no es atomico: dos hilos leen size=0, uno crece el arreglo y el otro escribe en un indice ya
 * invalido. Lo mismo valia para notificationConectionMap, que era un HashMap.
 *
 * Escritores reales: SellsideConnect (lineas 53, 178 y 248) y ActorPerSubscriptionMkd (117).
 * Lector: BuySideConnect (152 y 161), que las vuelca al cliente.
 */
class MainAppNotificationConcurrencyTest {

    private NotificationMessage.Notification notification(int i) {
        return NotificationMessage.Notification.newBuilder()
                .setTitle("Connect")
                .setTypeState(NotificationMessage.TypeState.CONNECTION)
                .setLevel(NotificationMessage.Level.SUCCESS)
                .setSecurityExchange("EXCH-" + i)
                .setMessage("Connected session: EXCH-" + i)
                .build();
    }

    @Test
    void lasColeccionesCompartidasSonConcurrentes() {
        List<NotificationMessage.Notification> list = MainApp.getNotificationMap();
        Map<String, NotificationMessage.Notification> map = MainApp.getNotificationConectionMap();

        assertEquals("CopyOnWriteArrayList", list.getClass().getSimpleName(),
                "un ArrayList plano se corrompe con varios SellsideConnect conectando a la vez");
        assertEquals("ConcurrentHashMap", map.getClass().getSimpleName(),
                "idem para el mapa por destino");
    }

    @Test
    void variasConexionesSimultaneasNoCorrompenLaLista() throws Exception {
        List<NotificationMessage.Notification> list = MainApp.getNotificationMap();
        Map<String, NotificationMessage.Notification> map = MainApp.getNotificationConectionMap();
        int before = list.size();

        int hilos = 8, porHilo = 250;
        ExecutorService pool = Executors.newFixedThreadPool(hilos);
        CountDownLatch salida = new CountDownLatch(1);
        AtomicReference<Throwable> fallo = new AtomicReference<>();
        CountDownLatch listos = new CountDownLatch(hilos);

        for (int h = 0; h < hilos; h++) {
            final int base = h * porHilo;
            pool.submit(() -> {
                try {
                    salida.await();                       // todos arrancan a la vez
                    for (int i = 0; i < porHilo; i++) {
                        NotificationMessage.Notification n = notification(base + i);
                        map.put(n.getSecurityExchange(), n);
                        list.add(n);
                    }
                } catch (Throwable t) {
                    fallo.compareAndSet(null, t);
                } finally {
                    listos.countDown();
                }
            });
        }
        salida.countDown();
        assertTrue(listos.await(30, TimeUnit.SECONDS), "los hilos deben terminar");
        pool.shutdownNow();

        assertNull(fallo.get(), () -> "ningun hilo debe reventar; antes saltaba "
                + "ArrayIndexOutOfBoundsException: " + fallo.get());
        assertEquals(before + hilos * porHilo, list.size(),
                "no se pierde ni se duplica ninguna notificacion");
        assertEquals(hilos * porHilo, map.size());

        // limpieza: la coleccion es estatica y compartida con el resto de la suite
        list.subList(before, list.size()).clear();
        map.clear();
    }

    @Test
    void sePuedeIterarMientrasOtroHiloEscribe() throws Exception {
        List<NotificationMessage.Notification> list = MainApp.getNotificationMap();
        int before = list.size();
        for (int i = 0; i < 100; i++) list.add(notification(i));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> escritor = pool.submit(() -> {
            for (int i = 100; i < 600; i++) list.add(notification(i));
        });

        // BuySideConnect hace exactamente esto: iterar la lista para volcarla al cliente.
        int vistos = 0;
        for (NotificationMessage.Notification ignored : list) vistos++;

        escritor.get(20, TimeUnit.SECONDS);
        pool.shutdownNow();
        assertTrue(vistos >= 100, "el snapshot del iterador debe ser estable, sin ConcurrentModificationException");

        list.subList(before, list.size()).clear();
    }
}
