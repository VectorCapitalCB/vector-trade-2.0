package cl.vc.blotter.utils;

import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.blotter.Repository;
import cl.vc.blotter.adaptor.OrderStateReconciler;
import cl.vc.blotter.controller.BasketTabController;
import cl.vc.blotter.controller.LibroEmergentePrincipalController;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.AnchorPane;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gestión de los tabs por canasta (basket) en el front.
 *
 * Cada canasta pegada abre un tab CERRABLE al lado de "Ejecutadas" (dentro de ruteoTabPane),
 * identificado por su basketID, con la lista de órdenes/ejecuciones en vivo y un panel de
 * estadísticas calculado EN EL FRONT.
 *
 * Persistencia: el mapeo orderId -> basketID se guarda en disco (Gson, por usuario, misma
 * carpeta que columnConfig). Al reiniciar el front se recarga y, cuando el servidor reenvía
 * las órdenes vivas, el tab de la canasta se RECREA on-demand y se vuelve a agrupar.
 */
@Slf4j
public final class BasketTabs {

    private static final Map<String, String> orderIdToBasketId = new ConcurrentHashMap<>();
    private static final Map<String, RoutingMessage.Order> latestOrdersById = new ConcurrentHashMap<>();
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static volatile boolean loaded = false;

    private BasketTabs() {
    }

    // ──────────────────────────── API ────────────────────────────

    /** Crea (o actualiza) el tab de la canasta y siembra sus órdenes. Debe ejecutarse en el hilo FX. */
    public static void openOrUpdate(BktStrategyProtos.Basket basket) {
        try {
            if (basket == null || Repository.getIsLight()) return;
            ensureLoaded();

            final String bid = basket.getBasketID();
            if (bid == null || bid.isEmpty()) return;

            boolean existed = Repository.getBasketTabController().containsKey(bid);
            BasketTabController ctrl = getOrCreateController(bid);
            if (ctrl == null) return; // ya notificado

            boolean changed = false;
            for (RoutingMessage.Order o : basket.getOrdersList()) {
                RoutingMessage.Order latest = rememberLatest(o);
                if (!bid.equals(orderIdToBasketId.put(latest.getId(), bid))) changed = true;
                ctrl.upsertOrder(latest);
            }
            ctrl.recomputeStats();
            if (changed) persist();

            if (!existed) {
                Notifier.INSTANCE.notifyInfo("Canasta abierta",
                        bid + " — " + basket.getOrdersCount() + " órdenes (pestaña Ruteo, al lado de Ejecutadas)");
            }

        } catch (Exception e) {
            log.error("No se pudo abrir/actualizar el tab de la canasta", e);
            Notifier.INSTANCE.notifyError("No se pudo abrir el tab de la canasta",
                    e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Enruta una orden entrante a su tab de canasta si pertenece a una. Recrea el tab si hace
     * falta (caso reinicio). Devuelve true si la manejó. Hilo FX.
     */
    public static boolean route(RoutingMessage.Order order) {
        try {
            if (order == null) return false;

            RoutingMessage.Order latest = rememberLatest(order);
            Repository.updateLiveOrderLevel(latest);
            LibroEmergentePrincipalController.refreshOwnOrderMarkers(latest.getSymbol());

            if (Repository.getIsLight()) return false;
            ensureLoaded();

            String bid = latest.getBasketID();
            if (bid == null || bid.isEmpty()) {
                bid = orderIdToBasketId.get(latest.getId());
            }
            if (bid == null || bid.isEmpty()) return false;

            BasketTabController ctrl = getOrCreateController(bid);
            if (ctrl == null) return false;

            boolean changed = !bid.equals(orderIdToBasketId.put(latest.getId(), bid));
            ctrl.upsertOrder(latest);
            ctrl.recomputeStats();

            // Las órdenes terminales no se reenvían tras reiniciar: se sacan del mapa persistido.
            if (OrderStateReconciler.isTerminal(latest.getOrdStatus())) {
                if (orderIdToBasketId.remove(latest.getId()) != null) changed = true;
            }
            if (changed) persist();
            return true;

        } catch (Exception e) {
            log.error("No se pudo enrutar la orden al tab de la canasta", e);
            return false;
        }
    }

    private static RoutingMessage.Order rememberLatest(RoutingMessage.Order incoming) {
        RoutingMessage.Order previous = latestOrdersById.get(incoming.getId());
        RoutingMessage.Order latest = OrderStateReconciler.latest(previous, incoming);
        latestOrdersById.put(incoming.getId(), latest);

        if (previous != null && latest == previous && previous != incoming) {
            log.warn("[OrderSync] Evento atrasado de canasta ignorado id={} currentStatus={} incomingStatus={} currentCumQty={} incomingCumQty={}",
                    incoming.getId(), previous.getOrdStatus(), incoming.getOrdStatus(),
                    previous.getCumQty(), incoming.getCumQty());
        }
        return latest;
    }

    public static void clearRuntimeOrderState() {
        latestOrdersById.clear();
    }

    // ──────────────────────────── interno ────────────────────────────

    /** Devuelve el controller de la canasta, creando el tab (en ruteoTabPane) si no existe. */
    private static BasketTabController getOrCreateController(String bid) {
        BasketTabController ctrl = Repository.getBasketTabController().get(bid);
        if (ctrl != null) return ctrl;

        try {
            TabPane pane = Repository.getRoutingController() == null
                    ? null : Repository.getRoutingController().getRuteoTabPane();
            if (pane == null) {
                Notifier.INSTANCE.notifyError("Canasta",
                        "No se encontró el panel de Ruteo para abrir el tab de la canasta.");
                return null;
            }

            FXMLLoader loader = new FXMLLoader(BasketTabs.class.getResource("/view/BasketMainTabView.fxml"));
            AnchorPane root = loader.load();
            ctrl = loader.getController();

            Tab tab = new Tab();
            tab.setClosable(true);
            tab.setContent(root);
            ctrl.bindTab(tab, bid);
            final String fbid = bid;
            tab.setOnClosed(e -> forgetBasket(fbid));

            Repository.getBasketTabController().put(bid, ctrl);
            pane.getTabs().add(tab);               // al lado de "Ejecutadas" (último tab del pane)
            pane.getSelectionModel().select(tab);
            return ctrl;

        } catch (Exception e) {
            log.error("No se pudo crear el tab de la canasta", e);
            Notifier.INSTANCE.notifyError("No se pudo abrir el tab de la canasta",
                    e.getClass().getSimpleName() + ": " + e.getMessage());
            return null;
        }
    }

    /** Cierra/olvida una canasta: saca su tab y sus órdenes del mapa persistido (no se recrea). */
    private static void forgetBasket(String bid) {
        Repository.getBasketTabController().remove(bid);
        orderIdToBasketId.entrySet().removeIf(en -> bid.equals(en.getValue()));
        persist();
    }

    // ──────────────────────────── persistencia (Gson) ────────────────────────────

    private static synchronized void ensureLoaded() {
        if (loaded) return;
        loaded = true;
        try {
            File f = basketFile();
            if (f.exists()) {
                try (FileReader r = new FileReader(f)) {
                    Type t = new TypeToken<Map<String, String>>() {}.getType();
                    Map<String, String> m = GSON.fromJson(r, t);
                    if (m != null) orderIdToBasketId.putAll(m);
                }
            }
        } catch (Exception e) {
            log.error("No se pudo cargar baskets.json", e);
        }
    }

    private static void persist() {
        try {
            File f = basketFile();
            File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            try (FileWriter w = new FileWriter(f)) {
                GSON.toJson(new HashMap<>(orderIdToBasketId), w);
            }
        } catch (Exception e) {
            log.error("No se pudo guardar baskets.json", e);
        }
    }

    /** Mismo esquema de carpeta/usuario que ConfigManager (user.home/{company}/{application}/{user}baskets.json). */
    private static File basketFile() {
        String userHome = System.getProperty("user.home");
        Properties props = Repository.getProperties();
        String company = props != null && props.getProperty("company") != null ? props.getProperty("company") : "vc";
        String application = props != null && props.getProperty("application") != null ? props.getProperty("application") : "VectorTrade";

        File dir = new File(userHome + File.separator + company + File.separator + application);
        if (!dir.exists() && !dir.mkdirs()) dir = new File(userHome);

        String user = Repository.getUsername();
        if (user == null || user.isBlank()) user = System.getProperty("user.name", "default");
        user = user.replaceAll("[\\\\/:*?\"<>|]", "_");
        return new File(dir, user + "baskets.json");
    }
}
