package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.MultibookApi;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;

/**
 * Una pagina del multibook: la grilla de {@value #BOOKS_PER_PAGE} libros.
 * <p>
 * Cada grilla abierta toma su propio bloque de posiciones globales, asi que dos ventanas pueden estar
 * en la misma pagina a la vez: son copias del mismo libro, cada una con sus suscripciones.
 */
@Slf4j
public class LibroEmergentePrincipalController implements Initializable {

    private static final int BOOKS_PER_PAGE = 10;

    @Getter
    private final static HashMap<Integer, LibroEmergenteController> mapsLibroMaps = new HashMap<>();

    @Getter
    private final HashMap<Integer, LibroEmergenteController> mapsLibroMapsInstance = new HashMap<>();

    public String id = IDGenerator.getID();

    /** Pagina del libro que muestra esta grilla. */
    @Getter
    private int page = -1;

    @Getter
    private int basePosition = -1;

    @FXML
    private LibroEmergenteController libroEmergente0Controller;
    @FXML
    private LibroEmergenteController libroEmergente1Controller;
    @FXML
    private LibroEmergenteController libroEmergente2Controller;
    @FXML
    private LibroEmergenteController libroEmergente3Controller;
    @FXML
    private LibroEmergenteController libroEmergente4Controller;
    @FXML
    private LibroEmergenteController libroEmergente5Controller;
    @FXML
    private LibroEmergenteController libroEmergente6Controller;
    @FXML
    private LibroEmergenteController libroEmergente7Controller;
    @FXML
    private LibroEmergenteController libroEmergente8Controller;
    @FXML
    private LibroEmergenteController libroEmergente9Controller;

    private final List<LibroEmergenteController> books = new ArrayList<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        books.addAll(Arrays.asList(
                libroEmergente0Controller, libroEmergente1Controller, libroEmergente2Controller,
                libroEmergente3Controller, libroEmergente4Controller, libroEmergente5Controller,
                libroEmergente6Controller, libroEmergente7Controller, libroEmergente8Controller,
                libroEmergente9Controller));
    }

    /** Toma un bloque libre de posiciones y suscribe los libros guardados de la pagina. */
    public void attach(int page, JSONArray savedBooks) {

        try {

            this.page = page;
            basePosition = nextAvailableBasePosition();

            for (int slot = 0; slot < books.size(); slot++) {
                LibroEmergenteController controller = books.get(slot);
                int position = basePosition + slot;
                controller.setPositions(position);
                mapsLibroMaps.put(position, controller);
                mapsLibroMapsInstance.put(position, controller);
                Repository.getLibroEmergenteMap().put(position, controller);
            }

            if (savedBooks == null) {
                return;
            }

            for (int i = 0; i < savedBooks.length(); i++) {
                try {
                    JSONObject book = savedBooks.getJSONObject(i);
                    int slot = book.optInt("slot", -1);
                    if (slot < 0 || slot >= books.size() || book.optString("symbol").isBlank()) {
                        continue;
                    }
                    books.get(slot).startSubscribe(MultibookApi.toSubscribe(book));
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /** Libros suscritos ahora mismo, en el formato del documento. */
    public JSONArray books() {
        JSONArray json = new JSONArray();
        for (int slot = 0; slot < books.size(); slot++) {
            LibroEmergenteController controller = books.get(slot);
            if (controller.getSubscribe() != null) {
                json.put(MultibookApi.toJson(controller.getSubscribe()).put("slot", slot));
            }
        }
        return json;
    }

    private int nextAvailableBasePosition() {
        int base = 0;
        while (blockInUse(base)) {
            base += BOOKS_PER_PAGE;
        }
        return base;
    }

    private boolean blockInUse(int base) {
        for (int i = 0; i < BOOKS_PER_PAGE; i++) {
            if (mapsLibroMaps.containsKey(base + i)) {
                return true;
            }
        }
        return false;
    }

    /** Libera las suscripciones al salir de la pagina o cerrar la ventana. */
    public void unsubscribe() {

        mapsLibroMapsInstance.forEach((key, value) -> {
            value.unsubscribe();
            value.isStart = false;
            mapsLibroMaps.remove(key);
            Repository.getLibroEmergenteMap().remove(key);
        });
        mapsLibroMapsInstance.clear();

    }
}
