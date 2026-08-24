package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.MultibookApi;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;

/** Una pagina dinámica del multibook, de 10 a 50 libros en cinco columnas. */
@Slf4j
public class LibroEmergentePrincipalController implements Initializable {

    public static final int MAX_BOOKS_PER_PAGE = 50;
    private static final int COLUMNS = 5;

    @Getter
    private final static HashMap<Integer, LibroEmergenteController> mapsLibroMaps = new HashMap<>();

    @Getter
    private final HashMap<Integer, LibroEmergenteController> mapsLibroMapsInstance = new HashMap<>();

    public String id = IDGenerator.getID();

    @Getter
    private int page = -1;

    @Getter
    private int basePosition = -1;

    @Getter
    private int bookCount = 10;

    @FXML
    private GridPane bookGrid;

    private final List<LibroEmergenteController> books = new ArrayList<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        for (int column = 0; column < COLUMNS; column++) {
            ColumnConstraints constraints = new ColumnConstraints();
            constraints.setPercentWidth(100.0 / COLUMNS);
            constraints.setHgrow(Priority.ALWAYS);
            bookGrid.getColumnConstraints().add(constraints);
        }
    }

    /** Crea solo los libros configurados y suscribe los símbolos guardados de la página. */
    public void attach(int page, JSONArray savedBooks, int requestedBookCount, int visibleDepth) {
        try {
            this.page = page;
            bookCount = normalizeBookCount(requestedBookCount);
            basePosition = nextAvailableBasePosition();

            for (int slot = 0; slot < bookCount; slot++) {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/LibroEmergente.fxml"));
                Parent view = loader.load();
                LibroEmergenteController controller = loader.getController();
                controller.setVisibleDepth(visibleDepth);

                int position = basePosition + slot;
                controller.setPositions(position);
                books.add(controller);
                mapsLibroMaps.put(position, controller);
                mapsLibroMapsInstance.put(position, controller);
                Repository.getLibroEmergenteMap().put(position, controller);

                GridPane.setColumnIndex(view, slot % COLUMNS);
                GridPane.setRowIndex(view, slot / COLUMNS);
                GridPane.setHgrow(view, Priority.ALWAYS);
                bookGrid.getChildren().add(view);
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

    static int normalizeBookCount(int count) {
        return switch (count) {
            case 20, 30, 40, 50 -> count;
            default -> 10;
        };
    }

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

    public void setSupplementaryVisibility(boolean statisticsVisible, boolean trendVisible) {
        books.forEach(book -> book.setSupplementaryVisibility(statisticsVisible, trendVisible));
    }

    public void setMultibookSettingsAction(Runnable action) {
        for (int i = 0; i < books.size(); i++) {
            books.get(i).setMultibookSettingsAction(i == 0 ? action : null);
        }
    }

    public static void refreshOwnOrderMarkers(String symbol) {
        mapsLibroMaps.values().forEach(book -> book.refreshOwnOrderMarker(symbol));
    }

    private int nextAvailableBasePosition() {
        int base = 0;
        while (blockInUse(base)) {
            base += MAX_BOOKS_PER_PAGE;
        }
        return base;
    }

    private boolean blockInUse(int base) {
        for (int i = 0; i < MAX_BOOKS_PER_PAGE; i++) {
            if (mapsLibroMaps.containsKey(base + i)) {
                return true;
            }
        }
        return false;
    }

    public void unsubscribe() {
        mapsLibroMapsInstance.forEach((key, value) -> {
            value.unsubscribe();
            value.isStart = false;
            mapsLibroMaps.remove(key);
            Repository.getLibroEmergenteMap().remove(key);
        });
        mapsLibroMapsInstance.clear();
        books.clear();
        bookGrid.getChildren().clear();
    }
}
