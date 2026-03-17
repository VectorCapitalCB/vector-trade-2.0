package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.HashMap;
import java.util.ResourceBundle;

@Slf4j
public class LibroEmergentePrincipalController implements Initializable {

    private static final int BOOKS_PER_WINDOW = 10;

    @Getter
    private final static HashMap<Integer, LibroEmergenteController> mapsLibroMaps = new HashMap<>();

    @Getter
    private final HashMap<Integer, LibroEmergenteController> mapsLibroMapsInstance = new HashMap<>();

    public String id = IDGenerator.getID();


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

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

        try {
            int basePosition = nextAvailableBasePosition();

            registerController(basePosition, libroEmergente0Controller);
            registerController(basePosition + 1, libroEmergente1Controller);
            registerController(basePosition + 2, libroEmergente2Controller);
            registerController(basePosition + 3, libroEmergente3Controller);
            registerController(basePosition + 4, libroEmergente4Controller);
            registerController(basePosition + 5, libroEmergente5Controller);
            registerController(basePosition + 6, libroEmergente6Controller);
            registerController(basePosition + 7, libroEmergente7Controller);
            registerController(basePosition + 8, libroEmergente8Controller);
            registerController(basePosition + 9, libroEmergente9Controller);

            mapsLibroMapsInstance.forEach((key, value) -> {
                value.setPositions(key);
                Repository.getLibroEmergenteMap().put(key, value);
            });


            if (Repository.getMultibook() != null) {

                Repository.getMultibook().getSubmultibookList().forEach(s -> {
                    try {
                        if (mapsLibroMaps.containsKey(s.getPositions())) {
                            LibroEmergenteController controller = mapsLibroMaps.get(s.getPositions());
                            Repository.getLibroEmergenteMap().put(s.getPositions(), controller);
                            controller.startSubscribe(s.getSubscribeBook());
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }

    private void registerController(int position, LibroEmergenteController controller) {
        mapsLibroMaps.put(position, controller);
        mapsLibroMapsInstance.put(position, controller);
    }

    private int nextAvailableBasePosition() {
        int base = 0;
        while (windowBlockInUse(base)) {
            base += BOOKS_PER_WINDOW;
        }
        return base;
    }

    private boolean windowBlockInUse(int base) {
        for (int i = 0; i < BOOKS_PER_WINDOW; i++) {
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

    }
}
