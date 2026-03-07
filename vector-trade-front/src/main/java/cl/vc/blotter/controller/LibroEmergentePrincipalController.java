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

            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente0Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente0Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente1Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente1Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente2Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente2Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente3Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente3Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente4Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente4Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente5Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente5Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente6Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente6Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente7Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente7Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente8Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente8Controller);
            Repository.countMultibook = Repository.countMultibook + 1;
            mapsLibroMaps.put(Repository.countMultibook, libroEmergente9Controller);
            mapsLibroMapsInstance.put(Repository.countMultibook, libroEmergente9Controller);
            Repository.countMultibook = Repository.countMultibook + 1;


            mapsLibroMaps.forEach((key, value) -> {
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

    public void unsubscribe() {

        mapsLibroMapsInstance.forEach((key, value) -> {
            value.unsubscribe();
            value.isStart = false;
        });

    }
}
