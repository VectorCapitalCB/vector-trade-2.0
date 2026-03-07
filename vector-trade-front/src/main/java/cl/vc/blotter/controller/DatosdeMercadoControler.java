package cl.vc.blotter.controller;

import javafx.fxml.FXML;
import javafx.scene.control.TabPane;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class DatosdeMercadoControler {

    @FXML
    private TabPane tpMkData;


    @FXML
    private void initialize() {
        try {

            System.out.printf("");

        }catch (Exception e){
            log.error(e.getMessage(), e);
        }
    }

}
