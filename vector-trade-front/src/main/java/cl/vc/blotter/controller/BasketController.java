package cl.vc.blotter.controller;

import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.OrdersHelper;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TabPane;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.awt.*;
import java.io.File;

@Slf4j
@Data
public class BasketController {

    @FXML
    TabPane tabBasket;
    @FXML
    private Button excelDownload;
    @FXML
    private Button basketCopy;
    @FXML
    private HBox menubasket;


    @FXML
    private void initialize() {


        Image image = new Image(getClass().getResourceAsStream("/blotter/img/excel.png"));
        ImageView imageView = new ImageView(image);
        imageView.setFitHeight(35);
        imageView.setFitWidth(35);
        this.excelDownload.setGraphic(imageView);

        imageView = new ImageView(new Image(getClass().getResourceAsStream("/blotter/img/basket.png")));
        imageView.setFitHeight(35);
        imageView.setFitWidth(35);
        this.basketCopy.setGraphic(imageView);

        ContextMenu contextMenu = new ContextMenu();
        MenuItem pasteBasket = new MenuItem("Paste basket");
        contextMenu.getItems().add(pasteBasket);

        basketCopy.setOnAction(e -> {
            BktStrategyProtos.Basket basket = OrdersHelper.sendNewBasketFromClipboard();
            Repository.getClientService().sendMessage(basket);
        });

        Repository.setBasketController(this);

    }

    @FXML
    private void exportBasketsFormat() {
        try {

            File excelFile = new File(BasketController.class.getResource("/excel/basket.xlsx").toURI());
            Desktop.getDesktop().open(excelFile);

        } catch (Exception e) {
            log.error("Could not open the baskets format excel.", e);
        }
    }
}
