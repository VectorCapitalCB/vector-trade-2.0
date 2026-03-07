package cl.vc.blotter.utils;

import cl.vc.blotter.model.StatisticVO;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.scene.control.TableCell;
import javafx.util.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BlinkingColoredCell extends TableCell<StatisticVO, String> {

    private Timeline blinkAnimation;

    public BlinkingColoredCell() {

        try {

            blinkAnimation = new Timeline(
                    new KeyFrame(Duration.seconds(0), evt -> setTransparent()),
                    new KeyFrame(Duration.seconds(0.25), evt -> setSemiTransparent()),
                    new KeyFrame(Duration.seconds(0.5), evt -> setTransparent())
            );
            blinkAnimation.setCycleCount(3);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void setTransparent() {
        setStyle("-fx-background-color: transparent;");
    }

    private void setSemiTransparent() {

        try {

            StatisticVO data = getTableRow().getItem();

            if (data != null) {

                Double imbalance = data.getImbalance();

                if ((imbalance) > 0) {

                    setStyle("-fx-background-color: linear-gradient(to bottom, #4F6228 0%, #86c00d 100%), " +
                            "radial-gradient(center 50% 50%, radius 100%, rgba(255,255,255,0.9), #8fd009);"
                            + "-fx-text-fill: #6ecc0f;");
                } else if ((imbalance) == 0) {

                    setStyle("-fx-background-color: "
                            + "linear-gradient(to bottom, #686868 0%, #A0A0A0 100%), "
                            + "radial-gradient(center 50% 50%, radius 100%, rgba(255,255,255,0.9), #505050);"
                            + "-fx-text-fill: #464844;");
                } else {

                    setStyle("-fx-background-color: "
                            + "linear-gradient(to bottom, #f30505 0%, #B0B0B0 100%), "
                            + "radial-gradient(center 50% 50%, radius 100%, rgba(255,255,255,0.9), #e70909);"
                            + "-fx-text-fill: #f30505;");
                }

            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    protected void updateItem(String item, boolean empty) {
        super.updateItem(item, empty);

        try {
            StatisticVO data = getTableRow().getItem();

            if (empty || item == null) {
                setStyle("");
                setText(null);

            } else {

                if (data == null) {
                    return;
                }
                setText(item);

            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}