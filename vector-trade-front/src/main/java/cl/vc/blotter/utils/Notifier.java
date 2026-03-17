package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import eu.hansolo.enzo.notification.Notification;
import javafx.animation.KeyFrame;
import javafx.animation.KeyValue;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.effect.DropShadow;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.stage.Popup;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.stage.Window;
import javafx.util.Duration;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
public enum Notifier {
    INSTANCE;

    private static final double ICON_WIDTH = 30;
    private static final double ICON_HEIGHT = 30;

    private static double width = 300;
    private static double height = 80;
    private static double offsetX = 10;
    private static double offsetY = 10;
    private static double spacingY = 25;
    private static Pos popupLocation = Pos.TOP_RIGHT;
    private static Stage stageRef = null;
    private Duration popupLifetime;
    private StackPane pane;
    private Scene scene;
    private ObservableList<Popup> popups;
    private List<String> alertBlock = new ArrayList<>();


    private Notifier() {
        init();
        initGraphics();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable task = new Runnable() {
            @Override
            public void run() {
                alertBlock.clear();
            }
        };

        scheduler.scheduleAtFixedRate(task, 0, 30, TimeUnit.SECONDS);

    }

    public static void setPopupLocation(Pos location) {
        popupLocation = location;
    }

    public static void setNotificationOwner(final Stage OWNER) {
        Repository.principal.initOwner(OWNER);
    }

    public static void setOffsetX(final double OFFSET_X) {
        Notifier.offsetX = OFFSET_X;
    }

    public static void setOffsetY(final double OFFSET_Y) {
        Notifier.offsetY = OFFSET_Y;
    }

    public static void setWidth(final double WIDTH) {
        Notifier.width = WIDTH;
    }

    public static void setHeight(final double HEIGHT) {
        Notifier.height = HEIGHT;
    }

    public static void setSpacingY(final double SPACING_Y) {
        Notifier.spacingY = SPACING_Y;
    }

    public static void setStage(Stage stage) {
        stageRef = stage;
    }

    private void init() {
        popupLifetime = Duration.millis(4000);
        popups = FXCollections.observableArrayList();
    }

    private void initGraphics() {
        pane = new StackPane();
        scene = new Scene(pane);
        scene.setFill(null);
        scene.getStylesheets().add(getClass().getResource(Repository.getSTYLE()).toExternalForm());

    }

    public void stop() {
        popups.clear();
        Repository.principal.close();
    }

    public Duration getPopupLifetime() {
        return popupLifetime;
    }

    public void setPopupLifetime(final Duration POPUP_LIFETIME) {
        popupLifetime = Duration.millis(clamp(2000, 20000, POPUP_LIFETIME.toMillis()));
    }

    public void notify(final Notification NOTIFICATION) {
        preOrder();
        showPopup(NOTIFICATION);
    }

    public void notify(final String TITLE, final String MESSAGE, final Image IMAGE) {
        Platform.runLater(new Runnable() {
            public void run() {
                if (!alertBlock.contains(MESSAGE) && Repository.isNotification()) {
                    alertBlock.add(MESSAGE);
                    Notifier.INSTANCE.notify(new Notification(TITLE, MESSAGE, IMAGE));
                }
            }
        });
    }

    public void notifyInfo(final String TITLE, final String MESSAGE) {
        Platform.runLater(new Runnable() {
            public void run() {

                if (!alertBlock.contains(MESSAGE) && Repository.isNotification()) {
                    alertBlock.add(MESSAGE);
                    Notifier.INSTANCE.notify(new Notification(TITLE, MESSAGE, Notification.INFO_ICON));
                }
            }
        });
    }

    public void notifyWarning(final String TITLE, final String MESSAGE) {
        Platform.runLater(new Runnable() {
            public void run() {

                if (!alertBlock.contains(MESSAGE) && Repository.isNotification()) {
                    alertBlock.add(MESSAGE);
                    Notifier.INSTANCE.notify(new Notification(TITLE, MESSAGE, Notification.WARNING_ICON));
                }
            }
        });
    }

    public void notifySuccess(final String TITLE, final String MESSAGE) {

        Platform.runLater(new Runnable() {
            public void run() {

                if (!alertBlock.contains(MESSAGE) && Repository.isNotification()) {
                    alertBlock.add(MESSAGE);
                    Notifier.INSTANCE.notify(new Notification(TITLE, MESSAGE, Notification.SUCCESS_ICON));
                }
            }
        });
    }

    public void notifyError(final String TITLE, final String MESSAGE) {

        Platform.runLater(new Runnable() {
            public void run() {
                if (!alertBlock.contains(MESSAGE) && Repository.isNotification()) {
                    alertBlock.add(MESSAGE);
                    Notifier.INSTANCE.notify(new Notification(TITLE, MESSAGE, Notification.ERROR_ICON));
                }
            }
        });

    }

    private double clamp(final double MIN, final double MAX, final double VALUE) {
        if (VALUE < MIN) return MIN;
        if (VALUE > MAX) return MAX;
        return VALUE;
    }

    private void preOrder() {
        double currentY = offsetY + height + spacingY;
        if (!popups.isEmpty()) {

            Popup lastPopup = popups.get(popups.size() - 1);
            currentY = lastPopup.getY() - spacingY - height;
        }


        for (int i = 0; i < popups.size(); i++) {
            popups.get(i).setY(currentY);
            currentY -= height + spacingY;
        }
    }

    private void showPopup(final Notification NOTIFICATION) {
        Region body = new Region();
        body.getStylesheets().add(getClass().getResource(Repository.getSTYLE()).toExternalForm());
        body.getStyleClass().addAll("body");
        body.setPrefSize(width, height);

        Label title = new Label(NOTIFICATION.TITLE);
        title.getStylesheets().add(getClass().getResource(Repository.getSTYLE()).toExternalForm());
        title.getStyleClass().add("title");

        ImageView icon = new ImageView(NOTIFICATION.IMAGE);
        icon.setFitWidth(ICON_WIDTH);
        icon.setFitHeight(ICON_HEIGHT);


        Label message = new Label(NOTIFICATION.MESSAGE);
        message.getStylesheets().add(getClass().getResource(Repository.getSTYLE()).toExternalForm());
        message.getStyleClass().add("message");

        VBox popupLayout = new VBox();
        popupLayout.setSpacing(10);
        popupLayout.setPadding(new Insets(10, 10, 10, 10));
        popupLayout.getChildren().addAll(title, message);

        HBox hb = new HBox(10);
        hb.setAlignment(Pos.CENTER_LEFT);
        hb.setPadding(new Insets(8, 8, 8, 18));
        hb.getChildren().addAll(icon, popupLayout);

        StackPane popupPane = new StackPane();
        popupPane.setMouseTransparent(true);
        popupPane.setFocusTraversable(false);

        DropShadow ds = new DropShadow();
        ds.setOffsetY(0.1);
        ds.setOffsetX(0.1);
        ds.setColor(Color.GRAY);
        popupPane.setEffect(ds);

        popupPane.getStylesheets().add(getClass().getResource(Repository.getSTYLE()).toExternalForm());
        popupPane.getStyleClass().add("notification");
        popupPane.getChildren().addAll(body, hb);

        final Popup POPUP = new Popup();
        POPUP.setAutoFix(false);
        POPUP.setAutoHide(false);
        POPUP.setHideOnEscape(false);
        POPUP.setX(getX());
        POPUP.setY(getY());
        POPUP.getContent().add(popupPane);

        popups.add(POPUP);


        KeyValue fadeOutBegin = new KeyValue(POPUP.opacityProperty(), 1.0);
        KeyValue fadeOutEnd = new KeyValue(POPUP.opacityProperty(), 0.0);

        KeyFrame kfBegin = new KeyFrame(Duration.ZERO, fadeOutBegin);
        KeyFrame kfEnd = new KeyFrame(Duration.millis(500), fadeOutEnd);

        Timeline timeline = new Timeline(kfBegin, kfEnd);
        timeline.setDelay(popupLifetime);

        timeline.setOnFinished(new EventHandler<ActionEvent>() {
            @Override
            public void handle(final ActionEvent e) {
                Platform.runLater(
                        new Runnable() {
                            @Override
                            public void run() {
                                POPUP.hide();
                                popups.remove(POPUP);
                            }
                        });
            }
        });


        Window owner = stageRef != null ? stageRef : Repository.getPrincipal();
        if (owner != null && owner.isShowing()) {
            POPUP.show(owner);
        } else {
            log.debug("Notification popup skipped: owner window is not showing");
            popups.remove(POPUP);
            return;
        }
        timeline.play();
    }

    private double getX() {
        Screen screen = Screen.getPrimary();
        if (stageRef != null) {
            for (Screen s : Screen.getScreens()) {
                if (s.getBounds().contains(stageRef.getX() + stageRef.getWidth()
                        / 2, stageRef.getY() + stageRef.getHeight() / 2)) {
                    screen = s;
                    break;
                }
            }
        }
        return calcX(screen.getVisualBounds().getMinX(), screen.getVisualBounds().getWidth());
    }

    private double getY() {
        Screen screen = Screen.getPrimary();
        if (stageRef != null) {
            for (Screen s : Screen.getScreens()) {
                if (s.getBounds().contains(stageRef.getX(), stageRef.getY())) {
                    screen = s;
                    break;
                }
            }
        }
        double calculatedY = calcY(screen.getVisualBounds().getMinY(), screen.getVisualBounds().getHeight());
        return calculatedY;
    }

    private double calcX(final double LEFT, final double TOTAL_WIDTH) {
        switch (popupLocation) {
            case TOP_LEFT:
            case CENTER_LEFT:
            case BOTTOM_LEFT:
                return LEFT + offsetX;
            case TOP_CENTER:
            case CENTER:
            case BOTTOM_CENTER:
                return LEFT + (TOTAL_WIDTH - width) * 0.5;
            case TOP_RIGHT:
            case CENTER_RIGHT:
            case BOTTOM_RIGHT:
                return LEFT + TOTAL_WIDTH - width - offsetX;
            default:
                return 0.0;
        }
    }

    private double calcY(final double TOP, final double TOTAL_HEIGHT) {
        switch (popupLocation) {
            case TOP_LEFT:
            case TOP_CENTER:
            case TOP_RIGHT:
                return TOP + offsetY;
            case CENTER_LEFT:
            case CENTER:
            case CENTER_RIGHT:
                return TOP + (TOTAL_HEIGHT - height) * 0.5;
            case BOTTOM_LEFT:
            case BOTTOM_CENTER:
            case BOTTOM_RIGHT:

                return TOP + TOTAL_HEIGHT - height - offsetY;
            default:
                return 0.0;
        }
    }

}
