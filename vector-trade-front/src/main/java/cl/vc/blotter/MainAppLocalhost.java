package cl.vc.blotter;

import cl.vc.blotter.controller.LoginController;
import cl.vc.blotter.utils.NativeLibraryLoader;
import cl.vc.blotter.utils.Notifier;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.control.ButtonType;
import javafx.scene.image.Image;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.stage.Stage;
import javafx.stage.StageStyle;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.System.exit;

@Slf4j
public class MainAppLocalhost extends Application {

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage principal) {
        try {

            Repository.getProperties().load(LoginController.class.getResourceAsStream("/blotter/enviroment/application.localhost.properties"));
            NativeLibraryLoader.loadNativeLibraries();

            try {
                Repository.setMediaPlayerNew(new MediaPlayer(new Media(MainAppLocalhost.class.getResource("/sounds/new.mp3").toExternalForm())));
                Repository.setMediaPlayerReject(new MediaPlayer(new Media(MainAppLocalhost.class.getResource("/sounds/rejected.mp3").toExternalForm())));
                Repository.setMediaPlayerTrade(new MediaPlayer(new Media(MainAppLocalhost.class.getResource("/sounds/trade.mp3").toExternalForm())));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            //Repository.setAutologin(true);

            FXMLLoader fxmlLoader = new FXMLLoader();
            fxmlLoader.setLocation(getClass().getResource("/view/Login.fxml"));
            AnchorPane loginLoader = fxmlLoader.load();
            LoginController loginController = fxmlLoader.getController();

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                // Notifier.setCoolingDown(false);
            }, 0, 3, TimeUnit.SECONDS);

            loginLoader.addEventHandler(KeyEvent.KEY_PRESSED, ev -> {
                if (ev.getCode() == KeyCode.ENTER) {
                    loginController.login();
                }
            });

            Scene stage = new Scene(loginLoader);
            principal.setScene(stage);
            principal.setOnCloseRequest(t -> {
                Platform.exit();
                exit(0);
                System.exit(0);
            });

            Image icon = new Image(getClass().getResourceAsStream("/blotter/img/icono.jpg"));
            principal.getIcons().add(icon);

            Repository.principal = principal;
            Repository.login = stage;

            principal.setScene(stage);
            principal.show();

            Notifier.setStage(principal);
            scheduleAppShutdown(principal);




        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void scheduleAppShutdown(Stage principal) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable shutdownTask = () -> {
            Platform.runLater(() -> showShutdownMessage(principal));
            scheduleAppShutdown(principal);
        };

        LocalDateTime now = LocalDateTime.now();
        ZonedDateTime zonedNow = now.atZone(ZoneId.of("America/Santiago"));
        ZonedDateTime zonedNext7AM = zonedNow.withHour(7).withMinute(0).withSecond(0).withNano(0);

        if (zonedNow.compareTo(zonedNext7AM) > 0) {
            zonedNext7AM = zonedNext7AM.plusDays(1);
        }

        long delay = ChronoUnit.MILLIS.between(zonedNow, zonedNext7AM);

        scheduler.schedule(shutdownTask, delay, TimeUnit.MILLISECONDS);
    }

    private void showShutdownMessage(Stage principal) {
        Alert alert = new Alert(AlertType.INFORMATION, "Alcanzaste el límite de tiempo. La aplicación se va a cerrar.", ButtonType.OK);
        alert.setTitle("Aviso");
        alert.setHeaderText(null);
        alert.initOwner(principal);


        Stage alertStage = (Stage) alert.getDialogPane().getScene().getWindow();
        alertStage.initStyle(StageStyle.UNDECORATED);


        alert.getDialogPane().getStylesheets().add(getClass().getResource("/blotter/css/style.css").toExternalForm());
        alert.getDialogPane().getStyleClass().add("your-dialog-class");


        Optional<ButtonType> result = alert.showAndWait();
        if (result.isPresent() && result.get().getButtonData() == ButtonData.OK_DONE) {
            Platform.exit();
            System.exit(0);
        }
    }
}
