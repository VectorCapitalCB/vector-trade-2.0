package cl.vc.blotter;

import cl.vc.blotter.controller.LoginController;
import cl.vc.blotter.utils.ConfigGenerator;
import cl.vc.blotter.utils.I18n;
import cl.vc.blotter.utils.NativeLibraryLoader;
import cl.vc.blotter.utils.Notifier;
import cl.vc.blotter.utils.SoundPlayer;
import cl.vc.blotter.utils.VelopackUpdater;
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
import javafx.stage.Stage;
import javafx.stage.StageStyle;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.System.exit;

@Slf4j
public class MainApp extends Application {

    private ScheduledExecutorService appShutdownScheduler;

    public static void main(String[] args) {
        // PRIMERA linea: Velopack lanza el exe con --veloapp-* y espera a que salga.
        VelopackUpdater.handleStartupHooks(args);
        launch(args);
    }

    @Override
    public void start(Stage principal) {
        try {

            Platform.runLater(() -> {

                try {

                    I18n.install();
                    log.info("Inicio UI: internacionalizacion instalada");

                    Repository.getProperties().load(LoginController.class.getResourceAsStream("/blotter/enviroment/application.production.properties"));

                    String appVer  = Repository.getProperties().getProperty("version", "dev");
                    Repository.setAppVersion(appVer);
                    principal.setTitle("Vector Trade 2.0  ·  " + appVer);

                    NativeLibraryLoader.loadNativeLibraries();
                    log.info("Inicio UI: librerias nativas cargadas");
                    // Migracion gradual: las instalaciones nuevas (Velopack) usan el updater
                    // nuevo; las viejas siguen con ConfigGenerator hasta que se reinstalen.
                    if (VelopackUpdater.isInstalled()) {
                        VelopackUpdater.checkForUpdate(principal);
                    } else {
                        ConfigGenerator.checkForUpdateAndGenerateConfig(principal);
                    }

                    SoundPlayer.initialize();
                    log.info("Inicio UI: sonidos inicializados; cargando Login.fxml");


                    FXMLLoader fxmlLoader = new FXMLLoader();
                    fxmlLoader.setLocation(getClass().getResource("/view/Login.fxml"));
                    AnchorPane loginLoader = fxmlLoader.load();
                    log.info("Inicio UI: Login.fxml cargado");
                    LoginController loginController = fxmlLoader.getController();

                    loginLoader.addEventHandler(KeyEvent.KEY_PRESSED, ev -> {
                        if (ev.getCode() == KeyCode.ENTER) {
                            loginController.login();
                        }
                    });


                    Scene stage = new Scene(loginLoader);
                    principal.setScene(stage);
                    principal.setOnCloseRequest(t -> {
                        shutdownSchedulers();
                        SoundPlayer.shutdown();
                        Platform.exit();
                        exit(0);
                        System.exit(0);
                    });

                    Image icon = new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/icono.jpg")));
                    principal.getIcons().add(icon);

                    Repository.principal = principal;
                    Repository.login = stage;

                    principal.setScene(stage);
                    I18n.apply(stage);
                    log.info("Inicio UI: escena preparada; mostrando ventana principal");
                    principal.show();
                    log.info("Inicio UI: ventana principal visible");

                    Notifier.setStage(principal);
                    scheduleAppShutdown(principal);





                } catch (Throwable e) {
                    log.error("Error fatal iniciando la interfaz", e);
                    Platform.exit();
                }

            });

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void scheduleAppShutdown(Stage principal) {
        shutdownSchedulers();
        appShutdownScheduler = Executors.newSingleThreadScheduledExecutor();

        LocalDateTime now = LocalDateTime.now();
        ZonedDateTime zonedNow = now.atZone(ZoneId.of("America/Santiago"));
        ZonedDateTime zonedNext7AM = zonedNow.withHour(7).withMinute(0).withSecond(0).withNano(0);

        if (zonedNow.compareTo(zonedNext7AM) > 0) {
            zonedNext7AM = zonedNext7AM.plusDays(1);
        }

        long delay = ChronoUnit.MILLIS.between(zonedNow, zonedNext7AM);

        appShutdownScheduler.schedule(() -> Platform.runLater(() -> showShutdownMessage(principal)), delay, TimeUnit.MILLISECONDS);
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
            shutdownSchedulers();
            Platform.exit();
            System.exit(0);
        }
    }

    @Override
    public void stop() {
        shutdownSchedulers();
    }

    private void shutdownSchedulers() {
        if (appShutdownScheduler != null) {
            appShutdownScheduler.shutdownNow();
            appShutdownScheduler = null;
        }
    }
}
