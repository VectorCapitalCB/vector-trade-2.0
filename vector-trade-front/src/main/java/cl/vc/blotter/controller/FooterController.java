package cl.vc.blotter.controller;

import cl.vc.blotter.utils.CandleWindow;
import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.I18n;
import cl.vc.blotter.utils.Language;
import cl.vc.blotter.utils.Notifier;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.crypt.AESEncryption;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import eu.hansolo.enzo.notification.Notification;
import javafx.application.Platform;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Rectangle2D;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Screen;
import javafx.stage.Stage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import cl.vc.blotter.controller.StadisticsController;
import org.kordamp.ikonli.javafx.FontIcon;
import java.io.IOException;
import java.net.URL;
import java.util.Objects;


@Data
@Slf4j
public class FooterController {

    @FXML
    private Button modo;
    @FXML
    private Button btnMarketStats;
    private Stage statsStage;
    @FXML
    private Button btnHistoricalOrders;
    private Stage historicalOrdersStage;
    @FXML
    private Label lbUser;
    @FXML
    private FontIcon enviroment;
    @FXML
    private Button sound;
    @FXML
    private Button notifications;
    @FXML
    private Button btnNotification;
    private Stage viewconsole;
    @FXML
    private Button news;
    @FXML
    private Button btnConnections;
    @FXML
    private Button btnChat;
    @FXML
    private Button btnCandles;
    @FXML
    public Button btnAdminUser;
    @FXML
    private Label lbEnviroment;
    @FXML
    private Label lblBid;
    @FXML
    private Label lblAsk;
    @FXML
    private Label lblLast;
    @FXML
    private Button reconnect;
    @FXML
    private Button changePassword;
    private Double lastBid = null;
    private Double lastAsk = null;
    private Double lastClose = null;
    private DoubleProperty bidProperty = new SimpleDoubleProperty();
    private DoubleProperty askProperty = new SimpleDoubleProperty();
    private DoubleProperty lastProperty = new SimpleDoubleProperty();
    private Stage chatStage;
    private Stage settingsStage;


    @FXML
    private void initialize() {
        try {

            Repository.setFooterController(this);
            lbUser.setText(Repository.getUsername());

            lblBid.textProperty().bind(bidProperty.asString());
            lblAsk.textProperty().bind(askProperty.asString());
            lblLast.textProperty().bind(lastProperty.asString());


            btnAdminUser.setDisable(true);
            btnAdminUser.setVisible(false);
            btnAdminUser.setManaged(false);




            if (Repository.enviroment != null && (Repository.enviroment.equals(SessionsMessage.Enviroment.PRODUCTION) ||
                    Repository.enviroment.equals(SessionsMessage.Enviroment.PRODUCTION_VPN))) {
                setEnvironmentBadge("Entorno: PRODUCCIÓN", "fth-shield", "footer-environment-production");

            } else if (Repository.enviroment != null && Repository.enviroment.equals(SessionsMessage.Enviroment.TEST)) {
                setEnvironmentBadge("Entorno: TEST", "fth-tool", "footer-environment-test");

            } else if (Repository.enviroment != null && Repository.enviroment.equals(SessionsMessage.Enviroment.QA)) {
                setEnvironmentBadge("Entorno: QA", "fth-check-circle", "footer-environment-qa");

            } else if (Repository.enviroment != null && Repository.enviroment.equals(SessionsMessage.Enviroment.LOCALHOST)) {
                setEnvironmentBadge("Entorno: LOCALHOST", "fth-monitor", "footer-environment-local");
            } else {
                String key = Repository.getEnviromentKey();
                setEnvironmentBadge("Entorno: " + (key != null ? key.toUpperCase() : ""),
                        "fth-server", "footer-environment-default");
            }


            installFooterIcon(btnConnections, "fth-activity");
            installFooterIcon(btnNotification, "fth-inbox");
            this.btnNotification.setVisible(false);
            this.btnNotification.setManaged(false);
            this.btnNotification.setDisable(true);

            installFooterIcon(btnChat, "fth-message-square");
            installFooterIcon(changePassword, "fth-settings");
            installFooterIcon(news, "fth-file-text");
            installFooterIcon(btnMarketStats, "fth-bar-chart-2");
            installFooterIcon(btnHistoricalOrders, "fth-clock");
            installFooterIcon(btnAdminUser, "fth-users");
            installFooterIcon(reconnect, "fth-refresh-cw");
            updateModeIcon();
            updateSoundIcon();
            updateNotificationIcon();
            this.btnMarketStats.setVisible(true);
            this.btnMarketStats.setManaged(true);

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void installFooterIcon(Button button, String iconLiteral) {
        if (button == null) {
            return;
        }
        FontIcon icon = new FontIcon(iconLiteral);
        icon.setIconSize(20);
        icon.setMouseTransparent(true);
        button.setGraphic(icon);
    }

    private void updateModeIcon() {
        installFooterIcon(modo, Repository.isDayMode() ? "fth-moon" : "fth-sun");
        modo.setTooltip(new Tooltip("Cambiar tema"));
    }

    private void updateSoundIcon() {
        installFooterIcon(sound, Repository.isSound() ? "fth-volume-2" : "fth-volume-x");
    }

    private void updateNotificationIcon() {
        installFooterIcon(notifications, Repository.isNotification() ? "fth-bell" : "fth-bell-off");
    }

    private void setEnvironmentBadge(String text, String iconLiteral, String toneClass) {
        String[] toneClasses = {
                "footer-environment-production",
                "footer-environment-test",
                "footer-environment-qa",
                "footer-environment-local",
                "footer-environment-default"
        };
        enviroment.getStyleClass().removeAll(toneClasses);
        lbEnviroment.getStyleClass().removeAll(toneClasses);
        enviroment.setIconLiteral(iconLiteral);
        enviroment.getStyleClass().add(toneClass);
        lbEnviroment.getStyleClass().add(toneClass);
        lbEnviroment.setText(text);
    }

    @FXML
    public void toggleDayNightMode(ActionEvent event) {
        if (Repository.isDayMode()) {
            Repository.getPrincipalController().setNightMode();
            Repository.setDayMode(false);  // Guardar en las preferencias que el modo noche está activado
        } else {
            Repository.getPrincipalController().setDayMode();
            Repository.setDayMode(true);  // Guardar en las preferencias que el modo día está activado
        }
        updateModeIcon();
    }

    @FXML
    public void handleImageClick() {

        Repository.setSound(!Repository.isSound());
        updateSoundIcon();

        if (Repository.isSound()) {
            Notifier.INSTANCE.notifyInfo("Sonido activado", "");
        } else {
            Notifier.INSTANCE.notifyInfo("Sonido desactivado", "");
        }

    }

    @FXML
    public void handleImageNotification() {

        Repository.setNotification(!Repository.isNotification());
        updateNotificationIcon();

        if (Repository.isNotification()) {
            Notifier.INSTANCE.notify(new Notification("Notificaciones", "activadas", Notification.INFO_ICON));
        } else {
            Notifier.INSTANCE.notify(new Notification("Notificaciones", "desactivadas", Notification.INFO_ICON));
        }

    }

    /**
     * Abre la pantalla de Configuración. Antes este metodo construia el dialogo entero a mano
     * (~95 lineas); ahora vive en Settings.fxml + SettingsController, como el resto del modulo.
     */
    @FXML
    public void changePasswords(ActionEvent actionEvent) {
        try {
            if (settingsStage != null && settingsStage.isShowing()) {
                settingsStage.toFront();
                settingsStage.requestFocus();
                return;
            }
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/Settings.fxml"));
            Parent root = loader.load();
            settingsStage = new Stage();
            Scene scene = new Scene(root);
            applyCurrentStyle(scene);
            settingsStage.setScene(scene);
            settingsStage.setTitle("Configuración");
            settingsStage.setOnHidden(e -> settingsStage = null);
            cl.vc.blotter.utils.WindowGeometryStore.restore(settingsStage, "settings", 820, 560);
            settingsStage.show();
        } catch (Exception ex) {
            log.error("No se pudo abrir Configuración: {}", ex.getMessage(), ex);
        }
    }

    private void setupButtonListeners(Button changeButton, PasswordField textField, PasswordField textField2) {
        changeButton.setOnAction(e -> {
            if (textField.getText().isEmpty() || textField2.getText().isEmpty()) {
                showAlert("Error", "Los campos no pueden estar vacíos", Alert.AlertType.ERROR);
            } else if (!textField.getText().equals(textField2.getText())) {
                showAlert("Error", "Las contraseñas no coinciden", Alert.AlertType.ERROR);
            } else {

                try {

                    BlotterMessage.User.Builder user = Repository.getUser().toBuilder().clone();
                    user.setStatusUser(BlotterMessage.StatusUser.UPDATE_USER);
                    user.setPassword(AESEncryption.encrypt(textField.getText()));
                    user.setUsername(AESEncryption.encrypt(user.getUsername()));
                    Repository.getClientService().sendMessage(user.build());

                    textField.clear();
                    textField2.clear();
                    showAlert("Éxito", "Contraseña cambiada exitosamente", Alert.AlertType.INFORMATION);
                } catch (Exception ex) {

                    log.error(ex.getMessage(), ex);
                }
            }
        });
    }

    private void showAlert(String title, String content, Alert.AlertType alertType) {
        Alert alert = new Alert(alertType);
        alert.setTitle(title);
        applyCurrentStyleToDialog(alert.getDialogPane().getScene());
        alert.setContentText(content);
        alert.setHeaderText(null);
        alert.showAndWait();
    }

    private void applyCurrentStyleToDialog(Scene scene) {
        if (scene != null) {
            scene.getStylesheets().clear();
            if (Repository.getPrincipalController().isDayMode()) {
                scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource("/blotter/css/daymode.css")).toExternalForm());
            } else {
                scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource(Repository.getSTYLE())).toExternalForm());
            }
        }
    }

    public void setupDesktopModeTab(Tab tab) {

        Image image = new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/bookVertical.png")));
        ImageView imageView = new ImageView(image);
        imageView.setFitHeight(150);
        imageView.setFitWidth(130);

        Image image2 = new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/bookHorizontal.png")));
        ImageView imageView2 = new ImageView(image2);
        imageView2.setFitHeight(100);
        imageView2.setFitWidth(190);

        CheckBox verticalCheckBox = new CheckBox("Libro Vertical");
        verticalCheckBox.setContentDisplay(ContentDisplay.TOP);

        CheckBox horizontalCheckBox = new CheckBox("Libro Horizontal");
        horizontalCheckBox.setContentDisplay(ContentDisplay.TOP);

        verticalCheckBox.selectedProperty().addListener((obs, wasSelected, isNowSelected) -> {
            if (isNowSelected) {
                horizontalCheckBox.setSelected(false);
                Repository.getMarketDataController().setConfByuser(true);
            }
        });

        horizontalCheckBox.selectedProperty().addListener((obs, wasSelected, isNowSelected) -> {
            if (isNowSelected) {
                verticalCheckBox.setSelected(false);
                Repository.getMarketDataController().setConfByuser(false);
            }
        });

        VBox vBox1 = new VBox(10); // Espaciado entre componentes
        vBox1.setAlignment(Pos.CENTER); // Alinea todos los elementos de VBox al centro
        vBox1.getChildren().addAll(imageView, verticalCheckBox);

        VBox vBox2 = new VBox(10); // Espaciado entre componentes
        vBox2.setAlignment(Pos.CENTER); // Alinea todos los elementos de VBox al centro
        vBox2.getChildren().addAll(imageView2, horizontalCheckBox);

        HBox hbox = new HBox(20);
        hbox.getChildren().addAll(vBox1, vBox2);
        hbox.setPadding(new Insets(20, 1, 0, 20));

        tab.setContent(hbox);
    }

    public void applyCurrentStyle(Scene scene) {

        scene.getStylesheets().clear();

        if (Repository.getPrincipalController().isDayMode()) {
            scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource("/blotter/css/daymode.css")).toExternalForm());
        } else {
            scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource(Repository.getSTYLE())).toExternalForm());
        }
    }

    private Alert setupAlert() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        applyCurrentStyleToDialog(alert.getDialogPane().getScene());
        alert.setTitle("Confirmar Cambio");
        alert.setHeaderText("¿Estás seguro de que quieres cambiar la contraseña?");
        alert.setContentText("Los datos serán modificados.");
        return alert;
    }

    @FXML
    public void actionNotification(ActionEvent actionEvent) {
        try {
            FXMLLoader loader = new FXMLLoader(this.getClass().getResource("/view/Notification.fxml"));
            AnchorPane mainPane = loader.load();
            Repository.setNotificationController(loader.getController());

            if (viewconsole != null && viewconsole.isShowing()) {
                Notifier.INSTANCE.notifyInfo("Console view", "is open");
                return;
            }

            viewconsole = new Stage();
            Scene scene = new Scene(mainPane);
            applyCurrentStyle(scene);
            viewconsole.setScene(scene);
            viewconsole.show();

            NotificationMessage.NotificationRequest notificationRequest = NotificationMessage.NotificationRequest.newBuilder()
                    .setId(IDGenerator.getID())
                    .setNotificationRequestType(NotificationMessage.NotificationRequestType.MESSAGES_REQUEST)
                    .build();

            Repository.getClientService().sendMessage(notificationRequest);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @FXML
    private void readNews() throws IOException {
        FXMLLoader loader = new FXMLLoader(this.getClass().getResource("/view/News.fxml"));
        AnchorPane mainPane = loader.load();
        NewsController newsController = loader.getController();

        if (viewconsole != null && viewconsole.isShowing()) {
            Notifier.INSTANCE.notifyInfo("Consola de Noticias", "está abierta");
            return;
        }

        viewconsole = new Stage();
        Scene scene = new Scene(mainPane, 1000, 700);
        applyCurrentStyle(scene);
        viewconsole.setScene(scene);
        viewconsole.setTitle("Vector Trade News");
        viewconsole.show();
    }

    @FXML
    private void openChatView() throws IOException {
        if (chatStage != null && chatStage.isShowing()) {
            chatStage.requestFocus();
            return;
        }
        FXMLLoader loader = new FXMLLoader(this.getClass().getResource("/view/Chat.fxml"));
        javafx.scene.Parent mainPane = loader.load();
        chatStage = new Stage();
        Scene scene = new Scene(mainPane, 640, 520);
        applyCurrentStyle(scene);
        chatStage.setScene(scene);
        chatStage.setTitle("Chat");
        chatStage.show();
    }

    @FXML
    private void openCandleView() {
        // Delegado a CandleWindow: mismo comportamiento de antes (reusa la ventana si ya esta
        // abierta, se centra en pantalla y respeta modo dia/noche), sin la copia local.
        CandleWindow.open(null);
    }

    @FXML
    private void readConnections() throws IOException {

        if (viewconsole != null && viewconsole.isShowing()) {
            viewconsole.requestFocus();
            return;
        }

        VBox content = new VBox(10);
        content.setPadding(new Insets(16));
        content.getChildren().addAll(
                createConnectionRow("SERVICE", Repository.serviceConnectedProperty().get(), Repository.getServiceEndpoint()),
                createConnectionRow("CANDLE", Repository.candleConnectedProperty().get(), Repository.getCandleEndpoint()),
                createConnectionRow("CHAT", Repository.chatConnectedProperty().get(), Repository.getChatEndpoint()),
                createConnectionRow("NEWS", Repository.newsConnectedProperty().get(), Repository.getNewsEndpoint())
        );

        viewconsole = new Stage();
        Scene scene = new Scene(content, 760, 240);
        applyCurrentStyle(scene);
        viewconsole.setScene(scene);
        viewconsole.setTitle("Estado de conexiones");
        viewconsole.show();
    }

    private HBox createConnectionRow(String channel, boolean connected, String endpoint) {
        Label status = new Label(channel + ": " + (connected ? "ON" : "OFF"));
        status.setMinWidth(130);
        status.setStyle("-fx-font-size: 12; -fx-font-weight: bold; -fx-text-fill: " + (connected ? "#39c16c" : "#ff5f5f") + ";");

        Label url = new Label(endpoint == null || endpoint.isBlank() ? "Sin endpoint configurado" : endpoint);
        url.setWrapText(true);
        HBox.setHgrow(url, Priority.ALWAYS);

        HBox row = new HBox(12, status, url);
        row.setAlignment(Pos.CENTER_LEFT);
        return row;
    }

    @FXML
    public void btnAdminUser(ActionEvent actionEvent) throws IOException {

        BlotterMessage.UserList userlist = BlotterMessage.UserList.newBuilder().setStatusUser(BlotterMessage.StatusUser.SNAPSHOT_USER).build();
        Repository.getClientService().sendMessage(userlist);

        FXMLLoader loader = new FXMLLoader(this.getClass().getResource("/view/AdminView.fxml"));
        AnchorPane mainPane = loader.load();
        AdminController adminController = loader.getController();

        Repository.setAdminController(adminController);

        Stage stage = new Stage();
        Scene scene = new Scene(mainPane);
        applyCurrentStyle(scene);
        stage.setScene(scene);
        stage.show();


    }

    @FXML
    private void reconnect() {
        try {
            Repository.getBookPortMaps().values().forEach(s->{
                s.creanBook();
            });;
            Notifier.INSTANCE.notify(new Notification("Reconexión", "se reconecta el aplicativo", Notification.INFO_ICON));
            if (LoginController.simpleWebSocketListenerService != null) {
                LoginController.simpleWebSocketListenerService.stopServiceForce();
            }
            if (LoginController.simpleWebSocketListenerCandle != null) {
                LoginController.simpleWebSocketListenerCandle.stopServiceForce();
            }
            if (LoginController.simpleWebSocketListenerChat != null) {
                LoginController.simpleWebSocketListenerChat.stopServiceForce();
            }
            if (LoginController.simpleWebSocketListenerNews != null) {
                LoginController.simpleWebSocketListenerNews.stopServiceForce();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void updateDollarStatistics(MarketDataMessage.Statistic statistic) {
        try {

            double newBid = statistic.getBidPx();
            double newAsk = statistic.getAskPx();
            double newClose = resolveDollarReferencePrice(statistic);

            if ((lastBid == null || !lastBid.equals(newBid)) ||
                    (lastAsk == null || !lastAsk.equals(newAsk)) ||
                    (lastClose == null || !lastClose.equals(newClose))) {
                bidProperty.set(newBid);
                askProperty.set(newAsk);
                lastProperty.set(newClose);
                lastBid = newBid;
                lastAsk = newAsk;
                lastClose = newClose;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    static double resolveDollarReferencePrice(MarketDataMessage.Statistic statistic) {
        double ohlcvClose = statistic.getOhlcv().getClose();
        if (ohlcvClose > 0d) {
            return ohlcvClose;
        }
        if (statistic.getClose() > 0d) {
            return statistic.getClose();
        }
        return statistic.getLast();
    }

    @FXML
    private void openMarketStats() throws IOException {
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/StadisticsView.fxml"));

        javafx.scene.Parent root = loader.load();

        StadisticsController statsController = loader.getController();

        Repository.setStatsController(statsController);

        statsStage = new Stage();
        Rectangle2D bounds = Screen.getPrimary().getVisualBounds();
        double w = Math.max(1000, bounds.getWidth() - 40);
        double h = Math.max(700, bounds.getHeight() - 40);
        Scene scene = new Scene(root, w, h);
        applyCurrentStyle(scene);
        applyCurrentStyle(scene);

        scene.getStylesheets().add(
                Objects.requireNonNull(
                        getClass().getResource(Repository.getSTYLE_ESTADISTICAS())
                ).toExternalForm()
        );
        statsStage.setScene(scene);
        statsStage.setTitle("Estadísticas de Mercado");
        statsStage.getIcons().add(new Image(
                Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/estadisticas.png"))));
        statsStage.setMaxWidth(bounds.getWidth());
        statsStage.setMaxHeight(bounds.getHeight());
        statsStage.setX(bounds.getMinX() + 20);
        statsStage.setY(bounds.getMinY() + 20);


        statsStage.setOnCloseRequest(event -> {
            Repository.setStatsController(null);
        });

        statsStage.show();

    }

    @FXML
    private void openHistoricalOrders() throws IOException {
        if (historicalOrdersStage != null && historicalOrdersStage.isShowing()) {
            historicalOrdersStage.toFront();
            historicalOrdersStage.requestFocus();
            return;
        }

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/HistoricalOrders.fxml"));
        AnchorPane root = loader.load();
        HistoricalOrdersController controller = loader.getController();
        Repository.setHistoricalOrdersController(controller);

        Rectangle2D bounds = Screen.getPrimary().getVisualBounds();
        double width = Math.max(1100, bounds.getWidth() - 40);
        double height = Math.max(700, bounds.getHeight() - 40);
        Scene scene = new Scene(root, width, height);
        applyCurrentStyle(scene);

        Stage stage = new Stage();
        historicalOrdersStage = stage;
        stage.setScene(scene);
        stage.setTitle("Órdenes Históricas");
        stage.setMinWidth(1100);
        stage.setMinHeight(650);
        stage.setMaxWidth(bounds.getWidth());
        stage.setMaxHeight(bounds.getHeight());
        stage.setX(bounds.getMinX() + 20);
        stage.setY(bounds.getMinY() + 20);
        stage.setOnCloseRequest(event -> closeHistoricalOrders(controller));
        stage.setOnHidden(event -> closeHistoricalOrders(controller));
        stage.show();
        controller.activate();
    }

    private void closeHistoricalOrders(HistoricalOrdersController controller) {
        controller.deactivate();
        if (Repository.getHistoricalOrdersController() == controller) {
            Repository.setHistoricalOrdersController(null);
        }
        historicalOrdersStage = null;
    }


}
