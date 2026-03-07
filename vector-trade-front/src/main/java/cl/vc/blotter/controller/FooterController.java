package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.Notifier;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.crypt.AESEncryption;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import eu.hansolo.enzo.notification.Notification;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Rectangle2D;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.util.Duration;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import cl.vc.blotter.controller.StadisticsController;
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
    private Label lbUser;
    @FXML
    private ImageView enviroment;
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
    @FXML
    private Label lblServiceConn;
    @FXML
    private Label lblCandleConn;
    @FXML
    private Label lblChatConn;
    @FXML
    private Label lblNewsConn;
    private Double lastBid = null;
    private Double lastAsk = null;
    private Double lastClose = null;
    private DoubleProperty bidProperty = new SimpleDoubleProperty();
    private DoubleProperty askProperty = new SimpleDoubleProperty();
    private DoubleProperty lastProperty = new SimpleDoubleProperty();
    private Timeline connectionStatusTimeline;
    private Stage chatStage;
    private Stage candleStage;


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




            if (Repository.enviroment.equals(SessionsMessage.Enviroment.PRODUCTION) ||
                    Repository.enviroment.equals(SessionsMessage.Enviroment.PRODUCTION_VPN)) {
                lbEnviroment.setText("Entorno: " + SessionsMessage.Enviroment.PRODUCTION.name());
                ImageView imageView = new ImageView();
                imageView.setFitHeight(35);
                imageView.setFitWidth(35);
                enviroment.setImage(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/prod.png"))));

            } else if (Repository.enviroment.equals(SessionsMessage.Enviroment.TEST)) {
                lbEnviroment.setText("Entorno: TEST");
                ImageView imageView = new ImageView();
                imageView.setFitHeight(35);
                imageView.setFitWidth(35);
                enviroment.setImage(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/desarrollo.png"))));

            } else if (Repository.enviroment.equals(SessionsMessage.Enviroment.QA)) {
                lbEnviroment.setText("Entorno: QA");
                ImageView imageView = new ImageView();
                imageView.setFitHeight(35);
                imageView.setFitWidth(35);
                enviroment.setImage(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/aprobado.png"))));


            } else if (Repository.enviroment.equals(SessionsMessage.Enviroment.LOCALHOST)) {
                lbEnviroment.setText("Entorno: LOCALHOST");
                ImageView imageView = new ImageView();
                imageView.setFitHeight(35);
                imageView.setFitWidth(35);
                enviroment.setImage(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/desarrollo.png"))));
            }


            ImageView imageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/conexions.png"))));
            imageView.setFitHeight(35);
            imageView.setFitWidth(35);
            this.btnConnections.setGraphic(imageView);

            imageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/mail_7500726.png"))));
            imageView.setFitHeight(35);
            imageView.setFitWidth(35);
            this.btnNotification.setGraphic(imageView);
            this.btnNotification.setVisible(false);
            this.btnNotification.setManaged(false);
            this.btnNotification.setDisable(true);

            ImageView chatImageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/mail_7500726.png"))));
            chatImageView.setFitHeight(35);
            chatImageView.setFitWidth(35);
            this.btnChat.setGraphic(chatImageView);

            imageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/tuerca.png"))));
            imageView.setFitHeight(35);
            imageView.setFitWidth(35);
            this.changePassword.setGraphic(imageView);

            imageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/notification.png"))));
            imageView.setFitHeight(35);
            imageView.setFitWidth(35);
            this.notifications.setGraphic(imageView);

            imageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/sound.png"))));
            imageView.setFitHeight(35);
            imageView.setFitWidth(35);
            this.sound.setGraphic(imageView);

            imageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/news.png"))));
            imageView.setFitHeight(35);
            imageView.setFitWidth(35);
            this.news.setGraphic(imageView);

            imageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/estadisticas.png"))));
            imageView.setFitHeight(35);
            imageView.setFitWidth(35);
            this.btnMarketStats.setGraphic(imageView);
            this.btnMarketStats.setVisible(true);
            this.btnMarketStats.setManaged(true);

            connectionStatusTimeline = new Timeline(new KeyFrame(Duration.seconds(1), e -> updateConnectionStatus()));
            connectionStatusTimeline.setCycleCount(Timeline.INDEFINITE);
            connectionStatusTimeline.play();
            updateConnectionStatus();
            bindConnectionStatus();

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void updateConnectionStatus() {
        updateConnectionLabel(lblServiceConn, "SERVICE", Repository.serviceConnectedProperty().get());
        updateConnectionLabel(lblCandleConn, "CANDLE", Repository.candleConnectedProperty().get());
        updateConnectionLabel(lblChatConn, "CHAT", Repository.chatConnectedProperty().get());
        updateConnectionLabel(lblNewsConn, "NEWS", Repository.newsConnectedProperty().get());
    }

    private void updateConnectionLabel(Label label, String name, boolean connected) {
        if (label == null) {
            return;
        }
        label.setText(name + ": " + (connected ? "ON" : "OFF"));
        label.setStyle("-fx-font-size: 11; -fx-font-weight: bold; -fx-text-fill: " + (connected ? "#39c16c" : "#ff5f5f") + ";");
    }

    private void bindConnectionStatus() {
        Repository.serviceConnectedProperty().addListener((obs, oldV, newV) ->
                updateConnectionLabel(lblServiceConn, "SERVICE", Boolean.TRUE.equals(newV)));
        Repository.candleConnectedProperty().addListener((obs, oldV, newV) ->
                updateConnectionLabel(lblCandleConn, "CANDLE", Boolean.TRUE.equals(newV)));
        Repository.chatConnectedProperty().addListener((obs, oldV, newV) ->
                updateConnectionLabel(lblChatConn, "CHAT", Boolean.TRUE.equals(newV)));
        Repository.newsConnectedProperty().addListener((obs, oldV, newV) ->
                updateConnectionLabel(lblNewsConn, "NEWS", Boolean.TRUE.equals(newV)));
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
    }

    @FXML
    public void handleImageClick() {

        Repository.setSound(!Repository.isSound());

        if (Repository.isSound()) {
            Notifier.INSTANCE.notifyInfo("Sonido activado", "");
        } else {
            Notifier.INSTANCE.notifyInfo("Sonido desactivado", "");
        }

    }

    @FXML
    public void handleImageNotification() {

        Repository.setNotification(!Repository.isNotification());

        if (Repository.isNotification()) {
            Notifier.INSTANCE.notify(new Notification("Notificaciones", "activadas", Notification.INFO_ICON));
        } else {
            Notifier.INSTANCE.notify(new Notification("Notificaciones", "desactivadas", Notification.INFO_ICON));
        }

    }

    public void changePasswords(ActionEvent actionEvent) {
        Label label = new Label("Contraseña");
        PasswordField textField = new PasswordField();
        Label label2 = new Label("Repetir Contraseña");
        PasswordField textField2 = new PasswordField();

        VBox contentChangePassword = new VBox(10);
        contentChangePassword.setPadding(new Insets(20));

        Button acceptButton = new Button("Aceptar");
        acceptButton.setId("acceptButton");
        Button cancelButton = new Button("Cancelar");
        cancelButton.setId("cancelButton");
        HBox buttonsBox = new HBox(10);
        buttonsBox.getChildren().addAll(acceptButton, cancelButton);

        contentChangePassword.getChildren().addAll(label, textField, label2, textField2, buttonsBox);

        TabPane tabPane = new TabPane();
        Tab tab1 = new Tab("Restablecer contraseña", contentChangePassword);
        tab1.setClosable(false);
        Tab tab2 = new Tab("Modo de Escritorio", new VBox());
        setupDesktopModeTab(tab2);
        tab2.setClosable(false);

        if (Repository.getIsLight()) {
            tabPane.getTabs().addAll(tab1);
        } else {
            tabPane.getTabs().addAll(tab1, tab2);
        }

        VBox vbox = new VBox(10, tabPane);
        vbox.setPrefHeight(400);
        vbox.setMinHeight(400);

        Scene scene = new Scene(vbox, 400, 400);

        applyCurrentStyle(scene);

        Stage stage = new Stage();
        stage.setScene(scene);
        stage.show();

        Alert alert = setupAlert();
        setupButtonListeners(acceptButton, cancelButton, textField, textField2, stage, alert);
    }

    private void setupButtonListeners(Button aceptarButton, Button cancelarButton, PasswordField textField, PasswordField textField2, Stage stage, Alert alerta) {
        aceptarButton.setOnAction(e -> {
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
        cancelarButton.setOnAction(e -> stage.close());
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
    private void openCandleView() throws IOException {
        if (candleStage != null && candleStage.isShowing()) {
            candleStage.requestFocus();
            return;
        }
        FXMLLoader loader = new FXMLLoader(this.getClass().getResource("/view/Candle.fxml"));
        AnchorPane mainPane = loader.load();
        candleStage = new Stage();
        Rectangle2D bounds = Screen.getPrimary().getVisualBounds();
        double w = Math.min(1100, Math.max(900, bounds.getWidth() - 40));
        double h = Math.min(700, Math.max(600, bounds.getHeight() - 40));
        Scene scene = new Scene(mainPane, w, h);
        applyCurrentStyle(scene);
        candleStage.setScene(scene);
        candleStage.setTitle("Grafico de Velas");
        candleStage.setMaxWidth(bounds.getWidth());
        candleStage.setMaxHeight(bounds.getHeight());
        candleStage.setX(bounds.getMinX() + (bounds.getWidth() - w) / 2);
        candleStage.setY(bounds.getMinY() + (bounds.getHeight() - h) / 2);
        candleStage.show();
    }

    @FXML
    private void readConnections() throws IOException {

        FXMLLoader loader = new FXMLLoader(this.getClass().getResource("/view/Notification.fxml"));
        AnchorPane mainPane = loader.load();
        Repository.setNotificationController(loader.getController());

        if (viewconsole != null && viewconsole.isShowing()) {
            Notifier.INSTANCE.notifyInfo("Vista de Consola", "está abierta");
            return;
        }

        viewconsole = new Stage();
        Scene scene = new Scene(mainPane);
        applyCurrentStyle(scene);
        viewconsole.setScene(scene);
        viewconsole.show();

        NotificationMessage.NotificationRequest notificationRequest = NotificationMessage.NotificationRequest.newBuilder()
                .setId(IDGenerator.getID())
                .setNotificationRequestType(NotificationMessage.NotificationRequestType.CONNECTION_REQUEST)
                .build();

        Repository.getClientService().sendMessage(notificationRequest);
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

    public void updateDollarStatistics(MarketDataMessage.Statistic statistic) throws Exception {
        try {

            double newBid = statistic.getBidPx();
            double newAsk = statistic.getAskPx();
            double newClose = statistic.getOhlcv().getClose();

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


}
