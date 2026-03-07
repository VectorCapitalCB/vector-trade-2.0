package cl.vc.blotter.controller;

import akka.routing.RoundRobinPool;
import cl.vc.blotter.Repository;
import cl.vc.blotter.adaptor.ClientActor;
import cl.vc.blotter.utils.EncryptionUtil;
import cl.vc.blotter.ws.SimpleWebSocketListener;
import cl.vc.module.protocolbuff.crypt.AESEncryption;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Pos;
import javafx.geometry.Rectangle2D;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.stage.Screen;
import javafx.stage.Stage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.eclipse.jetty.websocket.common.extensions.compress.PerMessageDeflateExtension;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Data
public class LoginController {

    public static SimpleWebSocketListener simpleWebSocketListener;
    public static SimpleWebSocketListener simpleWebSocketListenerService;
    public static SimpleWebSocketListener simpleWebSocketListenerCandle;
    public static SimpleWebSocketListener simpleWebSocketListenerChat;

    @FXML
    public TextField txtUsername;

    @FXML
    public TextField password;

    @FXML
    public Label label;

    @FXML
    public HBox loading;

    @FXML
    public CheckBox chkGuardarContrasena;
    public EncryptionUtil encryptionUtil;

    @FXML
    public ComboBox<String> enviroment;
    Properties props = Repository.getProperties();
    ObservableList<String> envKeys = FXCollections.observableArrayList();
    @FXML
    private AnchorPane anchorPane;

    @FXML
    private void initialize() {
        try {

            Repository.getStaticSecurityType().put("CFINASDAQ", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFISP500", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFIETFIPSA", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFIETFGE", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFIGC", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFIETFCC", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFIETFCD", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFMITNIPSA", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFMDIVO", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFIETFLP", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFI-ETFUSD", RoutingMessage.SecurityType.CS);
            Repository.getStaticSecurityType().put("CFIETF4060", RoutingMessage.SecurityType.CS);


            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(System::gc, 0, 1, TimeUnit.MINUTES);

            Set<String> validEnvs = new HashSet<>();
            Arrays.stream(SessionsMessage.Enviroment.values())
                    .map(v -> v.name().toLowerCase())
                    .forEach(validEnvs::add);

            for (String key : props.stringPropertyNames()) {
                String val = props.getProperty(key);
                if (validEnvs.contains(key) && (val.startsWith("ws://") || val.startsWith("wss://"))) {
                    envKeys.add(key);
                }
            }

            if (envKeys.size() > 1) {
                String segundo = envKeys.remove(1);
                envKeys.add(0, segundo);
            }

            enviroment.setItems(envKeys);
            enviroment.getSelectionModel().selectFirst();
            Repository.setEnviroment(SessionsMessage.Enviroment.valueOf(enviroment.getValue().toUpperCase()));
            enviroment.getSelectionModel().selectedItemProperty().addListener((obs, oldV, newV) -> Repository.setEnviroment(SessionsMessage.Enviroment.valueOf(newV.toUpperCase())));

            Repository.setLoginController(this);

            anchorPane.getStylesheets().add(Objects.requireNonNull(getClass().getResource(Repository.getSTYLE())).toExternalForm());

            Repository.setDolarSymbol(Repository.getProperties().getProperty("forex-symbol"));
            Repository.setDolarSecurity(Repository.getProperties().getProperty("forex-md-source"));

            encryptionUtil = new EncryptionUtil();

            if (encryptionUtil.credentialsExist(Repository.getCredencialPath())) {
                try (BufferedReader reader = new BufferedReader(new FileReader(Repository.getCredencialPath()))) {
                    String encryptedData = reader.readLine();
                    if (encryptedData != null && !encryptedData.isEmpty()) {
                        String decryptedData = encryptionUtil.decrypt(encryptedData);
                        String[] parts = decryptedData.split(":");
                        if (parts.length == 2) {
                            txtUsername.setText(parts[0]);
                            password.setText(parts[1]);
                            chkGuardarContrasena.setSelected(true);
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    label.setText("Error al cargar credenciales.");
                }
            }

            if (Repository.isAutologin()) {
                login();
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Error");
            alert.setContentText(ex.getMessage());
            DialogPane dialogPane = alert.getDialogPane();
            dialogPane.getStylesheets().add(getClass().getResource(Repository.getSTYLE()).toExternalForm());
            alert.showAndWait();
            return;
        }
    }


    @FXML
    public void login() {
        try {


            txtUsername.setText(txtUsername.getText().replace(" ", "").toLowerCase());

            Repository.username = txtUsername.getText();

            Repository.clientActor
                    = Repository.actorSystem.actorOf(new RoundRobinPool(2).props(ClientActor.props()));

            String credentials = AESEncryption.encrypt(txtUsername.getText())
                    + ":" + AESEncryption.encrypt(password.getText());

            Repository.setCredencial(credentials);

            String encodedCredentials = Base64.getEncoder()
                    .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

            Repository.setCredencial(credentials);
            String envKey = Repository.getEnviroment().name().toLowerCase();

            String serviceEndpoint = resolveEndpoint(envKey, "service");
            String candleEndpoint = resolveEndpoint(envKey, "candle");
            String chatEndpoint = resolveEndpoint(envKey, "chat");

            simpleWebSocketListenerService = connectSocket(serviceEndpoint, encodedCredentials, "service");
            simpleWebSocketListener = simpleWebSocketListenerService; // backward compatibility

            if (simpleWebSocketListenerService == null || !simpleWebSocketListenerService.isConnected()) {
                if (simpleWebSocketListenerService != null) {
                    simpleWebSocketListenerService.setCloseFailure(true);
                }
                Repository.setChannelConnected("service", false);
                throw new IllegalStateException("No se pudo conectar al servicio principal");
            }

            Repository.setClientService(simpleWebSocketListenerService);

            simpleWebSocketListenerCandle = connectSocket(candleEndpoint, encodedCredentials, "candle");
            if (simpleWebSocketListenerCandle != null && simpleWebSocketListenerCandle.isConnected()) {
                Repository.setCandleClientService(simpleWebSocketListenerCandle);
            } else {
                Repository.setCandleClientService(null);
                Repository.setChannelConnected("candle", false);
                log.warn("No se pudo conectar canal candle. endpoint={}", candleEndpoint);
            }

            simpleWebSocketListenerChat = connectSocket(chatEndpoint, encodedCredentials, "chat");
            if (simpleWebSocketListenerChat != null && simpleWebSocketListenerChat.isConnected()) {
                Repository.setChatClientService(simpleWebSocketListenerChat);
            } else {
                Repository.setChatClientService(null);
                Repository.setChannelConnected("chat", false);
                log.warn("No se pudo conectar canal chat. endpoint={}", chatEndpoint);
            }

            if (chkGuardarContrasena.isSelected()) {
                encryptionUtil.guardarCredenciales(txtUsername.getText(), password.getText());
            } else {
                encryptionUtil.eliminarCredenciales();
            }
            showSplash();


        } catch (Exception ex) {

            log.error(ex.getMessage(), ex);

            if (ex.getMessage().contains("401")) {
                Alert alert = new Alert(Alert.AlertType.ERROR);
                alert.setTitle("Error");
                alert.setHeaderText("El usuario o contraseña no son válidos.");
                alert.setContentText("reintentar o solicitar acceso");
                DialogPane dialogPane = alert.getDialogPane();
                dialogPane.getStylesheets().add(Objects.requireNonNull(getClass().getResource(Repository.getSTYLE())).toExternalForm());
                alert.showAndWait();
                return;
            }

            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Error");
            alert.setHeaderText("El usuario o contraseña no son válidos.");
            alert.setContentText(ex.getMessage());
            DialogPane dialogPane = alert.getDialogPane();
            dialogPane.getStylesheets().add(Objects.requireNonNull(getClass().getResource(Repository.getSTYLE())).toExternalForm());
            alert.showAndWait();
            System.exit(0);
        }
    }

    private String resolveEndpoint(String envKey, String channel) {
        String base = Repository.getProperties().getProperty(envKey);
        return switch (channel) {
            case "service" -> firstNonBlank(
                    Repository.getProperties().getProperty(envKey + ".service"),
                    Repository.getProperties().getProperty("service." + envKey),
                    Repository.getProperties().getProperty("service"),
                    base
            );
            case "candle" -> firstNonBlank(
                    Repository.getProperties().getProperty(envKey + ".candle"),
                    Repository.getProperties().getProperty("candle." + envKey),
                    Repository.getProperties().getProperty("candle"),
                    base
            );
            case "chat" -> firstNonBlank(
                    Repository.getProperties().getProperty(envKey + ".chat"),
                    Repository.getProperties().getProperty("chat." + envKey),
                    Repository.getProperties().getProperty("chat"),
                    base
            );
            default -> base;
        };
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }

    private SimpleWebSocketListener connectSocket(String endpoint, String encodedCredentials, String channelName) {
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }

        WebSocketClient client = null;
        ClientUpgradeRequest request = null;
        SimpleWebSocketListener listener = null;
        try {
            client = new WebSocketClient();
            client.getPolicy().setMaxTextMessageSize(100 * 1024 * 1024);
            client.getPolicy().setMaxBinaryMessageSize(100 * 1024 * 1024);
            client.getPolicy().setIdleTimeout(300000);
            client.addBean(new PerMessageDeflateExtension());
            client.start();

            request = new ClientUpgradeRequest();
            request.setHeader("Authorization", "Basic " + encodedCredentials);
            request.addExtensions("permessage-deflate");

            listener = new SimpleWebSocketListener(
                    client,
                    Repository.clientActor,
                    Repository.getActorSystem(),
                    NotificationMessage.Component.VECTOR_TRADE_SERVICES,
                    Repository.username,
                    endpoint,
                    request,
                    channelName
            );

            client.connect(listener, new URI(endpoint), request).get(5, TimeUnit.SECONDS);
            return listener;
        } catch (Exception e) {
            log.error("Error conectando websocket endpoint={}", endpoint, e);
            if (listener != null) {
                listener.startAutoReconnect();
                return listener;
            }
            return null;
        }
    }


    private void showSplash() {
        try {

            ProgressBar progressBar = new ProgressBar();
            label.setText("Loading Blotter...");
            label.setAlignment(Pos.CENTER);
            HBox hbox = new HBox(progressBar);
            hbox.setAlignment(Pos.CENTER);


            Task<Void> loadingTask = new Task<Void>() {
                @Override
                protected Void call() throws Exception {

                    Platform.runLater(() -> {
                        try {

                            updateProgress(1, 10);

                        } catch (Exception ex) {
                            log.error(ex.getMessage(), ex);
                            Alert alert = new Alert(Alert.AlertType.ERROR);
                            alert.setTitle("Error");
                            alert.setHeaderText("An error has occurred");
                            alert.setContentText(ex.getMessage());
                            DialogPane dialogPane = alert.getDialogPane();
                            dialogPane.getStylesheets().add(Objects.requireNonNull(getClass().getResource(Repository.getSTYLE())).toExternalForm());
                            alert.showAndWait();
                            System.exit(1);
                        }
                    });

                    return null;
                }
            };


            progressBar.progressProperty().bind(loadingTask.progressProperty());
            //loading.getChildren().addAll(hbox);

            loadingTask.setOnSucceeded(event -> {
                this.showMainStage();
                loadingTask.cancel();
            });

            new Thread(loadingTask).start();


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }

    }

    private void showMainStage() {

        try {


            Repository.getPrincipal().hide();
            Repository.getPrincipal().close();

            FXMLLoader fxmlLoader = new FXMLLoader();
            fxmlLoader.setLocation(getClass().getResource("/view/PrincipalView.fxml"));
            AnchorPane principal = fxmlLoader.load();

            Repository.setPrincipalController(fxmlLoader.getController());

            Scene scene = new Scene(principal);
            Repository.getPrincipal().setScene(scene);

            String v = Repository.getAppVersion();
            Repository.getPrincipal().setTitle(v == null || v.isBlank() ? "dev" : v);

            Screen screen = Screen.getPrimary();
            Rectangle2D screenBounds = screen.getVisualBounds();

            Repository.getPrincipal().setX(screenBounds.getMinX());
            Repository.getPrincipal().setY(screenBounds.getMinY());
            Repository.getPrincipal().setWidth(screenBounds.getWidth());
            Repository.getPrincipal().setHeight(screenBounds.getHeight());

            Repository.getFooterController().getLbUser().setText("User: " + txtUsername.getText());
            Repository.getFooterController().getLbUser().setStyle("-fx-text-fill: gold; -fx-font-weight: bold; -fx-font-size: 12;");

            Repository.getPrincipal().setMaximized(true);
            Repository.getPrincipal().show();






        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public Tab getTab() {
        try {
            Tab tab = new Tab();
            tab.setClosable(false);
            tab.setId("addTab");
            ImageView imageView = new ImageView();
            imageView.setFitHeight(16.0D);
            imageView.setFitWidth(16.0D);
            imageView.setSmooth(true);
            Image image = new Image("/blotter/img/add.png");
            imageView.setImage(image);
            tab.getStyleClass().add("tab");
            tab.setGraphic(imageView);
            tab.setContent(null);
            return tab;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}


