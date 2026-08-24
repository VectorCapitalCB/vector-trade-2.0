package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.BannerPrefs;
import cl.vc.blotter.utils.I18n;
import cl.vc.blotter.utils.Language;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.crypt.AESEncryption;
import javafx.fxml.FXML;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.HBox;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pantalla de Configuración con navegación lateral.
 *
 * Reemplaza el diálogo que se construía a mano dentro de {@code FooterController.changePasswords()}
 * (~95 líneas imperativas en un controller que ya hacía footer, ticker FX, chat, noticias y stats).
 */
@Slf4j
public class SettingsController {

    @FXML private VBox rail;
    @FXML private StackPane content;

    @FXML private ToggleButton navGeneral;
    @FXML private ToggleButton navBanner;
    @FXML private ToggleButton navSeguridad;

    @FXML private VBox paneGeneral;
    @FXML private VBox paneBanner;
    @FXML private VBox paneSeguridad;

    @FXML private ToggleButton btnEspanol;
    @FXML private ToggleButton btnIngles;

    @FXML private ToggleButton btnBannerRanking;
    @FXML private ToggleButton btnBannerPortafolio;
    @FXML private HBox boxCantidad;
    @FXML private Label lblBannerCopy;
    @FXML private Label lblBannerAviso;

    @FXML private PasswordField txtPassword;
    @FXML private PasswordField txtPassword2;
    @FXML private Button btnCambiarPassword;
    @FXML private Button btnCerrar;

    /** Secciones indexadas por su ToggleButton del rail. */
    private final Map<ToggleButton, Node> secciones = new LinkedHashMap<>();
    private Spinner<Integer> spPapeles;

    @FXML
    private void initialize() {
        secciones.put(navGeneral, paneGeneral);
        secciones.put(navBanner, paneBanner);
        secciones.put(navSeguridad, paneSeguridad);
        configurarRail();
        configurarIdioma();
        configurarBanner();
        configurarSeguridad();
        if (btnCerrar != null) {
            btnCerrar.setOnAction(e -> {
                if (btnCerrar.getScene() != null && btnCerrar.getScene().getWindow() != null) {
                    btnCerrar.getScene().getWindow().hide();
                }
            });
        }
    }

    // ------------------------------------------------------------------ rail

    private void configurarRail() {
        ToggleGroup grupo = new ToggleGroup();
        secciones.forEach((boton, panel) -> {
            boton.setToggleGroup(grupo);
            boton.setOnAction(e -> mostrar(panel));
        });
        // JavaFX permite deseleccionar un toggle de un grupo: sin esto el rail queda sin nada
        // marcado y el centro en blanco. Mismo guard que ya usaba el diálogo del idioma.
        grupo.selectedToggleProperty().addListener((obs, viejo, nuevo) -> {
            if (nuevo == null && viejo != null) grupo.selectToggle(viejo);
        });
        navGeneral.setSelected(true);
        mostrar(paneGeneral);
    }

    /** Sólo una sección visible: se apagan las otras con visible+managed para que no ocupen alto. */
    private void mostrar(Node panel) {
        for (Node n : secciones.values()) {
            boolean activo = n == panel;
            n.setVisible(activo);
            n.setManaged(activo);
        }
    }

    // ---------------------------------------------------------------- idioma

    private void configurarIdioma() {
        ToggleGroup grupo = new ToggleGroup();
        btnEspanol.setToggleGroup(grupo);
        btnIngles.setToggleGroup(grupo);
        if (I18n.getLanguage() == Language.ENGLISH) btnIngles.setSelected(true);
        else btnEspanol.setSelected(true);
        btnEspanol.setOnAction(e -> I18n.setLanguage(Language.SPANISH));
        btnIngles.setOnAction(e -> I18n.setLanguage(Language.ENGLISH));
        grupo.selectedToggleProperty().addListener((obs, viejo, nuevo) -> {
            if (nuevo == null && viejo != null) grupo.selectToggle(viejo);
        });
    }

    // ---------------------------------------------------------------- banner

    private void configurarBanner() {
        lblBannerCopy.setText("Elige qué instrumentos muestra la cinta superior. "
                + "El cambio se aplica al instante.");

        ToggleGroup grupo = new ToggleGroup();
        btnBannerRanking.setToggleGroup(grupo);
        btnBannerPortafolio.setToggleGroup(grupo);
        if (BannerPrefs.fuente() == BannerPrefs.Fuente.PORTAFOLIO) btnBannerPortafolio.setSelected(true);
        else btnBannerRanking.setSelected(true);
        grupo.selectedToggleProperty().addListener((obs, viejo, nuevo) -> {
            if (nuevo == null && viejo != null) {
                grupo.selectToggle(viejo);
                return;
            }
            BannerPrefs.setFuente(btnBannerPortafolio.isSelected()
                    ? BannerPrefs.Fuente.PORTAFOLIO : BannerPrefs.Fuente.RANKING);
            actualizarAvisoBanner();
            refrescarBanner();
        });

        spPapeles = new Spinner<>(new SpinnerValueFactory.IntegerSpinnerValueFactory(
                BannerPrefs.MIN_PAPELES, BannerPrefs.MAX_PAPELES, BannerPrefs.papeles()));
        spPapeles.setPrefWidth(110);
        spPapeles.valueProperty().addListener((obs, viejo, nuevo) -> {
            if (nuevo == null) return;
            BannerPrefs.setPapeles(nuevo);
            refrescarBanner();
        });
        boxCantidad.getChildren().setAll(spPapeles);
        actualizarAvisoBanner();
    }

    private void actualizarAvisoBanner() {
        if (BannerPrefs.fuente() == BannerPrefs.Fuente.PORTAFOLIO) {
            lblBannerAviso.setText("Usa los papeles de tus portafolios, que ya están suscritos: "
                    + "el precio se ve en vivo y no agrega carga al core. "
                    + "Si un portafolio está vacío, el banner cae al ranking del día.");
        } else {
            lblBannerAviso.setText("Muestra los papeles más transados del día. "
                    + "Se refresca con las estadísticas de mercado, cada 30 segundos.");
        }
    }

    /** Pide a la huincha que se reconstruya con la preferencia nueva, si está montada. */
    private void refrescarBanner() {
        try {
            HuinchaController huincha = Repository.getHuinchaController();
            if (huincha != null) huincha.aplicarPreferencias();
        } catch (Exception e) {
            log.warn("No se pudo refrescar el banner: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------- seguridad

    private void configurarSeguridad() {
        btnCambiarPassword.setOnAction(e -> cambiarPassword());
    }

    /**
     * Migrado tal cual del diálogo anterior: mismo mensaje protobuf y misma exigencia de AES
     * (el servidor desencripta username y password en KeycloakService).
     */
    private void cambiarPassword() {
        String p1 = txtPassword.getText();
        String p2 = txtPassword2.getText();
        if (p1 == null || p1.isEmpty() || p2 == null || p2.isEmpty()) {
            alerta("Error", "Los campos no pueden estar vacíos", Alert.AlertType.ERROR);
            return;
        }
        if (!p1.equals(p2)) {
            alerta("Error", "Las contraseñas no coinciden", Alert.AlertType.ERROR);
            return;
        }
        try {
            BlotterMessage.User.Builder user = Repository.getUser().toBuilder().clone();
            user.setStatusUser(BlotterMessage.StatusUser.UPDATE_USER);
            user.setPassword(AESEncryption.encrypt(p1));
            user.setUsername(AESEncryption.encrypt(user.getUsername()));
            Repository.getClientService().sendMessage(user.build());
            txtPassword.clear();
            txtPassword2.clear();
            // El backend no confirma: sólo se logea el envío. No se afirma exito que no consta.
            alerta("Solicitud enviada",
                    "Se envió el cambio de contraseña. Al aplicarse se cierran todas tus sesiones.",
                    Alert.AlertType.INFORMATION);
        } catch (Exception ex) {
            log.error("Error enviando cambio de contraseña: {}", ex.getMessage(), ex);
            alerta("Error", "No se pudo enviar el cambio de contraseña.", Alert.AlertType.ERROR);
        }
    }

    private void alerta(String titulo, String texto, Alert.AlertType tipo) {
        Alert a = new Alert(tipo);
        a.setTitle(titulo);
        a.setHeaderText(null);
        a.setContentText(texto);
        if (btnCerrar != null && btnCerrar.getScene() != null) {
            a.initOwner(btnCerrar.getScene().getWindow());
            a.getDialogPane().getStylesheets().addAll(btnCerrar.getScene().getStylesheets());
        }
        a.showAndWait();
    }
}
