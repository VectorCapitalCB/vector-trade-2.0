package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.MultibookApi;
import cl.vc.blotter.utils.MultibookLayoutStore;
import cl.vc.blotter.utils.Notifier;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.StackPane;
import javafx.stage.FileChooser;
import javafx.stage.Screen;
import javafx.stage.Stage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Ventana del Multibook 2.0: un libro con paginas numeradas.
 * <p>
 * El usuario tiene N configuraciones nombradas; cada una es un libro con sus paginas, y cada pagina
 * una grilla de 10 libros. El documento se guarda en el OMS (mapa {@code MultiBook2.0} de Redis) para
 * que la configuracion siga al usuario aunque cambie de PC, con copia local
 * ({@link MultibookLayoutStore}) que ademas es lo que salva el multibook si el servicio no responde.
 * <p>
 * Cada ventana es una copia del libro, con su propia configuracion y su propia pagina: se pueden
 * abrir varias y mirar paginas distintas a la vez, o incluso la misma, porque cada grilla toma su
 * propio bloque de posiciones.
 */
@Slf4j
public class MultibookController implements Initializable {

    private static final ExecutorService IO = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "multibook-api");
        thread.setDaemon(true);
        return thread;
    });

    /** Documento del usuario, compartido por todas las ventanas abiertas. */
    private static JSONObject document;

    /** Mientras se restaura una pagina, las suscripciones no son cambios del usuario: no se guardan. */
    private static boolean restoring;

    @FXML
    private ComboBox<String> configs;

    @FXML
    private HBox pageBar;

    @FXML
    private StackPane content;

    /** Indice de la ventana dentro del layout local guardado. */
    @Getter
    private int windowIndex;

    private Stage stage;
    private LibroEmergentePrincipalController grid;

    /** Configuracion y pagina que mira ESTA ventana; cada una es independiente. */
    private String layoutName;
    private int currentPage = -1;

    private JSONObject savedWindow;
    private boolean switchingConfig;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        configs.setOnAction(event -> {
            String selected = configs.getSelectionModel().getSelectedItem();
            if (!switchingConfig && selected != null && !selected.equals(layoutName)) {
                switchConfig(selected);
            }
        });
    }

    /** Monta la ventana con la copia local y refresca cuando llega la del OMS. */
    public void open(Stage stage, int windowIndex, JSONObject saved) {

        this.stage = stage;
        this.windowIndex = windowIndex;
        this.savedWindow = saved;

        applyGeometry(saved);
        stage.setOnCloseRequest(event -> close());

        boolean firstWindow = document == null;

        if (firstWindow) {
            JSONObject local = MultibookLayoutStore.document();
            document = local != null ? local : emptyDocument();
        }

        mount();

        if (!firstWindow) {
            return;
        }

        // La del OMS manda: es la compartida entre maquinas. Si no responde, seguimos con la local.
        IO.submit(() -> {
            JSONObject remote = MultibookApi.load();
            if (remote == null || remote.optJSONArray("layouts") == null) {
                return;
            }
            Platform.runLater(() -> {
                document = remote;
                MultibookLayoutStore.saveDocument(document);
                Repository.getControllerMultibook().values().forEach(MultibookController::remount);
            });
        });
    }

    private void mount() {
        layoutName = savedWindow == null ? null : savedWindow.optString("layout", null);
        if (findLayout(layoutName) == null) {
            layoutName = document.getJSONArray("layouts").getJSONObject(0).optString("name");
        }
        showPage(clampPage(savedWindow == null ? 0 : savedWindow.optInt("page", 0)));
        renderConfigBar();
    }

    /** Vuelve a montar la ventana tras reemplazar el documento, conservando lo que estaba mirando. */
    private void remount() {
        String layout = layoutName;
        int page = currentPage;
        release();
        layoutName = findLayout(layout) == null
                ? document.getJSONArray("layouts").getJSONObject(0).optString("name")
                : layout;
        showPage(clampPage(page));
        renderConfigBar();
    }

    // ------------------------------------------------------------------ documento

    private static JSONObject emptyDocument() {
        return new JSONObject()
                .put("version", 2)
                .put("active", "Default")
                .put("layouts", new JSONArray().put(newLayoutJson("Default")));
    }

    private static JSONObject newLayoutJson(String name) {
        return new JSONObject()
                .put("name", name)
                .put("pages", new JSONArray().put(
                        new JSONObject().put("name", "1").put("books", new JSONArray())));
    }

    private static JSONObject findLayout(String name) {
        if (document == null || name == null) {
            return null;
        }
        JSONArray layouts = document.getJSONArray("layouts");
        for (int i = 0; i < layouts.length(); i++) {
            if (name.equals(layouts.getJSONObject(i).optString("name"))) {
                return layouts.getJSONObject(i);
            }
        }
        return null;
    }

    private JSONObject layout() {
        JSONObject layout = findLayout(layoutName);
        return layout != null ? layout : document.getJSONArray("layouts").getJSONObject(0);
    }

    private JSONArray pages() {
        return layout().getJSONArray("pages");
    }

    private String pageName(int page) {
        if (page < 0 || page >= pages().length()) {
            return "";
        }
        return pages().getJSONObject(page).optString("name", String.valueOf(page + 1));
    }

    private int clampPage(int page) {
        return Math.min(Math.max(page, 0), pages().length() - 1);
    }

    /** Guarda local (sincrono, es lo que salva el multibook) y sube al OMS en segundo plano. */
    private static void saveDocument() {
        MultibookLayoutStore.saveDocument(document);
        JSONObject snapshot = new JSONObject(document.toString());
        IO.submit(() -> {
            if (MultibookApi.save(snapshot) == null) {
                Platform.runLater(() -> Notifier.INSTANCE.notifyError("Multibook",
                        "Guardado local: el servicio no respondió."));
            }
        });
    }

    /**
     * Cambio el simbolo del libro en esa posicion global: se reescribe solo la pagina de la ventana
     * que lo contiene. Si dos ventanas miran la misma pagina, escribir todas dejaria ganar a la
     * grilla desactualizada de la otra.
     */
    public static void bookChanged(int position) {

        if (document == null || restoring) {
            return;
        }

        Repository.getControllerMultibook().values().stream()
                .filter(window -> window.grid != null
                        && window.grid.getMapsLibroMapsInstance().containsKey(position))
                .findFirst()
                .ifPresent(window -> {
                    if (window.writePage()) {
                        saveDocument();
                    }
                });
    }

    /** Vuelca al documento lo que muestran todas las ventanas abiertas. */
    private static void flushOpenPages() {
        Repository.getControllerMultibook().values().forEach(MultibookController::writePage);
    }

    private boolean writePage() {
        if (grid == null || currentPage < 0 || currentPage >= pages().length()) {
            return false;
        }
        pages().getJSONObject(currentPage).put("books", grid.books());
        return true;
    }

    // ------------------------------------------------------------------ paginas

    private void showPage(int page) {

        try {

            if (page < 0 || page >= pages().length()) {
                return;
            }

            release();

            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/MultiLibroEmergente.fxml"));
            Parent gridPane = loader.load();
            grid = loader.getController();

            restoring = true;
            try {
                grid.attach(page, pages().getJSONObject(page).optJSONArray("books"));
            } finally {
                restoring = false;
            }

            content.getChildren().setAll(gridPane);
            currentPage = page;

            stage.setTitle("Multi Libro - " + layoutName + " - Página " + pageName(page));

            persist();
            renderPageBar();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /** Suelta la pagina actual y baja sus suscripciones. */
    private void release() {
        if (grid != null) {
            grid.unsubscribe();
            grid = null;
        }
        currentPage = -1;
        content.getChildren().clear();
    }

    // ------------------------------------------------------------------ configuraciones

    /** Cambia la configuracion SOLO de esta ventana; las demas siguen en la suya. */
    private void switchConfig(String name) {
        writePage();
        int page = currentPage;
        release();
        layoutName = name;
        document.put("active", name);
        showPage(clampPage(page));
        renderConfigBar();
        IO.submit(() -> MultibookApi.setActive(name));
    }

    /** Configuracion nueva y vacia, a diferencia de "Duplicar" que copia la actual. */
    @FXML
    private void newLayout() {
        prompt("Nueva configuración", "Nombre de la configuración",
                "Config " + (document.getJSONArray("layouts").length() + 1)).ifPresent(name -> {
            if (findLayout(name) != null) {
                Notifier.INSTANCE.notifyError("Multibook", "Ya existe una configuración '" + name + "'.");
                return;
            }
            document.getJSONArray("layouts").put(newLayoutJson(name));
            switchConfig(name);
            saveDocument();
            renderAllConfigBars();
        });
    }

    @FXML
    private void saveLayout() {
        flushOpenPages();
        saveDocument();
        Notifier.INSTANCE.notifyInfo("Multibook", "Configuración '" + layoutName + "' guardada.");
    }

    @FXML
    private void saveLayoutAs() {
        prompt("Duplicar configuración", "Nombre de la copia", layoutName + " copia").ifPresent(name -> {
            if (findLayout(name) != null) {
                Notifier.INSTANCE.notifyError("Multibook", "Ya existe una configuración '" + name + "'.");
                return;
            }
            writePage();
            document.getJSONArray("layouts").put(new JSONObject(layout().toString()).put("name", name));
            switchConfig(name);
            saveDocument();
            renderAllConfigBars();
        });
    }

    @FXML
    private void renameLayout() {
        String from = layoutName;
        prompt("Renombrar configuración", "Nuevo nombre para '" + from + "'", from).ifPresent(to -> {
            if (findLayout(to) != null) {
                Notifier.INSTANCE.notifyError("Multibook", "Ya existe una configuración '" + to + "'.");
                return;
            }
            layout().put("name", to);
            Repository.getControllerMultibook().values().forEach(window -> {
                if (from.equals(window.layoutName)) {
                    window.layoutName = to;
                    window.stage.setTitle("Multi Libro - " + to + " - Página " + window.pageName(window.currentPage));
                }
            });
            document.put("active", to);
            MultibookLayoutStore.saveDocument(document);
            IO.submit(() -> MultibookApi.rename(from, to));
            renderAllConfigBars();
        });
    }

    @FXML
    private void deleteLayout() {

        String name = layoutName;
        JSONArray layouts = document.getJSONArray("layouts");

        if (layouts.length() <= 1) {
            Notifier.INSTANCE.notifyError("Multibook", "No se puede eliminar la única configuración.");
            return;
        }

        for (int i = 0; i < layouts.length(); i++) {
            if (name.equals(layouts.getJSONObject(i).optString("name"))) {
                layouts.remove(i);
                break;
            }
        }

        String fallback = layouts.getJSONObject(0).optString("name");
        Repository.getControllerMultibook().values().forEach(window -> {
            if (name.equals(window.layoutName)) {
                window.switchConfig(fallback);
            }
        });

        MultibookLayoutStore.saveDocument(document);
        IO.submit(() -> MultibookApi.delete(name));
        renderAllConfigBars();
    }

    @FXML
    private void exportLayout() {

        writePage();
        saveDocument();

        FileChooser chooser = new FileChooser();
        chooser.setTitle("Exportar configuración");
        chooser.setInitialFileName(layoutName.replaceAll("[\\\\/:*?\"<>|]+", "_") + ".json");
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("JSON", "*.json"));

        File file = chooser.showSaveDialog(stage);
        if (file == null) {
            return;
        }

        try {
            Files.writeString(file.toPath(), layout().toString(2), StandardCharsets.UTF_8);
            Notifier.INSTANCE.notifyInfo("Multibook", "Exportada a " + file.getName());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Notifier.INSTANCE.notifyError("Multibook", "No se pudo exportar: " + e.getMessage());
        }
    }

    @FXML
    private void importLayout() {

        FileChooser chooser = new FileChooser();
        chooser.setTitle("Importar configuración");
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("JSON", "*.json"));

        File file = chooser.showOpenDialog(stage);
        if (file == null) {
            return;
        }

        try {
            JSONObject imported = new JSONObject(Files.readString(file.toPath(), StandardCharsets.UTF_8));
            if (imported.optJSONArray("pages") == null) {
                Notifier.INSTANCE.notifyError("Multibook", "El archivo no es una configuración de multibook.");
                return;
            }

            String name = imported.optString("name", file.getName().replaceFirst("\\.json$", ""));
            while (findLayout(name) != null) {
                name = name + " (importada)";
            }
            imported.put("name", name);

            document.getJSONArray("layouts").put(imported);
            switchConfig(name);
            saveDocument();
            renderAllConfigBars();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Notifier.INSTANCE.notifyError("Multibook", "No se pudo importar: " + e.getMessage());
        }
    }

    private Optional<String> prompt(String title, String header, String initial) {
        TextInputDialog dialog = new TextInputDialog(initial);
        dialog.setTitle(title);
        dialog.setHeaderText(header);
        dialog.initOwner(stage);
        dialog.getDialogPane().getStylesheets()
                .add(getClass().getResource(Repository.getSTYLE()).toExternalForm());
        return dialog.showAndWait().map(String::trim).filter(value -> !value.isEmpty());
    }

    // ------------------------------------------------------------------ barras

    private void renderConfigBar() {
        switchingConfig = true;
        try {
            List<String> names = new ArrayList<>();
            JSONArray layouts = document.getJSONArray("layouts");
            for (int i = 0; i < layouts.length(); i++) {
                names.add(layouts.getJSONObject(i).optString("name"));
            }
            configs.getItems().setAll(names);
            configs.getSelectionModel().select(layoutName);
        } finally {
            switchingConfig = false;
        }
    }

    private static void renderAllConfigBars() {
        Repository.getControllerMultibook().values().forEach(MultibookController::renderConfigBar);
    }

    private void renderPageBar() {

        pageBar.getChildren().clear();
        pageBar.getChildren().add(navButton("<", -1));

        ToggleGroup group = new ToggleGroup();

        for (int i = 0; i < pages().length(); i++) {

            int page = i;
            ToggleButton button = new ToggleButton(pageName(page));
            button.getStyleClass().addAll("button", "multibook-page");
            button.setToggleGroup(group);
            button.setSelected(page == currentPage);
            button.setTooltip(new Tooltip("Doble click para renombrar"));
            button.setContextMenu(pageMenu(page));
            button.setOnMouseClicked(event -> {
                if (event.getClickCount() == 2) {
                    renamePage(page);
                } else if (page != currentPage) {
                    showPage(page);
                } else {
                    button.setSelected(true);
                }
            });

            pageBar.getChildren().add(button);
        }

        pageBar.getChildren().add(navButton(">", 1));

        Button add = new Button("+");
        add.getStyleClass().addAll("button", "multibook-page");
        add.setTooltip(new Tooltip("Agregar página"));
        add.setOnAction(event -> {
            pages().put(new JSONObject()
                    .put("name", String.valueOf(pages().length() + 1))
                    .put("books", new JSONArray()));
            showPage(pages().length() - 1);
            saveDocument();
        });
        pageBar.getChildren().add(add);

        Button rename = new Button("Renombrar página");
        rename.getStyleClass().addAll("button", "multibook-page");
        rename.setDisable(currentPage < 0);
        rename.setOnAction(event -> renamePage(currentPage));
        pageBar.getChildren().add(rename);
    }

    /** Recorre el libro en circulo: en la ultima pagina, ">" vuelve a la primera. */
    private Button navButton(String text, int step) {
        Button button = new Button(text);
        button.getStyleClass().addAll("button", "multibook-page");
        button.setDisable(pages().length() < 2);
        button.setOnAction(event -> {
            int total = pages().length();
            showPage(((currentPage + step) % total + total) % total);
        });
        return button;
    }

    private void renamePage(int page) {
        if (page < 0 || page >= pages().length()) {
            return;
        }
        prompt("Multi Libro", "Nombre de la página", pageName(page)).ifPresent(name -> {
            pages().getJSONObject(page).put("name", name);
            if (page == currentPage) {
                stage.setTitle("Multi Libro - " + layoutName + " - Página " + name);
            }
            saveDocument();
            renderPageBar();
        });
    }

    private ContextMenu pageMenu(int page) {

        MenuItem rename = new MenuItem("Renombrar página");
        rename.setOnAction(event -> renamePage(page));

        // Solo la ultima: borrar una del medio correria la numeracion de las paginas siguientes.
        MenuItem delete = new MenuItem("Eliminar página");
        delete.setDisable(pages().length() == 1 || page != pages().length() - 1);
        delete.setOnAction(event -> {
            release();
            pages().remove(page);
            saveDocument();
            showPage(clampPage(page));
        });

        return new ContextMenu(rename, delete);
    }

    // ------------------------------------------------------------------ ventana

    private void applyGeometry(JSONObject saved) {

        if (saved == null || !saved.has("x")) {
            stage.setMaximized(true);
            return;
        }

        double x = saved.getDouble("x");
        double y = saved.getDouble("y");
        double width = saved.optDouble("w", 1200);
        double height = saved.optDouble("h", 800);

        // Si el monitor donde estaba ya no existe, se abre maximizada en el principal.
        if (Screen.getScreensForRectangle(x, y, width, height).isEmpty()) {
            stage.setMaximized(true);
            return;
        }

        stage.setX(x);
        stage.setY(y);
        stage.setWidth(width);
        stage.setHeight(height);
        stage.setMaximized(saved.optBoolean("max", false));
    }

    private void persist() {

        try {

            JSONObject state = new JSONObject()
                    .put("page", currentPage)
                    .put("layout", String.valueOf(layoutName));

            if (stage != null) {
                state.put("x", stage.getX())
                        .put("y", stage.getY())
                        .put("w", stage.getWidth())
                        .put("h", stage.getHeight())
                        .put("max", stage.isMaximized());
            }

            savedWindow = state;
            MultibookLayoutStore.saveWindow(windowIndex, state);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void close() {
        writePage();
        saveDocument();
        persist();
        release();
        Repository.getControllerMultibook().remove(windowIndex);
    }
}
